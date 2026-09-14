//! WebRTC driver for the Live protocol, using shared native ports and scheduling.
use super::{
    LiveModel,
    audio::{self, Audio},
    protocol::{Action, Protocol},
    signaling,
};
use crate::{
    native::{self, Buffered, Outputs},
    transport::webrtc::{AudioPacket, Peer, PeerConfig},
};
use std::sync::Arc;
use tokio::sync::mpsc;
use zhir_core::{Result, error::Error, model::*, resource::MediaChunk};

pub(super) fn open(model: LiveModel, open: SessionOpen) -> Result<ModelSession> {
    let deadline = zhir_policies::timing::deadline(&open.context.run)?;
    zhir_policies::timing::check(&open.context.cancellation, deadline)?;
    let (session, ports) = native::ports(
        Arc::new(model.clone()),
        &open.limits,
        true,
        open.output_epoch,
    );
    let cancellation = open.context.cancellation.clone();
    let event_port = ports.outputs.events.clone();
    let mut driver = Driver {
        protocol: Protocol::new(model.clone(), open.limits.max_inflight_operations),
        audio: Audio::new(open.output_epoch, open.limits.max_media_chunk_bytes),
        model,
        open,
        commands: ports.commands,
        input: ports.audio_input,
        outputs: ports.outputs,
        peer: None,
    };
    tokio::spawn(async move {
        let outcome = tokio::select! {
            result = native::guarded(driver.run(), &cancellation, deadline) => result,
            _ = event_port.closed() => Err(Error::Cancelled),
        };
        if let Some(peer) = &driver.peer {
            peer.close().await;
        }
        let _ = ports.terminal.send(driver.outputs.settle(outcome));
    });
    Ok(session)
}
struct Driver {
    model: LiveModel,
    open: SessionOpen,
    protocol: Protocol,
    audio: Audio,
    commands: mpsc::Receiver<SessionCommand>,
    input: mpsc::UnboundedReceiver<Buffered<MediaChunk>>,
    outputs: Outputs,
    peer: Option<Peer>,
}
impl Driver {
    async fn connect(&mut self, session: serde_json::Value) -> Result<()> {
        let config = &self.model.config;
        let connection = webrtc::peer_connection::configuration::RTCConfiguration {
            ice_servers: config
                .ice_servers
                .iter()
                .map(|url| webrtc::ice_transport::ice_server::RTCIceServer {
                    urls: vec![url.clone()],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        self.peer = Some(
            Peer::new(PeerConfig {
                connection,
                channel_label: "oai-events",
                audio_codec: audio::codec(),
                event_capacity: self.open.limits.max_session_events,
                max_event_bytes: config.max_event_bytes,
                max_audio_bytes: self.open.limits.max_media_chunk_bytes,
                media_budget: self.outputs.media_budget(),
            })
            .await?,
        );
        let peer = self.peer.as_mut().expect("created peer");
        tokio::time::timeout_at(
            self.protocol.deadline().expect("startup confirmation"),
            async {
                let offer = peer.offer().await?;
                let answer =
                    signaling::create(config, &self.open.session_id, offer, session).await?;
                peer.answer(answer).await
            },
        )
        .await
        .map_err(|_| Error::Uncertain("Live session startup timed out".into()))?
    }
    async fn apply(&mut self, actions: Vec<Action>) -> Result<()> {
        for action in actions {
            match action {
                Action::Connect(session) => self.connect(session).await?,
                Action::Send(value) => {
                    tokio::time::timeout(
                        self.model.config.command_timeout,
                        self.peer
                            .as_ref()
                            .ok_or(Error::Cancelled)?
                            .send(&value.to_string()),
                    )
                    .await
                    .map_err(|_| Error::Uncertain("Live command send timed out".into()))??;
                }
                Action::DrainInput => {
                    // Also enforce EndInput for direct ModelSession users.
                    self.input.close();
                    while let Some(packet) = self.input.recv().await {
                        self.send_audio(packet).await?;
                    }
                }
                Action::Event(body) => self.outputs.event(*body)?,
                Action::Disconnect => {
                    if let Some(peer) = &self.peer {
                        peer.close().await;
                    }
                }
                Action::Fail(error) => return Err(error),
            }
        }
        Ok(())
    }
    async fn send_audio(&self, packet: Buffered<MediaChunk>) -> Result<()> {
        if let Some(duration) = self.audio.input(self.protocol.turn()?, &packet.value)? {
            // Keep the reservation until the transport accepts the packet.
            tokio::time::timeout(
                self.model.config.command_timeout,
                self.peer
                    .as_ref()
                    .ok_or(Error::Cancelled)?
                    .audio(packet.value.bytes, duration),
            )
            .await
            .map_err(|_| Error::Uncertain("Live audio send timed out".into()))??;
        }
        Ok(())
    }
    async fn run(&mut self) -> Result<()> {
        let first = self.commands.recv().await.ok_or(Error::Cancelled)?;
        let actions = self.protocol.command(first)?;
        self.apply(actions).await?;
        loop {
            self.protocol.check_deadline()?;
            // Closing freezes incoming transport queues. Media reservations survive
            // the drain, and the final marker follows every accepted RTP packet.
            if self.protocol.draining()
                && !self.outputs.pending()
                && self
                    .peer
                    .as_ref()
                    .is_some_and(|peer| peer.audio.is_empty() && peer.events.is_empty())
            {
                if let Some(end) = self.audio.finish(self.protocol.turn()?) {
                    self.outputs.media(end)?;
                }
                self.outputs.end_media();
                let actions = self.protocol.finalize()?;
                self.apply(actions).await?;
            }
            if self.protocol.closed() && !self.outputs.pending() {
                return Ok(());
            }
            enum Next {
                Command(Option<SessionCommand>),
                Input(Option<Buffered<MediaChunk>>),
                Event(Option<String>),
                Audio(Option<Buffered<AudioPacket>>),
                Wake,
                Flushed(Result<()>),
            }
            let next = {
                let peer = self
                    .peer
                    .as_mut()
                    .ok_or_else(|| Error::Protocol("missing Live peer".into()))?;
                let receiving = !self.protocol.finished();
                let remote_open = receiving && !self.protocol.draining();
                if remote_open
                    && peer.events.is_empty()
                    && let Some(error) = peer.failure.borrow().clone()
                {
                    return Err(error);
                }
                let connection = *peer.connection.borrow();
                let connected = if !remote_open {
                    true
                } else {
                    match connection.connected(self.model.config.reconnect_timeout) {
                        Ok(connected) => connected,
                        Err(_) if !peer.events.is_empty() => false,
                        Err(error) => return Err(error),
                    }
                };
                let reconnect_deadline = connection
                    .deadline(self.model.config.reconnect_timeout)
                    .filter(|deadline| *deadline > tokio::time::Instant::now());
                tokio::select! {
                    command = self.commands.recv(), if connected && self.protocol.commands_allowed() && self.outputs.commands_allowed() => Next::Command(command),
                    packet = self.input.recv(), if connected && self.protocol.input_allowed() => Next::Input(packet),
                    // Reading confirmations is independent of public output pressure.
                    // Outputs enforces the staging bound and reports actual overflow.
                    event = peer.events.recv(), if receiving && (remote_open || !peer.events.is_empty()) => Next::Event(event),
                    packet = peer.audio.recv(), if receiving && self.outputs.can_receive_media() && (remote_open || !peer.audio.is_empty()) => Next::Audio(packet),
                    result = self.outputs.flush_one(), if self.outputs.pending() => Next::Flushed(result),
                    _ = peer.connection.changed(), if remote_open => Next::Wake,
                    _ = native::confirmation_deadline(reconnect_deadline), if remote_open => Next::Wake,
                    _ = peer.failure.changed(), if remote_open => Next::Wake,
                    _ = native::confirmation_deadline(self.protocol.deadline()) => Next::Wake,
                }
            };
            match next {
                Next::Command(Some(command)) => {
                    let actions = self.protocol.command(command)?;
                    self.apply(actions).await?;
                }
                Next::Event(Some(text)) => {
                    let actions = self.protocol.receive(&text)?;
                    self.apply(actions).await?;
                }
                Next::Audio(Some(packet)) => {
                    if let Some(packet) = self.audio.receive(self.protocol.turn()?, packet)? {
                        self.outputs.received_media(packet)?;
                    }
                }
                Next::Input(Some(packet)) => self.send_audio(packet).await?,
                Next::Wake => (),
                Next::Flushed(result) => result?,
                Next::Input(None) => return Err(Error::Cancelled),
                _ => {
                    return Err(Error::Uncertain(
                        "Live transport ended before finalization".into(),
                    ));
                }
            }
        }
    }
}
