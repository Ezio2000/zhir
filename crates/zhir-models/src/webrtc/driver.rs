use super::*;
use crate::{
    native::{self, Outputs},
    transport::webrtc::{Peer, PeerConfig},
};
use std::collections::VecDeque;
use tokio::sync::mpsc;

pub(super) fn open(
    model: WebRtcModel,
    open: SessionOpen,
    session: WebRtcSession,
) -> Result<ModelSession> {
    let deadline = zhir_policies::timing::deadline(&open.context.run)?;
    zhir_policies::timing::check(&open.context.cancellation, deadline)?;
    let (ports_session, ports) = native::ports(
        Arc::new(model.clone()),
        &open.limits,
        session.peer.audio_input,
        open.output_epoch,
    );
    let cancellation = open.context.cancellation.clone();
    let event_port = ports.outputs.events.clone();
    let mut driver = Driver {
        adapter: model.adapter,
        open,
        session,
        commands: ports.commands,
        input: ports.audio_input,
        outputs: ports.outputs,
        peer: None,
        stopped: false,
        drain_tail: None,
        actions: VecDeque::new(),
        observations: VecDeque::new(),
        write: None,
    };
    tokio::spawn(async move {
        let outcome = tokio::select! {
            result = native::guarded(driver.run(), &cancellation, deadline) => result,
            _ = event_port.closed() => Err(Error::Cancelled),
        };
        // Drop in-flight writes and their reservations before transport teardown.
        driver.write.take();
        if let Some(peer) = &driver.peer {
            peer.close().await;
        }
        let _ = ports.terminal.send(driver.outputs.settle(outcome));
    });
    Ok(ports_session)
}

struct Driver {
    adapter: Arc<dyn WebRtcAdapter>,
    open: SessionOpen,
    session: WebRtcSession,
    commands: mpsc::Receiver<SessionCommand>,
    input: mpsc::UnboundedReceiver<Buffered<MediaChunk>>,
    outputs: Outputs,
    peer: Option<Peer>,
    stopped: bool,
    drain_tail: Option<VecDeque<Action>>,
    // Command effects stay ordered, including the tail behind DrainInput.
    actions: VecDeque<Action>,
    // One received control frame's effects may progress during input draining
    // or an in-flight write. This queue never accumulates multiple frames.
    observations: VecDeque<Action>,
    write: Option<BoxFuture<'static, Result<()>>>,
}
impl Driver {
    async fn connect(&mut self, payload: Value, deadline: Instant, timeout: Error) -> Result<()> {
        if self.peer.is_some() {
            return Err(Error::Protocol("WebRTC peer already created".into()));
        }
        tokio::time::timeout_at(deadline, async {
            let settings = &self.session.peer;
            self.peer = Some(
                Peer::new(PeerConfig {
                    connection: settings.connection.clone(),
                    channel_label: settings.channel_label,
                    audio_codec: settings.audio_codec.clone(),
                    event_capacity: self.open.limits.max_session_events,
                    max_event_bytes: settings.max_event_bytes,
                    max_audio_bytes: self.open.limits.max_media_chunk_bytes,
                    media_budget: self.outputs.media_budget(),
                })
                .await?,
            );
            let peer = self.peer.as_mut().expect("created peer");
            let offer = peer.offer().await?;
            let answer = self
                .adapter
                .signal(&self.open.session_id, offer, payload)
                .await?;
            peer.answer(answer).await
        })
        .await
        .map_err(|_| timeout)?
    }

    fn send_audio(&mut self, packet: Buffered<MediaChunk>) -> Result<()> {
        let session_id = &self.open.session_id;
        if let Some(duration) = self.session.media.input(session_id, &packet.value)? {
            let peer = self.peer.as_ref().ok_or(Error::Cancelled)?;
            let packet = packet.map(|chunk| peer.audio(chunk.bytes, duration));
            let timeout = self.session.write_timeout;
            self.write = Some(Box::pin(async move {
                let result = tokio::time::timeout(timeout, packet.value)
                    .await
                    .map_err(|_| Error::Uncertain("WebRTC audio send timed out".into()))?;
                drop(packet.reservation);
                result
            }));
        }
        Ok(())
    }

    // Returns true when an effect was executed. Received events may be emitted
    // while a write is pending; command acknowledgements remain behind that write.
    async fn advance(&mut self) -> Result<bool> {
        let observations = !self.observations.is_empty();
        let queue = if observations {
            &mut self.observations
        } else {
            if self.drain_tail.is_some() || self.write.is_some() {
                return Ok(false);
            }
            &mut self.actions
        };
        let Some(action) = queue.front() else {
            return Ok(false);
        };
        if self.write.is_some()
            && matches!(
                action,
                Action::Send(_) | Action::Connect { .. } | Action::DrainInput
            )
        {
            return Ok(false);
        }
        if matches!(action, Action::Send(_))
            && let Some(peer) = &self.peer
            && !self.stopped
            && !self
                .session
                .connection
                .evaluate(*peer.connection.borrow(), !peer.events.is_empty())?
                .commands_allowed
        {
            return Ok(false);
        }
        let action = queue.pop_front().expect("queued effect");
        let drain_tail = matches!(action, Action::DrainInput).then(|| std::mem::take(queue));
        match action {
            Action::Connect {
                payload,
                deadline,
                timeout,
            } => self.connect(payload, deadline, timeout).await?,
            Action::Send(text) => {
                let send = self.peer.as_ref().ok_or(Error::Cancelled)?.send(&text);
                let timeout = self.session.write_timeout;
                self.write = Some(Box::pin(async move {
                    tokio::time::timeout(timeout, send)
                        .await
                        .map_err(|_| Error::Uncertain("WebRTC control send timed out".into()))?
                }));
            }
            Action::DrainInput => {
                if self.drain_tail.is_some() {
                    return Err(Error::Protocol("overlapping WebRTC input drains".into()));
                }
                self.input.close();
                if self.peer.is_none() && !self.input.is_empty() {
                    return Err(Error::Protocol(
                        "cannot drain accepted audio without a WebRTC peer".into(),
                    ));
                }
                self.drain_tail = drain_tail;
            }
            Action::Event(body) => self.outputs.event(*body)?,
            Action::Disconnect => {
                self.disconnect().await?;
            }
            Action::Fail(error) => return Err(error),
        }
        Ok(true)
    }

    async fn disconnect(&mut self) -> Result<()> {
        if let Some(peer) = &self.peer {
            peer.close().await;
        }
        self.stopped = true;
        if self.drain_tail.is_some() && (!self.input.is_empty() || self.write.is_some()) {
            return Err(Error::Uncertain(
                "WebRTC closed before accepted input drained".into(),
            ));
        }
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        self.actions = self.session.protocol.initial()?.into();
        loop {
            self.session.protocol.check_deadline()?;
            if self.write.is_none()
                && self.input.is_empty()
                && let Some(mut tail) = self.drain_tail.take()
            {
                // Admission is closed. The suspended batch resumes only after
                // every accepted input write, regardless of the action's source.
                tail.append(&mut self.actions);
                self.actions = tail;
            }
            if self.advance().await? {
                continue;
            }
            if self.session.protocol.draining() && !self.stopped {
                // Stop ingress even when public delivery is currently blocked.
                self.disconnect().await?;
            }
            let idle = self.write.is_none()
                && self.drain_tail.is_none()
                && self.actions.is_empty()
                && self.observations.is_empty();
            if self.session.protocol.draining() && idle && !self.outputs.pending() {
                // Empty queues establish a boundary only after producers stop.
                if self
                    .peer
                    .as_ref()
                    .is_none_or(|peer| peer.audio.is_empty() && peer.events.is_empty())
                {
                    if let Some(end) = self.session.media.finish(&self.open.session_id) {
                        self.outputs.media(end)?;
                    }
                    self.outputs.end_media();
                    self.actions = self.session.protocol.finalize()?.into();
                    continue;
                }
            }
            if self.session.protocol.closed() && idle && !self.outputs.pending() {
                return Ok(());
            }

            enum Next {
                Command(Option<SessionCommand>),
                Input(Option<Buffered<MediaChunk>>),
                Event(Option<String>),
                Audio(Option<Buffered<AudioPacket>>),
                Written(Result<()>),
                Flushed(Result<()>),
                Wake,
            }
            let protocol = &self.session.protocol;
            let next = if let Some(peer) = &mut self.peer {
                let receiving = !protocol.finished();
                let remote_open = receiving && !self.stopped;
                if remote_open
                    && peer.events.is_empty()
                    && let Some(error) = peer.failure.borrow().clone()
                {
                    return Err(error);
                }
                let state = if remote_open {
                    self.session
                        .connection
                        .evaluate(*peer.connection.borrow(), !peer.events.is_empty())?
                } else {
                    ConnectionStatus {
                        commands_allowed: true,
                        input_allowed: false,
                        deadline: None,
                    }
                };
                tokio::select! {
                    command = self.commands.recv(), if idle && state.commands_allowed && protocol.commands_allowed() && self.outputs.commands_allowed(self.session.command_headroom) => Next::Command(command),
                    packet = self.input.recv(), if self.write.is_none() && state.input_allowed && (self.drain_tail.is_some() || (idle && self.session.peer.audio_input && protocol.input_allowed())) => Next::Input(packet),
                    event = peer.events.recv(), if receiving && self.observations.is_empty() && (remote_open || !peer.events.is_empty()) => Next::Event(event),
                    packet = peer.audio.recv(), if receiving && self.outputs.can_receive_media() && (remote_open || !peer.audio.is_empty()) => Next::Audio(packet),
                    result = async { self.write.as_mut().expect("pending write").await }, if self.write.is_some() => Next::Written(result),
                    result = self.outputs.flush_one(), if self.outputs.pending() => Next::Flushed(result),
                    _ = peer.connection.changed(), if remote_open => Next::Wake,
                    _ = peer.failure.changed(), if remote_open => Next::Wake,
                    _ = native::confirmation_deadline(state.deadline), if remote_open => Next::Wake,
                    _ = native::confirmation_deadline(protocol.deadline()) => Next::Wake,
                }
            } else {
                // A protocol may emit observations or accept several commands
                // before requesting a peer. No transport-specific startup order.
                tokio::select! {
                    command = self.commands.recv(), if idle && protocol.commands_allowed() && self.outputs.commands_allowed(self.session.command_headroom) => Next::Command(command),
                    result = self.outputs.flush_one(), if self.outputs.pending() => Next::Flushed(result),
                    _ = native::confirmation_deadline(protocol.deadline()) => Next::Wake,
                }
            };
            match next {
                Next::Command(Some(command)) => {
                    self.actions = self.session.protocol.command(command)?.into()
                }
                Next::Event(Some(text)) => {
                    self.observations = self.session.protocol.receive(&text)?.into()
                }
                Next::Audio(Some(packet)) => {
                    let Buffered { value, reservation } = packet;
                    let payload_len = value.payload.len();
                    if let Some(chunk) = self.session.media.receive(&self.open.session_id, value)? {
                        if chunk.bytes.len() > payload_len {
                            return Err(Error::Invalid(
                                "media mapping grew the reserved payload".into(),
                            ));
                        }
                        self.outputs.received_media(Buffered {
                            value: chunk,
                            reservation,
                        })?;
                    }
                }
                Next::Input(Some(packet)) => self.send_audio(packet)?,
                Next::Input(None) | Next::Command(None) => return Err(Error::Cancelled),
                Next::Written(result) => {
                    self.write.take();
                    result?;
                }
                Next::Flushed(result) => result?,
                Next::Wake => (),
                _ => {
                    return Err(Error::Uncertain(
                        "WebRTC transport ended before finalization".into(),
                    ));
                }
            }
        }
    }
}
