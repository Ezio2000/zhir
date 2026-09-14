use super::{AUDIO_TYPE, LiveModel, codec, signaling};
use crate::transport::webrtc::{Frame, Peer};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    message::{Content, Message, Output},
    model::*,
    operation::DelegationRequest,
    profile::NegotiatedProfile,
    resource::{MediaChunk, MediaReceiver, MediaSender},
};

pub(super) fn open(model: LiveModel, open: SessionOpen) -> Result<ModelSession> {
    let deadline = zhir_policies::timing::deadline(&open.context.run)?;
    zhir_policies::timing::check(&open.context.cancellation, deadline)?;
    let (tx, commands) = mpsc::channel(open.limits.max_control_commands);
    let (events, rx) = mpsc::channel(open.limits.max_session_events);
    let (terminal, result) = oneshot::channel();
    let capacity =
        (open.limits.max_buffered_media_bytes / open.limits.max_media_chunk_bytes).max(1);
    let (audio_in, input) = mpsc::channel(capacity);
    let (media, audio_out) = mpsc::channel(capacity);
    let mut driver = Driver {
        model: model.clone(),
        open,
        commands,
        events,
        input,
        media: Some(media),
        peer: None,
        sequence: 0,
        audio_sequence: 0,
        audio_timestamp: None,
        turn: None,
        start_id: None,
        end_id: None,
        close_id: None,
        started: false,
        finished: false,
        protocol_deadline: None,
        delegations: Default::default(),
    };
    let cancellation = driver.open.context.cancellation.clone();
    let event_port = driver.events.clone();
    tokio::spawn(async move {
        let outcome = {
            let run = driver.run();
            tokio::pin!(run);
            loop {
                tokio::select! {
                    result = &mut run => break result,
                    _ = event_port.closed() => break Err(Error::Cancelled),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        if let Err(e) = zhir_policies::timing::check(&cancellation, deadline) { break Err(e); }
                    }
                }
            }
        };
        if let Some(peer) = &driver.peer {
            peer.close().await;
        }
        let _ = terminal.send(outcome);
    });
    Ok(ModelSession {
        input: Arc::new(Input { model, sender: tx }),
        output: Box::new(Events {
            receiver: rx,
            terminal: Some(result),
        }),
        media_input: Some(Arc::new(AudioInput(audio_in))),
        media_output: Some(Box::new(AudioOutput(audio_out))),
    })
}
struct Input {
    model: LiveModel,
    sender: mpsc::Sender<SessionCommand>,
}
impl SessionSender for Input {
    fn capabilities(&self) -> &CapabilitySet {
        self.model.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.model.negotiate(request)
    }
    fn send(&self, command: SessionCommand) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.sender
                .send(command)
                .await
                .map_err(|_| Error::Cancelled)
        })
    }
}
struct Events {
    receiver: mpsc::Receiver<SessionEvent>,
    terminal: Option<oneshot::Receiver<Result<()>>>,
}
impl SessionReceiver for Events {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        Box::pin(async move {
            if let Some(event) = self.receiver.recv().await {
                return Ok(Some(event));
            }
            if let Some(done) = self.terminal.take() {
                done.await
                    .map_err(|_| Error::Uncertain("Live session worker stopped".into()))??;
            }
            Ok(None)
        })
    }
}
struct AudioInput(mpsc::Sender<MediaChunk>);
impl MediaSender for AudioInput {
    fn send(&self, chunk: MediaChunk) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.0.send(chunk).await.map_err(|_| Error::Cancelled) })
    }
}
struct AudioOutput(mpsc::Receiver<MediaChunk>);
impl MediaReceiver for AudioOutput {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<MediaChunk>>> {
        Box::pin(async move { Ok(self.0.recv().await) })
    }
}
struct Driver {
    model: LiveModel,
    open: SessionOpen,
    commands: mpsc::Receiver<SessionCommand>,
    events: mpsc::Sender<SessionEvent>,
    input: mpsc::Receiver<MediaChunk>,
    media: Option<mpsc::Sender<MediaChunk>>,
    peer: Option<Peer>,
    sequence: u64,
    audio_sequence: u64,
    audio_timestamp: Option<u32>,
    turn: Option<String>,
    start_id: Option<String>,
    end_id: Option<String>,
    close_id: Option<String>,
    started: bool,
    finished: bool,
    protocol_deadline: Option<tokio::time::Instant>,
    delegations: std::collections::BTreeSet<String>,
}
impl Driver {
    async fn event(&mut self, body: SessionEventBody) -> Result<()> {
        let sequence = self.sequence;
        self.sequence = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("Live event sequence overflow".into()))?;
        self.events
            .send(SessionEvent { sequence, body })
            .await
            .map_err(|_| Error::Cancelled)
    }
    async fn ack(&mut self, command_id: String) -> Result<()> {
        self.event(SessionEventBody::Acknowledged {
            command_id,
            recovery: None,
        })
        .await
    }
    async fn send(&self, event: serde_json::Value) -> Result<()> {
        tokio::time::timeout(
            self.model.config.command_timeout,
            self.peer
                .as_ref()
                .ok_or_else(|| Error::Protocol("Live not connected".into()))?
                .send(&event),
        )
        .await
        .map_err(|_| Error::Uncertain("Live command send timed out".into()))?
    }
    async fn start(&mut self, id: String, turn_id: String, request: ModelRequest) -> Result<()> {
        if self.turn.is_some() {
            return Err(Error::Protocol("Live execution already started".into()));
        }
        self.model.negotiate(&request)?;
        self.turn = Some(turn_id);
        self.start_id = Some(id);
        let config = self.model.config.clone();
        self.protocol_deadline = Some(tokio::time::Instant::now() + config.connect_timeout);
        let session = json!({"model":config.model,"instructions":config.instructions,"audio":{"output":{"voice":config.voice}},
            "delegation":{"type":"client","ack_filler":false},"initial_items":codec::initial_items(&request)?});
        let configuration = webrtc::peer_connection::configuration::RTCConfiguration {
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
            Peer::new(
                self.open.limits.max_session_events,
                config.max_event_bytes,
                configuration,
            )
            .await?,
        );
        let peer = self.peer.as_mut().expect("created peer");
        tokio::time::timeout(config.connect_timeout, async {
            let sdp = peer.offer().await?;
            let answer = signaling::create(&config, &self.open.session_id, sdp, session).await?;
            peer.answer(answer).await
        })
        .await
        .map_err(|_| Error::Uncertain("Live session startup timed out".into()))?
    }
    async fn command(&mut self, command: SessionCommand) -> Result<()> {
        if command.id.is_empty() {
            return Err(Error::Invalid("empty Live command id".into()));
        }
        match command.body {
            SessionCommandBody::StartTurn { turn_id, request } => {
                self.start(command.id, turn_id, *request).await
            }
            SessionCommandBody::Input { message }
                if self.started && !self.finished && self.end_id.is_none() =>
            {
                for event in codec::context(
                    "session.context.append",
                    &command.id,
                    &codec::text(&message)?,
                    None,
                ) {
                    self.send(event).await?;
                }
                // Transport acceptance only; this endpoint does not echo a correlating
                // command ID on context.appended. It is not a speech-completion receipt.
                self.ack(command.id).await
            }
            SessionCommandBody::DelegationContext {
                origin, content, ..
            } if self.started && !self.finished => {
                let text = context_text(content)?;
                for event in codec::context(
                    "delegation.context.append",
                    &command.id,
                    &text,
                    Some(&origin.call_id),
                ) {
                    self.send(event).await?;
                }
                self.ack(command.id).await
            }
            SessionCommandBody::DelegationResult {
                origin, outcome, ..
            } if self.started && !self.finished => {
                let text = match outcome {
                    zhir_core::operation::OperationOutcome::Success {
                        content,
                        structured,
                    } => {
                        let mut text = context_text(content)?;
                        if !structured.is_null() {
                            if !text.is_empty() {
                                text.push('\n');
                            }
                            text.push_str(&structured.to_string());
                        }
                        text
                    }
                    zhir_core::operation::OperationOutcome::Failure { error } => {
                        format!("Task failed: {}", error.message)
                    }
                    zhir_core::operation::OperationOutcome::Cancelled { reason } => {
                        format!("Task cancelled: {reason}")
                    }
                };
                for mut event in codec::context(
                    "delegation.context.append",
                    &command.id,
                    &text,
                    Some(&origin.call_id),
                ) {
                    event["channel"] = json!("speakable");
                    self.send(event).await?;
                }
                self.delegations.remove(&origin.call_id);
                self.ack(command.id).await?;
                if self.end_id.is_some() && self.delegations.is_empty() {
                    self.close_remote().await?;
                }
                Ok(())
            }
            SessionCommandBody::EndInput
                if self.started && !self.finished && self.end_id.is_none() =>
            {
                while let Ok(chunk) = self.input.try_recv() {
                    self.audio(chunk).await?;
                }
                self.end_id = Some(command.id);
                if self.delegations.is_empty() {
                    self.close_remote().await?;
                }
                Ok(())
            }
            SessionCommandBody::Close if self.finished => self.ack(command.id).await,
            SessionCommandBody::Close if self.started => {
                self.close_id = Some(command.id);
                self.protocol_deadline =
                    Some(tokio::time::Instant::now() + self.model.config.command_timeout);
                self.send(json!({"type":"session.close"})).await
            }
            _ => Err(Error::Invalid(
                "unsupported Live command or command order".into(),
            )),
        }
    }
    async fn close_remote(&mut self) -> Result<()> {
        self.protocol_deadline =
            Some(tokio::time::Instant::now() + self.model.config.command_timeout);
        self.send(json!({"type":"session.close"})).await
    }
    async fn frame(&mut self, frame: Frame) -> Result<()> {
        match frame {
            Frame::Audio { payload, timestamp } => {
                let first = *self.audio_timestamp.get_or_insert(timestamp);
                let chunk = MediaChunk {
                    stream_id: "live-audio".into(),
                    turn_id: self
                        .turn
                        .clone()
                        .ok_or_else(|| Error::Protocol("audio before execution".into()))?,
                    epoch: self.open.output_epoch,
                    sequence: self.audio_sequence,
                    timestamp_us: u64::from(timestamp.wrapping_sub(first)) * 1_000_000 / 48000,
                    media_type: AUDIO_TYPE.into(),
                    bytes: payload,
                    end: false,
                };
                chunk.validate(self.open.limits.max_media_chunk_bytes)?;
                self.audio_sequence = self
                    .audio_sequence
                    .checked_add(1)
                    .ok_or_else(|| Error::Protocol("audio sequence overflow".into()))?;
                self.media
                    .as_ref()
                    .ok_or(Error::Cancelled)?
                    .send(chunk)
                    .await
                    .map_err(|_| Error::Cancelled)
            }
            Frame::Event(text) => {
                let value: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|_| Error::Protocol("invalid Live event JSON".into()))?;
                let event: codec::ServerEvent = serde_json::from_value(value.clone())
                    .map_err(|_| Error::Protocol("invalid Live event fields".into()))?;
                match event {
                    codec::ServerEvent::Started => {
                        if self.started {
                            return Err(Error::Protocol("duplicate Live start".into()));
                        }
                        self.started = true;
                        self.protocol_deadline = None;
                        let id = self
                            .start_id
                            .take()
                            .ok_or_else(|| Error::Protocol("unsolicited Live start".into()))?;
                        self.ack(id).await
                    }
                    codec::ServerEvent::Turn { turn } => {
                        let message = match turn.role.as_str() {
                            "user" => Message::user(turn.transcript),
                            "assistant" => Message::Assistant {
                                output: vec![Output::text(turn.transcript)],
                                provider_data: serde_json::Value::Null,
                            },
                            _ => return Err(Error::Protocol("invalid Live speaker".into())),
                        };
                        self.event(SessionEventBody::ConversationItem {
                            item_id: turn.id,
                            message,
                        })
                        .await
                    }
                    codec::ServerEvent::Delegation { item } => {
                        if item.kind != "delegation"
                            || item.target != "client"
                            || item.content.iter().any(|part| part.kind != "input_text")
                        {
                            return Err(Error::Protocol("unsupported Live delegation".into()));
                        }
                        if !self.delegations.contains(&item.id)
                            && self.delegations.len() >= self.open.limits.max_inflight_operations
                        {
                            return Err(Error::Protocol(
                                "Live delegation capacity exceeded".into(),
                            ));
                        }
                        self.delegations.insert(item.id.clone());
                        self.event(SessionEventBody::Output {
                            turn_id: self.turn.clone().ok_or_else(|| {
                                Error::Protocol("delegation before execution".into())
                            })?,
                            item_id: item.id.clone(),
                            caller_id: "live".into(),
                            output: Output::Delegation {
                                request: DelegationRequest {
                                    id: item.id,
                                    prompt: item
                                        .content
                                        .into_iter()
                                        .map(|part| part.text)
                                        .collect(),
                                },
                            },
                        })
                        .await
                    }
                    codec::ServerEvent::Closed { reason, usage } => {
                        if self.finished {
                            return Err(Error::Protocol("duplicate Live closure".into()));
                        }
                        self.finished = true;
                        self.protocol_deadline = None;
                        if let Some(peer) = &self.peer {
                            peer.close().await;
                        }
                        // Media received before finalization is drained before its end marker.
                        while let Some(frame) = self
                            .peer
                            .as_mut()
                            .and_then(|peer| peer.frames.try_recv().ok())
                        {
                            Box::pin(self.frame(frame)).await?;
                        }
                        if self.audio_sequence > 0 {
                            self.media
                                .as_ref()
                                .ok_or(Error::Cancelled)?
                                .send(MediaChunk {
                                    stream_id: "live-audio".into(),
                                    turn_id: self.turn.clone().expect("started turn"),
                                    epoch: self.open.output_epoch,
                                    sequence: self.audio_sequence,
                                    timestamp_us: 0,
                                    media_type: AUDIO_TYPE.into(),
                                    bytes: vec![],
                                    end: true,
                                })
                                .await
                                .map_err(|_| Error::Cancelled)?;
                        }
                        self.media.take();
                        if let Some(id) = self.end_id.take() {
                            self.ack(id).await?;
                        }
                        if let Some(id) = self.close_id.take() {
                            self.ack(id).await?;
                        }
                        self.event(SessionEventBody::TurnFinished {
                            turn_id: self.turn.clone().expect("started turn"),
                            disposition: TurnDisposition::Finished,
                            usage: Usage::default(),
                            model_id: Some(self.model.config.model.clone()),
                            response_id: None,
                            finish_reason: Some(reason.clone()),
                            provider_data: json!({"session_usage":usage,"close_reason":reason}),
                            effective: Default::default(),
                        })
                        .await?;
                        self.event(SessionEventBody::Closed).await
                    }
                    codec::ServerEvent::Error { error } => Err(Error::Protocol(format!(
                        "Live error: {}",
                        error
                            .get("code")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                    ))),
                    codec::ServerEvent::Observation => {
                        self.event(SessionEventBody::Delta {
                            turn_id: self.turn.clone().unwrap_or_default(),
                            delta: ModelDelta::ProtocolEvent {
                                output_index: 0,
                                data: value,
                            },
                        })
                        .await
                    }
                }
            }
        }
    }
    async fn audio(&self, chunk: MediaChunk) -> Result<()> {
        chunk.validate(self.open.limits.max_media_chunk_bytes)?;
        if chunk.media_type != AUDIO_TYPE
            || chunk.epoch != 0
            || self.turn.as_ref() != Some(&chunk.turn_id)
        {
            return Err(Error::Invalid(
                "Live requires current-turn Opus input with epoch zero".into(),
            ));
        }
        if !chunk.bytes.is_empty() {
            let duration = codec::opus_duration(&chunk.bytes)?;
            self.peer
                .as_ref()
                .ok_or(Error::Cancelled)?
                .audio(chunk.bytes, duration)
                .await?;
        }
        Ok(())
    }
    async fn run(&mut self) -> Result<()> {
        let first = self.commands.recv().await.ok_or(Error::Cancelled)?;
        self.command(first).await?;
        loop {
            if self
                .protocol_deadline
                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
            {
                return Err(Error::Uncertain(
                    "Live protocol acknowledgement timed out".into(),
                ));
            }
            if self.finished {
                let command = self.commands.recv().await.ok_or(Error::Cancelled)?;
                self.command(command).await?;
                continue;
            }
            enum Next {
                Command(Option<SessionCommand>),
                Audio(Option<MediaChunk>),
                Frame(Option<Frame>),
                Failed,
            }
            let next = {
                let peer = self
                    .peer
                    .as_mut()
                    .ok_or_else(|| Error::Protocol("missing Live peer".into()))?;
                if let Some(error) = peer.failure.borrow().clone() {
                    return Err(error);
                }
                tokio::select! {
                    command=self.commands.recv(), if self.started=>Next::Command(command),
                    audio=self.input.recv(),if self.started && self.end_id.is_none()=>Next::Audio(audio),
                    frame=peer.frames.recv()=>Next::Frame(frame),
                    _=peer.failure.changed()=>Next::Failed,
                    _=tokio::time::sleep(Duration::from_millis(10)), if self.protocol_deadline.is_some()=>Next::Failed,
                }
            };
            match next {
                Next::Command(Some(command)) => self.command(command).await?,
                Next::Frame(Some(frame)) => self.frame(frame).await?,
                Next::Audio(Some(chunk)) => self.audio(chunk).await?,
                Next::Failed => continue,
                Next::Audio(None) => return Err(Error::Cancelled),
                _ => {
                    return Err(Error::Uncertain(
                        "Live transport ended before finalization".into(),
                    ));
                }
            }
        }
    }
}
fn context_text(content: Vec<Content>) -> Result<String> {
    content
        .into_iter()
        .map(|part| match part {
            Content::Text { text } => Ok(text),
            _ => Err(Error::Invalid(
                "Live delegation context requires text".into(),
            )),
        })
        .collect()
}
