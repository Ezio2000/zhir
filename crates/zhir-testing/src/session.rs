//! Native session fixture. Protocol faults and synthetic providers live here only.
use std::{future::Future, sync::Arc};
use tokio::sync::mpsc;
use zhir_core::{
    BoxFuture, Result, error::Error, model::*, profile::NegotiatedProfile, resource::*,
};
type Handler = dyn Fn(SessionOpen, SessionPeer) -> BoxFuture<'static, Result<()>> + Send + Sync;
pub struct SessionModel {
    capabilities: CapabilitySet,
    handler: Arc<Handler>,
}
impl SessionModel {
    pub fn new<F, Fut>(capabilities: CapabilitySet, handler: F) -> Self
    where
        F: Fn(SessionOpen, SessionPeer) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            capabilities,
            handler: Arc::new(move |open, peer| Box::pin(handler(open, peer))),
        }
    }
}
pub struct SessionPeer {
    pub commands: mpsc::Receiver<SessionCommand>,
    pub media_input: mpsc::Receiver<MediaChunk>,
    pub media_output: Arc<dyn MediaSender>,
    events: mpsc::Sender<Result<SessionEvent>>,
    sequence: u64,
    input_position: u64,
    seed: Vec<zhir_core::message::Message>,
    projection: Vec<zhir_core::run::HistoryEntry>,
}
impl SessionPeer {
    pub fn projection(&self) -> Vec<zhir_core::message::Message> {
        let mut messages = self.seed.clone();
        messages.extend(conversation(self.projection.clone()));
        messages
    }
    /// Apply kernel-accepted output to the fixture projection before exposing host commands.
    pub async fn command(&mut self) -> Result<Option<SessionCommand>> {
        while let Some(command) = self.commands.recv().await {
            match &command.body {
                SessionCommandBody::Append { entry, .. } => self.projection.push(entry.clone()),
                SessionCommandBody::ReplaceContext { entries, .. } => {
                    self.seed.clear();
                    self.projection = entries.clone();
                }
                _ => (),
            }
            if matches!(
                command.body,
                SessionCommandBody::Append {
                    source: AppendSource::Accepted,
                    ..
                }
            ) {
                self.acknowledge(&command, None).await?;
            } else {
                return Ok(Some(command));
            }
        }
        Ok(None)
    }
    pub async fn close(&mut self) -> Result<()> {
        if let Some(command) = self.command().await? {
            if !matches!(command.body, SessionCommandBody::Close) {
                return Err(Error::Protocol(
                    "unexpected command while waiting for graceful close".into(),
                ));
            }
            self.acknowledge(&command, None).await?;
            self.event(SessionEventBody::Closed {
                reason: "host_request".into(),
                provider_data: serde_json::Value::Null,
            })
            .await?;
            return Ok(());
        }
        Err(Error::Protocol("host did not request close".into()))
    }
    pub async fn event(&mut self, body: SessionEventBody) -> Result<()> {
        let sequence = self.sequence;
        self.sequence += 1;
        self.event_at(sequence, body).await
    }
    pub async fn event_at(&self, sequence: u64, body: SessionEventBody) -> Result<()> {
        self.events
            .send(Ok(SessionEvent { sequence, body }))
            .await
            .map_err(|_| Error::Cancelled)
    }
    pub async fn acknowledge(
        &mut self,
        command: &SessionCommand,
        recovery: Option<zhir_core::operation::RecoveryRef>,
    ) -> Result<()> {
        self.event(SessionEventBody::Acknowledged {
            command_id: command.id.clone(),
            recovery,
            level: Acknowledgement::Provider,
        })
        .await?;
        if let SessionCommandBody::Generate {
            generation_id,
            input_position,
            ..
        } = &command.body
        {
            self.input_position = *input_position;
            self.event(SessionEventBody::ResponseStarted {
                generation_id: generation_id.clone(),
                input_position: *input_position,
            })
            .await?;
        }
        Ok(())
    }
    pub async fn finished(
        &mut self,
        generation_id: String,
        response_status: ResponseStatus,
    ) -> Result<()> {
        self.event(SessionEventBody::ResponseFinished {
            generation_id,
            input_position: self.input_position,
            response_status,
            usage: Default::default(),
            model_id: None,
            response_id: None,
            finish_reason: None,
            provider_data: serde_json::Value::Null,
            effective: Default::default(),
        })
        .await
    }
}
struct Control {
    sender: mpsc::Sender<SessionCommand>,
    capabilities: CapabilitySet,
}
impl SessionControl for Control {
    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        zhir_policies::negotiation::negotiate(request, &self.capabilities)
    }
    fn submit(&self, command: SessionCommand) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.sender
                .send(command)
                .await
                .map_err(|_| Error::Cancelled)
        })
    }
}
struct Events(mpsc::Receiver<Result<SessionEvent>>);
impl SessionEvents for Events {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        Box::pin(async move { self.0.recv().await.transpose() })
    }
}
struct MediaInput {
    sender: mpsc::Sender<MediaChunk>,
    limit: usize,
}
impl MediaSender for MediaInput {
    fn send(&self, chunk: MediaChunk) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            chunk.validate(self.limit)?;
            self.sender.send(chunk).await.map_err(|_| Error::Cancelled)
        })
    }
}
struct MediaOutput(mpsc::Receiver<MediaChunk>);
impl MediaReceiver for MediaOutput {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<MediaChunk>>> {
        Box::pin(async move { Ok(self.0.recv().await) })
    }
}
impl Model for SessionModel {
    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        zhir_policies::negotiation::negotiate(request, &self.capabilities)
    }
    fn open_session(&self, open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            self.negotiate(&open.request)?;
            let (sender, commands) = mpsc::channel(open.limits.max_control_commands);
            let (events, receiver) = mpsc::channel(open.limits.max_session_events);
            let limit = open.limits.max_media_chunk_bytes;
            let capacity = (open.limits.max_buffered_media_bytes / limit).max(1);
            let (media_input, media_commands) = mpsc::channel(capacity);
            let (media_output, media_events) = mpsc::channel(capacity);
            let mut peer = SessionPeer {
                commands,
                events: events.clone(),
                sequence: open.after_sequence.map_or(0, |s| s + 1),
                input_position: open.input_position,
                seed: open.request.messages.clone(),
                projection: vec![],
                media_input: media_commands,
                media_output: Arc::new(MediaInput {
                    sender: media_output,
                    limit,
                }),
            };
            let handler = self.handler.clone();
            tokio::spawn(async move {
                if let Err(error) = peer
                    .event(SessionEventBody::Ready {
                        context_revision: open.context_revision,
                    })
                    .await
                {
                    let _ = events.send(Err(error)).await;
                    return;
                }
                if let Err(error) = handler(open, peer).await {
                    let _ = events.send(Err(error)).await;
                }
            });
            let duplex = self.capabilities.supports(Capability::Duplex);
            Ok(ModelSession {
                control: Arc::new(Control {
                    sender,
                    capabilities: self.capabilities.clone(),
                }),
                events: Box::new(Events(receiver)),
                media: MediaPorts {
                    input: duplex.then(|| {
                        Arc::new(MediaInput {
                            sender: media_input,
                            limit,
                        }) as Arc<dyn MediaSender>
                    }),
                    output: duplex
                        .then(|| Box::new(MediaOutput(media_events)) as Box<dyn MediaReceiver>),
                },
            })
        })
    }
}
