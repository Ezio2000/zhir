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
}
impl SessionPeer {
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
        })
        .await
    }
    pub async fn finished(&mut self, turn_id: String, disposition: TurnDisposition) -> Result<()> {
        self.event(SessionEventBody::TurnFinished {
            turn_id,
            disposition,
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
struct Input {
    sender: mpsc::Sender<SessionCommand>,
    capabilities: CapabilitySet,
}
impl SessionSender for Input {
    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        zhir_policies::negotiation::negotiate(request, &self.capabilities)
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
struct Events(mpsc::Receiver<Result<SessionEvent>>);
impl SessionReceiver for Events {
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
            let peer = SessionPeer {
                commands,
                events: events.clone(),
                sequence: open.after_sequence.map_or(0, |s| s + 1),
                media_input: media_commands,
                media_output: Arc::new(MediaInput {
                    sender: media_output,
                    limit,
                }),
            };
            let handler = self.handler.clone();
            tokio::spawn(async move {
                if let Err(error) = handler(open, peer).await {
                    let _ = events.send(Err(error)).await;
                }
            });
            let duplex = self.capabilities.supports(Capability::Duplex);
            Ok(ModelSession {
                input: Arc::new(Input {
                    sender,
                    capabilities: self.capabilities.clone(),
                }),
                output: Box::new(Events(receiver)),
                media_input: duplex.then(|| {
                    Arc::new(MediaInput {
                        sender: media_input,
                        limit,
                    }) as Arc<dyn MediaSender>
                }),
                media_output: duplex
                    .then(|| Box::new(MediaOutput(media_events)) as Box<dyn MediaReceiver>),
            })
        })
    }
}
