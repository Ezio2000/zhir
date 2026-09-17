//! Caller-owned transformations of session commands and normalized events.
use std::{future::Future, sync::Arc};
use zhir_core::{BoxFuture, Result, model::*, profile::NegotiatedProfile};
type Prepare =
    dyn Fn(ModelRequest, ModelContext) -> BoxFuture<'static, Result<ModelRequest>> + Send + Sync;
type MapCommand = dyn Fn(SessionCommand, ModelContext) -> BoxFuture<'static, Result<SessionCommand>>
    + Send
    + Sync;
type MapEvent =
    dyn Fn(SessionEvent, ModelContext) -> BoxFuture<'static, Result<SessionEvent>> + Send + Sync;
pub struct TransformModel {
    inner: Arc<dyn Model>,
    prepare: Arc<Prepare>,
    events: Vec<Arc<MapEvent>>,
    commands: Vec<Arc<MapCommand>>,
}
impl TransformModel {
    pub fn new<F, Fut>(inner: Arc<dyn Model>, prepare: F) -> Self
    where
        F: Fn(ModelRequest, ModelContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ModelRequest>> + Send + 'static,
    {
        Self {
            inner,
            prepare: Arc::new(move |r, c| Box::pin(prepare(r, c))),
            events: vec![],
            commands: vec![],
        }
    }
    pub fn map_command<F, Fut>(mut self, transform: F) -> Self
    where
        F: Fn(SessionCommand, ModelContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<SessionCommand>> + Send + 'static,
    {
        self.commands.push(Arc::new(move |command, context| {
            Box::pin(transform(command, context))
        }));
        self
    }
    pub fn map_event<F, Fut>(mut self, transform: F) -> Self
    where
        F: Fn(SessionEvent, ModelContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<SessionEvent>> + Send + 'static,
    {
        self.events.push(Arc::new(move |event, context| {
            Box::pin(transform(event, context))
        }));
        self
    }
}
struct Input {
    inner: Arc<dyn SessionSender>,
    prepare: Arc<Prepare>,
    config: tokio::sync::Mutex<ModelRequest>,
    context: ModelContext,
    transforms: Vec<Arc<MapCommand>>,
}
impl SessionSender for Input {
    fn capabilities(&self) -> &CapabilitySet {
        self.inner.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn send(&self, mut command: SessionCommand) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut current_config = self.config.lock().await;
            if let SessionCommandBody::ReplaceContext {
                entries,
                context_revision,
            } = &mut command.body
            {
                let mut request = current_config.clone();
                request.messages = conversation(entries.clone());
                let prepared = (self.prepare)(request, self.context.clone()).await?;
                let mut config = prepared.clone();
                config.messages = current_config.messages.clone();
                if config != *current_config {
                    return Err(zhir_core::error::Error::Invalid(
                        "context transform changed session configuration".into(),
                    ));
                }
                *entries = prepared
                    .messages
                    .into_iter()
                    .enumerate()
                    .map(|(index, message)| zhir_core::run::HistoryEntry {
                        id: format!("projection:{context_revision}:{index}"),
                        origin: None,
                        message,
                    })
                    .collect();
            }
            for transform in &self.transforms {
                self.context.cancellation.check()?;
                command = transform(command, self.context.clone()).await?;
            }
            self.context.cancellation.check()?;
            let profile = match &command.body {
                SessionCommandBody::UpdateProfile { profile, .. } => Some(profile.clone()),
                _ => None,
            };
            self.inner.send(command).await?;
            if let Some(profile) = profile {
                current_config.profile = profile;
            }
            Ok(())
        })
    }
}
struct Events {
    inner: Box<dyn SessionReceiver>,
    transforms: Vec<Arc<MapEvent>>,
    context: ModelContext,
}
impl SessionReceiver for Events {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        Box::pin(async move {
            let Some(mut event) = self.inner.receive().await? else {
                return Ok(None);
            };
            for transform in &self.transforms {
                event = transform(event, self.context.clone()).await?;
            }
            Ok(Some(event))
        })
    }
}
impl Model for TransformModel {
    fn capabilities(&self) -> &CapabilitySet {
        self.inner.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn open_session(&self, mut open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            let context = open.context.clone();
            context.cancellation.check()?;
            open.request = (self.prepare)(open.request, context.clone()).await?;
            context.cancellation.check()?;
            let mut config = open.request.clone();
            config.messages.clear();
            let mut session = self.inner.open_session(open).await?;
            session.input = Arc::new(Input {
                inner: session.input,
                prepare: self.prepare.clone(),
                config: tokio::sync::Mutex::new(config),
                transforms: self.commands.clone(),
                context: context.clone(),
            });
            session.output = Box::new(Events {
                inner: session.output,
                transforms: self.events.clone(),
                context,
            });
            Ok(session)
        })
    }
}
