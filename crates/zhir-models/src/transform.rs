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
    opening: std::sync::Mutex<Option<(ModelRequest, ModelRequest)>>,
    inner: Arc<dyn SessionSender>,
    prepare: Arc<Prepare>,
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
            if let SessionCommandBody::StartTurn { request, .. } = &mut command.body {
                self.context.cancellation.check()?;
                let opening = self.opening.lock().expect("prepared opening").take();
                **request = if let Some((original, prepared)) =
                    opening.filter(|(original, _)| original == request.as_ref())
                {
                    let _ = original;
                    prepared
                } else {
                    (self.prepare)(*request.clone(), self.context.clone()).await?
                };
                self.context.cancellation.check()?;
            }
            for transform in &self.transforms {
                self.context.cancellation.check()?;
                command = transform(command, self.context.clone()).await?;
            }
            self.context.cancellation.check()?;
            self.inner.send(command).await
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
            let original = open.request.clone();
            open.request = (self.prepare)(open.request, context.clone()).await?;
            context.cancellation.check()?;
            let prepared = open.request.clone();
            let mut session = self.inner.open_session(open).await?;
            session.input = Arc::new(Input {
                opening: std::sync::Mutex::new(Some((original, prepared))),
                inner: session.input,
                prepare: self.prepare.clone(),
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
