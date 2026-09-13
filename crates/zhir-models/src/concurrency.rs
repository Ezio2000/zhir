//! Shared bounded admission for the lifetime of model sessions.
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zhir_core::{BoxFuture, Result, error::Error, model::*, profile::NegotiatedProfile};
#[derive(Clone)]
pub struct ConcurrencyLimitedModel {
    inner: Arc<dyn Model>,
    permits: Arc<Semaphore>,
}
impl ConcurrencyLimitedModel {
    pub fn new(inner: Arc<dyn Model>, limit: usize) -> Result<Self> {
        if limit == 0 || limit > Semaphore::MAX_PERMITS {
            return Err(Error::Invalid("invalid session concurrency limit".into()));
        }
        Ok(Self {
            inner,
            permits: Arc::new(Semaphore::new(limit)),
        })
    }
}
struct LeasedEvents {
    inner: Box<dyn SessionReceiver>,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl SessionReceiver for LeasedEvents {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        self.inner.receive()
    }
}
struct LeasedInput {
    inner: Arc<dyn SessionSender>,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl SessionSender for LeasedInput {
    fn capabilities(&self) -> &CapabilitySet {
        self.inner.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn send(&self, command: SessionCommand) -> BoxFuture<'_, Result<()>> {
        self.inner.send(command)
    }
}
struct LeasedMediaSender {
    inner: Arc<dyn zhir_core::resource::MediaSender>,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl zhir_core::resource::MediaSender for LeasedMediaSender {
    fn send(&self, chunk: zhir_core::resource::MediaChunk) -> BoxFuture<'_, Result<()>> {
        self.inner.send(chunk)
    }
}
struct LeasedMediaReceiver {
    inner: Box<dyn zhir_core::resource::MediaReceiver>,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl zhir_core::resource::MediaReceiver for LeasedMediaReceiver {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<zhir_core::resource::MediaChunk>>> {
        self.inner.receive()
    }
}
impl Model for ConcurrencyLimitedModel {
    fn capabilities(&self) -> &CapabilitySet {
        self.inner.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn open_session(&self, open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            let deadline = crate::retry_wait::deadline(&open.context.run)?;
            let acquire = self.permits.clone().acquire_owned();
            tokio::pin!(acquire);
            let permit = loop {
                crate::retry_wait::check(&open.context.cancellation, deadline)?;
                tokio::select! { result = &mut acquire => break result.map_err(|_| Error::Cancelled)?, _ = tokio::time::sleep(Duration::from_millis(10)) => () }
            };
            let mut session = self.inner.open_session(open).await?;
            let permit = Arc::new(permit);
            session.input = Arc::new(LeasedInput {
                inner: session.input,
                _permit: permit.clone(),
            });
            session.media_input = session.media_input.map(|inner| {
                Arc::new(LeasedMediaSender {
                    inner,
                    _permit: permit.clone(),
                }) as Arc<dyn zhir_core::resource::MediaSender>
            });
            session.media_output = session.media_output.map(|inner| {
                Box::new(LeasedMediaReceiver {
                    inner,
                    _permit: permit.clone(),
                }) as Box<dyn zhir_core::resource::MediaReceiver>
            });
            session.output = Box::new(LeasedEvents {
                inner: session.output,
                _permit: permit,
            });
            Ok(session)
        })
    }
}
