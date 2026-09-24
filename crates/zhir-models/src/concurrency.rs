//! Shared bounded admission for model sessions and individual model requests.
use std::{sync::Arc, time::Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zhir_core::{
    BoxFuture, Cancellation, Result, error::Error, model::*, profile::NegotiatedProfile,
};
fn semaphore(limit: usize, name: &str) -> Result<Arc<Semaphore>> {
    if limit == 0 || limit > Semaphore::MAX_PERMITS {
        return Err(Error::Invalid(format!("invalid {name} concurrency limit")));
    }
    Ok(Arc::new(Semaphore::new(limit)))
}
async fn acquire(
    permits: &Arc<Semaphore>,
    cancellation: &Cancellation,
    deadline: Option<Instant>,
) -> Result<OwnedSemaphorePermit> {
    zhir_policies::timing::check(cancellation, deadline)?;
    tokio::select! {
        biased;
        result = permits.clone().acquire_owned() => result.map_err(|_| Error::Cancelled),
        error = zhir_policies::timing::interrupted(cancellation, deadline, |at| tokio::time::sleep_until(at.into())) => Err(error),
    }
}
/// A shared bound on concurrent model requests. A permit covers one whole request,
/// including retries and reading its response stream.
#[derive(Clone)]
pub struct RequestLimit(Arc<Semaphore>);
impl RequestLimit {
    pub fn new(limit: usize) -> Result<Self> {
        semaphore(limit, "request").map(Self)
    }
    /// Waits for a permit, returning early with the cancellation or deadline error.
    pub async fn acquire(
        &self,
        cancellation: &Cancellation,
        deadline: Option<Instant>,
    ) -> Result<RequestPermit> {
        acquire(&self.0, cancellation, deadline)
            .await
            .map(|_permit| RequestPermit { _permit })
    }
}
/// Holds one [`RequestLimit`] slot until dropped.
pub struct RequestPermit {
    _permit: OwnedSemaphorePermit,
}
#[derive(Clone)]
pub struct ConcurrencyLimitedModel {
    inner: Arc<dyn Model>,
    permits: Arc<Semaphore>,
}
impl ConcurrencyLimitedModel {
    pub fn new(inner: Arc<dyn Model>, limit: usize) -> Result<Self> {
        Ok(Self {
            inner,
            permits: semaphore(limit, "session")?,
        })
    }
}
struct LeasedEvents {
    inner: Box<dyn SessionEvents>,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl SessionEvents for LeasedEvents {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        self.inner.receive()
    }
}
struct LeasedControl {
    inner: Arc<dyn SessionControl>,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl SessionControl for LeasedControl {
    fn binding(&self) -> Option<zhir_core::model::ModelBinding> {
        self.inner.binding()
    }
    fn capabilities(&self) -> &CapabilitySet {
        self.inner.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn submit(&self, command: SessionCommand) -> BoxFuture<'_, Result<()>> {
        self.inner.submit(command)
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
            let deadline = zhir_policies::timing::deadline(&open.context.run)?;
            let permit = acquire(&self.permits, &open.context.cancellation, deadline).await?;
            let mut session = self.inner.open_session(open).await?;
            let permit = Arc::new(permit);
            session.control = Arc::new(LeasedControl {
                inner: session.control,
                _permit: permit.clone(),
            });
            session.media.input = session.media.input.map(|inner| {
                Arc::new(LeasedMediaSender {
                    inner,
                    _permit: permit.clone(),
                }) as Arc<dyn zhir_core::resource::MediaSender>
            });
            session.media.output = session.media.output.map(|inner| {
                Box::new(LeasedMediaReceiver {
                    inner,
                    _permit: permit.clone(),
                }) as Box<dyn zhir_core::resource::MediaReceiver>
            });
            session.events = Box::new(LeasedEvents {
                inner: session.events,
                _permit: permit,
            });
            Ok(session)
        })
    }
}
