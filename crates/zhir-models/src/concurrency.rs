//! Shared concurrency over complete model invocations, including streamed sinks.
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    model::{Capabilities, Model, ModelContext, ModelRequest, ModelResponse},
    run::now_ms,
};

#[derive(Clone)]
pub struct ConcurrencyLimitedModel {
    inner: Arc<dyn Model>,
    permits: Arc<Semaphore>,
}
impl ConcurrencyLimitedModel {
    pub fn new(inner: Arc<dyn Model>, limit: usize) -> Result<Self> {
        if limit == 0 || limit > Semaphore::MAX_PERMITS {
            return Err(Error::Invalid("invalid model concurrency limit".into()));
        }
        Ok(Self {
            inner,
            permits: Arc::new(Semaphore::new(limit)),
        })
    }
}
impl Model for ConcurrencyLimitedModel {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            request.validate(self.capabilities())?;
            let deadline = context
                .run
                .deadline_at_ms
                .map(|ms| {
                    Instant::now()
                        .checked_add(Duration::from_millis(ms.saturating_sub(now_ms())))
                        .ok_or_else(|| {
                            Error::Invalid(
                                "model deadline exceeds the monotonic clock range".into(),
                            )
                        })
                })
                .transpose()?;
            let check = || -> Result<()> {
                context.cancellation.check()?;
                if deadline.is_some_and(|at| Instant::now() >= at) {
                    return Err(Error::Deadline);
                }
                Ok(())
            };
            check()?;
            let acquire = self.permits.acquire();
            tokio::pin!(acquire);
            let permit = loop {
                tokio::select! {
                    result = &mut acquire => break result.map_err(|_| Error::Invalid("model concurrency limiter closed".into()))?,
                    _ = tokio::time::sleep(Duration::from_millis(10)) => check()?,
                }
            };
            check()?;
            let future = self.inner.invoke(request, context.clone());
            tokio::pin!(future);
            let result = loop {
                tokio::select! {
                    result = &mut future => break result,
                    _ = tokio::time::sleep(Duration::from_millis(10)) => check()?,
                }
            };
            check()?;
            drop(permit);
            let response = result?;
            response.validate()?;
            Ok(response)
        })
    }
}
