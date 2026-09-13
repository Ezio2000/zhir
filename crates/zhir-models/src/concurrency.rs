//! Shared concurrency over complete model invocations, including streamed sinks.
use crate::retry_wait;
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    model::{Capabilities, Model, ModelContext, ModelRequest, ModelResponse},
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
            let deadline = retry_wait::deadline(&context.run)?;
            let check = || retry_wait::check(&context.cancellation, deadline);
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
