//! Asynchronous request preparation owned by the caller.
use std::{future::Future, sync::Arc};
use zhir_core::{
    BoxFuture, Result,
    model::{Capabilities, Model, ModelContext, ModelRequest, ModelResponse},
};

/// Prepare a request before the inner model is invoked, using the same context.
///
/// The transform receives a context clone and returns only the request. Deadlines,
/// cancellation, run identity and the downstream sink remain owned by the caller.
/// No background task is started. Like other Model decorators, placement relative
/// to RetryingModel determines whether preparation happens per attempt or per call.
/// The input capabilities default to the inner model's. Declare different input
/// capabilities explicitly when preparation converts modalities or features.
pub struct TransformModel<F> {
    inner: Arc<dyn Model>,
    capabilities: Capabilities,
    transform: F,
}
impl<F> TransformModel<F> {
    pub fn new<Fut>(inner: Arc<dyn Model>, transform: F) -> Self
    where
        F: Fn(ModelRequest, ModelContext) -> Fut + Send + Sync,
        Fut: Future<Output = Result<ModelRequest>> + Send + 'static,
    {
        Self {
            capabilities: inner.capabilities().clone(),
            inner,
            transform,
        }
    }
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self
    }
}
impl<F, Fut> Model for TransformModel<F>
where
    F: Fn(ModelRequest, ModelContext) -> Fut + Send + Sync,
    Fut: Future<Output = Result<ModelRequest>> + Send + 'static,
{
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            context.cancellation.check()?;
            request.validate(&self.capabilities)?;
            let request = (self.transform)(request, context.clone()).await?;
            context.cancellation.check()?;
            request.validate(self.inner.capabilities())?;
            let cancellation = context.cancellation.clone();
            let response = self.inner.invoke(request, context).await?;
            cancellation.check()?;
            response.validate()?;
            Ok(response)
        })
    }
}
