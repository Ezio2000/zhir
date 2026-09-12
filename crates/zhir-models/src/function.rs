//! Closure adapters for the existing Model and DeltaSink ports.
use std::future::Future;
use zhir_core::{
    BoxFuture, Result,
    model::{
        Capabilities, DeltaSink, Model, ModelContext, ModelDelta, ModelRequest, ModelResponse,
    },
};

pub struct FunctionModel<F> {
    capabilities: Capabilities,
    callback: F,
}
impl<F> FunctionModel<F> {
    pub fn new<Fut>(capabilities: Capabilities, callback: F) -> Self
    where
        F: Fn(ModelRequest, ModelContext) -> Fut + Send + Sync,
        Fut: Future<Output = Result<ModelResponse>> + Send + 'static,
    {
        Self {
            capabilities,
            callback,
        }
    }
}
impl<F, Fut> Model for FunctionModel<F>
where
    F: Fn(ModelRequest, ModelContext) -> Fut + Send + Sync,
    Fut: Future<Output = Result<ModelResponse>> + Send + 'static,
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
            let cancellation = context.cancellation.clone();
            let response = (self.callback)(request, context).await?;
            cancellation.check()?;
            response.validate()?;
            Ok(response)
        })
    }
}

/// Each emission awaits the callback; no queue, task or retention is added.
pub struct FunctionDeltaSink<F>(F);
impl<F> FunctionDeltaSink<F> {
    pub fn new<Fut>(callback: F) -> Self
    where
        F: Fn(ModelDelta) -> Fut + Send + Sync,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self(callback)
    }
}
impl<F, Fut> DeltaSink for FunctionDeltaSink<F>
where
    F: Fn(ModelDelta) -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    fn emit(&self, delta: ModelDelta) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { (self.0)(delta).await })
    }
}
