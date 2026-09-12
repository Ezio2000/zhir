//! Asynchronous request and response transformations owned by the caller.
use std::{future::Future, sync::Arc};
use zhir_core::{
    BoxFuture, Result,
    model::{Capabilities, Model, ModelContext, ModelRequest, ModelResponse},
};
type Prepare =
    dyn Fn(ModelRequest, ModelContext) -> BoxFuture<'static, Result<ModelRequest>> + Send + Sync;
type MapResponse =
    dyn Fn(ModelResponse, ModelContext) -> BoxFuture<'static, Result<ModelResponse>> + Send + Sync;

/// Transformations run in declaration order and retain the original run context.
/// Placement relative to retry determines per-attempt versus per-call execution.
/// Response maps do not rewrite already emitted deltas.
pub struct TransformModel {
    inner: Arc<dyn Model>,
    capabilities: Capabilities,
    prepare: Arc<Prepare>,
    responses: Vec<Arc<MapResponse>>,
}
impl TransformModel {
    pub fn new<F, Fut>(inner: Arc<dyn Model>, prepare: F) -> Self
    where
        F: Fn(ModelRequest, ModelContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ModelRequest>> + Send + 'static,
    {
        Self {
            capabilities: inner.capabilities().clone(),
            inner,
            prepare: Arc::new(move |r, c| Box::pin(prepare(r, c))),
            responses: vec![],
        }
    }
    pub fn response<F, Fut>(inner: Arc<dyn Model>, transform: F) -> Self
    where
        F: Fn(ModelResponse, ModelContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ModelResponse>> + Send + 'static,
    {
        Self::new(inner, |request, _| async move { Ok(request) }).map_response(transform)
    }
    pub fn map_response<F, Fut>(mut self, transform: F) -> Self
    where
        F: Fn(ModelResponse, ModelContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ModelResponse>> + Send + 'static,
    {
        self.responses
            .push(Arc::new(move |r, c| Box::pin(transform(r, c))));
        self
    }
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self
    }
}
impl Model for TransformModel {
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
            let request = (self.prepare)(request, context.clone()).await?;
            context.cancellation.check()?;
            request.validate(self.inner.capabilities())?;
            let mut response = self.inner.invoke(request, context.clone()).await?;
            context.cancellation.check()?;
            response.validate()?;
            for transform in &self.responses {
                response = transform(response, context.clone()).await?;
                context.cancellation.check()?;
                response.validate()?;
            }
            Ok(response)
        })
    }
}
