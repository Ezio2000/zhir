//! Durable outputs and asynchronous reference resolution before protocol encoding.
mod replay;
mod resolve;
mod save;
use std::{collections::BTreeMap, sync::Arc};
use zhir_core::{
    BoxFuture, Result,
    artifact::ArtifactStore,
    error::Error,
    model::{Capabilities, Model, ModelContext, ModelRequest, ModelResponse},
};
pub struct ArtifactModel {
    inner: Arc<dyn Model>,
    store: Arc<dyn ArtifactStore>,
}
impl ArtifactModel {
    pub fn new(inner: Arc<dyn Model>, store: Arc<dyn ArtifactStore>) -> Self {
        Self { inner, store }
    }
}
fn invalid(message: impl Into<String>) -> Error {
    Error::Protocol(message.into())
}
fn json_error(error: serde_json::Error) -> Error {
    invalid(error.to_string())
}
impl Model for ArtifactModel {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }
    fn invoke(
        &self,
        mut request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            context.cancellation.check()?;
            request.validate(self.capabilities())?;
            let mut cache = BTreeMap::new();
            for message in &mut request.messages {
                resolve::message(
                    message,
                    self.store.as_ref(),
                    &context.cancellation,
                    &mut cache,
                )
                .await?;
            }
            context.cancellation.check()?;
            let response = self.inner.invoke(request, context.clone()).await?;
            response.validate()?;
            context.cancellation.check()?;
            save::response(
                response,
                &context.run,
                self.store.as_ref(),
                &context.cancellation,
            )
            .await
        })
    }
}
