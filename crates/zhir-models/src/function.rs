//! Closure adapters for the existing Model and DeltaSink ports.
use std::future::Future;
use zhir_core::{
    BoxFuture, Result,
    model::{CapabilitySet, DeltaSink, Model, ModelContext, ModelDelta, ModelRequest, TurnOutput},
};

pub struct FunctionModel {
    capabilities: CapabilitySet,
    exchange: std::sync::Arc<crate::session::Exchange>,
}
impl FunctionModel {
    pub fn new<F, Fut>(capabilities: CapabilitySet, callback: F) -> Self
    where
        F: Fn(ModelRequest, ModelContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<TurnOutput>> + Send + 'static,
    {
        Self {
            capabilities,
            exchange: std::sync::Arc::new(move |r, c| Box::pin(callback(r, c))),
        }
    }
}
impl Model for FunctionModel {
    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<zhir_core::profile::NegotiatedProfile> {
        crate::session::validate_capabilities(&self.capabilities)?;
        zhir_policies::negotiation::negotiate(request, &self.capabilities)
    }
    fn open_session(
        &self,
        open: zhir_core::model::SessionOpen,
    ) -> BoxFuture<'_, Result<zhir_core::model::ModelSession>> {
        Box::pin(async move {
            crate::session::open(open, self.exchange.clone(), self.capabilities.clone(), {
                let caps = self.capabilities.clone();
                std::sync::Arc::new(move |request| {
                    zhir_policies::negotiation::negotiate(request, &caps)
                })
            })
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
