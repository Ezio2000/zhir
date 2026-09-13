use std::sync::Arc;
use zhir_core::{
    BoxFuture, Result,
    tool::{
        RuntimeToolBinding, RuntimeToolCall, RuntimeToolCatalog, RuntimeToolCatalogProvider,
        RuntimeToolSpec,
    },
};
pub(crate) struct EmptyTools;
impl RuntimeToolCatalog for EmptyTools {
    fn specs(&self) -> Vec<RuntimeToolSpec> {
        vec![]
    }
    fn bind(&self, call: &RuntimeToolCall) -> Result<Arc<dyn RuntimeToolBinding>> {
        Err(zhir_core::error::CatalogError::NotFound {
            name: call.name.clone(),
        }
        .into())
    }
}
impl RuntimeToolCatalogProvider for EmptyTools {
    fn open_catalog(
        &self,
        context: zhir_core::tool::CatalogContext,
    ) -> BoxFuture<'_, Result<Arc<dyn RuntimeToolCatalog>>> {
        Box::pin(async move {
            context.cancellation.check()?;
            Ok(Arc::new(Self) as Arc<dyn RuntimeToolCatalog>)
        })
    }
}
