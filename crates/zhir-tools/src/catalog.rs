//! Composition of immutable catalog snapshots from caller-owned sources.
use std::{collections::BTreeMap, sync::Arc};
use zhir_core::{
    BoxFuture, Result,
    error::CatalogError,
    tool::{
        CatalogContext, RuntimeToolBinding, RuntimeToolCall, RuntimeToolCatalog,
        RuntimeToolCatalogProvider, RuntimeToolSpec,
    },
};

pub struct CompositeRuntimeTools {
    sources: Vec<Arc<dyn RuntimeToolCatalogProvider>>,
}
impl CompositeRuntimeTools {
    pub fn new(sources: impl IntoIterator<Item = Arc<dyn RuntimeToolCatalogProvider>>) -> Self {
        Self {
            sources: sources.into_iter().collect(),
        }
    }
}
struct Entry {
    source: usize,
    spec: RuntimeToolSpec,
}
struct Catalog {
    snapshots: Vec<Arc<dyn RuntimeToolCatalog>>,
    entries: BTreeMap<String, Entry>,
}
impl RuntimeToolCatalogProvider for CompositeRuntimeTools {
    fn open_catalog(
        &self,
        context: CatalogContext,
    ) -> BoxFuture<'_, Result<Arc<dyn RuntimeToolCatalog>>> {
        Box::pin(async move {
            let mut snapshots = Vec::new();
            let mut entries: BTreeMap<String, Entry> = BTreeMap::new();
            for (index, source) in self.sources.iter().enumerate() {
                context.cancellation.check()?;
                let snapshot = source.open_catalog(context.clone()).await?;
                context.cancellation.check()?;
                for spec in snapshot.specs() {
                    if spec.name.is_empty() {
                        return Err(CatalogError::EmptyName.into());
                    }
                    if let Some(old) = entries.get(&spec.name) {
                        return Err(CatalogError::Duplicate {
                            name: spec.name.clone(),
                            sources: vec![old.source.to_string(), index.to_string()],
                        }
                        .into());
                    }
                    entries.insert(
                        spec.name.clone(),
                        Entry {
                            source: index,
                            spec,
                        },
                    );
                }
                snapshots.push(snapshot);
            }
            Ok(Arc::new(Catalog { snapshots, entries }) as Arc<dyn RuntimeToolCatalog>)
        })
    }
}
impl RuntimeToolCatalog for Catalog {
    fn specs(&self) -> Vec<RuntimeToolSpec> {
        self.entries.values().map(|v| v.spec.clone()).collect()
    }
    fn bind(&self, call: &RuntimeToolCall) -> Result<Arc<dyn RuntimeToolBinding>> {
        call.validate()?;
        let entry = self
            .entries
            .get(&call.name)
            .ok_or_else(|| CatalogError::NotFound {
                name: call.name.clone(),
            })?;
        let binding = self.snapshots[entry.source].bind(call)?;
        if binding.spec() != &entry.spec {
            return Err(CatalogError::BindingChanged {
                name: call.name.clone(),
            }
            .into());
        }
        Ok(binding)
    }
}
