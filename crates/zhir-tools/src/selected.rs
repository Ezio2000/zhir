//! Immutable views over one runtime catalog snapshot per invocation.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    tool::{
        RuntimeToolBinding, RuntimeToolCall, RuntimeToolCatalog, RuntimeToolCatalogProvider,
        RuntimeToolSpec,
    },
};

pub struct SelectedRuntimeTools {
    inner: Arc<dyn RuntimeToolCatalogProvider>,
    names: BTreeSet<String>,
}
impl SelectedRuntimeTools {
    pub fn new(
        inner: Arc<dyn RuntimeToolCatalogProvider>,
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let mut selected = BTreeSet::new();
        for name in names {
            let name = name.into();
            if name.is_empty() || !selected.insert(name) {
                return Err(Error::Invalid(
                    "selected runtime tool names must be nonempty and unique".into(),
                ));
            }
        }
        Ok(Self {
            inner,
            names: selected,
        })
    }
}
struct Catalog {
    inner: Arc<dyn RuntimeToolCatalog>,
    specs: BTreeMap<String, RuntimeToolSpec>,
}
impl RuntimeToolCatalogProvider for SelectedRuntimeTools {
    fn open_catalog(&self) -> BoxFuture<'_, Result<Arc<dyn RuntimeToolCatalog>>> {
        Box::pin(async move {
            let inner = self.inner.open_catalog().await?;
            let mut specs = BTreeMap::new();
            for spec in inner.specs() {
                if self.names.contains(&spec.name)
                    && specs.insert(spec.name.clone(), spec).is_some()
                {
                    return Err(Error::Invalid("duplicate runtime tool in catalog".into()));
                }
            }
            if let Some(name) = self.names.iter().find(|name| !specs.contains_key(*name)) {
                return Err(Error::Invalid(format!(
                    "selected runtime tool not found: {name}"
                )));
            }
            Ok(Arc::new(Catalog { inner, specs }) as Arc<dyn RuntimeToolCatalog>)
        })
    }
}
impl RuntimeToolCatalog for Catalog {
    fn specs(&self) -> Vec<RuntimeToolSpec> {
        self.specs.values().cloned().collect()
    }
    fn bind(&self, call: &RuntimeToolCall) -> Result<Arc<dyn RuntimeToolBinding>> {
        call.validate()?;
        let spec = self.specs.get(&call.name).ok_or_else(|| {
            Error::Invalid(format!("runtime tool is not selected: {}", call.name))
        })?;
        let binding = self.inner.bind(call)?;
        if binding.spec() != spec {
            return Err(Error::Invalid(
                "catalog binding changed its declared specification".into(),
            ));
        }
        Ok(binding)
    }
}
