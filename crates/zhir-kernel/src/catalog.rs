use std::{collections::BTreeMap, sync::Arc};
use zhir_core::{
    Result,
    error::CatalogError,
    tool::{
        RuntimeToolBinding, RuntimeToolCall, RuntimeToolCatalog, RuntimeToolSelection,
        RuntimeToolSpec,
    },
};

pub(crate) fn select_catalog(
    selection: &RuntimeToolSelection,
    inner: Arc<dyn RuntimeToolCatalog>,
) -> Result<Arc<dyn RuntimeToolCatalog>> {
    selection.validate()?;
    let mut specs = BTreeMap::new();
    for spec in inner.specs() {
        let name = spec.name.clone();
        if name.is_empty() {
            return Err(CatalogError::EmptyName.into());
        }
        if specs.insert(name.clone(), spec).is_some() {
            return Err(CatalogError::Duplicate {
                name,
                sources: vec!["catalog".into()],
            }
            .into());
        }
    }
    match selection {
        RuntimeToolSelection::All => {}
        RuntimeToolSelection::None => specs.clear(),
        RuntimeToolSelection::Only { names } => {
            for name in names {
                if !specs.contains_key(name) {
                    return Err(CatalogError::NotFound { name: name.clone() }.into());
                }
            }
            specs.retain(|name, _| names.contains(name));
        }
    }
    Ok(Arc::new(SelectedCatalog { inner, specs }))
}
struct SelectedCatalog {
    inner: Arc<dyn RuntimeToolCatalog>,
    specs: BTreeMap<String, RuntimeToolSpec>,
}
impl RuntimeToolCatalog for SelectedCatalog {
    fn specs(&self) -> Vec<RuntimeToolSpec> {
        self.specs.values().cloned().collect()
    }
    fn bind(&self, call: &RuntimeToolCall) -> Result<Arc<dyn RuntimeToolBinding>> {
        call.validate()?;
        let spec = self
            .specs
            .get(&call.name)
            .ok_or_else(|| CatalogError::NotSelected {
                name: call.name.clone(),
            })?;
        let binding = self.inner.bind(call)?;
        if binding.spec() != spec {
            return Err(CatalogError::BindingChanged {
                name: call.name.clone(),
            }
            .into());
        }
        Ok(binding)
    }
}
