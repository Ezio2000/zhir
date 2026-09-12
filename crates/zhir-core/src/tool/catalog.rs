use super::{RuntimeToolBinding, RuntimeToolCall, RuntimeToolCatalog, RuntimeToolSpec};
use crate::{Cancellation, Result, error::CatalogError, run::RunContext};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Debug, Clone, Default)]
pub struct CatalogContext {
    pub run: RunContext,
    pub cancellation: Cancellation,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeToolSelection {
    #[default]
    All,
    Only {
        names: Vec<String>,
    },
    None,
}
impl RuntimeToolSelection {
    pub fn only(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::Only {
            names: names.into_iter().map(Into::into).collect(),
        }
    }
    pub fn validate(&self) -> Result<()> {
        if let Self::Only { names } = self {
            let mut seen = BTreeSet::new();
            for name in names {
                if name.is_empty() {
                    return Err(CatalogError::EmptyName.into());
                }
                if !seen.insert(name) {
                    return Err(CatalogError::Duplicate {
                        name: name.clone(),
                        sources: vec!["selection".into()],
                    }
                    .into());
                }
            }
        }
        Ok(())
    }
    /// A pure, executor-independent view used for both declaration and binding.
    pub fn select(
        &self,
        inner: Arc<dyn RuntimeToolCatalog>,
    ) -> Result<Arc<dyn RuntimeToolCatalog>> {
        self.validate()?;
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
        match self {
            Self::All => {}
            Self::None => specs.clear(),
            Self::Only { names } => {
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
