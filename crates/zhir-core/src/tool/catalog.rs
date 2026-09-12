use crate::{Cancellation, Result, error::CatalogError, run::RunContext};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
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
}
