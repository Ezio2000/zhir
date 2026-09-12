use crate::validation::{compile, validate};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use zhir_core::{
    BoxFuture, Result,
    error::{CatalogError, Error, ValidationError},
    tool::{
        InputSpec, RuntimeTool, RuntimeToolBinding, RuntimeToolCall, RuntimeToolCatalog,
        RuntimeToolCatalogProvider, RuntimeToolContext, RuntimeToolInput, RuntimeToolResult,
        RuntimeToolSpec,
    },
};
struct Entry {
    tool: Arc<dyn RuntimeTool>,
    spec: RuntimeToolSpec,
    input: Option<jsonschema::Validator>,
    output: Option<jsonschema::Validator>,
}
#[derive(Default)]
pub struct RuntimeToolRegistry {
    entries: RwLock<BTreeMap<String, Arc<Entry>>>,
}
impl RuntimeToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register(&self, tool: Arc<dyn RuntimeTool>) -> Result<()> {
        let spec = tool.spec().clone();
        spec.execution.validate()?;
        if spec.name.is_empty() {
            return Err(CatalogError::EmptyName.into());
        }
        let input = match &spec.input {
            InputSpec::Structured { schema } => Some(compile(schema)?),
            InputSpec::Freeform { .. } => None,
        };
        let output = spec.output_schema.as_ref().map(compile).transpose()?;
        let mut entries = self
            .entries
            .write()
            .map_err(|_| Error::Invalid("poisoned registry".into()))?;
        if entries.contains_key(&spec.name) {
            return Err(CatalogError::Duplicate {
                name: spec.name.clone(),
                sources: vec!["registry".into()],
            }
            .into());
        }
        entries.insert(
            spec.name.clone(),
            Arc::new(Entry {
                tool,
                spec,
                input,
                output,
            }),
        );
        Ok(())
    }
    pub fn from_tools(
        runtime_tools: impl IntoIterator<Item = Arc<dyn RuntimeTool>>,
    ) -> Result<Self> {
        let registry = Self::new();
        for tool in runtime_tools {
            registry.register(tool)?;
        }
        Ok(registry)
    }
}
struct Catalog {
    entries: BTreeMap<String, Arc<Entry>>,
}
impl RuntimeToolCatalogProvider for RuntimeToolRegistry {
    fn open_catalog(
        &self,
        context: zhir_core::tool::CatalogContext,
    ) -> BoxFuture<'_, Result<Arc<dyn RuntimeToolCatalog>>> {
        Box::pin(async move {
            context.cancellation.check()?;
            Ok(Arc::new(Catalog {
                entries: self
                    .entries
                    .read()
                    .map_err(|_| Error::Invalid("poisoned registry".into()))?
                    .clone(),
            }) as Arc<dyn RuntimeToolCatalog>)
        })
    }
}
impl RuntimeToolCatalog for Catalog {
    fn specs(&self) -> Vec<RuntimeToolSpec> {
        self.entries.values().map(|e| e.spec.clone()).collect()
    }
    fn bind(&self, call: &RuntimeToolCall) -> Result<Arc<dyn RuntimeToolBinding>> {
        call.validate()?;
        let entry = self
            .entries
            .get(&call.name)
            .ok_or_else(|| {
                Error::Catalog(CatalogError::NotFound {
                    name: call.name.clone(),
                })
            })?
            .clone();
        match (&entry.spec.input, &call.input) {
            (InputSpec::Structured { .. }, RuntimeToolInput::Structured(value)) => {
                validate(entry.input.as_ref().expect("compiled input schema"), value)?
            }
            (InputSpec::Freeform { .. }, RuntimeToolInput::Freeform(_)) => {}
            _ => return Err(ValidationError::InputKind.into()),
        }
        Ok(Arc::new(Binding {
            entry,
            call: call.clone(),
        }))
    }
}
struct Binding {
    entry: Arc<Entry>,
    call: RuntimeToolCall,
}
impl RuntimeToolBinding for Binding {
    fn spec(&self) -> &RuntimeToolSpec {
        &self.entry.spec
    }
    fn invoke(&self, context: RuntimeToolContext) -> BoxFuture<'_, Result<RuntimeToolResult>> {
        Box::pin(async move {
            let result = self.entry.tool.invoke(self.call.clone(), context).await?;
            result.validate()?;
            if let (Some(schema), Some(value)) = (&self.entry.output, result.outcome.structured()) {
                validate(schema, value)?;
            }
            Ok(result)
        })
    }
}
