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
        RuntimeToolCatalogProvider, RuntimeToolContext, RuntimeToolInput, RuntimeToolSpec,
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
    fn start(
        &self,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<zhir_core::operation::ToolExecution>> {
        Box::pin(async move {
            wrap(
                self.entry.tool.start(self.call.clone(), context).await?,
                self.entry.clone(),
            )
        })
    }
    fn recover(
        &self,
        record: zhir_core::operation::OperationRecord,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<zhir_core::operation::ToolExecution>> {
        Box::pin(async move {
            wrap(
                self.entry.tool.recover(record, context).await?,
                self.entry.clone(),
            )
        })
    }
}
fn check_output(outcome: &zhir_core::operation::OperationOutcome, entry: &Entry) -> Result<()> {
    outcome.validate()?;
    if let (Some(schema), Some(value)) = (&entry.output, outcome.structured()) {
        validate(schema, value)?;
    }
    Ok(())
}
fn wrap(
    execution: zhir_core::operation::ToolExecution,
    entry: Arc<Entry>,
) -> Result<zhir_core::operation::ToolExecution> {
    use zhir_core::operation::ToolExecution;
    match execution {
        ToolExecution::Finished(outcome) => {
            Ok(ToolExecution::Finished(validated_outcome(outcome, &entry)))
        }
        ToolExecution::Active(mut handle) => {
            handle.events = Box::new(ValidatedEvents {
                inner: handle.events,
                entry,
            });
            Ok(ToolExecution::Active(handle))
        }
    }
}
struct ValidatedEvents {
    inner: Box<dyn zhir_core::operation::OperationEvents>,
    entry: Arc<Entry>,
}
impl zhir_core::operation::OperationEvents for ValidatedEvents {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<zhir_core::operation::OperationEvent>>> {
        Box::pin(async move {
            let mut event = self.inner.receive().await?;
            if let Some(zhir_core::operation::OperationEvent {
                update: zhir_core::operation::OperationUpdate::Finished { outcome },
                ..
            }) = &mut event
            {
                *outcome = validated_outcome(outcome.clone(), &self.entry);
            }
            Ok(event)
        })
    }
}

fn validated_outcome(
    outcome: zhir_core::operation::OperationOutcome,
    entry: &Entry,
) -> zhir_core::operation::OperationOutcome {
    match check_output(&outcome, entry) {
        Ok(()) => outcome,
        Err(error) => zhir_core::operation::OperationOutcome::Failure {
            error: zhir_core::error::Failure::new("invalid_tool_output", error.to_string()),
        },
    }
}
