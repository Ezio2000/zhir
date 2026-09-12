//! Default configuration and run creation owned by the execution layer.
/// Create a fresh run identity and capture its start time.
pub fn context() -> zhir_core::run::RunContext {
    zhir_core::run::RunContext::new(crate::environment::new_id(), crate::environment::now_ms())
}
/// Default execution budgets for a new run.
pub fn limits() -> zhir_core::run::Limits {
    zhir_core::run::Limits {
        max_planning_steps: 100,
        max_runtime_tool_calls: 1000,
        max_runtime_tool_batch_size: 32,
        max_runtime_tool_concurrency: 8,
        max_progress_events: 256,
        max_buffered_progress: 256,
        max_total_tokens: None,
        elapsed_ms: None,
        commit_timeout_ms: 5000,
    }
}
/// Fully resolved defaults persisted with each new run.
pub fn run_options() -> zhir_core::run::RunOptions {
    zhir_core::run::RunOptions {
        runtime_tools: zhir_core::tool::RuntimeToolSelection::All,
        limits: limits(),
        model: Default::default(),
        provider_tools: vec![],
        tool_choice: Default::default(),
        response_format: None,
        stream: false,
    }
}

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    run::Checkpoint,
    storage::{Commit, RunStore},
    tool::{
        BatchPolicy, RuntimeToolBatch, RuntimeToolBinding, RuntimeToolCall, RuntimeToolCatalog,
        RuntimeToolCatalogProvider, RuntimeToolSpec,
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
pub(crate) struct DefaultBatch;
impl BatchPolicy for DefaultBatch {
    fn select(
        &self,
        candidates: &[RuntimeToolCall],
        specs: &std::collections::BTreeMap<String, RuntimeToolSpec>,
    ) -> Result<RuntimeToolBatch> {
        let first = candidates
            .first()
            .ok_or_else(|| Error::Invalid("empty batch candidates".into()))?;
        let safe = |c: &RuntimeToolCall| {
            specs
                .get(&c.name)
                .is_some_and(|s| s.execution.parallel_safe())
        };
        let calls = if safe(first) {
            candidates
                .iter()
                .take_while(|c| safe(c))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            vec![first.clone()]
        };
        Ok(RuntimeToolBatch {
            parallel: calls.len() > 1,
            calls,
        })
    }
}
struct Memory {
    head: Option<Arc<Checkpoint>>,
    ids: HashMap<String, String>,
}
pub(crate) struct Ephemeral {
    state: Mutex<Memory>,
}
impl Ephemeral {
    pub fn new(head: Option<Arc<Checkpoint>>) -> Self {
        let mut ids = HashMap::new();
        if let Some(c) = &head {
            ids.insert(
                c.id.clone(),
                zhir_core::wire::CheckpointCore::from(c.as_ref())
                    .digest()
                    .expect("valid checkpoint"),
            );
        }
        Self {
            state: Mutex::new(Memory { head, ids }),
        }
    }
}
impl RunStore for Ephemeral {
    fn commit(&self, commit: Commit) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Storage("poisoned ephemeral store".into()))?;
            let digest = commit.digest()?;
            if let Some(old) = state.ids.get(&commit.checkpoint.id) {
                return if old == &digest {
                    Ok(())
                } else {
                    Err(Error::Storage("checkpoint id reused".into()))
                };
            }
            commit.validate_against(state.head.as_deref())?;
            state.ids.insert(commit.checkpoint.id.clone(), digest);
            state.head = Some(commit.checkpoint);
            Ok(())
        })
    }
    fn load_head(&self, run_id: &str) -> BoxFuture<'_, Result<Option<Arc<Checkpoint>>>> {
        let run_id = run_id.to_owned();
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .map_err(|_| Error::Storage("poisoned ephemeral store".into()))?
                .head
                .as_ref()
                .filter(|c| c.context.run_id == run_id)
                .cloned())
        })
    }
}
