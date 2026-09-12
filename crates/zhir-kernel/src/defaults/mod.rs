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
        Err(Error::Invalid(format!("unknown tool {}", call.name)))
    }
}
impl RuntimeToolCatalogProvider for EmptyTools {
    fn open_catalog(&self) -> BoxFuture<'_, Result<Arc<dyn RuntimeToolCatalog>>> {
        Box::pin(async { Ok(Arc::new(Self) as Arc<dyn RuntimeToolCatalog>) })
    }
}
pub(crate) struct DefaultBatch;
impl BatchPolicy for DefaultBatch {
    fn select(
        &self,
        candidates: &[RuntimeToolCall],
        specs: &[RuntimeToolSpec],
    ) -> Result<RuntimeToolBatch> {
        let first = candidates
            .first()
            .ok_or_else(|| Error::Invalid("empty batch candidates".into()))?;
        let safe = |c: &RuntimeToolCall| {
            specs
                .iter()
                .find(|s| s.name == c.name)
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
