use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    run::Checkpoint,
    storage::{Commit, RunStore},
};
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
            if let Some(old) = state.ids.get(&commit.checkpoint().id) {
                return if old == &digest {
                    Ok(())
                } else {
                    Err(Error::Storage("checkpoint id reused".into()))
                };
            }
            commit.check_deadline(std::time::Instant::now())?;
            let previous = state
                .head
                .as_deref()
                .map(zhir_core::wire::CheckpointCore::from);
            commit.validate_against(previous.as_ref())?;
            state.ids.insert(commit.checkpoint().id.clone(), digest);
            state.head = Some(commit.into_checkpoint());
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
    fn delete(&self, run_id: &str) -> BoxFuture<'_, Result<()>> {
        let run_id = run_id.to_owned();
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Storage("poisoned ephemeral store".into()))?;
            if state
                .head
                .as_ref()
                .is_some_and(|c| c.context.run_id == run_id)
            {
                state.head = None;
                state.ids.clear();
            }
            Ok(())
        })
    }
}
