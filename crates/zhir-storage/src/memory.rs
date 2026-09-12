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
#[derive(Default)]
struct State {
    heads: HashMap<String, Arc<Checkpoint>>,
    ids: HashMap<(String, String), String>,
}
#[derive(Default)]
pub struct MemoryRunStore {
    state: Mutex<State>,
}
impl MemoryRunStore {
    pub fn new() -> Self {
        Self::default()
    }
}
impl RunStore for MemoryRunStore {
    fn commit(&self, commit: Commit) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Storage("poisoned memory store".into()))?;
            let key = (
                commit.checkpoint.context.run_id.clone(),
                commit.checkpoint.id.clone(),
            );
            let digest = commit.digest()?;
            if let Some(old) = state.ids.get(&key) {
                return if old == &digest {
                    Ok(())
                } else {
                    Err(Error::Storage("checkpoint id reused".into()))
                };
            }
            commit.validate_against(state.heads.get(&key.0).map(Arc::as_ref))?;
            state.ids.insert(key, digest);
            state
                .heads
                .insert(commit.checkpoint.context.run_id.clone(), commit.checkpoint);
            Ok(())
        })
    }
    fn load_head(&self, run_id: &str) -> BoxFuture<'_, Result<Option<Arc<Checkpoint>>>> {
        let id = run_id.to_owned();
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .map_err(|_| Error::Storage("poisoned memory store".into()))?
                .heads
                .get(&id)
                .cloned())
        })
    }
}
