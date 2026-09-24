use crate::sql::SqlStore;
use std::sync::Arc;
use zhir_core::{
    BoxFuture, Result,
    run::Checkpoint,
    storage::{Commit, RunStore},
};
#[derive(Clone)]
pub struct SqliteRunStore {
    inner: SqlStore,
}
impl SqliteRunStore {
    /// Opens a store with one connection: SQLite has a single writer, and the database
    /// runs in WAL mode with a busy timeout.
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self {
            inner: SqlStore::connect(url, 1).await?,
        })
    }
    pub async fn close(&self) {
        self.inner.close().await;
    }
}
impl RunStore for SqliteRunStore {
    fn commit(&self, commit: Commit) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.inner.commit(commit))
    }
    fn load_head(&self, run_id: &str) -> BoxFuture<'_, Result<Option<Arc<Checkpoint>>>> {
        let id = run_id.to_owned();
        Box::pin(async move { self.inner.load_head(&id).await })
    }
    fn delete(&self, run_id: &str) -> BoxFuture<'_, Result<()>> {
        let id = run_id.to_owned();
        Box::pin(async move { self.inner.delete(&id).await })
    }
}
