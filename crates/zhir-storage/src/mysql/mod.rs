use crate::sql::SqlStore;
use std::sync::Arc;
use zhir_core::{
    BoxFuture, Result,
    run::Checkpoint,
    storage::{Commit, RunStore},
};
#[derive(Clone)]
pub struct MysqlRunStore {
    inner: SqlStore,
}
impl MysqlRunStore {
    /// Opens a store with a pool of 8 connections.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with(url, 8).await
    }
    /// Opens a store with a pool of `max_connections` connections. Concurrent commits to
    /// one run still resolve to a single head through the revision check.
    pub async fn connect_with(url: &str, max_connections: u32) -> Result<Self> {
        Ok(Self {
            inner: SqlStore::connect(url, max_connections).await?,
        })
    }
    pub async fn close(&self) {
        self.inner.close().await;
    }
}
impl RunStore for MysqlRunStore {
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
