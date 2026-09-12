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
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self {
            inner: SqlStore::connect(url).await?,
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
}
