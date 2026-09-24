use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    run::Checkpoint,
    storage::{Commit, RunStore},
};

pub struct RecordingStore {
    inner: Arc<dyn RunStore>,
    commits: Mutex<Vec<Commit>>,
}
impl RecordingStore {
    pub fn new(inner: Arc<dyn RunStore>) -> Self {
        Self {
            inner,
            commits: Mutex::new(vec![]),
        }
    }
    pub fn commits(&self) -> Vec<Commit> {
        self.commits.lock().expect("store records lock").clone()
    }
    /// Verify each recorded run in revision order. Successful idempotent writes
    /// are checked for equal digests and count once. Returns unique checkpoints.
    pub fn verify_traces(&self) -> Result<usize> {
        let mut runs: BTreeMap<String, BTreeMap<u64, Commit>> = BTreeMap::new();
        let commits = self.commits();
        if commits.is_empty() {
            return Err(Error::Invalid("no recorded commits".into()));
        }
        for commit in commits {
            let revisions = runs
                .entry(commit.checkpoint.context.run_id.clone())
                .or_default();
            if let Some(previous) = revisions.get(&commit.checkpoint.revision) {
                if previous.checkpoint.id != commit.checkpoint.id
                    || previous.digest()? != commit.digest()?
                {
                    return Err(Error::Protocol(
                        "recorded revision has conflicting commits".into(),
                    ));
                }
            } else {
                revisions.insert(commit.checkpoint.revision, commit);
            }
        }
        let mut count = 0;
        for revisions in runs.into_values() {
            let checkpoints: Vec<_> = revisions.into_values().map(|c| c.checkpoint).collect();
            crate::verify_trace(&checkpoints)?;
            count += checkpoints.len();
        }
        Ok(count)
    }
}
impl RunStore for RecordingStore {
    fn commit(&self, commit: Commit) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.inner.commit(commit.clone()).await?;
            self.commits
                .lock()
                .expect("store records lock")
                .push(commit);
            Ok(())
        })
    }
    fn load_head(&self, run_id: &str) -> BoxFuture<'_, Result<Option<Arc<Checkpoint>>>> {
        self.inner.load_head(run_id)
    }
}

/// Keeps every commit up to and including the first one `crash` matches, then rejects
/// later commits as a stopped process would. The durable head is the crash point.
pub struct CrashingStore {
    inner: Arc<dyn RunStore>,
    crash: Box<dyn Fn(&Checkpoint) -> bool + Send + Sync>,
    crashed: std::sync::atomic::AtomicBool,
}
impl CrashingStore {
    pub fn new(
        inner: Arc<dyn RunStore>,
        crash: impl Fn(&Checkpoint) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner,
            crash: Box::new(crash),
            crashed: Default::default(),
        }
    }
    pub fn crashed(&self) -> bool {
        self.crashed.load(std::sync::atomic::Ordering::SeqCst)
    }
}
impl RunStore for CrashingStore {
    fn commit(&self, commit: Commit) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if self.crashed() {
                return Err(Error::Storage("process stopped".into()));
            }
            let crash = (self.crash)(&commit.checkpoint);
            self.inner.commit(commit).await?;
            self.crashed
                .store(crash, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
    }
    fn load_head(&self, run_id: &str) -> BoxFuture<'_, Result<Option<Arc<Checkpoint>>>> {
        self.inner.load_head(run_id)
    }
}
