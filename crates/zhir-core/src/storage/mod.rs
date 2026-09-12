use crate::{
    BoxFuture, Result,
    error::Error,
    message::Message,
    run::{Checkpoint, History},
    wire::CheckpointCore,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "messages",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum HistoryDelta {
    Initial(Vec<Message>),
    Append(Vec<Message>),
    Replace(Vec<Message>),
    Unchanged,
}
#[derive(Debug, Clone)]
pub struct Commit {
    pub checkpoint: Arc<Checkpoint>,
    pub history: HistoryDelta,
    pub deadline: Option<std::time::Instant>,
}
impl Commit {
    pub fn new(checkpoint: Arc<Checkpoint>, history: HistoryDelta) -> Self {
        Self {
            checkpoint,
            history,
            deadline: None,
        }
    }
    pub fn check_deadline(&self) -> Result<()> {
        if self
            .deadline
            .is_some_and(|d| std::time::Instant::now() >= d)
        {
            Err(Error::Deadline)
        } else {
            Ok(())
        }
    }
    pub fn expected_revision(&self) -> Option<u64> {
        self.checkpoint.revision.checked_sub(1)
    }
    pub fn core(&self) -> CheckpointCore {
        CheckpointCore::from(self.checkpoint.as_ref())
    }
    pub fn digest(&self) -> Result<String> {
        self.core().digest()
    }
    pub fn validate_against(&self, previous: Option<&Checkpoint>) -> Result<()> {
        self.check_deadline()?;
        self.checkpoint.options.validate()?;
        let actual = previous.map(|p| p.revision);
        if actual != self.expected_revision() {
            return Err(Error::Conflict {
                expected: self.expected_revision(),
                actual,
            });
        }
        if previous.is_some_and(|p| p.options != self.checkpoint.options) {
            return Err(Error::Storage("run options changed after start".into()));
        }
        if previous.map(|p| &p.id) != self.checkpoint.parent_id.as_ref() {
            return Err(Error::Storage("checkpoint parent mismatch".into()));
        }
        if previous.is_some_and(|p| p.context.run_id != self.checkpoint.context.run_id) {
            return Err(Error::Storage("run mismatch".into()));
        }
        let history = match (&self.history, previous) {
            (HistoryDelta::Initial(messages), None) => History::new(messages.clone())?,
            (HistoryDelta::Append(messages), Some(p)) if !messages.is_empty() => {
                p.history.append(messages.clone())?
            }
            (HistoryDelta::Replace(messages), Some(_)) => History::new(messages.clone())?,
            (HistoryDelta::Unchanged, Some(p)) => p.history.clone(),
            _ => return Err(Error::Storage("invalid history delta".into())),
        };
        if history.len() != self.checkpoint.history.len()
            || history.digest() != self.checkpoint.history.digest()
        {
            return Err(Error::Storage(
                "history delta does not produce checkpoint".into(),
            ));
        }
        self.checkpoint.state.validate()
    }
}
/// Implementations settle atomic writes before returning. A dropped caller does not
/// cancel an ambiguous database commit; close waits for owned write operations.
pub trait RunStore: Send + Sync {
    fn commit(&self, commit: Commit) -> BoxFuture<'_, Result<()>>;
    fn load_head(&self, run_id: &str) -> BoxFuture<'_, Result<Option<Arc<Checkpoint>>>>;
}
