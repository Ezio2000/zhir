use super::HistoryDelta;
use crate::{
    Result,
    error::Error,
    run::{Checkpoint, append_history_digest},
    wire::CheckpointCore,
};
use std::sync::Arc;

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
    pub fn check_deadline(&self, now: std::time::Instant) -> Result<()> {
        if self.deadline.is_some_and(|d| now >= d) {
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
    pub fn validate_against(&self, previous: Option<&CheckpointCore>) -> Result<()> {
        let next = self.core();
        next.options.validate()?;
        let actual = previous.map(|c| c.revision);
        if actual != self.expected_revision() {
            return Err(Error::Conflict {
                expected: self.expected_revision(),
                actual,
            });
        }
        if previous.is_some_and(|p| p.options != next.options) {
            return Err(Error::Storage("run options changed after start".into()));
        }
        if previous.map(|c| &c.id) != next.parent_id.as_ref()
            || previous.is_some_and(|c| c.context.run_id != next.context.run_id)
        {
            return Err(Error::Storage("checkpoint parent/run mismatch".into()));
        }
        let (mut count, mut digest, messages) = match (&self.history, previous) {
            (HistoryDelta::Initial(m), None) | (HistoryDelta::Replace(m), Some(_)) => {
                (0, [0u8; 32], Some(m))
            }
            (HistoryDelta::Append(m), Some(p)) if !m.is_empty() => {
                (p.history_count, p.history_digest, Some(m))
            }
            (HistoryDelta::Unchanged, Some(p)) => (p.history_count, p.history_digest, None),
            _ => return Err(Error::Storage("invalid history change".into())),
        };
        if let Some(messages) = messages {
            for m in messages {
                m.validate()?;
                digest = append_history_digest(digest, m)?;
                count += 1;
            }
        }
        if count != next.history_count || digest != next.history_digest {
            return Err(Error::Storage("history delta mismatch".into()));
        }
        self.checkpoint.validate()
    }
}
