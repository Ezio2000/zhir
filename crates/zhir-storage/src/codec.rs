//! Only compact checkpoint cores and accepted history deltas enter commit I/O.
#![cfg(any(feature = "sqlite", feature = "mysql", feature = "redis"))]
use zhir_core::{
    Result,
    error::Error,
    run::append_history_digest,
    storage::{Commit, HistoryDelta},
    wire::CheckpointCore,
};
pub(crate) fn validate(commit: &Commit, previous: Option<&CheckpointCore>) -> Result<()> {
    commit.check_deadline()?;
    let next = commit.core();
    next.options.validate()?;
    let actual = previous.map(|c| c.revision);
    if actual != commit.expected_revision() {
        return Err(Error::Conflict {
            expected: commit.expected_revision(),
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
    let (mut count, mut digest, messages) = match (&commit.history, previous) {
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
    next.state.validate()
}
pub(crate) fn storage_error(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}
