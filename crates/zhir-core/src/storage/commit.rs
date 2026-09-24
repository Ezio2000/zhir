use super::HistoryDelta;
use crate::{
    Result,
    error::Error,
    run::{Checkpoint, append_history_digest},
    wire::CheckpointCore,
};
use std::sync::{Arc, OnceLock};

/// One checkpoint write. The durable core and its digest are derived once and shared by
/// validation and every store.
#[derive(Debug, Clone)]
pub struct Commit {
    checkpoint: Arc<Checkpoint>,
    history: HistoryDelta,
    pub deadline: Option<std::time::Instant>,
    core: OnceLock<CheckpointCore>,
    digest: OnceLock<String>,
}
impl Commit {
    pub fn new(checkpoint: Arc<Checkpoint>, history: HistoryDelta) -> Self {
        Self {
            checkpoint,
            history,
            deadline: None,
            core: OnceLock::new(),
            digest: OnceLock::new(),
        }
    }
    pub fn checkpoint(&self) -> &Arc<Checkpoint> {
        &self.checkpoint
    }
    pub fn history(&self) -> &HistoryDelta {
        &self.history
    }
    pub fn into_checkpoint(self) -> Arc<Checkpoint> {
        self.checkpoint
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
    pub fn core(&self) -> &CheckpointCore {
        self.core
            .get_or_init(|| CheckpointCore::from(self.checkpoint.as_ref()))
    }
    pub fn digest(&self) -> Result<String> {
        if let Some(digest) = self.digest.get() {
            return Ok(digest.clone());
        }
        let digest = self.core().digest()?;
        Ok(self.digest.get_or_init(|| digest).clone())
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
        if let Some(previous) = previous {
            if previous.context.started_at_ms != next.context.started_at_ms
                || previous.context.deadline_at_ms != next.context.deadline_at_ms
                || previous.context.parent_run_id != next.context.parent_run_id
                || previous.context.parent_runtime_tool_call_id
                    != next.context.parent_runtime_tool_call_id
            {
                return Err(Error::Storage("immutable run context changed".into()));
            }
            if previous.state.terminal() {
                return Err(Error::Storage("terminal run cannot advance".into()));
            }
            if matches!(previous.state, crate::run::State::Suspended { .. })
                && !matches!(next.fact, crate::run::Fact::Attached)
            {
                return Err(Error::Storage("suspended run requires attach".into()));
            }
            if next.active.session.id != previous.active.session.id
                || next.active.session.output_epoch < previous.active.session.output_epoch
                || next.active.session.profile_revision < previous.active.session.profile_revision
                || next.active.session.context_revision < previous.active.session.context_revision
                || next.active.session.acknowledged_context_revision
                    < previous.active.session.acknowledged_context_revision
                || next.active.session.input_position < previous.active.session.input_position
                || next.active.session.generated_input_position
                    < previous.active.session.generated_input_position
                || next.metrics.generation_requests < previous.metrics.generation_requests
                || next.metrics.runtime_tool_calls < previous.metrics.runtime_tool_calls
            {
                return Err(Error::Storage("non-monotonic run state".into()));
            }
            for (id, old) in &previous.active.operations {
                match next.active.operations.get(id) {
                    Some(current) => {
                        if old.origin != current.origin
                            || old.owner != current.owner
                            || old.call_entry != current.call_entry
                        {
                            return Err(Error::Storage(
                                "operation identity changed after admission".into(),
                            ));
                        }
                        if old.state.terminal() && old != current {
                            return Err(Error::Storage("terminal operation changed".into()));
                        }
                        if old.last_sequence.is_some_and(|sequence| {
                            current.last_sequence.is_none_or(|next| next < sequence)
                        }) {
                            return Err(Error::Storage("operation cursor moved backwards".into()));
                        }
                    }
                    None if !old.state.terminal() => {
                        return Err(Error::Storage("unfinished operation removed".into()));
                    }
                    None => (),
                }
            }
            for old in &previous.active.commands {
                if let Some(current) = next
                    .active
                    .commands
                    .iter()
                    .find(|command| command.id == old.id)
                    && (old.intent != current.intent || old.sent && !current.sent)
                {
                    return Err(Error::Storage(
                        "pending command changed after admission".into(),
                    ));
                }
            }
            if matches!(self.history, HistoryDelta::Replace(_))
                && (!previous.active.operations.is_empty()
                    || !previous.active.commands.is_empty()
                    || !previous.active.media.is_empty()
                    || !matches!(next.fact, crate::run::Fact::HistoryRewrite { .. }))
            {
                return Err(Error::Storage("history rewrite crosses active work".into()));
            }
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
