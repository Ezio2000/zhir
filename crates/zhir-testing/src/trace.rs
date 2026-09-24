//! Offline checks over committed facts. This module performs no effects.
use std::sync::Arc;
use zhir_core::{
    Result,
    error::Error,
    run::{Checkpoint, ControlAction, Fact, State},
    storage::{Commit, HistoryDelta},
};
pub fn verify_trace(checkpoints: &[Arc<Checkpoint>]) -> Result<()> {
    let first = checkpoints
        .first()
        .ok_or_else(|| Error::Invalid("empty trace".into()))?;
    first.validate()?;
    if first.revision == 0 && !matches!(first.fact, Fact::Started) {
        return Err(Error::Protocol("initial checkpoint must be Started".into()));
    }
    for pair in checkpoints.windows(2) {
        let before = &pair[0];
        let after = &pair[1];
        if before.state.terminal() {
            return Err(Error::Protocol("terminal run has a successor".into()));
        }
        let history = if matches!(after.fact, Fact::HistoryRewrite { .. }) {
            HistoryDelta::Replace(after.history.entries())
        } else {
            let entries = after.history.appended_since(&before.history)?;
            if entries.is_empty() {
                HistoryDelta::Unchanged
            } else {
                HistoryDelta::Append(entries)
            }
        };
        Commit::new(after.clone(), history).validate_against(Some(&before.as_ref().into()))?;
        match &after.fact {
            Fact::Started => {
                return Err(Error::Protocol(
                    "Started appears after revision zero".into(),
                ));
            }
            Fact::Attached if !after.state.active() => {
                return Err(Error::Protocol("attach requires running state".into()));
            }
            Fact::Operation {
                operation_id,
                state,
            } if !after
                .active
                .operations
                .get(operation_id)
                .is_some_and(|o| o.state == *state) =>
            {
                return Err(Error::Protocol("operation fact and state disagree".into()));
            }
            Fact::Session {
                session_id,
                sequence,
            } if (&after.active.session.id != session_id
                || after.active.session.last_sequence != Some(*sequence)) =>
            {
                return Err(Error::Protocol("session fact and cursor disagree".into()));
            }
            Fact::GenerationAbandoned { generation_id, .. }
                if (after.active.session.generation_id.as_ref() != Some(generation_id)
                    || after.active.session.response_status
                        != Some(zhir_core::model::ResponseStatus::Continuation)
                    || !after.active.session.needs_generation) =>
            {
                return Err(Error::Protocol(
                    "generation abandonment and session disagree".into(),
                ));
            }
            Fact::Control { action } => {
                let valid = matches!(
                    (action, &after.state),
                    (ControlAction::Finished, State::Completed { .. })
                        | (ControlAction::Failed, State::Failed { .. })
                        | (ControlAction::Limited, State::Limited { .. })
                        | (ControlAction::Cancelled, State::Cancelled)
                        | (ControlAction::Suspended, State::Suspended { .. })
                );
                if !valid {
                    return Err(Error::Protocol("control fact and state disagree".into()));
                }
            }
            _ => (),
        }
    }
    Ok(())
}
