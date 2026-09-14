use super::*;
use crate::{
    message::Output,
    operation::{OperationOwner, OperationRecord, OperationState},
};
impl Checkpoint {
    pub fn validate(&self) -> Result<()> {
        self.options.validate()?;
        if self.id.is_empty() || self.context.run_id.is_empty() {
            return Err(Error::Invalid("empty checkpoint identity".into()));
        }
        if (self.revision == 0) != self.parent_id.is_none() {
            return Err(Error::Invalid("revision and parent disagree".into()));
        }
        self.state.validate()?;
        self.history.validate()?;
        if self.active.session.turn_start > self.history.len() {
            return Err(Error::Invalid("turn start exceeds history".into()));
        }
        if self.active.session.id.is_empty() {
            return Err(Error::Invalid("empty session identity".into()));
        }
        let mut ids = std::collections::HashSet::new();
        for command in &self.active.commands {
            if command.id.is_empty() || !ids.insert(&command.id) {
                return Err(Error::Invalid("invalid pending command identity".into()));
            }
        }
        if self.active.commands.len() > self.options.limits.max_control_commands
            || self.active.operations.len() > self.options.limits.max_inflight_operations
            || self.active.media.len() > self.options.limits.max_media_streams
        {
            return Err(Error::Invalid("active capacity exceeded".into()));
        }
        for command in &self.active.commands {
            match &command.intent {
                CommandIntent::StartTurn {
                    history_count,
                    turn_id,
                    ..
                } if *history_count > self.history.len()
                    || self.active.session.turn_id.as_ref() != Some(turn_id) =>
                {
                    return Err(Error::Invalid("invalid turn command".into()));
                }
                CommandIntent::InterruptOutput {
                    turn_id,
                    output_epoch,
                } if self.active.session.turn_id.as_ref() != Some(turn_id)
                    || *output_epoch == 0
                    || *output_epoch > self.active.session.output_epoch =>
                {
                    return Err(Error::Invalid(
                        "invalid interrupt output epoch or turn".into(),
                    ));
                }
                CommandIntent::Input { entry }
                    if self.history.get(*entry).is_none_or(|e| {
                        !matches!(e.message, Message::User { .. } | Message::External { .. })
                    }) =>
                {
                    return Err(Error::Invalid("invalid input command".into()));
                }
                CommandIntent::ToolResult {
                    operation_id,
                    entry,
                }
                | CommandIntent::DelegationResult {
                    operation_id,
                    entry,
                } if self
                    .active
                    .operations
                    .get(operation_id)
                    .is_none_or(|op| !op.state.terminal() || op.result_entry != Some(*entry)) =>
                {
                    return Err(Error::Invalid("invalid result command".into()));
                }
                CommandIntent::DelegationContext {
                    operation_id,
                    content,
                } => {
                    if self
                        .active
                        .operations
                        .get(operation_id)
                        .is_none_or(|op| op.owner != OperationOwner::Delegation)
                    {
                        return Err(Error::Invalid("invalid delegation context command".into()));
                    }
                    for part in content {
                        part.validate()?;
                    }
                }
                CommandIntent::UpdateProfile { revision, .. }
                    if *revision <= self.active.session.profile_revision =>
                {
                    return Err(Error::Invalid("non-increasing profile revision".into()));
                }
                _ => (),
            }
        }
        for command in &self.active.commands {
            let invalid = match &command.intent {
                CommandIntent::DelegationResult { operation_id, .. } => self
                    .active
                    .operations
                    .get(operation_id)
                    .is_none_or(|op| op.owner != OperationOwner::Delegation),
                CommandIntent::ToolResult { operation_id, .. } => self
                    .active
                    .operations
                    .get(operation_id)
                    .is_none_or(|op| !matches!(op.owner, OperationOwner::RuntimeTool { .. })),
                _ => false,
            };
            if invalid {
                return Err(Error::Invalid("result command owner mismatch".into()));
            }
        }
        if let Some(archive) = &self.active.session.media_archive {
            archive.validate()?;
        }
        self.validate_operations()?;
        if matches!(self.state, State::Completed { .. })
            && (!self.active.commands.is_empty()
                || self.active.session.disposition != Some(crate::model::TurnDisposition::Finished))
        {
            return Err(Error::Invalid(
                "completed run has pending session work".into(),
            ));
        }
        if matches!(self.state, State::Completed { .. })
            && self
                .active
                .operations
                .values()
                .any(|op| !op.state.terminal())
        {
            return Err(Error::Invalid(
                "completed run has unfinished operations".into(),
            ));
        }
        Ok(())
    }
}

impl Checkpoint {
    fn validate_operations(&self) -> Result<()> {
        for origin in self.history.pending_delegations() {
            if !self.active.operations.values().any(|op| {
                &op.origin == origin
                    && op.owner == OperationOwner::Delegation
                    && !op.state.terminal()
            }) {
                return Err(Error::Invalid(
                    "pending delegation has no active operation".into(),
                ));
            }
        }
        for (origin, call) in self.history.pending_calls() {
            let present = self.active.operations.values().any(|op| {
                &op.origin == origin && !op.state.terminal()
                    && matches!(&op.owner, OperationOwner::RuntimeTool { name } if name == &call.name)
            });
            if !present {
                return Err(Error::Invalid(
                    "pending call has no active operation".into(),
                ));
            }
        }
        for (id, operation) in &self.active.operations {
            operation.validate()?;
            let call_entry = self
                .history
                .get(operation.call_entry)
                .ok_or_else(|| Error::Invalid("missing operation call".into()))?;
            if call_entry.origin.as_ref() != Some(&operation.origin) {
                return Err(Error::Invalid(
                    "operation origin differs from history".into(),
                ));
            }
            if let crate::operation::OperationOwner::RuntimeTool { name } = &operation.owner {
                if !call_matches(&call_entry.message, operation, name) {
                    return Err(Error::Invalid("operation call mismatch".into()));
                }
                if let Some(index) = operation.result_entry
                    && !self
                        .history
                        .get(index)
                        .is_some_and(|entry| result_matches(entry, operation, name))
                {
                    return Err(Error::Invalid("operation outcome mismatch".into()));
                }
            }
            if operation.owner == OperationOwner::Delegation {
                let valid = matches!(&call_entry.message, Message::Assistant { output, .. }
                    if output.iter().any(|o| matches!(o, Output::Delegation { request } if request.id == operation.origin.call_id)));
                if !valid {
                    return Err(Error::Invalid("delegation call mismatch".into()));
                }
                if let Some(index) = operation.result_entry
                    && !self.history.get(index).is_some_and(|entry| {
                        entry.origin.as_ref() == Some(&operation.origin)
                            && matches!(&entry.message, Message::DelegationResult { id, outcome }
                                if id == &operation.origin.call_id && OperationState::from(outcome) == operation.state)
                    }) { return Err(Error::Invalid("delegation outcome mismatch".into())); }
            }
            if id != &operation.id
                || operation.call_entry >= self.history.len()
                || operation
                    .result_entry
                    .is_some_and(|i| i >= self.history.len())
            {
                return Err(Error::Invalid(
                    "operation history reference mismatch".into(),
                ));
            }
        }
        Ok(())
    }
}

fn call_matches(message: &Message, operation: &OperationRecord, name: &str) -> bool {
    let Message::Assistant { output, .. } = message else {
        return false;
    };
    output.iter().any(|item| match item {
        Output::RuntimeToolCall { call } => {
            call.name == name && call.id == operation.origin.call_id
        }
        _ => false,
    })
}
fn result_matches(entry: &HistoryEntry, operation: &OperationRecord, name: &str) -> bool {
    let Message::RuntimeTool {
        name: result_name,
        call_id,
        outcome,
    } = &entry.message
    else {
        return false;
    };
    entry.origin.as_ref() == Some(&operation.origin)
        && name == result_name
        && call_id == &operation.origin.call_id
        && operation.state == OperationState::from(outcome)
}
