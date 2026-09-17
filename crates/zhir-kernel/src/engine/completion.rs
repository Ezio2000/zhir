use super::*;
use zhir_core::operation::OperationOutcome;

impl Engine {
    pub(super) async fn finish(&mut self, id: &str, outcome: OperationOutcome) -> Result<()> {
        self.finish_at(id, outcome, None).await
    }
    pub(super) async fn finish_at(
        &mut self,
        id: &str,
        outcome: OperationOutcome,
        sequence: Option<u64>,
    ) -> Result<()> {
        outcome.validate()?;
        if !self.current.active.operations.contains_key(id) {
            return if self
                .current
                .history
                .by_id(&format!("operation:{id}:result"))
                .is_some_and(|entry| same_outcome(&entry.message, &outcome))
            {
                Ok(())
            } else {
                Err(Error::Protocol(
                    "unknown or conflicting operation completion".into(),
                ))
            };
        }
        let record = self
            .current
            .active
            .operations
            .get(id)
            .ok_or_else(|| Error::Protocol("result for unknown operation".into()))?
            .clone();
        if record.state.terminal() {
            if self
                .current
                .history
                .get(record.result_entry.expect("terminal result"))
                .is_some_and(|entry| same_outcome(&entry.message, &outcome))
            {
                return Ok(());
            }
            return Err(Error::Protocol("conflicting operation completion".into()));
        }
        let name = match &record.owner {
            OperationOwner::RuntimeTool { name } => Some(name.clone()),
            OperationOwner::Delegation => None,
            OperationOwner::Provider { .. } => {
                return self.finish_provider(record, outcome, sequence).await;
            }
        };
        let update = sequence.map(|_| OperationUpdate::Finished {
            outcome: outcome.clone(),
        });
        let state = OperationState::from(&outcome);
        let entry = HistoryEntry {
            id: format!("operation:{id}:result"),
            origin: Some(record.origin.clone()),
            message: match name {
                Some(name) => Message::RuntimeTool {
                    call_id: record.origin.call_id,
                    name,
                    outcome,
                },
                None => Message::DelegationResult {
                    id: record.origin.call_id,
                    outcome,
                },
            },
        };
        let mut next = self.current.as_ref().clone();
        let index = next.history.len();
        next.history = next.history.append(vec![entry.clone()])?;
        let operation = next.active.operations.get_mut(id).expect("operation");
        operation.state = state;
        operation.last_sequence = sequence.or(operation.last_sequence);
        operation.last_update = update.or(operation.last_update.take());
        operation.result_entry = Some(index);
        operation.wait = None;
        Self::append_command(&mut next, index, AppendSource::Submitted, Some(id.into()));
        self.commit(
            next,
            Fact::Operation {
                operation_id: id.into(),
                state,
            },
            HistoryDelta::Append(vec![entry]),
        )
        .await?;
        self.tokens.remove(id);
        self.operation_controls.remove(id);
        self.emitter.emit(EventData::OperationChanged {
            operation_id: id.into(),
            state,
        });
        Ok(())
    }
    pub(super) async fn finish_provider(
        &mut self,
        record: OperationRecord,
        outcome: OperationOutcome,
        sequence: Option<u64>,
    ) -> Result<()> {
        let entry = self
            .current
            .history
            .get(record.call_entry)
            .expect("validated call entry");
        let Message::Assistant { output, .. } = &entry.message else {
            return Err(Error::Protocol("provider call entry mismatch".into()));
        };
        let mut call = output
            .iter()
            .find_map(|item| {
                if let Output::ProviderToolCall { call } = item {
                    Some(call.clone())
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::Protocol("provider call missing".into()))?;
        let state = OperationState::from(&outcome);
        let update = sequence.map(|_| OperationUpdate::Finished {
            outcome: outcome.clone(),
        });
        call.status = match state {
            OperationState::Succeeded => ProviderToolStatus::Completed,
            OperationState::Cancelled => ProviderToolStatus::Cancelled,
            _ => ProviderToolStatus::Failed,
        };
        call.output = outcome.content().to_vec();
        call.outcome = Some((&outcome).into());
        let entry = HistoryEntry {
            id: format!("operation:{}:result", record.id),
            origin: Some(record.origin),
            message: Message::Assistant {
                output: vec![Output::ProviderToolCall { call }],
                provider_data: serde_json::Value::Null,
            },
        };
        let mut next = self.current.as_ref().clone();
        let operation = next
            .active
            .operations
            .get_mut(&record.id)
            .expect("registered provider operation");
        operation.state = state;
        operation.last_sequence = sequence.or(operation.last_sequence);
        operation.last_update = update.or(operation.last_update.take());
        operation.result_entry = Some(next.history.len());
        operation.wait = None;
        let index = next.history.len();
        next.history = next.history.append(vec![entry.clone()])?;
        Self::append_command(&mut next, index, AppendSource::Accepted, None);
        self.commit(
            next,
            Fact::Operation {
                operation_id: record.id,
                state,
            },
            HistoryDelta::Append(vec![entry]),
        )
        .await
    }
}

pub(super) fn provider_operation_id(origin: &CallRef) -> String {
    format!(
        "provider:{}",
        serde_json::json!([
            origin.session_id,
            origin.generation_id,
            origin.caller_id,
            origin.call_id
        ])
    )
}

pub(super) fn same_outcome(message: &Message, outcome: &OperationOutcome) -> bool {
    match message {
        Message::RuntimeTool {
            outcome: previous, ..
        }
        | Message::DelegationResult {
            outcome: previous, ..
        } => previous == outcome,
        Message::Assistant { output, .. } => output.iter().any(|item| match item {
            Output::ProviderToolCall { call } => call.matches_outcome(outcome),
            _ => false,
        }),
        _ => false,
    }
}
