use super::*;

impl Engine {
    pub(super) async fn operation_event(&mut self, id: &str, event: OperationEvent) -> Result<()> {
        if let OperationUpdate::Context { content } = &event.update {
            let record = self
                .current
                .active
                .operations
                .get(id)
                .ok_or_else(|| Error::Protocol("unknown context operation".into()))?;
            if record.owner != OperationOwner::Delegation || record.state.terminal() {
                return Err(Error::Protocol(
                    "context requires an active delegation".into(),
                ));
            }
            if let Some(previous) = record.last_sequence {
                if event.sequence < previous {
                    return Err(Error::Protocol("delegation sequence regressed".into()));
                }
                if event.sequence == previous {
                    return if record.last_update.as_ref() == Some(&event.update) {
                        Ok(())
                    } else {
                        Err(Error::Protocol("conflicting delegation context".into()))
                    };
                }
            }
            for part in content {
                part.validate()?;
            }
            let mut next = self.current.as_ref().clone();
            let op = next
                .active
                .operations
                .get_mut(id)
                .expect("validated operation");
            op.last_sequence = Some(event.sequence);
            op.last_update = Some(event.update.clone());
            next.active.commands.push(PendingCommand {
                id: new_id(),
                sent: false,
                intent: CommandIntent::DelegationContext {
                    operation_id: id.into(),
                    content: content.clone(),
                },
            });
            return self
                .commit(
                    next,
                    Fact::Operation {
                        operation_id: id.into(),
                        state: record.state,
                    },
                    HistoryDelta::Unchanged,
                )
                .await;
        }
        if let Some(record) = self.current.active.operations.get(id)
            && let Some(sequence) = record.last_sequence
        {
            if event.sequence == sequence {
                return if record.last_update.as_ref() == Some(&event.update) {
                    Ok(())
                } else {
                    Err(Error::Protocol(
                        "conflicting operation event sequence".into(),
                    ))
                };
            }
            if event.sequence < sequence {
                return Err(Error::Protocol(
                    "operation event sequence moved backwards".into(),
                ));
            }
        }
        if let OperationUpdate::Finished { outcome } = &event.update {
            return self
                .finish_at(id, outcome.clone(), Some(event.sequence))
                .await;
        }
        let record = self
            .current
            .active
            .operations
            .get(id)
            .ok_or_else(|| Error::Protocol("unknown operation event".into()))?;
        if let OperationUpdate::Progress { value } = event.update {
            self.emitter.emit(EventData::OperationProgress {
                operation_id: id.into(),
                value,
            });
            return Ok(());
        }
        if let OperationUpdate::Finished { outcome } = event.update {
            return self.finish(id, outcome).await;
        }
        if record.state.terminal() {
            return Err(Error::Protocol("update follows terminal operation".into()));
        }
        let mut next = self.current.as_ref().clone();
        let record = next.active.operations.get_mut(id).expect("record");
        record.last_sequence = Some(event.sequence);
        record.last_update = Some(event.update.clone());
        match event.update {
            OperationUpdate::Running { recovery } => {
                record.state = OperationState::Running;
                record.recovery = recovery.or(record.recovery.take());
                record.wait = None;
            }
            OperationUpdate::Waiting { prompt, recovery } => {
                record.state = OperationState::Waiting;
                record.wait = Some(prompt);
                record.recovery = recovery.or(record.recovery.take());
            }
            OperationUpdate::Unknown { reason } => {
                record.state = OperationState::Unknown;
                record.wait = Some(serde_json::json!({"reason":reason}));
            }
            _ => unreachable!(),
        }
        let state = record.state;
        self.commit(
            next,
            Fact::Operation {
                operation_id: id.into(),
                state,
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        self.emitter.emit(EventData::OperationChanged {
            operation_id: id.into(),
            state,
        });
        Ok(())
    }
    pub(super) async fn unknown(&mut self, id: &str, reason: String) -> Result<()> {
        self.local_operation_update(
            id,
            OperationState::Unknown,
            Some(serde_json::json!({"reason":reason})),
        )
        .await
    }
    pub(super) async fn local_operation_update(
        &mut self,
        id: &str,
        state: OperationState,
        wait: Option<serde_json::Value>,
    ) -> Result<()> {
        let mut next = self.current.as_ref().clone();
        let op = next
            .active
            .operations
            .get_mut(id)
            .ok_or_else(|| Error::Protocol("unknown operation".into()))?;
        if op.state.terminal() {
            return Err(Error::Protocol("update follows terminal operation".into()));
        }
        op.state = state;
        op.wait = wait;
        self.commit(
            next,
            Fact::Operation {
                operation_id: id.into(),
                state,
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        self.emitter.emit(EventData::OperationChanged {
            operation_id: id.into(),
            state,
        });
        Ok(())
    }
}

impl Engine {
    pub(super) async fn install_operation(
        &mut self,
        id: String,
        handle: OperationHandle,
    ) -> Result<()> {
        let mut next = self.current.as_ref().clone();
        let op = next
            .active
            .operations
            .get_mut(&id)
            .ok_or_else(|| Error::Protocol("unknown started operation".into()))?;
        op.recovery = handle.recovery.or(op.recovery.take());
        let state = op.state;
        self.commit(
            next,
            Fact::Operation {
                operation_id: id.clone(),
                state,
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        if state == OperationState::Cancelling {
            let control = handle.control.clone();
            self.tasks.spawn(async move {
                let _ = control.cancel().await;
            });
        }
        self.operation_controls.insert(id.clone(), handle.control);
        let tx = self.work_tx.clone();
        let mut events = handle.events;
        self.tasks.spawn(async move {
            loop {
                let event = events.receive().await;
                let stop = !matches!(event, Ok(Some(_)));
                if tx.send(Work::Operation(id.clone(), event)).await.is_err() || stop {
                    break;
                }
            }
        });
        Ok(())
    }
}
