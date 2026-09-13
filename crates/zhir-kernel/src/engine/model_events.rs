use super::*;

impl Engine {
    pub(super) async fn model_event(&mut self, event: SessionEvent) -> Result<()> {
        if self
            .session_sequence
            .is_some_and(|sequence| event.sequence <= sequence)
        {
            return Err(Error::Protocol(
                "session event sequence did not increase".into(),
            ));
        }
        self.session_sequence = Some(event.sequence);
        if self
            .current
            .active
            .session
            .last_sequence
            .is_some_and(|sequence| event.sequence <= sequence)
        {
            return Ok(());
        }
        if let SessionEventBody::Delta { delta, .. } = event.body {
            self.emitter.emit(EventData::ModelDelta { delta });
            return Ok(());
        }
        let mut next = self.current.as_ref().clone();
        next.active.session.last_sequence = Some(event.sequence);
        let mut entries = vec![];
        match event.body {
            SessionEventBody::Acknowledged {
                command_id,
                recovery,
            } => {
                self.acknowledge_command(&mut next, command_id, recovery)?;
            }
            body @ SessionEventBody::Output { .. } => {
                return self.accept_output(next, event.sequence, body).await;
            }
            body @ SessionEventBody::TurnFinished { .. } => {
                entries.push(self.complete_turn(&mut next, body)?);
            }
            SessionEventBody::Recovery { reference } => {
                next.active.session.recovery = Some(reference)
            }
            SessionEventBody::Closed => {
                self.model_closed = true;
                next.active.session.closed = true;
                if next.active.session.disposition.is_none() {
                    return self.suspend(WaitReason::Recovery).await;
                }
            }
            SessionEventBody::Operation {
                origin,
                event: operation_event,
            } => {
                return self
                    .provider_event(event.sequence, origin, operation_event)
                    .await;
            }
            SessionEventBody::Delta { .. } => unreachable!(),
        }
        self.commit_session(next, event.sequence, entries).await
    }
}

impl Engine {
    fn acknowledge_command(
        &mut self,
        next: &mut Checkpoint,
        command_id: String,
        recovery: Option<RecoveryRef>,
    ) -> Result<()> {
        let index = next
            .active
            .commands
            .iter()
            .position(|c| c.id == command_id)
            .ok_or_else(|| Error::Protocol("acknowledgement has no pending command".into()))?;
        let command = next.active.commands.remove(index);
        if let CommandIntent::ToolResult { operation_id, .. } = &command.intent {
            next.active.operations.remove(operation_id);
        }
        if let CommandIntent::UpdateProfile { revision, profile } = command.intent {
            let mut request = self.model_request(next.history.len());
            request.profile = profile.clone();
            next.active.session.negotiated = self.negotiate(&request)?;
            next.active.session.effective.values = next
                .active
                .session
                .negotiated
                .selected
                .keys()
                .map(|key| (key.clone(), zhir_core::profile::Confirmation::Unknown))
                .collect();
            next.active.session.profile_revision = revision;
            next.active.session.profile = profile;
        }
        if let Some(reference) = recovery {
            next.active.session.recovery = Some(reference);
        }
        self.sent.remove(&command_id);
        Ok(())
    }
    async fn accept_output(
        &mut self,
        mut next: Checkpoint,
        sequence: u64,
        body: SessionEventBody,
    ) -> Result<()> {
        let SessionEventBody::Output {
            turn_id,
            item_id,
            caller_id,
            output,
        } = body
        else {
            unreachable!("output event")
        };

        if next.active.session.turn_id.as_ref() != Some(&turn_id) {
            return Err(Error::Protocol("output belongs to another turn".into()));
        }
        zhir_core::message::validate_output(std::slice::from_ref(&output))?;
        let call_id = match &output {
            Output::RuntimeToolCall { call } => call.id.clone(),
            Output::ProviderToolCall { call } => call.id.clone(),
            _ => item_id.clone(),
        };
        if item_id.is_empty() || caller_id.is_empty() {
            return Err(Error::Protocol(
                "output requires nonempty item and caller identities".into(),
            ));
        }
        let output_turn = turn_id.clone();
        let mut origin = CallRef {
            session_id: next.active.session.id.clone(),
            turn_id,
            caller_id,
            call_id,
        };
        if let Output::ProviderToolCall { call } = &output
            && let Some(existing) = next.active.operations.values().find(|op| {
                !op.state.terminal()
                    && op.origin.call_id == call.id
                    && op.origin.caller_id == origin.caller_id
                    && matches!(&op.owner, OperationOwner::Provider { provider }
                        if provider == &call.provider)
            })
        {
            origin = existing.origin.clone();
        }
        let entry = HistoryEntry {
            id: serde_json::json!([
                next.active.session.id,
                output_turn,
                origin.caller_id,
                item_id
            ])
            .to_string(),
            origin: Some(origin.clone()),
            message: Message::Assistant {
                output: vec![output.clone()],
                provider_data: serde_json::Value::Null,
            },
        };
        if let Some(previous) = next.history.by_id(&entry.id) {
            if previous != &entry {
                return Err(Error::Protocol("conflicting output identity".into()));
            }
            return self
                .commit(
                    next,
                    Fact::Session {
                        session_id: self.current.active.session.id.clone(),
                        sequence,
                    },
                    HistoryDelta::Unchanged,
                )
                .await;
        }
        self.record_output(&mut next, output, origin)?;
        self.commit_session(next, sequence, vec![entry]).await
    }
    fn record_output(&self, next: &mut Checkpoint, output: Output, origin: CallRef) -> Result<()> {
        if let Output::RuntimeToolCall { call } = output {
            if next
                .active
                .operations
                .values()
                .filter(|op| !op.state.terminal())
                .count()
                >= next.options.limits.max_inflight_operations
            {
                return Err(Error::Invalid(
                    "inflight operation capacity exceeded".into(),
                ));
            }
            let id = new_id();
            next.active.operations.insert(
                id.clone(),
                OperationRecord {
                    id,
                    origin,
                    owner: OperationOwner::RuntimeTool { name: call.name },
                    state: OperationState::Queued,
                    call_entry: next.history.len(),
                    result_entry: None,
                    recovery: None,
                    last_sequence: None,
                    last_update: None,
                    wait: None,
                },
            );
        } else if let Output::ProviderToolCall { call } = output {
            let id = provider_operation_id(&origin);
            let state = match call.status {
                ProviderToolStatus::Pending | ProviderToolStatus::Running => {
                    OperationState::Running
                }
                ProviderToolStatus::Failed | ProviderToolStatus::Incomplete => {
                    OperationState::Failed
                }
                ProviderToolStatus::Cancelled => OperationState::Cancelled,
                _ => OperationState::Succeeded,
            };
            let record = next
                .active
                .operations
                .entry(id.clone())
                .or_insert(OperationRecord {
                    id,
                    origin,
                    owner: OperationOwner::Provider {
                        provider: call.provider,
                    },
                    state,
                    call_entry: next.history.len(),
                    result_entry: None,
                    recovery: None,
                    last_sequence: None,
                    last_update: None,
                    wait: None,
                });
            record.state = state;
            record.result_entry = state.terminal().then_some(next.history.len());
        }

        Ok(())
    }
    fn complete_turn(
        &mut self,
        next: &mut Checkpoint,
        body: SessionEventBody,
    ) -> Result<HistoryEntry> {
        let SessionEventBody::TurnFinished {
            turn_id,
            disposition,
            usage,
            model_id,
            response_id,
            finish_reason,
            provider_data,
            effective,
        } = body
        else {
            unreachable!("turn completion")
        };

        if next.active.session.turn_id.as_ref() != Some(&turn_id)
            || next.active.session.disposition.is_some()
        {
            return Err(Error::Protocol(
                "duplicate or foreign turn completion".into(),
            ));
        }
        next.active.operations.retain(|_, op| {
            !matches!(op.owner, OperationOwner::Provider { .. }) || !op.state.terminal()
        });
        next.active.session.disposition = Some(disposition.clone());
        next.active.session.effective.values = next
            .active
            .session
            .negotiated
            .selected
            .keys()
            .map(|key| (key.clone(), zhir_core::profile::Confirmation::Unknown))
            .collect();
        next.active
            .session
            .effective
            .values
            .extend(effective.values);
        next.metrics.usage.add(&usage);
        next.metrics.model_turns += 1;
        let mut data = provider_data;
        if let Some(object) = data.as_object_mut() {
            object.insert("completion".into(), serde_json::json!({"model_id":model_id,"response_id":response_id,"finish_reason":finish_reason}));
        } else {
            data = serde_json::json!({"completion":{"model_id":model_id,"response_id":response_id,"finish_reason":finish_reason},"native":data});
        }
        let entry = HistoryEntry {
            id: format!("{}:{turn_id}:finished", next.active.session.id),
            origin: Some(CallRef {
                session_id: next.active.session.id.clone(),
                turn_id,
                caller_id: "model".into(),
                call_id: "completion".into(),
            }),
            message: Message::Assistant {
                output: vec![],
                provider_data: data,
            },
        };
        self.needs_turn |= disposition != TurnDisposition::Finished;
        if next.options.limits.max_total_tokens.is_some_and(|limit| {
            next.metrics
                .usage
                .total_tokens
                .is_some_and(|tokens| tokens >= limit)
        }) {
            next.state = State::Limited {
                reason: LimitReason::TotalTokens,
            };
        }

        Ok(entry)
    }
    async fn provider_event(
        &mut self,
        sequence: u64,
        origin: CallRef,
        operation_event: OperationEvent,
    ) -> Result<()> {
        let record = self
            .current
            .active
            .operations
            .values()
            .find(|record| record.origin == origin)
            .cloned();
        let Some(record) = record else {
            if let OperationUpdate::Finished { outcome } = operation_event.update {
                self.finish(&provider_operation_id(&origin), outcome)
                    .await?;
                let mut next = self.current.as_ref().clone();
                next.active.session.last_sequence = Some(sequence);
                return self
                    .commit(
                        next,
                        Fact::Session {
                            session_id: self.current.active.session.id.clone(),
                            sequence,
                        },
                        HistoryDelta::Unchanged,
                    )
                    .await;
            }
            return Err(Error::Protocol("provider operation has no call".into()));
        };
        if !matches!(record.owner, OperationOwner::Provider { .. }) {
            return Err(Error::Protocol(
                "model cannot own local tool operations".into(),
            ));
        }
        if !self.current.active.operations.contains_key(&record.id) {
            return Err(Error::Protocol(
                "provider operation must be introduced by an output item".into(),
            ));
        }
        self.operation_event(&record.id, operation_event).await?;
        let mut next = self.current.as_ref().clone();
        next.active.session.last_sequence = Some(sequence);
        return self
            .commit(
                next,
                Fact::Session {
                    session_id: self.current.active.session.id.clone(),
                    sequence,
                },
                HistoryDelta::Unchanged,
            )
            .await;
    }
    async fn commit_session(
        &mut self,
        mut next: Checkpoint,
        sequence: u64,
        entries: Vec<HistoryEntry>,
    ) -> Result<()> {
        let history = if entries.is_empty() {
            HistoryDelta::Unchanged
        } else {
            next.history = next.history.append(entries.clone())?;
            HistoryDelta::Append(entries)
        };
        self.commit(
            next,
            Fact::Session {
                session_id: self.current.active.session.id.clone(),
                sequence,
            },
            history,
        )
        .await
    }
}
