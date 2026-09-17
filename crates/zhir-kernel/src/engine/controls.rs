use super::*;
use zhir_core::operation::OperationOutcome;

impl Engine {
    pub(super) async fn resolve(&mut self, resolution: RecoveryResolution) -> Result<()> {
        match resolution {
            RecoveryResolution::Complete {
                operation_id,
                outcome,
            } => self.finish(&operation_id, outcome).await,
            RecoveryResolution::Abandon {
                operation_id,
                reason,
            } => {
                self.finish(&operation_id, OperationOutcome::Cancelled { reason })
                    .await
            }
            RecoveryResolution::Attach {
                operation_id,
                reference,
            } => {
                let mut next = self.current.as_ref().clone();
                let op = next
                    .active
                    .operations
                    .get_mut(&operation_id)
                    .ok_or_else(|| Error::Invalid("unknown operation resolution".into()))?;
                if op.state.terminal() {
                    return Err(Error::Invalid("cannot attach terminal work".into()));
                }
                op.recovery = Some(reference);
                op.state = OperationState::Running;
                self.commit(
                    next,
                    Fact::Operation {
                        operation_id,
                        state: OperationState::Running,
                    },
                    HistoryDelta::Unchanged,
                )
                .await
            }
        }
    }
    pub(super) async fn insert(&mut self, message: Message, source: String) -> Result<String> {
        if (self.current.active.session.input_closed && matches!(message, Message::User { .. }))
            || self.current.active.session.closing
        {
            return Err(Error::Invalid("user input is sealed".into()));
        }
        message.validate()?;
        let id = new_id();
        let entry = HistoryEntry {
            id: id.clone(),
            origin: None,
            message,
        };
        let mut next = self.current.as_ref().clone();
        let index = next.history.len();
        next.history = next.history.append(vec![entry.clone()])?;
        next.active.session.input_position += 1;
        next.active.session.needs_generation = true;
        Self::append_command(&mut next, index, AppendSource::Submitted, None);
        self.commit(
            next,
            Fact::ConversationInsert { source },
            HistoryDelta::Append(vec![entry]),
        )
        .await?;
        Ok(id)
    }
    pub(super) async fn control(&mut self, control: Control) -> Result<()> {
        let result: Result<String> = async {
            match control.body {
                ControlBody::Input(message, source) => self.insert(message, source).await,
                ControlBody::Pause(suspension) => {
                    let mut next = self.current.as_ref().clone();
                    next.state = State::Suspended { suspension };
                    self.commit(
                        next,
                        Fact::Control {
                            action: ControlAction::Suspended,
                        },
                        HistoryDelta::Unchanged,
                    )
                    .await?;
                    Ok(new_id())
                }
                ControlBody::UpdateProfile(profile) => self.update_profile(profile).await,
                ControlBody::InterruptOutput => {
                    if !self
                        .session_capabilities()
                        .supports(Capability::InterruptOutput)
                    {
                        return Err(Error::Invalid(
                            "model does not support native interruption".into(),
                        ));
                    }
                    let generation_id = self
                        .current
                        .active
                        .session
                        .generation_id
                        .clone()
                        .ok_or_else(|| Error::Invalid("no generation to interrupt".into()))?;
                    let output_epoch = self
                        .current
                        .active
                        .session
                        .output_epoch
                        .checked_add(1)
                        .ok_or_else(|| Error::Invalid("output epoch overflow".into()))?;
                    self.prepare_command(CommandIntent::InterruptOutput {
                        generation_id,
                        output_epoch,
                    })
                    .await
                }
                ControlBody::FlushInput => {
                    if !self.session_capabilities().supports(Capability::FlushInput) {
                        return Err(Error::Invalid(
                            "session does not support input flushing".into(),
                        ));
                    }
                    if self.current.active.session.input_closed {
                        return Err(Error::Invalid("input is closed".into()));
                    }
                    self.prepare_command(CommandIntent::FlushInput).await
                }
                ControlBody::SetInputAudio(enabled) => {
                    if !self
                        .session_capabilities()
                        .supports(Capability::InputAudioControl)
                    {
                        return Err(Error::Invalid(
                            "session does not support audio input control".into(),
                        ));
                    }
                    if self.current.active.session.input_closed {
                        return Err(Error::Invalid("input is closed".into()));
                    }
                    self.prepare_command(CommandIntent::SetInputAudio { enabled })
                        .await
                }
                ControlBody::SealUserInput => {
                    self.prepare_command(CommandIntent::SealUserInput).await
                }
                ControlBody::CancelOperation(id) => self.cancel_operation(id).await,
                ControlBody::ReplyOperation(id, value) => self.reply_operation(id, value).await,
            }
        }
        .await;
        let fatal = result
            .as_ref()
            .err()
            .filter(|e| matches!(e, Error::Storage(_) | Error::Conflict { .. }))
            .cloned();
        let _ = control.reply.send(result.map(|command_id| ControlReceipt {
            command_id,
            revision: self.current.revision,
        }));
        if let Some(error) = fatal {
            Err(error)
        } else {
            Ok(())
        }
    }
}

impl Engine {
    pub(super) async fn apply_recovery(&mut self, request: Request) -> Result<()> {
        if let Request::Recover {
            resolutions,
            messages,
            ..
        } = request
        {
            for resolution in resolutions {
                self.resolve(resolution).await?;
            }
            for message in messages {
                self.insert(message, "resume".into()).await?;
            }
            if self
                .current
                .active
                .operations
                .values()
                .any(|o| o.state == OperationState::Unknown)
            {
                return self.suspend(WaitReason::Recovery).await;
            }
            let in_generation = self.current.active.session.generation_id.is_some()
                && self.current.active.session.response_status.is_none();
            if (in_generation || self.current.active.commands.iter().any(|c| c.sent))
                && self.current.active.session.recovery.is_none()
            {
                return self.suspend(WaitReason::Recovery).await;
            }
            if !self.current.active.media.is_empty()
                && self.current.active.session.recovery.is_none()
            {
                return self.suspend(WaitReason::Recovery).await;
            }
            if self.current.active.session.establishment == SessionEstablishment::Opening
                && self.current.active.session.recovery.is_none()
            {
                return self.suspend(WaitReason::Recovery).await;
            }
            if self.current.active.session.establishment == SessionEstablishment::Established
                && self.current.active.session.recovery.is_none()
                && !self
                    .session_capabilities()
                    .supports(Capability::LocalProjection)
            {
                return self.suspend(WaitReason::Recovery).await;
            }
        }
        Ok(())
    }
}

impl Engine {
    async fn update_profile(
        &mut self,
        profile: zhir_core::profile::RequestProfile,
    ) -> Result<String> {
        if !self
            .session_capabilities()
            .supports(Capability::ProfileUpdates)
        {
            return Err(Error::Invalid(
                "model does not support running profile updates".into(),
            ));
        }
        let mut request = self.model_request(self.current.history.len());
        request.profile = profile.clone();
        self.negotiate(&request)?;
        self.prepare_command(CommandIntent::UpdateProfile {
            revision: self
                .current
                .active
                .commands
                .iter()
                .filter_map(|c| {
                    if let CommandIntent::UpdateProfile { revision, .. } = c.intent {
                        Some(revision)
                    } else {
                        None
                    }
                })
                .max()
                .unwrap_or(self.current.active.session.profile_revision)
                + 1,
            profile,
        })
        .await
    }
    async fn cancel_operation(&mut self, id: String) -> Result<String> {
        let record = self
            .current
            .active
            .operations
            .get(&id)
            .ok_or_else(|| Error::Invalid("unknown operation".into()))?;
        if record.state.terminal() {
            return Ok(id);
        }
        if record.state == OperationState::Queued {
            self.finish(
                &id,
                OperationOutcome::Cancelled {
                    reason: "cancelled before dispatch".into(),
                },
            )
            .await?;
            return Ok(id);
        }
        let mut next = self.current.as_ref().clone();
        next.active
            .operations
            .get_mut(&id)
            .expect("operation")
            .state = OperationState::Cancelling;
        self.commit(
            next,
            Fact::Operation {
                operation_id: id.clone(),
                state: OperationState::Cancelling,
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        if let Some(token) = self.tokens.get(&id) {
            token.cancel();
        }
        if let Some(handle) = self.operation_controls.get(&id).cloned() {
            let tx = self.work_tx.clone();
            let opid = id.clone();
            self.tasks.spawn(async move {
                if let Err(error) = handle.cancel().await {
                    let _ = tx.send(Work::Started(opid, Err(error))).await;
                }
            });
        }
        Ok(id)
    }
    async fn reply_operation(&mut self, id: String, value: serde_json::Value) -> Result<String> {
        let handle = self
            .operation_controls
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::Invalid("operation has no input channel".into()))?;
        // Reply is an external effect with a durable uncertain boundary.
        self.unknown(&id, "reply awaiting external confirmation".into())
            .await?;
        let tx = self.work_tx.clone();
        let opid = id.clone();
        self.pending_replies.insert(id.clone());
        self.tasks.spawn(async move {
            let result = handle.reply(value).await;
            let _ = tx.send(Work::ReplyDone(opid, result)).await;
        });
        Ok(id)
    }
}
