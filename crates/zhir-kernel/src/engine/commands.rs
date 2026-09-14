use super::*;

impl Engine {
    pub(super) async fn prepare_command(&mut self, intent: CommandIntent) -> Result<String> {
        if self.current.active.commands.len() >= self.current.options.limits.max_control_commands {
            return Err(Error::Invalid("pending command capacity exceeded".into()));
        }
        let end_input = matches!(intent, CommandIntent::EndInput);
        let interrupt = matches!(intent, CommandIntent::InterruptOutput { .. });
        let id = new_id();
        let mut next = self.current.as_ref().clone();
        match &intent {
            CommandIntent::Close => next.active.session.closing = true,
            CommandIntent::InterruptOutput { output_epoch, .. } => {
                next.active.session.output_epoch = *output_epoch;
                let retired: Vec<_> = next
                    .active
                    .media
                    .iter()
                    .filter(|(key, _)| key.starts_with("output:"))
                    .map(|(key, cursor)| (key.clone(), cursor.sealed.clone()))
                    .collect();
                for (key, sealed) in retired {
                    self.archive_media(&mut next, key.clone(), sealed, false)
                        .await?;
                    next.active.media.remove(&key);
                }
            }
            CommandIntent::EndInput => next.active.session.input_closed = true,
            CommandIntent::SetInputAudio { enabled } => {
                next.active.session.input_audio_enabled = *enabled
            }
            _ => (),
        }
        next.active.commands.push(PendingCommand {
            id: id.clone(),
            intent,
            sent: false,
        });
        self.commit(
            next,
            Fact::Command {
                command_id: id.clone(),
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        if interrupt {
            self.media_output
                .invalidate_before(self.current.active.session.output_epoch);
        }
        if end_input {
            self.media.close();
        }
        Ok(id)
    }
    pub(super) async fn dispatch_commands(&mut self) -> Result<()> {
        let Some(input) = self.session.clone() else {
            return Ok(());
        };
        if self.sending {
            return Ok(());
        }
        let Some(command) = self
            .current
            .active
            .commands
            .iter()
            .find(|command| self.can_send(command))
            .cloned()
        else {
            return Ok(());
        };
        let body = self.command_body(command.intent)?;
        // A crash after this commit is an explicitly uncertain send, never an implicit retry.
        let mut next = self.current.as_ref().clone();
        next.active
            .commands
            .iter_mut()
            .find(|c| c.id == command.id)
            .expect("pending command")
            .sent = true;
        self.commit(
            next,
            Fact::Command {
                command_id: command.id.clone(),
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        self.check()?;
        self.sent.insert(command.id.clone());
        self.sending = true;
        let tx = self.work_tx.clone();
        let input = input.clone();
        self.tasks.spawn(async move {
            let result = input
                .send(SessionCommand {
                    id: command.id,
                    body,
                })
                .await;
            let _ = tx.send(Work::CommandSent(result)).await;
        });
        Ok(())
    }
}

impl Engine {
    fn can_send(&self, command: &PendingCommand) -> bool {
        if self.sent.contains(&command.id) {
            return false;
        }
        let busy_turn = self.current.active.session.disposition.is_none()
            && self.current.active.session.turn_id.is_some();
        match &command.intent {
            CommandIntent::EndInput | CommandIntent::Close | CommandIntent::FlushInput => {
                !self.input_sending && self.media.is_empty()
            }
            CommandIntent::ToolResult { .. } if busy_turn => self
                .session_capabilities()
                .supports(Capability::AsyncResults),
            CommandIntent::Input { .. } if busy_turn => {
                self.session_capabilities().supports(Capability::Steering)
            }
            _ => true,
        }
    }
    fn command_body(&self, intent: CommandIntent) -> Result<SessionCommandBody> {
        Ok(match intent {
            CommandIntent::DelegationContext {
                operation_id,
                content,
            } => {
                let origin = self
                    .current
                    .active
                    .operations
                    .get(&operation_id)
                    .ok_or_else(|| Error::Protocol("delegation context origin missing".into()))?
                    .origin
                    .clone();
                SessionCommandBody::DelegationContext {
                    operation_id,
                    origin,
                    content,
                }
            }
            CommandIntent::DelegationResult {
                operation_id,
                entry,
            } => {
                let Message::DelegationResult { outcome, .. } = &self
                    .current
                    .history
                    .get(entry)
                    .ok_or_else(|| Error::Protocol("delegation result missing".into()))?
                    .message
                else {
                    return Err(Error::Protocol("delegation result entry mismatch".into()));
                };
                let origin = self
                    .current
                    .active
                    .operations
                    .get(&operation_id)
                    .ok_or_else(|| Error::Protocol("delegation origin missing".into()))?
                    .origin
                    .clone();
                SessionCommandBody::DelegationResult {
                    operation_id,
                    origin,
                    outcome: outcome.clone(),
                }
            }
            CommandIntent::StartTurn {
                turn_id,
                history_count,
                profile,
                runtime_tools,
            } => {
                let mut request = self.model_request(history_count);
                request.profile = profile;
                request.runtime_tools = runtime_tools;
                SessionCommandBody::StartTurn {
                    turn_id,
                    request: Box::new(request),
                }
            }
            CommandIntent::Input { entry } => SessionCommandBody::Input {
                message: self
                    .current
                    .history
                    .get(entry)
                    .ok_or_else(|| Error::Protocol("missing input entry".into()))?
                    .message
                    .clone(),
            },
            CommandIntent::ToolResult {
                operation_id,
                entry,
            } => {
                let Message::RuntimeTool { outcome, .. } = &self
                    .current
                    .history
                    .get(entry)
                    .ok_or_else(|| Error::Protocol("missing result entry".into()))?
                    .message
                else {
                    return Err(Error::Protocol("command result entry mismatch".into()));
                };
                SessionCommandBody::ToolResult {
                    origin: self
                        .current
                        .active
                        .operations
                        .get(&operation_id)
                        .ok_or_else(|| Error::Protocol("result origin missing".into()))?
                        .origin
                        .clone(),
                    operation_id,
                    outcome: outcome.clone(),
                }
            }
            CommandIntent::UpdateProfile { revision, profile } => {
                SessionCommandBody::UpdateProfile { revision, profile }
            }
            CommandIntent::InterruptOutput {
                turn_id,
                output_epoch,
            } => SessionCommandBody::InterruptOutput {
                turn_id,
                output_epoch,
            },
            CommandIntent::FlushInput => SessionCommandBody::FlushInput,
            CommandIntent::SetInputAudio { enabled } => {
                SessionCommandBody::SetInputAudio { enabled }
            }
            CommandIntent::EndInput => SessionCommandBody::EndInput,
            CommandIntent::Close => SessionCommandBody::Close,
        })
    }
}
