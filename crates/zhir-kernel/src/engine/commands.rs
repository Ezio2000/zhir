use super::*;

impl Engine {
    pub(super) fn append_command(
        next: &mut Checkpoint,
        entry: usize,
        source: AppendSource,
        operation_id: Option<String>,
    ) {
        next.active.session.context_revision += 1;
        next.active.commands.push(PendingCommand {
            id: new_id(),
            intent: CommandIntent::Append {
                entry,
                context_revision: next.active.session.context_revision,
                input_position: next.active.session.input_position,
                source,
                operation_id,
            },
            sent: false,
        });
    }
    pub(super) async fn prepare_command(&mut self, intent: CommandIntent) -> Result<String> {
        if self.current.active.commands.len() >= self.current.options.limits.max_control_commands {
            return Err(Error::Invalid("pending command capacity exceeded".into()));
        }
        let seal_user_input = matches!(intent, CommandIntent::SealUserInput);
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
            CommandIntent::SealUserInput => next.active.session.input_closed = true,
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
        if seal_user_input {
            self.media.close();
        }
        Ok(id)
    }
    pub(super) async fn dispatch_commands(&mut self) -> Result<()> {
        let Some(input) = self.session.clone() else {
            return Ok(());
        };
        if self.sending || !self.current.active.session.ready {
            return Ok(());
        }
        let Some(command) = self
            .current
            .active
            .commands
            .iter()
            .find(|command| !self.sent.contains(&command.id))
            .filter(|command| self.can_send(command))
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
        let busy_generation = self.current.active.session.response_status.is_none()
            && self.current.active.session.generation_id.is_some();
        match &command.intent {
            CommandIntent::SealUserInput | CommandIntent::Close | CommandIntent::FlushInput => {
                !self.input_sending && self.media.is_empty()
            }
            CommandIntent::Append {
                operation_id: Some(_),
                ..
            } if busy_generation
                && !self
                    .session_capabilities()
                    .supports(Capability::ReplaceContext) =>
            {
                self.session_capabilities()
                    .supports(Capability::AsyncResults)
            }
            CommandIntent::Append {
                source: AppendSource::Submitted,
                ..
            } if busy_generation
                && !self
                    .session_capabilities()
                    .supports(Capability::ReplaceContext) =>
            {
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
            CommandIntent::Generate {
                generation_id,
                context_revision,
                input_position,
                profile_revision,
            } => SessionCommandBody::Generate {
                generation_id,
                context_revision,
                input_position,
                profile_revision,
            },
            CommandIntent::Append {
                entry,
                context_revision,
                input_position,
                source,
                ..
            } => SessionCommandBody::Append {
                entry: self
                    .current
                    .history
                    .get(entry)
                    .ok_or_else(|| Error::Protocol("missing input entry".into()))?
                    .clone(),
                context_revision,
                input_position,
                source,
            },
            CommandIntent::ReplaceContext { context_revision } => {
                SessionCommandBody::ReplaceContext {
                    entries: self.current.history.entries(),
                    context_revision,
                }
            }
            CommandIntent::UpdateProfile { revision, profile } => {
                SessionCommandBody::UpdateProfile { revision, profile }
            }
            CommandIntent::InterruptOutput {
                generation_id,
                output_epoch,
            } => SessionCommandBody::InterruptOutput {
                generation_id,
                output_epoch,
            },
            CommandIntent::FlushInput => SessionCommandBody::FlushInput,
            CommandIntent::SetInputAudio { enabled } => {
                SessionCommandBody::SetInputAudio { enabled }
            }
            CommandIntent::SealUserInput => SessionCommandBody::SealUserInput,
            CommandIntent::Close => SessionCommandBody::Close,
        })
    }
}
