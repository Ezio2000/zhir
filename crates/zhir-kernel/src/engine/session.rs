use super::*;

impl Engine {
    /// The session is a local projection rebuilt from committed history.
    pub(super) fn local_projection(&self) -> bool {
        self.session_capabilities()
            .supports(Capability::LocalProjection)
    }
    pub(super) fn session_capabilities(&self) -> &CapabilitySet {
        self.session_control.as_ref().map_or_else(
            || {
                self.current
                    .active
                    .session
                    .capabilities
                    .as_ref()
                    .unwrap_or_else(|| self.config.model.capabilities())
            },
            |control| control.capabilities(),
        )
    }
    pub(super) fn negotiate(
        &self,
        request: &ModelRequest,
    ) -> Result<zhir_core::profile::NegotiatedProfile> {
        self.session_control.as_ref().map_or_else(
            || self.config.model.negotiate(request),
            |control| control.negotiate(request),
        )
    }
    /// A request over `entries`, which may include history staged but not yet committed.
    pub(super) fn model_request<'a>(
        &self,
        entries: impl IntoIterator<Item = &'a zhir_core::run::HistoryEntry>,
    ) -> ModelRequest {
        let messages = zhir_core::model::conversation(entries);
        ModelRequest {
            messages,
            runtime_tools: self.catalog.specs(),
            provider_tools: self.current.options.provider_tools.clone(),
            profile: self.current.active.session.profile.clone(),
            tool_choice: self.current.options.tool_choice.clone(),
            response_format: self.current.options.response_format.clone(),
            stream: self.current.options.stream,
        }
    }
    pub(super) async fn generate(&mut self) -> Result<()> {
        if self.current.metrics.generation_requests
            >= self.current.options.limits.max_generation_requests
        {
            let mut next = self.current.as_ref().clone();
            next.state = State::Limited {
                reason: LimitReason::GenerationRequests,
            };
            return self
                .commit(
                    next,
                    Fact::Control {
                        action: ControlAction::Limited,
                    },
                    HistoryDelta::Unchanged,
                )
                .await;
        }
        if self.current.active.operations.is_empty()
            && self.current.active.commands.is_empty()
            && self.current.active.media.is_empty()
            && self.current.active.session.reduced_context_revision
                != Some(self.current.active.session.context_revision)
            && let Some(reducer) = self.config.history_reducer.clone()
            && let Some(rewrite) = interruptible(
                reducer.reduce(self.current.clone()),
                self.cancellation.clone(),
                self.deadline,
            )
            .await?
        {
            let mut next = self.current.as_ref().clone();
            let produced: std::collections::BTreeSet<_> = self
                .current
                .history
                .iter()
                .skip(self.current.active.session.run_start)
                .map(|entry| entry.id.as_str())
                .collect();
            next.active.session.run_start = rewrite
                .entries
                .iter()
                .take_while(|entry| !produced.contains(entry.id.as_str()))
                .count();
            next.history = History::from_entries(rewrite.entries.clone())?;
            next.active.session.response_start = next.history.len();
            next.active.session.context_revision += 1;
            next.active.session.reduced_context_revision =
                Some(next.active.session.context_revision);
            next.active.commands.push(PendingCommand {
                id: new_id(),
                intent: CommandIntent::ReplaceContext {
                    context_revision: next.active.session.context_revision,
                },
                sent: false,
            });
            self.commit(
                next,
                Fact::HistoryRewrite {
                    reason: rewrite.reason,
                },
                HistoryDelta::Replace(rewrite.entries),
            )
            .await?;
            return Ok(());
        }
        let generation_id = new_id();
        let id = new_id();
        let mut next = self.current.as_ref().clone();
        next.active.session.generation_id = Some(generation_id.clone());
        next.active.session.response_start = next.history.len();
        next.active.session.response_status = None;
        next.active.session.generation_started = false;
        next.active.session.generated_input_position = next.active.session.input_position;
        next.active.session.needs_generation = false;
        next.metrics.generation_requests += 1;
        next.active.commands.push(PendingCommand {
            id: id.clone(),
            intent: CommandIntent::Generate {
                generation_id,
                context_revision: next.active.session.context_revision,
                input_position: next.active.session.input_position,
                profile_revision: next.active.session.profile_revision,
            },
            // Before its send, a local generation has no effect a crash could leave behind:
            // the committed need to generate reproduces it. Its send boundary is therefore
            // recorded with the command, and dispatch follows without another commit.
            sent: self.local_projection(),
        });
        self.commit(
            next,
            Fact::Command { command_id: id },
            HistoryDelta::Unchanged,
        )
        .await?;
        Ok(())
    }
}

impl Engine {
    pub(super) async fn open_catalog(&mut self) -> Result<()> {
        let provider = self.config.runtime_tools.clone();
        let opening = provider.open_catalog(CatalogContext {
            run: self.current.context.clone(),
            cancellation: self.cancellation.clone(),
        });
        tokio::pin!(opening);
        loop {
            tokio::select! {
                result = &mut opening => {
                    self.catalog = crate::catalog::select_catalog(&self.current.options.runtime_tools, result?)?;
                    break;
                },
                Some(control) = self.controls.recv(), if !self.controls.is_closed() || !self.controls.is_empty() => {
                    self.control(control).await?;
                    if !self.current.state.active() {
                        return Ok(());
                    }
                },
                _ = interruption(self.cancellation.clone(), self.deadline) => self.check()?,
            }
        }
        Ok(())
    }
    pub(super) async fn open_model(&mut self) -> Result<()> {
        if self.current.options.mode == RunMode::Task
            && !self
                .session_capabilities()
                .supports(Capability::ResponseEvents)
        {
            return Err(Error::Invalid(
                "Task mode requires verifiable response boundaries".into(),
            ));
        }
        if self.config.history_reducer.is_some()
            && !self
                .session_capabilities()
                .supports(Capability::ReplaceContext)
        {
            return Err(Error::Invalid(
                "model does not support context replacement required by the history reducer".into(),
            ));
        }
        // Opening a local projection has no external effect, so it needs no boundary of
        // its own; the commit after opening records the session.
        if !self.local_projection() {
            let mut next = self.current.as_ref().clone();
            next.active.session.establishment = SessionEstablishment::Opening;
            next.active.session.ready = false;
            self.commit(
                next,
                Fact::Command {
                    command_id: new_id(),
                },
                HistoryDelta::Unchanged,
            )
            .await?;
        }
        let seed_end = self
            .current
            .active
            .commands
            .iter()
            .filter_map(|command| match command.intent {
                CommandIntent::Append { entry, .. } => Some(entry),
                _ => None,
            })
            .min()
            .unwrap_or(self.current.history.len());
        let seed_input_position = self
            .current
            .active
            .commands
            .iter()
            .find_map(|command| match command.intent {
                CommandIntent::Append {
                    input_position,
                    entry,
                    source: AppendSource::Submitted,
                    ..
                } if self.current.history.get(entry).is_some_and(|e| {
                    matches!(e.message, Message::User { .. } | Message::External { .. })
                }) =>
                {
                    Some(input_position - 1)
                }
                _ => None,
            })
            .unwrap_or(self.current.active.session.input_position);
        let open = SessionOpen {
            binding: self.current.active.session.binding.clone(),
            request: self.model_request(self.current.history.iter().take(seed_end)),
            context_revision: self.current.active.session.acknowledged_context_revision,
            input_position: seed_input_position,
            profile_revision: self.current.active.session.profile_revision,
            mode: self.current.options.mode,
            session_id: self.current.active.session.id.clone(),
            after_sequence: self
                .current
                .active
                .session
                .recovery
                .as_ref()
                .and(self.current.active.session.last_sequence),
            output_epoch: self.current.active.session.output_epoch,
            limits: self.current.options.limits.clone(),
            recovery: self.current.active.session.recovery.clone(),
            context: ModelContext {
                run: self.current.context.clone(),
                cancellation: self.cancellation.clone(),
                deltas: None,
            },
        };
        let opening_request = open.request.clone();
        let model = self.config.model.clone();
        let opening = model.open_session(open);
        tokio::pin!(opening);
        let session = loop {
            tokio::select! {
                result = &mut opening => match result {
                    Ok(session) => break session,
                    Err(_) if self.current.active.session.recovery.is_some() => {
                        return self.suspend(WaitReason::Recovery).await;
                    }
                    Err(Error::Uncertain(_)) => return self.suspend(WaitReason::Recovery).await,
                    Err(error) => return Err(error),
                },
                Some(control) = self.controls.recv(), if !self.controls.is_closed() || !self.controls.is_empty() => {
                    self.control(control).await?;
                    if !self.current.state.active() {
                        return Ok(());
                    }
                },
                _ = interruption(self.cancellation.clone(), self.deadline) => self.check()?,
            }
        };
        let mut next = self.current.as_ref().clone();
        next.active.session.establishment = SessionEstablishment::Opening;
        next.active.session.ready = false;
        if next.active.session.recovery.is_some()
            && next
                .active
                .session
                .capabilities
                .as_ref()
                .is_some_and(|caps| caps != session.control.capabilities())
        {
            return self.suspend(WaitReason::Recovery).await;
        }
        let binding = session.control.binding();
        if next.active.session.binding.is_some() && next.active.session.binding != binding {
            return Err(Error::Protocol(
                "model binding changed during recovery".into(),
            ));
        }
        next.active.session.binding = binding;
        if next.options.mode == RunMode::Task
            && !session
                .control
                .capabilities()
                .supports(Capability::ResponseEvents)
        {
            return Err(Error::Invalid(
                "bound model has no verifiable response boundaries".into(),
            ));
        }
        next.active.session.capabilities = Some(session.control.capabilities().clone());
        next.active.session.negotiated = session.control.negotiate(&opening_request)?;
        if self.config.history_reducer.is_some()
            && !session
                .control
                .capabilities()
                .supports(Capability::ReplaceContext)
        {
            return Err(Error::Invalid(
                "bound model does not support context replacement".into(),
            ));
        }
        if next.active.session.recovery.is_none() {
            next.active.session.last_sequence = None;
        }
        self.commit(
            next,
            Fact::Command {
                command_id: new_id(),
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        let ModelSession {
            control,
            mut events,
            media: zhir_core::resource::MediaPorts { input, output },
        } = session;
        self.session_control = Some(control);
        self.model_media_input = input;
        let tx = self.work_tx.clone();
        self.tasks.spawn(async move {
            loop {
                let event = events.receive().await;
                let stop = !matches!(event, Ok(Some(_)));
                if tx.send(Work::Model(event)).await.is_err() || stop {
                    break;
                }
            }
        });
        self.start_media_output(output)?;
        Ok(())
    }
}
