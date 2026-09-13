use super::*;

impl Engine {
    pub(super) fn session_capabilities(&self) -> &CapabilitySet {
        self.session.as_ref().map_or_else(
            || self.config.model.capabilities(),
            |input| input.capabilities(),
        )
    }
    pub(super) fn negotiate(
        &self,
        request: &ModelRequest,
    ) -> Result<zhir_core::profile::NegotiatedProfile> {
        self.session.as_ref().map_or_else(
            || self.config.model.negotiate(request),
            |input| input.negotiate(request),
        )
    }
    pub(super) fn model_request(&self, history_count: usize) -> ModelRequest {
        let messages = zhir_core::model::conversation(
            self.current
                .history
                .entries()
                .into_iter()
                .take(history_count),
        );
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
    pub(super) async fn start_turn(&mut self) -> Result<()> {
        if self.current.metrics.model_turns >= self.current.options.limits.max_model_turns {
            let mut next = self.current.as_ref().clone();
            next.state = State::Limited {
                reason: LimitReason::ModelTurns,
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
            && let Some(reducer) = self.config.history_reducer.clone()
            && let Some(rewrite) = interruptible(
                reducer.reduce(self.current.clone()),
                self.cancellation.clone(),
                self.deadline,
            )
            .await?
        {
            let mut next = self.current.as_ref().clone();
            next.history = History::from_entries(rewrite.entries.clone())?;
            next.active.session.turn_start = next.history.len();
            self.commit(
                next,
                Fact::HistoryRewrite {
                    reason: rewrite.reason,
                },
                HistoryDelta::Replace(rewrite.entries),
            )
            .await?;
        }
        let turn_id = new_id();
        let request = self.model_request(self.current.history.len());
        let negotiated = self.negotiate(&request)?;
        let id = new_id();
        let mut next = self.current.as_ref().clone();
        next.active.session.turn_id = Some(turn_id.clone());
        next.active.session.turn_start = next.history.len();
        next.active.session.disposition = None;
        next.active.session.negotiated = negotiated;
        next.active.commands.push(PendingCommand {
            id: id.clone(),
            intent: CommandIntent::StartTurn {
                turn_id,
                history_count: next.history.len(),
                profile: next.active.session.profile.clone(),
                runtime_tools: self.catalog.specs(),
            },
            sent: false,
        });
        self.commit(
            next,
            Fact::Command { command_id: id },
            HistoryDelta::Unchanged,
        )
        .await?;
        self.needs_turn = false;
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
                _ = tokio::time::sleep(Duration::from_millis(10)) => self.check()?,
            }
        }
        Ok(())
    }
    pub(super) async fn open_model(&mut self) -> Result<()> {
        let open = SessionOpen {
            request: self.model_request(self.current.history.len()),
            session_id: self.current.active.session.id.clone(),
            after_sequence: self
                .current
                .active
                .session
                .recovery
                .as_ref()
                .and(self.current.active.session.last_sequence),
            epoch: self.current.active.session.epoch,
            limits: self.current.options.limits.clone(),
            recovery: self.current.active.session.recovery.clone(),
            context: ModelContext {
                run: self.current.context.clone(),
                cancellation: self.cancellation.clone(),
                deltas: None,
            },
        };
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
                    Err(error) => return Err(error),
                },
                Some(control) = self.controls.recv(), if !self.controls.is_closed() || !self.controls.is_empty() => {
                    self.control(control).await?;
                    if !self.current.state.active() {
                        return Ok(());
                    }
                },
                _ = tokio::time::sleep(Duration::from_millis(10)) => self.check()?,
            }
        };
        let mut next = self.current.as_ref().clone();
        if next.active.session.recovery.is_some()
            && next
                .active
                .session
                .capabilities
                .as_ref()
                .is_some_and(|caps| caps != session.input.capabilities())
        {
            return self.suspend(WaitReason::Recovery).await;
        }
        next.active.session.capabilities = Some(session.input.capabilities().clone());
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
        self.session = Some(session.input);
        self.model_media = session.media_input;
        let tx = self.work_tx.clone();
        let mut receiver = session.output;
        self.tasks.spawn(async move {
            loop {
                let event = receiver.receive().await;
                let stop = !matches!(event, Ok(Some(_)));
                if tx.send(Work::Model(event)).await.is_err() || stop {
                    break;
                }
            }
        });
        self.start_media_output(session.media_output)?;
        Ok(())
    }
}
