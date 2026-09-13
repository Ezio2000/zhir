use super::*;

impl Engine {
    pub(super) async fn initialize(&mut self, request: Request) -> Result<()> {
        match request {
            Request::Start {
                history,
                context,
                options,
            } => {
                let messages = history.messages();
                let checkpoint = Arc::new(Checkpoint {
                    options: *options,
                    id: new_id(),
                    parent_id: None,
                    revision: 0,
                    context,
                    history,
                    state: State::Planning {
                        provider_turn_pending: false,
                    },
                    metrics: Metrics::default(),
                    fact: Fact::Started,
                });
                self.persist(Commit::new(checkpoint, HistoryDelta::Initial(messages)))
                    .await?;
            }
            Request::Continue(_) => {}
            Request::Resume {
                checkpoint,
                messages,
                metadata,
            } => {
                if self.expired() {
                    return self.limit(LimitReason::Deadline).await;
                }
                let State::Suspended { resume_to, .. } = &checkpoint.state else {
                    unreachable!("validated resume");
                };
                let mut context = checkpoint.context.clone();
                context.metadata.extend(metadata);
                let delta = if messages.is_empty() {
                    HistoryDelta::Unchanged
                } else {
                    HistoryDelta::Append(messages)
                };
                self.advance_context(
                    resume_to.clone().into_state(),
                    Fact::Resumed,
                    delta,
                    None,
                    Some(context),
                )
                .await?;
            }
        }
        Ok(())
    }
    pub(super) async fn persist(&mut self, mut commit: Commit) -> Result<()> {
        if !matches!(commit.checkpoint.fact, Fact::Started)
            && !matches!(
                commit.checkpoint.state,
                State::Limited {
                    reason: LimitReason::Deadline
                }
            )
        {
            commit.deadline = self.deadline.map(Instant::into_std);
        }
        let checkpoint = commit.checkpoint.clone();
        let store = self.store.clone();
        // The write task is always joined. Timeout requests do not detach an
        // operation which may already have crossed its atomic storage boundary.
        let mut task = tokio::spawn(async move { store.commit(commit).await });
        let timeout = Duration::from_millis(self.options.limits.commit_timeout_ms);
        let result = match tokio::time::timeout(timeout, &mut task).await {
            Ok(r) => r,
            Err(_) => task.await,
        };
        result.map_err(|e| Error::Storage(format!("commit worker: {e}")))??;
        self.last = Some(checkpoint.clone());
        self.emitter.emit(EventData::CheckpointCommitted {
            checkpoint_id: checkpoint.id.clone(),
            revision: checkpoint.revision,
            state: checkpoint.state.kind(),
            fact: checkpoint.fact.clone(),
        });
        Ok(())
    }
    pub(super) async fn advance(
        &mut self,
        state: State,
        fact: Fact,
        delta: HistoryDelta,
        metrics: Option<Metrics>,
    ) -> Result<()> {
        self.advance_context(state, fact, delta, metrics, None)
            .await
    }
    pub(super) async fn advance_context(
        &mut self,
        state: State,
        fact: Fact,
        delta: HistoryDelta,
        metrics: Option<Metrics>,
        context: Option<zhir_core::run::RunContext>,
    ) -> Result<()> {
        let previous = self.current();
        state.validate()?;
        let history = match &delta {
            HistoryDelta::Append(m) => previous.history.append(m.clone())?,
            HistoryDelta::Replace(m) => History::new(m.clone())?,
            HistoryDelta::Unchanged => previous.history.clone(),
            HistoryDelta::Initial(_) => {
                return Err(Error::Protocol("initial history after start".into()));
            }
        };
        let checkpoint = Arc::new(Checkpoint {
            options: previous.options.clone(),
            id: new_id(),
            parent_id: Some(previous.id.clone()),
            revision: previous
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::Protocol("revision overflow".into()))?,
            context: context.unwrap_or_else(|| previous.context.clone()),
            history,
            state,
            metrics: metrics.unwrap_or_else(|| previous.metrics.clone()),
            fact,
        });
        self.persist(Commit::new(checkpoint, delta)).await
    }
    pub(super) async fn limit(&mut self, reason: LimitReason) -> Result<()> {
        self.advance(
            State::Limited { reason },
            Fact::Control {
                action: ControlAction::Limited,
            },
            HistoryDelta::Unchanged,
            None,
        )
        .await
    }
    pub(super) async fn suspend(&mut self, suspension: Suspension) -> Result<()> {
        let active = self
            .current()
            .state
            .active()
            .ok_or_else(|| Error::Protocol("suspend requires active state".into()))?;
        self.advance(
            State::Suspended {
                resume_to: active,
                suspension,
            },
            Fact::Control {
                action: ControlAction::Suspended,
            },
            HistoryDelta::Unchanged,
            None,
        )
        .await
    }
}
