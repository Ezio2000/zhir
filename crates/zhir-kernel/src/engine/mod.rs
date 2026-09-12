mod batch;
use crate::environment::{new_id, now_ms};
use crate::{
    control::Control,
    defaults::Ephemeral,
    invocation::{Emitter, EngineResult, Progress, RunError},
    runtime::{Config, Request},
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};
use zhir_core::{
    BoxFuture, Cancellation, Result,
    error::{Error, Failure},
    message::{Message, Output, visible_content},
    model::{ModelContext, ModelRequest},
    run::{
        ActiveState, Checkpoint, ControlAction, EventData, Fact, History, LimitReason, Metrics,
        PendingCalls, State, Suspension, validate_history,
    },
    storage::{Commit, HistoryDelta, RunStore},
    tool::{
        RuntimeToolBinding, RuntimeToolCall, RuntimeToolCatalog, RuntimeToolContext,
        RuntimeToolResult,
    },
};

type ActiveTools = Arc<Mutex<HashMap<String, Cancellation>>>;
struct Engine {
    config: Arc<Config>,
    options: zhir_core::run::RunOptions,
    store: Arc<dyn RunStore>,
    catalog: Option<Arc<crate::catalog::SelectedCatalog>>,
    last: Option<Arc<Checkpoint>>,
    controls: mpsc::UnboundedReceiver<Control>,
    emitter: Emitter,
    deadline: Option<Instant>,
    pause: Option<Suspension>,
    inserts: VecDeque<(Message, String)>,
    active: ActiveTools,
}
enum Effect<T> {
    Done(Result<T>),
    Interrupted(Control),
    Deadline,
    Aborted,
}

pub(crate) async fn execute(
    config: Arc<Config>,
    request: Request,
    controls: mpsc::UnboundedReceiver<Control>,
    emitter: Emitter,
) -> EngineResult {
    let initial = match &request {
        Request::Start { .. } => None,
        Request::Continue(c) | Request::Resume { checkpoint: c, .. } => Some(c.clone()),
    };
    let context = match &request {
        Request::Start { context, .. } => context,
        Request::Continue(c) | Request::Resume { checkpoint: c, .. } => &c.context,
    };
    let deadline = context
        .deadline_at_ms
        .map(|ms| Instant::now() + Duration::from_millis(ms.saturating_sub(now_ms())));
    let store = config
        .store
        .clone()
        .unwrap_or_else(|| Arc::new(Ephemeral::new(initial.clone())));
    let mut engine = Engine {
        options: request.options().clone(),
        config,
        store,
        catalog: None,
        last: initial,
        controls,
        emitter,
        deadline,
        pause: None,
        inserts: VecDeque::new(),
        active: Arc::new(Mutex::new(HashMap::new())),
    };
    let outcome = engine.initialize(request).await;
    let outcome = match outcome {
        Ok(()) => engine.drive().await,
        Err(e) => Err(e),
    };
    match outcome {
        Ok(()) => Ok(engine.last.expect("initialized checkpoint")),
        Err(error) => {
            if matches!(error, Error::Deadline) {
                return match engine.limit(LimitReason::Deadline).await {
                    Ok(()) => Ok(engine.current()),
                    Err(error) => Err(RunError {
                        error,
                        last_checkpoint: engine.last,
                    }),
                };
            }
            if !matches!(
                error,
                Error::Storage(_) | Error::Conflict { .. } | Error::Cancelled
            ) && engine.last.as_ref().is_some_and(|c| !c.state.terminal())
            {
                let fact = Fact::Control {
                    action: ControlAction::Failed,
                };
                if let Err(error) = engine
                    .advance(
                        State::Failed {
                            error: crate::failure::failure(&error),
                        },
                        fact,
                        HistoryDelta::Unchanged,
                        None,
                    )
                    .await
                {
                    return Err(RunError {
                        error,
                        last_checkpoint: engine.last,
                    });
                }
                return Ok(engine.last.expect("failed checkpoint"));
            }
            Err(RunError {
                error,
                last_checkpoint: engine.last,
            })
        }
    }
}
impl Engine {
    fn current(&self) -> Arc<Checkpoint> {
        self.last.as_ref().expect("initialized engine").clone()
    }
    fn expired(&self) -> bool {
        self.deadline.is_some_and(|d| Instant::now() >= d)
    }
    async fn initialize(&mut self, request: Request) -> Result<()> {
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
    async fn persist(&mut self, mut commit: Commit) -> Result<()> {
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
    async fn advance(
        &mut self,
        state: State,
        fact: Fact,
        delta: HistoryDelta,
        metrics: Option<Metrics>,
    ) -> Result<()> {
        self.advance_context(state, fact, delta, metrics, None)
            .await
    }
    async fn advance_context(
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
    async fn limit(&mut self, reason: LimitReason) -> Result<()> {
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
    async fn suspend(&mut self, suspension: Suspension) -> Result<()> {
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
    fn cancel_active(&self, id: &str) {
        if let Some(token) = self.active.lock().expect("active tool lock").get(id) {
            token.cancel();
            self.emitter
                .emit(EventData::RuntimeToolCancelRequested { call_id: id.into() });
        }
    }
    fn cancel_all(&self) {
        for token in self.active.lock().expect("active tool lock").values() {
            token.cancel();
        }
    }
    fn queue(&mut self, c: Control) -> Result<()> {
        match c {
            Control::Pause(s) => {
                if self.pause.is_none() {
                    self.pause = Some(s);
                }
            }
            Control::Insert(m, s) => self.inserts.push_back((m, s)),
            Control::CancelTool(id) => self.cancel_active(&id),
            Control::Abort => return Err(Error::Cancelled),
        };
        Ok(())
    }
    async fn effect<T>(
        &mut self,
        mut future: BoxFuture<'_, Result<T>>,
        cancellation: Cancellation,
        interrupt: bool,
        defer_insert: bool,
    ) -> Effect<T> {
        let deadline = self.deadline;
        let timer = async move {
            if let Some(d) = deadline {
                tokio::time::sleep_until(d).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(timer);
        loop {
            tokio::select! {biased;
                _=&mut timer=>{cancellation.cancel();self.cancel_all();return Effect::Deadline;}
                result=&mut future=>return Effect::Done(result),
                command=self.controls.recv()=>{
                    match command {
                        Some(Control::Abort)|None=>{cancellation.cancel();self.cancel_all();return Effect::Aborted;}
                        Some(Control::CancelTool(id))=>self.cancel_active(&id),
                        Some(c@Control::Pause(_)) if interrupt=>{cancellation.cancel();return Effect::Interrupted(c);}
                        Some(c@Control::Insert(_,_)) if interrupt && !defer_insert=>{cancellation.cancel();return Effect::Interrupted(c);}
                        Some(c)=>{if self.queue(c).is_err() {cancellation.cancel();return Effect::Aborted;}},
                    }
                }
            }
        }
    }
    async fn interruption<T>(&mut self, effect: Effect<T>) -> Result<Option<T>> {
        match effect {
            Effect::Done(value) => value.map(Some),
            Effect::Deadline => {
                self.limit(LimitReason::Deadline).await?;
                Ok(None)
            }
            Effect::Aborted => Err(Error::Cancelled),
            Effect::Interrupted(Control::Pause(s)) => {
                if self.expired() {
                    self.limit(LimitReason::Deadline).await?;
                } else {
                    self.suspend(s).await?;
                }
                Ok(None)
            }
            Effect::Interrupted(Control::Insert(m, source)) => {
                if self.expired() {
                    self.limit(LimitReason::Deadline).await?;
                } else {
                    self.advance(
                        State::Planning {
                            provider_turn_pending: false,
                        },
                        Fact::ConversationInsert { source },
                        HistoryDelta::Append(vec![m]),
                        None,
                    )
                    .await?;
                }
                Ok(None)
            }
            Effect::Interrupted(_) => Err(Error::Protocol("invalid effect interruption".into())),
        }
    }
    async fn drive(&mut self) -> Result<()> {
        loop {
            let current = self.current();
            if current.state.terminal() || matches!(current.state, State::Suspended { .. }) {
                return Ok(());
            }
            if self.expired() {
                self.limit(LimitReason::Deadline).await?;
                continue;
            }
            while let Ok(control) = self.controls.try_recv() {
                self.queue(control)?;
            }
            if let Some(suspension) = self.pause.take() {
                self.suspend(suspension).await?;
                continue;
            }
            if matches!(
                current.state,
                State::Planning {
                    provider_turn_pending: false
                }
            ) && let Some((message, source)) = self.inserts.pop_front()
            {
                self.advance(
                    current.state.clone(),
                    Fact::ConversationInsert { source },
                    HistoryDelta::Append(vec![message]),
                    None,
                )
                .await?;
                continue;
            }
            if self.catalog.is_none() {
                let tools = self.config.runtime_tools.clone();
                let cancellation = Cancellation::default();
                let context = zhir_core::tool::CatalogContext {
                    run: current.context.clone(),
                    cancellation: cancellation.clone(),
                };
                let selection = self.options.runtime_tools.clone();
                let future = Box::pin(async move {
                    crate::catalog::select_catalog(&selection, tools.open_catalog(context).await?)
                });
                let defer = !matches!(
                    current.state,
                    State::Planning {
                        provider_turn_pending: false
                    }
                );
                let effect = self.effect(future, cancellation, true, defer).await;
                let Some(catalog) = self.interruption(effect).await? else {
                    continue;
                };
                self.catalog = Some(catalog);
            }
            match &current.state {
                State::Planning {
                    provider_turn_pending,
                } => self.planning(*provider_turn_pending).await?,
                State::RuntimeToolsPending {
                    calls,
                    provider_turn_pending,
                } => self.runtime_tools(*calls, *provider_turn_pending).await?,
                _ => unreachable!("active state dispatch"),
            }
        }
    }
    async fn planning(&mut self, provider_pending: bool) -> Result<()> {
        if self.current().metrics.planning_steps >= self.options.limits.max_planning_steps {
            return self.limit(LimitReason::PlanningSteps).await;
        }
        if !provider_pending && let Some(reducer) = self.config.history_reducer.clone() {
            let checkpoint = self.current();
            let future = Box::pin(async move { reducer.reduce(checkpoint).await });
            let effect = self
                .effect(future, Cancellation::default(), true, false)
                .await;
            let Some(rewrite) = self.interruption(effect).await? else {
                return Ok(());
            };
            if let Some(rewrite) = rewrite {
                let history = History::new(rewrite.messages.clone())?;
                validate_history(
                    &history,
                    Some(&ActiveState::Planning {
                        provider_turn_pending: false,
                    }),
                )?;
                self.advance(
                    State::Planning {
                        provider_turn_pending: false,
                    },
                    Fact::HistoryRewrite {
                        reason: rewrite.reason,
                    },
                    HistoryDelta::Replace(rewrite.messages),
                    None,
                )
                .await?;
            }
        }
        let checkpoint = self.current();
        let model = self.config.model.clone();
        let request = ModelRequest {
            messages: checkpoint.history.messages(),
            runtime_tools: self.catalog.as_ref().expect("opened catalog").specs(),
            provider_tools: self.options.provider_tools.clone(),
            options: self.options.model.clone(),
            tool_choice: self.options.tool_choice.clone(),
            response_format: self.options.response_format.clone(),
            stream: self.options.stream,
        };
        request.validate(model.capabilities())?;
        let cancellation = Cancellation::default();
        let context = ModelContext {
            run: checkpoint.context.clone(),
            cancellation: cancellation.clone(),
            deltas: if self.options.stream {
                Some(Arc::new(self.emitter.clone()))
            } else {
                None
            },
        };
        self.emitter.emit(EventData::ModelStarted);
        let effect = self
            .effect(
                Box::pin(async move { model.invoke(request, context).await }),
                cancellation,
                true,
                provider_pending,
            )
            .await;
        let Some(response) = self.interruption(effect).await? else {
            return Ok(());
        };
        if self.expired() {
            return self.limit(LimitReason::Deadline).await;
        }
        response.validate()?;
        self.emitter.emit(EventData::ModelFinished);
        let mut metrics = checkpoint.metrics.clone();
        metrics.planning_steps += 1;
        metrics.usage.add(&response.usage);
        let calls = response
            .output
            .iter()
            .filter_map(|o| {
                if let Output::RuntimeToolCall { call } = o {
                    Some(call.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let state = if self
            .options
            .limits
            .max_total_tokens
            .is_some_and(|limit| metrics.usage.total_tokens.is_some_and(|n| n >= limit))
        {
            State::Limited {
                reason: LimitReason::TotalTokens,
            }
        } else if !calls.is_empty() {
            State::RuntimeToolsPending {
                calls: PendingCalls {
                    message_index: checkpoint.history.len(),
                    next: 0,
                    end: calls.len(),
                },
                provider_turn_pending: response.provider_turn_pending,
            }
        } else if response.provider_turn_pending || !self.inserts.is_empty() {
            State::Planning {
                provider_turn_pending: response.provider_turn_pending,
            }
        } else {
            State::Completed {
                content: visible_content(&response.output),
            }
        };
        let fact = Fact::ModelTurn {
            runtime_tool_call_ids: calls.iter().map(|c| c.id.clone()).collect(),
            result: state.kind(),
        };
        self.advance(
            state,
            fact,
            HistoryDelta::Append(vec![Message::Assistant {
                output: response.output,
                provider_data: response.provider_data,
            }]),
            Some(metrics),
        )
        .await
    }
    async fn runtime_tools(&mut self, pending: PendingCalls, provider_pending: bool) -> Result<()> {
        let current = self.current();
        let remaining = self
            .options
            .limits
            .max_runtime_tool_calls
            .saturating_sub(current.metrics.runtime_tool_calls);
        if remaining == 0 {
            return self.limit(LimitReason::RuntimeToolCalls).await;
        }
        let cap = self
            .options
            .limits
            .max_runtime_tool_batch_size
            .min(remaining.try_into().unwrap_or(usize::MAX))
            .min(pending.len());
        let mut batch = self.prepare_batch(current.history.resolve_pending(pending)?, cap)?;
        if !self.approve_batch(&mut batch, &current).await? {
            return Ok(());
        }
        if !self.execute_batch(&mut batch, &current).await? {
            return Ok(());
        }
        self.commit_batch(batch, pending, provider_pending, &current)
            .await
    }
}
struct ActiveGuard {
    id: String,
    active: ActiveTools,
    token: Cancellation,
}
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.token.cancel();
        self.active
            .lock()
            .expect("active tool lock")
            .remove(&self.id);
    }
}
async fn run_tool(
    index: usize,
    call: RuntimeToolCall,
    binding: Arc<dyn RuntimeToolBinding>,
    run: zhir_core::run::RunContext,
    emitter: Emitter,
    active: ActiveTools,
    progress_limit: usize,
) -> (usize, RuntimeToolResult) {
    let token = Cancellation::default();
    active
        .lock()
        .expect("active tool lock")
        .insert(call.id.clone(), token.clone());
    let _guard = ActiveGuard {
        id: call.id.clone(),
        active,
        token: token.clone(),
    };
    emitter.emit(EventData::RuntimeToolStarted {
        call_id: call.id.clone(),
    });
    let (sender, mut receiver) = mpsc::channel(progress_limit.max(1));
    let context = RuntimeToolContext {
        run,
        cancellation: token,
        progress: Some(Arc::new(Progress { sender })),
    };
    let mut future = binding.invoke(context);
    let invoked = loop {
        tokio::select! {result=&mut future=>break result,value=receiver.recv()=>{if let Some(value)=value {emitter.emit(EventData::RuntimeToolProgress {call_id:call.id.clone(),value});}}}
    };
    while let Ok(value) = receiver.try_recv() {
        emitter.emit(EventData::RuntimeToolProgress {
            call_id: call.id.clone(),
            value,
        });
    }
    let result = match invoked {
        Ok(r) => match r.validate() {
            Ok(()) => r,
            Err(e) => RuntimeToolResult::failure(crate::failure::failure(&e)),
        },
        Err(e) => RuntimeToolResult::failure(crate::failure::failure(&e)),
    };
    emitter.emit(EventData::RuntimeToolFinished {
        call_id: call.id,
        outcome: result.outcome.kind(),
    });
    (index, result)
}
