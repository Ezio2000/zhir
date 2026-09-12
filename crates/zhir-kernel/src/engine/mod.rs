use crate::{
    control::Control,
    defaults::Ephemeral,
    invocation::{Emitter, Progress, RunError, RunResult},
    runtime::{Config, Request},
};
use futures::{StreamExt, stream};
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
        ActiveState, Checkpoint, EventData, Fact, History, LimitReason, Metrics, State, Suspension,
        new_id, now_ms, validate_history,
    },
    storage::{Commit, HistoryDelta, RunStore},
    tool::{
        ApprovalDecision, ApprovalRequest, RuntimeToolBinding, RuntimeToolCall, RuntimeToolCatalog,
        RuntimeToolContext, RuntimeToolResult,
    },
};

type ActiveTools = Arc<Mutex<HashMap<String, Cancellation>>>;
struct Engine {
    config: Arc<Config>,
    options: zhir_core::run::RunOptions,
    store: Arc<dyn RunStore>,
    catalog: Option<Arc<dyn RuntimeToolCatalog>>,
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
) -> RunResult {
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
                    action: "failed".into(),
                };
                if let Err(error) = engine
                    .advance(
                        State::Failed {
                            error: error.failure(),
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
            state: checkpoint.state.kind().into(),
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
                action: "limited".into(),
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
                action: "suspended".into(),
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
                let future = Box::pin(async move { tools.open_catalog().await });
                let defer = !matches!(
                    current.state,
                    State::Planning {
                        provider_turn_pending: false
                    }
                );
                let effect = self
                    .effect(future, Cancellation::default(), true, defer)
                    .await;
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
                } => {
                    self.runtime_tools(calls.clone(), *provider_turn_pending)
                        .await?
                }
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
                calls: calls.clone(),
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
            result: state.kind().into(),
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
    async fn runtime_tools(
        &mut self,
        pending: Vec<RuntimeToolCall>,
        provider_pending: bool,
    ) -> Result<()> {
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
        let catalog = self.catalog.as_ref().expect("opened catalog").clone();
        let specs = catalog.specs();
        let batch = self.config.batch.select(&pending[..cap], &specs)?;
        if batch.calls.is_empty()
            || batch.calls.len() > cap
            || pending[..batch.calls.len()] != batch.calls
            || (!batch.parallel && batch.calls.len() != 1)
        {
            return Err(Error::Protocol(
                "batch policy must select a nonempty bounded prefix".into(),
            ));
        }
        if batch.parallel
            && batch.calls.iter().any(|c| {
                !specs
                    .iter()
                    .any(|s| s.name == c.name && s.execution.parallel_safe())
            })
        {
            return Err(Error::Protocol(
                "parallel batch contains an ineligible tool".into(),
            ));
        }
        let mut bindings = Vec::new();
        let mut results = vec![None; batch.calls.len()];
        let mut requests = Vec::new();
        let mut valid = Vec::new();
        for (index, call) in batch.calls.iter().enumerate() {
            match catalog.bind(call) {
                Ok(binding) => {
                    requests.push(ApprovalRequest {
                        call: call.clone(),
                        spec: binding.spec().clone(),
                    });
                    valid.push(index);
                    bindings.push(Some(binding));
                }
                Err(error) => {
                    results[index] = Some(RuntimeToolResult::failure(error.failure()));
                    bindings.push(None);
                }
            }
        }
        if let Some(policy) = self.config.approval.clone()
            && !requests.is_empty()
        {
            let context = current.context.clone();
            let count = requests.len();
            for request in &requests {
                self.emitter.emit(EventData::ApprovalRequested {
                    call_id: request.call.id.clone(),
                });
            }
            let effect = self
                .effect(
                    Box::pin(async move { policy.decide(requests, context).await }),
                    Cancellation::default(),
                    true,
                    true,
                )
                .await;
            let Some(decisions) = self.interruption(effect).await? else {
                return Ok(());
            };
            if decisions.len() != count {
                return Err(Error::Protocol("approval count mismatch".into()));
            }
            for (index, decision) in valid.iter().zip(&decisions) {
                self.emitter.emit(EventData::ApprovalDecided {
                    call_id: batch.calls[*index].id.clone(),
                    decision: match decision {
                        ApprovalDecision::Allow => "allow",
                        ApprovalDecision::Deny(_) => "deny",
                        ApprovalDecision::Suspend(_) => "suspend",
                    }
                    .into(),
                });
            }
            if let Some(s) = decisions.iter().find_map(|d| {
                if let ApprovalDecision::Suspend(s) = d {
                    Some(s.clone())
                } else {
                    None
                }
            }) {
                return self.suspend(s).await;
            }
            for (index, decision) in valid.into_iter().zip(decisions) {
                if let ApprovalDecision::Deny(message) = decision {
                    results[index] =
                        Some(RuntimeToolResult::failure(Failure::new("denied", message)));
                    bindings[index] = None;
                }
            }
        }
        let mut work = Vec::new();
        for (index, binding) in bindings.into_iter().enumerate() {
            if let Some(binding) = binding {
                work.push((index, batch.calls[index].clone(), binding));
            }
        }
        let context = current.context.clone();
        let emitter = self.emitter.clone();
        let active = self.active.clone();
        let concurrency = if batch.parallel {
            self.options.limits.max_runtime_tool_concurrency
        } else {
            1
        };
        let progress_limit = self.options.limits.max_buffered_progress;
        let futures: Vec<BoxFuture<'static, (usize, RuntimeToolResult)>> = work
            .into_iter()
            .map(|(index, call, binding)| {
                Box::pin(run_tool(
                    index,
                    call,
                    binding,
                    context.clone(),
                    emitter.clone(),
                    active.clone(),
                    progress_limit,
                )) as BoxFuture<'static, (usize, RuntimeToolResult)>
            })
            .collect();
        let future = Box::pin(async move {
            let completed = stream::iter(futures)
                .buffer_unordered(concurrency)
                .collect::<Vec<_>>()
                .await;
            Ok(completed)
        });
        let effect = self
            .effect(future, Cancellation::default(), false, true)
            .await;
        let Some(completed) = self.interruption(effect).await? else {
            return Ok(());
        };
        if self.expired() {
            return self.limit(LimitReason::Deadline).await;
        }
        for (index, result) in completed {
            results[index] = Some(result);
        }
        let results = results
            .into_iter()
            .map(|r| r.expect("every selected call settled"))
            .collect::<Vec<_>>();
        let rest = pending[batch.calls.len()..].to_vec();
        let active = if rest.is_empty() {
            ActiveState::Planning {
                provider_turn_pending: provider_pending,
            }
        } else {
            ActiveState::RuntimeToolsPending {
                calls: rest,
                provider_turn_pending: provider_pending,
            }
        };
        while let Ok(control) = self.controls.try_recv() {
            self.queue(control)?;
        }
        let suspension = results
            .iter()
            .find_map(|r| r.suspension.clone())
            .or_else(|| self.pause.take());
        let state = match suspension {
            Some(suspension) => State::Suspended {
                resume_to: active,
                suspension,
            },
            None => active.into_state(),
        };
        let messages = batch
            .calls
            .iter()
            .zip(&results)
            .map(|(call, result)| Message::RuntimeTool {
                call_id: call.id.clone(),
                name: call.name.clone(),
                outcome: result.outcome.clone(),
            })
            .collect();
        let fact = Fact::RuntimeToolBatch {
            call_ids: batch.calls.iter().map(|c| c.id.clone()).collect(),
            outcomes: results.iter().map(|r| r.outcome.kind().into()).collect(),
            parallel: batch.parallel,
        };
        let mut metrics = current.metrics.clone();
        metrics.runtime_tool_calls += batch.calls.len() as u64;
        self.advance(state, fact, HistoryDelta::Append(messages), Some(metrics))
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
            Err(e) => RuntimeToolResult::failure(e.failure()),
        },
        Err(e) => RuntimeToolResult::failure(e.failure()),
    };
    emitter.emit(EventData::RuntimeToolFinished {
        call_id: call.id,
        outcome: result.outcome.kind().into(),
    });
    (index, result)
}
