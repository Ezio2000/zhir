mod batch;
mod commit;
mod effects;
mod planning;
mod tool_call;
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
