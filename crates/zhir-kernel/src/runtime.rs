use crate::{ResumeRequest, ResumeTarget, RunRequest, invocation::Invocation};
use std::sync::Arc;
pub use zhir_core::run::{RunOptions, SuspensionTicket};
use zhir_core::{
    Result,
    error::{Error, ResumeError},
    model::Model,
    resource::ResourceStore,
    run::{Checkpoint, History, HistoryReducer, State},
    storage::RunStore,
    tool::{ApprovalPolicy, RuntimeToolCatalogProvider, SchedulingPolicy},
};

pub(crate) struct Config {
    pub delegation: Option<Arc<dyn zhir_core::operation::DelegationHandler>>,
    pub model: Arc<dyn Model>,
    pub runtime_tools: Arc<dyn RuntimeToolCatalogProvider>,
    pub store: Option<Arc<dyn RunStore>>,
    pub resources: Option<Arc<dyn ResourceStore>>,
    pub approval: Option<Arc<dyn ApprovalPolicy>>,
    pub scheduler: Arc<dyn SchedulingPolicy>,
    pub history_reducer: Option<Arc<dyn HistoryReducer>>,
}
pub struct RuntimeBuilder {
    defaults: RunOptions,
    config: Config,
}
impl RuntimeBuilder {
    pub fn new(model: Arc<dyn Model>) -> Self {
        Self {
            defaults: crate::defaults::run_options(),
            config: Config {
                delegation: None,
                model,
                runtime_tools: Arc::new(crate::defaults::EmptyTools),
                store: None,
                resources: None,
                approval: None,
                scheduler: Arc::new(crate::defaults::DefaultScheduling),
                history_reducer: None,
            },
        }
    }
    pub fn runtime_tools(mut self, value: Arc<dyn RuntimeToolCatalogProvider>) -> Self {
        self.config.runtime_tools = value;
        self
    }
    pub fn delegation(mut self, handler: Arc<dyn zhir_core::operation::DelegationHandler>) -> Self {
        self.config.delegation = Some(handler);
        self
    }
    pub fn store(mut self, value: Arc<dyn RunStore>) -> Self {
        self.config.store = Some(value);
        self
    }
    pub fn resources(mut self, value: Arc<dyn ResourceStore>) -> Self {
        self.config.resources = Some(value);
        self
    }
    pub fn defaults(mut self, configure: impl FnOnce(RunOptions) -> RunOptions) -> Self {
        self.defaults = configure(self.defaults);
        self
    }
    pub fn approval(mut self, value: Arc<dyn ApprovalPolicy>) -> Self {
        self.config.approval = Some(value);
        self
    }
    pub fn scheduling(mut self, value: Arc<dyn SchedulingPolicy>) -> Self {
        self.config.scheduler = value;
        self
    }
    pub fn history_reducer(mut self, value: Arc<dyn HistoryReducer>) -> Self {
        self.config.history_reducer = Some(value);
        self
    }
    pub fn build(self) -> Result<Runtime> {
        self.defaults.validate()?;
        Ok(Runtime {
            defaults: self.defaults,
            config: Arc::new(self.config),
        })
    }
}
#[derive(Clone)]
pub struct Runtime {
    defaults: RunOptions,
    pub(crate) config: Arc<Config>,
}
pub(crate) enum Request {
    Start {
        history: History,
        context: zhir_core::run::RunContext,
        options: Box<RunOptions>,
    },
    Recover {
        checkpoint: Arc<Checkpoint>,
        messages: Vec<zhir_core::message::Message>,
        resolutions: Vec<zhir_core::operation::RecoveryResolution>,
        metadata: std::collections::BTreeMap<String, serde_json::Value>,
    },
}
impl Request {
    pub(crate) fn options(&self) -> &RunOptions {
        match self {
            Self::Start { options, .. } => options,
            Self::Recover { checkpoint, .. } => &checkpoint.options,
        }
    }
    pub(crate) fn context(&self) -> &zhir_core::run::RunContext {
        match self {
            Self::Start { context, .. } => context,
            Self::Recover { checkpoint, .. } => &checkpoint.context,
        }
    }
}
impl Runtime {
    pub fn builder(model: Arc<dyn Model>) -> RuntimeBuilder {
        RuntimeBuilder::new(model)
    }
    pub fn start(&self, request: RunRequest) -> Result<Invocation> {
        let (messages, mut context, options) = request.into_parts(&self.defaults);
        options.validate()?;
        if context.run_id.is_empty() {
            return Err(Error::Invalid("empty run id".into()));
        }
        let history = History::new(messages)?;
        history.validate()?;
        if history.pending_calls().next().is_some()
            || history.pending_delegations().next().is_some()
        {
            return Err(Error::Invalid("new run has unresolved work".into()));
        }
        if let Some(ms) = options.limits.elapsed_ms {
            let limit = context.started_at_ms.saturating_add(ms);
            context.deadline_at_ms =
                Some(context.deadline_at_ms.map_or(limit, |old| old.min(limit)));
        }
        Ok(Invocation::new(
            self.config.clone(),
            Request::Start {
                history,
                context,
                options: Box::new(options),
            },
        ))
    }
    pub async fn load_checkpoint(&self, run_id: &str) -> Result<Option<Arc<Checkpoint>>> {
        self.config
            .store
            .as_ref()
            .ok_or(ResumeError::StoreRequired)?
            .load_head(run_id)
            .await
    }
    /// Continues an active checkpoint left by an interrupted process. The Attached CAS
    /// rejects a stale checkpoint, but it does not stop an executor that is still alive:
    /// the host must guarantee that a run has at most one live executor.
    pub fn continue_from(&self, checkpoint: Arc<Checkpoint>) -> Result<Invocation> {
        checkpoint.validate()?;
        if !checkpoint.state.active() {
            return Err(ResumeError::NotActive.into());
        }
        Ok(Invocation::new(
            self.config.clone(),
            Request::Recover {
                checkpoint,
                messages: vec![],
                resolutions: vec![],
                metadata: Default::default(),
            },
        ))
    }
    /// Resumes a Suspended run with host input and recovery resolutions. As with
    /// `continue_from`, the host must guarantee a single live executor per run.
    pub async fn resume(&self, request: ResumeRequest) -> Result<Invocation> {
        let checkpoint = match request.target {
            ResumeTarget::Checkpoint(checkpoint) => checkpoint,
            ResumeTarget::Ticket(ticket) => {
                ticket.validate()?;
                let store = self
                    .config
                    .store
                    .as_ref()
                    .ok_or(ResumeError::StoreRequired)?;
                let checkpoint = store.load_head(&ticket.run_id).await?.ok_or_else(|| {
                    ResumeError::RunNotFound {
                        run_id: ticket.run_id.clone(),
                    }
                })?;
                ticket.check(&checkpoint)?;
                checkpoint
            }
        };
        checkpoint.validate()?;
        let State::Suspended { suspension } = &checkpoint.state else {
            return Err(ResumeError::NotSuspended.into());
        };
        if request
            .selector
            .as_ref()
            .is_some_and(|s| !s.matches(suspension))
        {
            return Err(ResumeError::SelectorMismatch.into());
        }
        Ok(Invocation::new(
            self.config.clone(),
            Request::Recover {
                checkpoint,
                messages: request.messages,
                resolutions: request.resolutions,
                metadata: request.metadata,
            },
        ))
    }
}
