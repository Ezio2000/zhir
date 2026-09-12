use crate::{
    defaults::{DefaultBatch, EmptyTools},
    invocation::Invocation,
};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use zhir_core::{
    Result,
    error::{Error, ResumeError},
    message::Message,
    model::Model,
    run::{
        ActiveState, Checkpoint, History, HistoryReducer, ResumeTarget, RunContext, State,
        validate_history,
    },
    storage::RunStore,
    tool::{ApprovalPolicy, BatchPolicy, RuntimeToolCatalogProvider},
};

use crate::{ResumeRequest, RunRequest};
pub use zhir_core::run::{RunOptions, SuspensionTicket};

pub(crate) struct Config {
    pub model: Arc<dyn Model>,
    pub runtime_tools: Arc<dyn RuntimeToolCatalogProvider>,
    pub store: Option<Arc<dyn RunStore>>,
    pub approval: Option<Arc<dyn ApprovalPolicy>>,
    pub batch: Arc<dyn BatchPolicy>,
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
                model,
                runtime_tools: Arc::new(EmptyTools),
                store: None,
                approval: None,
                batch: Arc::new(DefaultBatch),
                history_reducer: None,
            },
        }
    }
    pub fn runtime_tools(mut self, runtime_tools: Arc<dyn RuntimeToolCatalogProvider>) -> Self {
        self.config.runtime_tools = runtime_tools;
        self
    }
    pub fn store(mut self, store: Arc<dyn RunStore>) -> Self {
        self.config.store = Some(store);
        self
    }
    /// Configure defaults for new runs. Resumption uses the checkpoint's frozen options.
    pub fn defaults(mut self, configure: impl FnOnce(RunOptions) -> RunOptions) -> Self {
        self.defaults = configure(self.defaults);
        self
    }
    pub fn approval(mut self, policy: Arc<dyn ApprovalPolicy>) -> Self {
        self.config.approval = Some(policy);
        self
    }
    pub fn batch_policy(mut self, policy: Arc<dyn BatchPolicy>) -> Self {
        self.config.batch = policy;
        self
    }
    pub fn history_reducer(mut self, reducer: Arc<dyn HistoryReducer>) -> Self {
        self.config.history_reducer = Some(reducer);
        self
    }
    pub fn build(self) -> Result<Runtime> {
        self.defaults.validate()?;
        Ok(Runtime {
            config: Arc::new(self.config),
            defaults: self.defaults,
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
        context: RunContext,
        options: Box<RunOptions>,
    },
    Continue(Arc<Checkpoint>),
    Resume {
        checkpoint: Arc<Checkpoint>,
        messages: Vec<Message>,
        metadata: BTreeMap<String, Value>,
    },
}
impl Runtime {
    pub fn builder(model: Arc<dyn Model>) -> RuntimeBuilder {
        RuntimeBuilder::new(model)
    }
    pub fn start(&self, request: RunRequest) -> Result<Invocation> {
        let (messages, mut context, options) = request.into_parts(&self.defaults);
        options.validate()?;
        let history = History::new(messages)?;
        validate_history(
            &history,
            Some(&ActiveState::Planning {
                provider_turn_pending: false,
            }),
        )?;
        if context.run_id.is_empty() {
            return Err(Error::Invalid("empty run id".into()));
        }
        if let Some(ms) = options.limits.elapsed_ms {
            let deadline = context.started_at_ms.saturating_add(ms);
            context.deadline_at_ms = Some(
                context
                    .deadline_at_ms
                    .map_or(deadline, |old| old.min(deadline)),
            );
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
    pub fn continue_from(&self, checkpoint: Arc<Checkpoint>) -> Result<Invocation> {
        checkpoint.validate()?;
        if checkpoint.state.active().is_none() {
            return Err(ResumeError::NotActive.into());
        }
        Ok(Invocation::new(
            self.config.clone(),
            Request::Continue(checkpoint),
        ))
    }
    pub async fn resume(&self, request: ResumeRequest) -> Result<Invocation> {
        let checkpoint = match &request.target {
            ResumeTarget::Checkpoint(checkpoint) => checkpoint.clone(),
            ResumeTarget::Ticket(ticket) => {
                ticket.validate()?;
                let store = self
                    .config
                    .store
                    .as_ref()
                    .ok_or(Error::Resume(ResumeError::StoreRequired))?;
                let checkpoint = store.load_head(&ticket.run_id).await?.ok_or_else(|| {
                    Error::Resume(ResumeError::RunNotFound {
                        run_id: ticket.run_id.clone(),
                    })
                })?;
                ticket.check(&checkpoint)?;
                checkpoint
            }
        };
        checkpoint.validate()?;
        let State::Suspended {
            resume_to,
            suspension,
        } = &checkpoint.state
        else {
            return Err(ResumeError::NotSuspended.into());
        };
        if request
            .selector
            .as_ref()
            .is_some_and(|s| !s.matches(suspension))
        {
            return Err(ResumeError::SelectorMismatch.into());
        }
        if !request.messages.is_empty()
            && !matches!(
                resume_to,
                ActiveState::Planning {
                    provider_turn_pending: false
                }
            )
        {
            return Err(ResumeError::MessagesNotAllowed.into());
        }
        let history = checkpoint.history.append(request.messages.clone())?;
        validate_history(&history, Some(resume_to))?;
        Ok(Invocation::new(
            self.config.clone(),
            Request::Resume {
                checkpoint,
                messages: request.messages,
                metadata: request.metadata,
            },
        ))
    }
}

impl Request {
    pub(crate) fn options(&self) -> &RunOptions {
        match self {
            Self::Start { options, .. } => options,
            Self::Continue(checkpoint) | Self::Resume { checkpoint, .. } => &checkpoint.options,
        }
    }
}
