mod completion;
mod context;
mod history;
mod pending;
pub use completion::{RunCompletion, RunOutcome};
pub use context::ContextKey;
pub use pending::PendingCalls;
mod options;
mod ticket;
use crate::{
    BoxFuture, Result,
    error::{Error, Failure},
    message::{Content, Message},
    model::{ModelDelta, Usage},
};
pub use history::{History, append_digest as append_history_digest};
pub use options::RunOptions;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
pub use ticket::SuspensionTicket;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunContext {
    pub run_id: String,
    pub started_at_ms: u64,
    pub deadline_at_ms: Option<u64>,
    pub parent_run_id: Option<String>,
    pub parent_runtime_tool_call_id: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}
impl RunContext {
    pub fn new(run_id: impl Into<String>, started_at_ms: u64) -> Self {
        Self {
            run_id: run_id.into(),
            started_at_ms,
            deadline_at_ms: None,
            parent_run_id: None,
            parent_runtime_tool_call_id: None,
            metadata: BTreeMap::new(),
        }
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Suspension {
    pub reason: String,
    pub source: String,
    pub wait_id: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}
impl Suspension {
    pub fn validate(&self) -> Result<()> {
        if self.reason.is_empty()
            || self.source.is_empty()
            || self.wait_id.as_ref().is_some_and(String::is_empty)
        {
            Err(Error::Invalid("invalid suspension identity".into()))
        } else {
            Ok(())
        }
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActiveState {
    Planning {
        provider_turn_pending: bool,
    },
    RuntimeToolsPending {
        calls: PendingCalls,
        provider_turn_pending: bool,
    },
}
impl ActiveState {
    pub fn provider_pending(&self) -> bool {
        match self {
            Self::Planning {
                provider_turn_pending,
            }
            | Self::RuntimeToolsPending {
                provider_turn_pending,
                ..
            } => *provider_turn_pending,
        }
    }
    pub fn into_state(self) -> State {
        match self {
            Self::Planning {
                provider_turn_pending,
            } => State::Planning {
                provider_turn_pending,
            },
            Self::RuntimeToolsPending {
                calls,
                provider_turn_pending,
            } => State::RuntimeToolsPending {
                calls,
                provider_turn_pending,
            },
        }
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateKind {
    Planning,
    RuntimeToolsPending,
    Suspended,
    Completed,
    Failed,
    Limited,
}
impl StateKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::RuntimeToolsPending => "runtime_tools_pending",
            Self::Suspended => "suspended",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Limited => "limited",
        }
    }
}
impl std::fmt::Display for StateKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum State {
    Planning {
        provider_turn_pending: bool,
    },
    RuntimeToolsPending {
        calls: PendingCalls,
        provider_turn_pending: bool,
    },
    Suspended {
        resume_to: ActiveState,
        suspension: Suspension,
    },
    Completed {
        content: Vec<Content>,
    },
    Failed {
        error: Failure,
    },
    Limited {
        reason: LimitReason,
    },
}
impl State {
    pub fn kind(&self) -> StateKind {
        match self {
            Self::Planning { .. } => StateKind::Planning,
            Self::RuntimeToolsPending { .. } => StateKind::RuntimeToolsPending,
            Self::Suspended { .. } => StateKind::Suspended,
            Self::Completed { .. } => StateKind::Completed,
            Self::Failed { .. } => StateKind::Failed,
            Self::Limited { .. } => StateKind::Limited,
        }
    }
    pub fn active(&self) -> Option<ActiveState> {
        match self {
            Self::Planning {
                provider_turn_pending,
            } => Some(ActiveState::Planning {
                provider_turn_pending: *provider_turn_pending,
            }),
            Self::RuntimeToolsPending {
                calls,
                provider_turn_pending,
            } => Some(ActiveState::RuntimeToolsPending {
                calls: *calls,
                provider_turn_pending: *provider_turn_pending,
            }),
            _ => None,
        }
    }
    pub fn terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Failed { .. } | Self::Limited { .. }
        )
    }
    pub fn validate(&self) -> Result<()> {
        let active = match self {
            Self::Suspended {
                resume_to,
                suspension,
            } => {
                suspension.validate()?;
                Some(resume_to.clone())
            }
            _ => self.active(),
        };
        if let Some(ActiveState::RuntimeToolsPending { calls, .. }) = active {
            calls.validate()?;
        }
        if let Self::Completed { content } = self {
            for c in content {
                c.validate()?;
            }
        }
        Ok(())
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitReason {
    Deadline,
    PlanningSteps,
    RuntimeToolCalls,
    TotalTokens,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub max_planning_steps: u64,
    pub max_runtime_tool_calls: u64,
    pub max_runtime_tool_batch_size: usize,
    pub max_runtime_tool_concurrency: usize,
    pub max_progress_events: usize,
    pub max_buffered_progress: usize,
    pub max_total_tokens: Option<u64>,
    pub elapsed_ms: Option<u64>,
    pub commit_timeout_ms: u64,
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        if self.max_runtime_tool_batch_size == 0
            || self.max_runtime_tool_concurrency == 0
            || self.commit_timeout_ms == 0
        {
            Err(Error::Invalid(
                "batch, concurrency and commit timeout must be positive".into(),
            ))
        } else {
            Ok(())
        }
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metrics {
    pub planning_steps: u64,
    pub runtime_tool_calls: u64,
    pub usage: Usage,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlAction {
    Failed,
    Limited,
    Suspended,
}
impl ControlAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Failed => "failed",
            Self::Limited => "limited",
            Self::Suspended => "suspended",
        }
    }
}
impl std::fmt::Display for ControlAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Fact {
    Started,
    Resumed,
    ModelTurn {
        runtime_tool_call_ids: Vec<String>,
        result: StateKind,
    },
    RuntimeToolBatch {
        call_ids: Vec<String>,
        outcomes: Vec<crate::tool::RuntimeToolOutcomeKind>,
        parallel: bool,
    },
    ConversationInsert {
        source: String,
    },
    HistoryRewrite {
        reason: String,
    },
    Control {
        action: ControlAction,
    },
}
impl Fact {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Resumed => "resumed",
            Self::ModelTurn { .. } => "model_turn",
            Self::RuntimeToolBatch { .. } => "runtime_tool_batch",
            Self::ConversationInsert { .. } => "conversation_insert",
            Self::HistoryRewrite { .. } => "history_rewrite",
            Self::Control { .. } => "control",
        }
    }
}
#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub options: RunOptions,
    pub id: String,
    pub parent_id: Option<String>,
    pub revision: u64,
    pub context: RunContext,
    pub history: History,
    pub state: State,
    pub metrics: Metrics,
    pub fact: Fact,
}
impl Checkpoint {
    pub fn validate(&self) -> Result<()> {
        self.options.validate()?;
        if self.id.is_empty() || self.context.run_id.is_empty() {
            return Err(Error::Invalid("empty checkpoint identity".into()));
        }
        if (self.revision == 0) != self.parent_id.is_none() {
            return Err(Error::Invalid("revision and parent disagree".into()));
        }
        self.state.validate()?;
        let active = self.state.active().or_else(|| {
            if let State::Suspended { resume_to, .. } = &self.state {
                Some(resume_to.clone())
            } else {
                None
            }
        });
        validate_history(&self.history, active.as_ref())
    }
}
pub fn validate_history(history: &History, active: Option<&ActiveState>) -> Result<()> {
    let pending = history.pending()?;
    match active {
        Some(ActiveState::RuntimeToolsPending { calls, .. }) if Some(*calls) == pending => Ok(()),
        Some(ActiveState::RuntimeToolsPending { .. }) => {
            Err(Error::Invalid("pending state differs from history".into()))
        }
        Some(ActiveState::Planning { .. }) if pending.is_some() => Err(Error::Invalid(
            "planning history has unresolved tools".into(),
        )),
        _ => Ok(()),
    }
}

#[derive(Debug, Clone)]
pub struct HistoryRewrite {
    pub messages: Vec<Message>,
    pub reason: String,
}
pub trait HistoryReducer: Send + Sync {
    fn reduce(&self, checkpoint: Arc<Checkpoint>) -> BoxFuture<'_, Result<Option<HistoryRewrite>>>;
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventData {
    ModelStarted,
    ModelFinished,
    ApprovalRequested {
        call_id: String,
    },
    ApprovalDecided {
        call_id: String,
        decision: crate::tool::ApprovalDecisionKind,
    },
    RuntimeToolCancelRequested {
        call_id: String,
    },
    ModelDelta {
        delta: ModelDelta,
    },
    RuntimeToolStarted {
        call_id: String,
    },
    RuntimeToolProgress {
        call_id: String,
        value: Value,
    },
    RuntimeToolFinished {
        call_id: String,
        outcome: crate::tool::RuntimeToolOutcomeKind,
    },
    CheckpointCommitted {
        checkpoint_id: String,
        revision: u64,
        state: StateKind,
        fact: Fact,
    },
}
impl EventData {
    pub fn lossy(&self) -> bool {
        matches!(
            self,
            Self::ModelDelta { .. } | Self::RuntimeToolProgress { .. }
        )
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub run_id: String,
    pub invocation_id: String,
    pub sequence: u64,
    pub data: EventData,
}
