mod history;
mod options;
mod request;
use crate::{
    BoxFuture, Result,
    error::{Error, Failure},
    message::{Content, Message, Output},
    model::{ModelDelta, Usage},
    tool::RuntimeToolCall,
};
pub use history::{History, append_digest as append_history_digest};
pub use options::RunOptions;
pub use request::{ResumeRequest, ResumeTarget, RunRequest, SuspensionTicket};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
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
impl Default for RunContext {
    fn default() -> Self {
        Self {
            run_id: new_id(),
            started_at_ms: now_ms(),
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
    pub fn pause() -> Self {
        Self {
            reason: "pause".into(),
            source: "host".into(),
            wait_id: None,
            metadata: BTreeMap::new(),
        }
    }
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
#[derive(Debug, Clone, Default)]
pub struct SuspensionSelector {
    pub reason: Option<String>,
    pub source: Option<String>,
    pub wait_id: Option<String>,
    pub metadata: BTreeMap<String, Value>,
}
impl SuspensionSelector {
    pub fn matches(&self, s: &Suspension) -> bool {
        (self.reason.is_some()
            || self.source.is_some()
            || self.wait_id.is_some()
            || !self.metadata.is_empty())
            && self.reason.as_ref().is_none_or(|v| v == &s.reason)
            && self.source.as_ref().is_none_or(|v| v == &s.source)
            && self
                .wait_id
                .as_ref()
                .is_none_or(|v| Some(v) == s.wait_id.as_ref())
            && self
                .metadata
                .iter()
                .all(|(k, v)| s.metadata.get(k) == Some(v))
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
        calls: Vec<RuntimeToolCall>,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum State {
    Planning {
        provider_turn_pending: bool,
    },
    RuntimeToolsPending {
        calls: Vec<RuntimeToolCall>,
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
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Planning { .. } => "planning",
            Self::RuntimeToolsPending { .. } => "runtime_tools_pending",
            Self::Suspended { .. } => "suspended",
            Self::Completed { .. } => "completed",
            Self::Failed { .. } => "failed",
            Self::Limited { .. } => "limited",
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
                calls: calls.clone(),
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
            if calls.is_empty() {
                return Err(Error::Invalid(
                    "runtime_tools_pending requires calls".into(),
                ));
            }
            let mut ids = std::collections::HashSet::new();
            for c in calls {
                c.validate()?;
                if !ids.insert(c.id) {
                    return Err(Error::Invalid("duplicate pending call".into()));
                }
            }
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
#[serde(default, deny_unknown_fields)]
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
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_planning_steps: 100,
            max_runtime_tool_calls: 1000,
            max_runtime_tool_batch_size: 32,
            max_runtime_tool_concurrency: 8,
            max_progress_events: 256,
            max_buffered_progress: 256,
            max_total_tokens: None,
            elapsed_ms: None,
            commit_timeout_ms: 5000,
        }
    }
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Fact {
    Started,
    Resumed,
    ModelTurn {
        runtime_tool_call_ids: Vec<String>,
        result: String,
    },
    RuntimeToolBatch {
        call_ids: Vec<String>,
        outcomes: Vec<String>,
        parallel: bool,
    },
    ConversationInsert {
        source: String,
    },
    HistoryRewrite {
        reason: String,
    },
    Control {
        action: String,
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
    if history.is_empty() {
        return Err(Error::Invalid("empty history".into()));
    }
    let mut pending: Vec<RuntimeToolCall> = vec![];
    for msg in history.messages() {
        msg.validate()?;
        match msg {
            Message::Assistant { output, .. } => {
                if !pending.is_empty() {
                    return Err(Error::Invalid("assistant interrupts pending tools".into()));
                }
                pending = output
                    .into_iter()
                    .filter_map(|o| {
                        if let Output::RuntimeToolCall { call } = o {
                            Some(call)
                        } else {
                            None
                        }
                    })
                    .collect();
            }
            Message::RuntimeTool { call_id, name, .. } => {
                if pending
                    .first()
                    .is_none_or(|c| c.id != call_id || c.name != name)
                {
                    return Err(Error::Invalid(
                        "tool message does not match pending order".into(),
                    ));
                }
                pending.remove(0);
            }
            _ if !pending.is_empty() => {
                return Err(Error::Invalid("message interrupts pending tools".into()));
            }
            _ => {}
        }
    }
    match active {
        Some(ActiveState::RuntimeToolsPending { calls, .. }) if *calls == pending => Ok(()),
        Some(ActiveState::RuntimeToolsPending { .. }) => {
            Err(Error::Invalid("pending state differs from history".into()))
        }
        Some(ActiveState::Planning { .. }) if !pending.is_empty() => Err(Error::Invalid(
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
        decision: String,
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
        outcome: String,
    },
    CheckpointCommitted {
        checkpoint_id: String,
        revision: u64,
        state: String,
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
