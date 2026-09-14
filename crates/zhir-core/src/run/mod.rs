mod completion;
mod context;
mod history;
pub use completion::{RunCompletion, RunOutcome};
pub use context::ContextKey;
mod options;
mod ticket;
use crate::{
    BoxFuture, Result,
    error::{Error, Failure},
    message::{Content, Message},
    model::{ModelDelta, Usage},
};
pub use history::{History, HistoryEntry, append_digest as append_history_digest};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    Task,
    Interactive,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateKind {
    Running,
    Suspended,
    Completed,
    Failed,
    Cancelled,
    Limited,
}
impl StateKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Suspended => "suspended",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
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
    Running,
    Suspended { suspension: Suspension },
    Completed { content: Vec<Content> },
    Failed { error: Failure },
    Cancelled,
    Limited { reason: LimitReason },
}
impl State {
    pub fn kind(&self) -> StateKind {
        match self {
            Self::Running => StateKind::Running,
            Self::Suspended { .. } => StateKind::Suspended,
            Self::Completed { .. } => StateKind::Completed,
            Self::Failed { .. } => StateKind::Failed,
            Self::Cancelled => StateKind::Cancelled,
            Self::Limited { .. } => StateKind::Limited,
        }
    }
    pub fn active(&self) -> bool {
        matches!(self, Self::Running)
    }
    pub fn terminal(&self) -> bool {
        !matches!(self, Self::Running | Self::Suspended { .. })
    }
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Suspended { suspension } => suspension.validate(),
            Self::Completed { content } => {
                for c in content {
                    c.validate()?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandIntent {
    DelegationContext {
        operation_id: String,
        content: Vec<crate::message::Content>,
    },
    DelegationResult {
        operation_id: String,
        entry: usize,
    },
    StartTurn {
        turn_id: String,
        history_count: usize,
        profile: crate::profile::RequestProfile,
        runtime_tools: Vec<crate::tool::RuntimeToolSpec>,
    },
    Input {
        entry: usize,
    },
    ToolResult {
        operation_id: String,
        entry: usize,
    },
    UpdateProfile {
        revision: u64,
        profile: crate::profile::RequestProfile,
    },
    InterruptOutput {
        turn_id: String,
        output_epoch: u64,
    },
    FlushInput,
    /// Enable or pause remote audio input processing without closing the input port.
    SetInputAudio {
        enabled: bool,
    },
    EndInput,
    Close,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingCommand {
    pub id: String,
    pub intent: CommandIntent,
    pub sent: bool,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSnapshot {
    pub capabilities: Option<crate::model::CapabilitySet>,
    pub id: String,
    pub turn_id: Option<String>,
    pub turn_start: usize,
    pub last_sequence: Option<u64>,
    pub disposition: Option<crate::model::TurnDisposition>,
    pub recovery: Option<crate::operation::RecoveryRef>,
    pub output_epoch: u64,
    pub media_archive: Option<crate::resource::ResourceRef>,
    pub input_closed: bool,
    /// Latest committed audio input mode; pending commands record remote settlement.
    pub input_audio_enabled: bool,
    pub closing: bool,
    pub closed: bool,
    pub profile_revision: u64,
    pub profile: crate::profile::RequestProfile,
    pub negotiated: crate::profile::NegotiatedProfile,
    pub effective: crate::profile::EffectiveProfile,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamCursor {
    pub sequence: u64,
    pub epoch: u64,
    pub sealed: crate::resource::ResourceRef,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveState {
    pub session: SessionSnapshot,
    pub operations: BTreeMap<String, crate::operation::OperationRecord>,
    pub commands: Vec<PendingCommand>,
    pub media: BTreeMap<String, StreamCursor>,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitReason {
    Deadline,
    ModelTurns,
    RuntimeToolCalls,
    TotalTokens,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub max_model_turns: u64,
    pub max_runtime_tool_calls: u64,
    pub max_inflight_operations: usize,
    pub max_control_commands: usize,
    pub max_session_events: usize,
    pub max_media_streams: usize,
    pub max_media_chunk_bytes: usize,
    pub max_buffered_media_bytes: usize,
    pub max_operation_concurrency: usize,
    pub max_observer_events: usize,
    pub max_total_tokens: Option<u64>,
    pub elapsed_ms: Option<u64>,
    pub commit_timeout_ms: u64,
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        if self.max_inflight_operations == 0
            || self.max_control_commands == 0
            || self.max_session_events == 0
            || self.max_observer_events == 0
            || self.max_media_streams == 0
            || [
                self.max_control_commands,
                self.max_session_events,
                self.max_observer_events,
            ]
            .iter()
            .any(|size| *size > u32::MAX as usize)
            || self.max_buffered_media_bytes > u32::MAX as usize
            || self.max_media_chunk_bytes == 0
            || self.max_buffered_media_bytes < self.max_media_chunk_bytes
            || self.max_operation_concurrency == 0
            || self.commit_timeout_ms == 0
        {
            Err(Error::Invalid(
                "invalid queue, media, concurrency or commit limits".into(),
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
    pub model_turns: u64,
    pub runtime_tool_calls: u64,
    pub usage: Usage,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlAction {
    Finished,
    Failed,
    Limited,
    Suspended,
    Cancelled,
}
impl ControlAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Finished => "finished",
            Self::Failed => "failed",
            Self::Limited => "limited",
            Self::Suspended => "suspended",
            Self::Cancelled => "cancelled",
        }
    }
}
impl std::fmt::Display for ControlAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Fact {
    Started,
    Attached,
    Session {
        session_id: String,
        sequence: u64,
    },
    Operation {
        operation_id: String,
        state: crate::operation::OperationState,
    },
    Command {
        command_id: String,
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
    Media {
        stream_id: String,
        sequence: u64,
    },
}
impl Fact {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Attached => "attached",
            Self::Session { .. } => "session",
            Self::Operation { .. } => "operation",
            Self::Command { .. } => "command",
            Self::ConversationInsert { .. } => "conversation_insert",
            Self::HistoryRewrite { .. } => "history_rewrite",
            Self::Control { .. } => "control",
            Self::Media { .. } => "media",
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
    pub active: ActiveState,
    pub metrics: Metrics,
    pub fact: Fact,
}
mod validation;
pub fn validate_history(history: &History) -> Result<()> {
    history.validate()
}

#[derive(Debug, Clone)]
pub struct HistoryRewrite {
    pub entries: Vec<HistoryEntry>,
    pub reason: String,
}
pub trait HistoryReducer: Send + Sync {
    fn reduce(&self, checkpoint: Arc<Checkpoint>) -> BoxFuture<'_, Result<Option<HistoryRewrite>>>;
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventData {
    ObservationGap {
        count: usize,
    },
    OperationChanged {
        operation_id: String,
        state: crate::operation::OperationState,
    },
    ModelDelta {
        delta: ModelDelta,
    },
    OperationProgress {
        operation_id: String,
        value: Value,
    },
    CheckpointCommitted {
        checkpoint_id: String,
        revision: u64,
        state: StateKind,
        fact: Fact,
    },
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
