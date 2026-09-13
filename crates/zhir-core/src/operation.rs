//! Durable work identity and lifecycle. Only kernel commits these records.
use crate::{BoxFuture, Result, error::Error, tool::RuntimeToolOutcome};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallRef {
    pub session_id: String,
    pub turn_id: String,
    pub caller_id: String,
    pub call_id: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Queued,
    Running,
    Waiting,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    Unknown,
}
impl OperationState {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryRef {
    pub adapter: String,
    pub data: Value,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperationOwner {
    RuntimeTool { name: String },
    Provider { provider: String },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRecord {
    pub id: String,
    pub origin: CallRef,
    pub owner: OperationOwner,
    pub state: OperationState,
    pub call_entry: usize,
    pub result_entry: Option<usize>,
    pub recovery: Option<RecoveryRef>,
    pub last_sequence: Option<u64>,
    pub last_update: Option<OperationUpdate>,
    pub wait: Option<Value>,
}
impl OperationRecord {
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty()
            || self.origin.session_id.is_empty()
            || self.origin.turn_id.is_empty()
            || self.origin.caller_id.is_empty()
            || self.origin.call_id.is_empty()
        {
            return Err(Error::Invalid("empty operation identity".into()));
        }
        if self.last_sequence.is_some() != self.last_update.is_some() {
            return Err(Error::Invalid(
                "operation cursor requires its acknowledged update".into(),
            ));
        }
        if self.state.terminal() != self.result_entry.is_some() {
            return Err(Error::Invalid(
                "operation terminal state and result disagree".into(),
            ));
        }
        if self.recovery.as_ref().is_some_and(|r| r.adapter.is_empty()) {
            return Err(Error::Invalid("empty recovery adapter".into()));
        }
        Ok(())
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperationUpdate {
    Running {
        recovery: Option<RecoveryRef>,
    },
    Waiting {
        prompt: Value,
        recovery: Option<RecoveryRef>,
    },
    Progress {
        value: Value,
    },
    Finished {
        outcome: RuntimeToolOutcome,
    },
    Unknown {
        reason: String,
    },
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationEvent {
    pub sequence: u64,
    pub update: OperationUpdate,
}

pub trait OperationControl: Send + Sync {
    fn cancel(&self) -> BoxFuture<'_, Result<()>>;
    fn reply(&self, value: Value) -> BoxFuture<'_, Result<()>>;
}
pub trait OperationEvents: Send {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<OperationEvent>>>;
}
pub struct OperationHandle {
    pub recovery: Option<RecoveryRef>,
    pub control: std::sync::Arc<dyn OperationControl>,
    pub events: Box<dyn OperationEvents>,
}
pub enum ToolExecution {
    Finished(RuntimeToolOutcome),
    Active(OperationHandle),
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecoveryResolution {
    Attach {
        operation_id: String,
        reference: RecoveryRef,
    },
    Complete {
        operation_id: String,
        outcome: RuntimeToolOutcome,
    },
    Abandon {
        operation_id: String,
        reason: String,
    },
}

impl From<&RuntimeToolOutcome> for OperationState {
    fn from(outcome: &RuntimeToolOutcome) -> Self {
        match outcome {
            RuntimeToolOutcome::Success { .. } => Self::Succeeded,
            RuntimeToolOutcome::Failure { .. } => Self::Failed,
            RuntimeToolOutcome::Cancelled { .. } => Self::Cancelled,
        }
    }
}
