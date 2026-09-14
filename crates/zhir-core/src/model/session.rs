use super::{ModelContext, ModelDelta, ModelRequest, Usage};
use crate::{
    BoxFuture, Result,
    message::{Message, Output},
    operation::{CallRef, OperationEvent, RecoveryRef},
    profile::{EffectiveProfile, RequestProfile},
    resource::{MediaReceiver, MediaSender},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone)]
pub struct SessionOpen {
    pub session_id: String,
    pub after_sequence: Option<u64>,
    pub output_epoch: u64,
    pub limits: crate::run::Limits,
    pub request: ModelRequest,
    pub recovery: Option<RecoveryRef>,
    pub context: ModelContext,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCommand {
    pub id: String,
    pub body: SessionCommandBody,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionCommandBody {
    DelegationContext {
        operation_id: String,
        origin: CallRef,
        content: Vec<crate::message::Content>,
    },
    DelegationResult {
        operation_id: String,
        origin: CallRef,
        outcome: crate::operation::OperationOutcome,
    },
    StartTurn {
        turn_id: String,
        request: Box<ModelRequest>,
    },
    Input {
        message: Message,
    },
    ToolResult {
        operation_id: String,
        origin: CallRef,
        outcome: crate::operation::OperationOutcome,
    },
    UpdateProfile {
        revision: u64,
        profile: RequestProfile,
    },
    InterruptOutput {
        turn_id: String,
    },
    EndInput,
    Close,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnDisposition {
    Finished,
    AwaitingTools,
    Continue,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionEventBody {
    /// A complete conversation message with its own identity, independent of execution turns.
    ConversationItem {
        item_id: String,
        message: Message,
    },
    Acknowledged {
        command_id: String,
        recovery: Option<RecoveryRef>,
    },
    Delta {
        turn_id: String,
        delta: ModelDelta,
    },
    Output {
        turn_id: String,
        item_id: String,
        caller_id: String,
        output: Output,
    },
    Operation {
        origin: CallRef,
        event: OperationEvent,
    },
    TurnFinished {
        turn_id: String,
        disposition: TurnDisposition,
        usage: Usage,
        model_id: Option<String>,
        response_id: Option<String>,
        finish_reason: Option<String>,
        provider_data: Value,
        effective: EffectiveProfile,
    },
    Recovery {
        reference: RecoveryRef,
    },
    Closed,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionEvent {
    pub sequence: u64,
    pub body: SessionEventBody,
}

pub trait SessionSender: Send + Sync {
    fn capabilities(&self) -> &super::CapabilitySet;
    fn negotiate(&self, request: &ModelRequest) -> Result<crate::profile::NegotiatedProfile>;
    fn send(&self, command: SessionCommand) -> BoxFuture<'_, Result<()>>;
}
pub trait SessionReceiver: Send {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>>;
}
pub struct ModelSession {
    pub input: Arc<dyn SessionSender>,
    pub output: Box<dyn SessionReceiver>,
    pub media_input: Option<Arc<dyn MediaSender>>,
    pub media_output: Option<Box<dyn MediaReceiver>>,
}
