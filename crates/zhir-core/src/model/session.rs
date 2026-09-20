use super::{ModelContext, ModelDelta, ModelRequest, Usage};
use crate::{
    BoxFuture, Result,
    message::{Message, Output},
    operation::{CallRef, OperationEvent, RecoveryRef},
    profile::{EffectiveProfile, RequestProfile},
    resource::MediaPorts,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Stable adapter selection. This is not evidence of remote session recovery.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelBinding {
    pub adapter: String,
    pub data: Value,
}

#[derive(Clone)]
pub struct SessionOpen {
    pub binding: Option<ModelBinding>,
    pub session_id: String,
    pub after_sequence: Option<u64>,
    pub output_epoch: u64,
    pub context_revision: u64,
    pub input_position: u64,
    pub profile_revision: u64,
    pub mode: crate::run::RunMode,
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
    Generate {
        generation_id: String,
        context_revision: u64,
        input_position: u64,
        profile_revision: u64,
    },
    Append {
        entry: crate::run::HistoryEntry,
        context_revision: u64,
        input_position: u64,
        source: AppendSource,
    },
    ReplaceContext {
        entries: Vec<crate::run::HistoryEntry>,
        context_revision: u64,
    },
    UpdateProfile {
        revision: u64,
        profile: RequestProfile,
    },
    InterruptOutput {
        generation_id: String,
        output_epoch: u64,
    },
    /// Materialize buffered input without ending the session. Acknowledgement
    /// confirms the provider's flush boundary, not device playback completion.
    FlushInput,
    /// Enable or pause remote audio input processing without closing the input port.
    SetInputAudio {
        enabled: bool,
    },
    SealUserInput,
    Close,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseStatus {
    Completed,
    RequiresResults,
    Continuation,
    Failed,
    Cancelled,
    Incomplete,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppendSource {
    Submitted,
    Accepted,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Acknowledgement {
    Projection,
    Transport,
    Provider,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionEventBody {
    Ready {
        context_revision: u64,
    },
    ResponseStarted {
        generation_id: String,
        input_position: u64,
    },
    /// A complete conversation message with its own identity, independent of execution turns.
    ConversationItem {
        item_id: String,
        message: Message,
    },
    Acknowledged {
        command_id: String,
        recovery: Option<RecoveryRef>,
        level: Acknowledgement,
    },
    Delta {
        generation_id: Option<String>,
        delta: ModelDelta,
    },
    Output {
        generation_id: Option<String>,
        item_id: String,
        caller_id: String,
        output: Output,
    },
    Operation {
        origin: CallRef,
        event: OperationEvent,
    },
    ResponseFinished {
        generation_id: String,
        input_position: u64,
        response_status: ResponseStatus,
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
    Closed {
        reason: String,
        provider_data: Value,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionEvent {
    pub sequence: u64,
    pub body: SessionEventBody,
}

/// Shareable control handle for the bound model session.
pub trait SessionControl: Send + Sync {
    /// Identity to persist independently of optional remote recovery credentials.
    fn binding(&self) -> Option<ModelBinding> {
        None
    }
    fn capabilities(&self) -> &super::CapabilitySet;
    fn negotiate(&self, request: &ModelRequest) -> Result<crate::profile::NegotiatedProfile>;
    /// Submit an operation intent. Success does not confirm provider execution;
    /// the matching acknowledgement arrives through `SessionEvents`.
    fn submit(&self, command: SessionCommand) -> BoxFuture<'_, Result<()>>;
}
/// Ordered session events, including content deltas, complete outputs and closure.
/// Terminal errors are delivered after already admitted events.
pub trait SessionEvents: Send {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>>;
}
/// Independently owned control, event and optional media endpoints.
pub struct ModelSession {
    pub control: Arc<dyn SessionControl>,
    pub events: Box<dyn SessionEvents>,
    pub media: MediaPorts,
}
