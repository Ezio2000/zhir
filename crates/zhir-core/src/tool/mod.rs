use crate::{
    BoxFuture, Cancellation, Result,
    error::{Error, Failure},
    message::Content,
    run::{RunContext, Suspension},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RuntimeToolInput {
    Structured(Value),
    Freeform(String),
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeToolCall {
    pub id: String,
    pub name: String,
    pub input: RuntimeToolInput,
}
impl RuntimeToolCall {
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.name.is_empty() {
            return Err(Error::Invalid("empty tool identity".into()));
        }
        if matches!(&self.input, RuntimeToolInput::Structured(v) if !v.is_object()) {
            return Err(Error::Invalid(
                "structured arguments must be an object".into(),
            ));
        }
        Ok(())
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InputSpec {
    Structured { schema: Value },
    Freeform { format: Option<Value> },
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Execution {
    pub parallel: bool,
    pub read_only: bool,
    pub idempotent: bool,
}
impl Execution {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
    pub fn parallel_safe(&self) -> bool {
        self.parallel
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeToolSpec {
    pub name: String,
    pub description: String,
    pub input: InputSpec,
    pub output_schema: Option<Value>,
    #[serde(default)]
    pub execution: Execution,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeToolOutcomeKind {
    Success,
    Failure,
    Cancelled,
}
impl RuntimeToolOutcomeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Cancelled => "cancelled",
        }
    }
}
impl std::fmt::Display for RuntimeToolOutcomeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeToolOutcome {
    Success {
        content: Vec<Content>,
        structured: Value,
    },
    Failure {
        error: Failure,
    },
    Cancelled {
        reason: String,
    },
}
impl RuntimeToolOutcome {
    pub fn structured(&self) -> Option<&Value> {
        if let Self::Success { structured, .. } = self {
            Some(structured)
        } else {
            None
        }
    }
    pub fn content(&self) -> &[Content] {
        if let Self::Success { content, .. } = self {
            content
        } else {
            &[]
        }
    }
    pub fn kind(&self) -> RuntimeToolOutcomeKind {
        match self {
            Self::Success { .. } => RuntimeToolOutcomeKind::Success,
            Self::Failure { .. } => RuntimeToolOutcomeKind::Failure,
            Self::Cancelled { .. } => RuntimeToolOutcomeKind::Cancelled,
        }
    }
    pub fn validate(&self) -> Result<()> {
        for content in self.content() {
            content.validate()?;
        }
        Ok(())
    }
}
pub trait ProgressSink: Send + Sync {
    fn emit(&self, value: Value) -> BoxFuture<'_, Result<()>>;
}
#[derive(Clone)]
pub struct RuntimeToolContext {
    pub run: RunContext,
    pub operation_id: String,
    pub cancellation: Cancellation,
    pub progress: Option<Arc<dyn ProgressSink>>,
}
impl RuntimeToolContext {
    pub async fn emit_progress(&self, value: Value) -> Result<()> {
        self.cancellation.check()?;
        if let Some(sink) = &self.progress {
            sink.emit(value).await?;
        }
        Ok(())
    }
}
pub trait RuntimeTool: Send + Sync {
    fn spec(&self) -> &RuntimeToolSpec;
    fn start(
        &self,
        call: RuntimeToolCall,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<crate::operation::ToolExecution>>;
    fn recover(
        &self,
        _record: crate::operation::OperationRecord,
        _context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<crate::operation::ToolExecution>> {
        Box::pin(async {
            Err(Error::Protocol(
                "operation cannot be recovered by this tool".into(),
            ))
        })
    }
}
pub trait RuntimeToolBinding: Send + Sync {
    fn spec(&self) -> &RuntimeToolSpec;
    fn start(
        &self,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<crate::operation::ToolExecution>>;
    fn recover(
        &self,
        record: crate::operation::OperationRecord,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<crate::operation::ToolExecution>>;
}
pub trait RuntimeToolCatalog: Send + Sync {
    fn specs(&self) -> Vec<RuntimeToolSpec>;
    fn bind(&self, call: &RuntimeToolCall) -> Result<Arc<dyn RuntimeToolBinding>>;
}
pub mod catalog;
pub use catalog::{CatalogContext, RuntimeToolSelection};
pub trait RuntimeToolCatalogProvider: Send + Sync {
    fn open_catalog(
        &self,
        context: CatalogContext,
    ) -> BoxFuture<'_, Result<Arc<dyn RuntimeToolCatalog>>>;
}

#[derive(Debug, Clone)]
pub struct ApprovalRequest {
    pub call: RuntimeToolCall,
    pub spec: RuntimeToolSpec,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecisionKind {
    Allow,
    Deny,
    Suspend,
}
impl ApprovalDecisionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
            Self::Suspend => "suspend",
        }
    }
}
impl std::fmt::Display for ApprovalDecisionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
#[derive(Debug, Clone)]
pub enum ApprovalDecision {
    Allow,
    Deny(String),
    Suspend(Suspension),
}
impl ApprovalDecision {
    pub fn kind(&self) -> ApprovalDecisionKind {
        match self {
            Self::Allow => ApprovalDecisionKind::Allow,
            Self::Deny(_) => ApprovalDecisionKind::Deny,
            Self::Suspend(_) => ApprovalDecisionKind::Suspend,
        }
    }
}
pub trait ApprovalPolicy: Send + Sync {
    fn decide(
        &self,
        requests: Vec<ApprovalRequest>,
        context: RunContext,
    ) -> BoxFuture<'_, Result<Vec<ApprovalDecision>>>;
}
#[derive(Debug, Clone)]
pub struct Admission {
    pub calls: Vec<RuntimeToolCall>,
    pub parallel: bool,
}
pub trait SchedulingPolicy: Send + Sync {
    fn select(
        &self,
        candidates: &[RuntimeToolCall],
        specs: &std::collections::BTreeMap<String, RuntimeToolSpec>,
    ) -> Result<Admission>;
}
