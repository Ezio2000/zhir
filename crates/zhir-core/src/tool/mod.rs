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
        if self.parallel && (!self.read_only || !self.idempotent) {
            Err(Error::Invalid(
                "parallel execution requires read-only and idempotent facts".into(),
            ))
        } else {
            Ok(())
        }
    }
    pub fn parallel_safe(&self) -> bool {
        self.parallel && self.read_only && self.idempotent
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
    Accepted,
    Waiting,
}
impl RuntimeToolOutcomeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Accepted => "accepted",
            Self::Waiting => "waiting",
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
    Accepted {
        task_id: String,
        content: Vec<Content>,
        structured: Value,
    },
    Waiting {
        wait_id: String,
        content: Vec<Content>,
        structured: Value,
    },
}
impl RuntimeToolOutcome {
    pub fn structured(&self) -> Option<&Value> {
        match self {
            Self::Success { structured, .. }
            | Self::Accepted { structured, .. }
            | Self::Waiting { structured, .. } => Some(structured),
            Self::Failure { .. } => None,
        }
    }
    pub fn content(&self) -> Vec<Content> {
        match self {
            Self::Success { content, .. }
            | Self::Accepted { content, .. }
            | Self::Waiting { content, .. } => content.clone(),
            Self::Failure { error } => vec![Content::text(&error.message)],
        }
    }
    pub fn kind(&self) -> RuntimeToolOutcomeKind {
        match self {
            Self::Success { .. } => RuntimeToolOutcomeKind::Success,
            Self::Failure { .. } => RuntimeToolOutcomeKind::Failure,
            Self::Accepted { .. } => RuntimeToolOutcomeKind::Accepted,
            Self::Waiting { .. } => RuntimeToolOutcomeKind::Waiting,
        }
    }
    pub fn validate(&self) -> Result<()> {
        if matches!(self, Self::Waiting {wait_id,..} if wait_id.is_empty())
            || matches!(self, Self::Accepted {task_id,..} if task_id.is_empty())
        {
            return Err(Error::Invalid("empty tool task/wait identity".into()));
        }
        for content in self.content() {
            content.validate()?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeToolResult {
    pub outcome: RuntimeToolOutcome,
    pub suspension: Option<Suspension>,
}
impl RuntimeToolResult {
    pub fn json(value: Value) -> Self {
        let text = value
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| value.to_string());
        Self {
            outcome: RuntimeToolOutcome::Success {
                content: vec![Content::text(text)],
                structured: value,
            },
            suspension: None,
        }
    }
    pub fn failure(error: Failure) -> Self {
        Self {
            outcome: RuntimeToolOutcome::Failure { error },
            suspension: None,
        }
    }
    pub fn waiting(wait_id: impl Into<String>, value: Value, source: impl Into<String>) -> Self {
        let wait_id = wait_id.into();
        Self {
            outcome: RuntimeToolOutcome::Waiting {
                wait_id: wait_id.clone(),
                content: vec![Content::text(value.to_string())],
                structured: value,
            },
            suspension: Some(Suspension {
                reason: "waiting".into(),
                source: source.into(),
                wait_id: Some(wait_id),
                metadata: Default::default(),
            }),
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.outcome.validate()?;
        match (&self.outcome, &self.suspension) {
            (RuntimeToolOutcome::Waiting { wait_id, .. }, Some(s))
                if s.wait_id.as_ref() == Some(wait_id) =>
            {
                s.validate()
            }
            (RuntimeToolOutcome::Waiting { .. }, _) | (_, Some(_)) => Err(Error::Invalid(
                "waiting outcome and suspension must agree".into(),
            )),
            _ => Ok(()),
        }
    }
}
pub trait ProgressSink: Send + Sync {
    fn emit(&self, value: Value) -> BoxFuture<'_, Result<()>>;
}
#[derive(Clone)]
pub struct RuntimeToolContext {
    pub run: RunContext,
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
    fn invoke(
        &self,
        call: RuntimeToolCall,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<RuntimeToolResult>>;
}
pub trait RuntimeToolBinding: Send + Sync {
    fn spec(&self) -> &RuntimeToolSpec;
    fn invoke(&self, context: RuntimeToolContext) -> BoxFuture<'_, Result<RuntimeToolResult>>;
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
pub struct RuntimeToolBatch {
    pub calls: Vec<RuntimeToolCall>,
    pub parallel: bool,
}
pub trait BatchPolicy: Send + Sync {
    fn select(
        &self,
        candidates: &[RuntimeToolCall],
        specs: &std::collections::BTreeMap<String, RuntimeToolSpec>,
    ) -> Result<RuntimeToolBatch>;
}
