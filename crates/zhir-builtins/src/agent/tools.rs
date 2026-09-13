use super::AgentBackend;
use crate::common::spec;
use serde::Deserialize;
use std::sync::Arc;
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    operation::{OperationRecord, ToolExecution},
    tool::{RuntimeTool, RuntimeToolCall, RuntimeToolContext, RuntimeToolSpec},
};

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct Start {
    #[schemars(length(min = 1))]
    prompt: String,
}
struct AgentTool {
    backend: Arc<dyn AgentBackend>,
    spec: RuntimeToolSpec,
}
impl RuntimeTool for AgentTool {
    fn spec(&self) -> &RuntimeToolSpec {
        &self.spec
    }
    fn start(
        &self,
        call: RuntimeToolCall,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<ToolExecution>> {
        Box::pin(async move {
            let args: Start = serde_json::from_value(match call.input {
                zhir_core::tool::RuntimeToolInput::Structured(value) => value,
                _ => return Err(Error::Invalid("agent_run requires structured input".into())),
            })
            .map_err(|e| Error::Invalid(e.to_string()))?;
            if args.prompt.is_empty() {
                return Err(Error::Invalid("child prompt is empty".into()));
            }
            self.backend
                .start(args.prompt, context)
                .await
                .map(ToolExecution::Active)
        })
    }
    fn recover(
        &self,
        operation: OperationRecord,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<ToolExecution>> {
        Box::pin(async move {
            self.backend
                .recover(operation, context)
                .await
                .map(ToolExecution::Active)
        })
    }
}
pub fn tools(backend: Arc<dyn AgentBackend>) -> Result<Vec<Arc<dyn RuntimeTool>>> {
    Ok(vec![Arc::new(AgentTool {
        backend,
        spec: spec::<Start>(
            "agent_run",
            "Run a child Agent; progress, replies and cancellation use its operation handle.",
            false,
        ),
    })])
}
