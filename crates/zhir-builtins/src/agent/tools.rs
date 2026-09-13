use super::{AgentBackend, AgentSnapshot};
use crate::common::spec;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use zhir_core::{
    Result,
    error::Error,
    message::Message,
    run::{Checkpoint, State},
    tool::RuntimeTool,
};

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct Start {
    #[schemars(length(min = 1))]
    key: String,
    #[schemars(length(min = 1))]
    prompt: String,
}
#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct Id {
    #[schemars(length(min = 1))]
    id: String,
}
#[derive(Clone, Copy)]
enum Operation {
    Get,
    Wait,
    Cancel,
}
impl Operation {
    const fn name(self) -> &'static str {
        match self {
            Self::Get => "agent_get",
            Self::Wait => "agent_wait",
            Self::Cancel => "agent_cancel",
        }
    }
}
pub fn tools(backend: Arc<dyn AgentBackend>) -> Result<Vec<Arc<dyn RuntimeTool>>> {
    let mut runtime_tools: Vec<Arc<dyn RuntimeTool>> = Vec::new();
    let start_backend = backend.clone();
    runtime_tools.push(Arc::new(zhir_tools::function::structured(
        spec::<Start>(
            "agent_start",
            "Start an idempotent child Agent task.",
            false,
        ),
        move |a: Start, context| {
            let backend = start_backend.clone();
            async move {
                let snapshot = backend.start_or_get(a.key, a.prompt, context.run).await?;
                Ok(zhir_tools::reply::json(
                    serde_json::to_value(snapshot).map_err(|e| Error::Protocol(e.to_string()))?,
                ))
            }
        },
    )?));
    for operation in [Operation::Get, Operation::Wait, Operation::Cancel] {
        let backend = backend.clone();
        runtime_tools.push(Arc::new(zhir_tools::function::structured(
            spec::<Id>(
                operation.name(),
                "Inspect, wait for or cancel a child Agent.",
                false,
            ),
            move |a: Id, context| {
                let backend = backend.clone();
                async move {
                    let snapshot = if matches!(operation, Operation::Cancel) {
                        backend.cancel(a.id, context.run).await?
                    } else {
                        backend.get(a.id, context.run).await?
                    };
                    let value = serde_json::to_value(&snapshot)
                        .map_err(|e| Error::Protocol(e.to_string()))?;
                    if matches!(operation, Operation::Wait) && !snapshot.terminal() {
                        Ok(zhir_tools::reply::waiting(
                            snapshot.id,
                            value,
                            Operation::Wait.name(),
                        ))
                    } else {
                        Ok(zhir_tools::reply::json(value))
                    }
                }
            },
        )?));
    }
    Ok(runtime_tools)
}
pub fn response(checkpoint: &Checkpoint, snapshot: AgentSnapshot) -> Result<Message> {
    let State::Suspended { suspension, .. } = &checkpoint.state else {
        return Err(Error::Invalid("agent response requires suspension".into()));
    };
    if suspension.source != Operation::Wait.name()
        || suspension.wait_id.as_deref() != Some(&snapshot.id)
    {
        return Err(Error::Invalid("agent response selector mismatch".into()));
    }
    if !snapshot.terminal() {
        return Err(Error::Invalid(
            "agent response requires settled child".into(),
        ));
    }
    let value: Value =
        serde_json::to_value(snapshot).map_err(|e| Error::Protocol(e.to_string()))?;
    Ok(Message::external(value.to_string()))
}
