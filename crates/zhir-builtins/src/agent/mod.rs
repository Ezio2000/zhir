use crate::common::spec;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    message::{Content, Message},
    run::{Checkpoint, RunContext, State},
    tool::{RuntimeTool, RuntimeToolResult},
};
use zhir_kernel::{Runtime, control::ControlHandle};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSnapshot {
    pub id: String,
    pub status: String,
    pub content: Vec<Content>,
    pub error: Option<String>,
}
impl AgentSnapshot {
    pub fn terminal(&self) -> bool {
        matches!(
            self.status.as_str(),
            "completed" | "failed" | "limited" | "cancelled" | "suspended"
        )
    }
}
pub trait AgentBackend: Send + Sync {
    fn start_or_get(
        &self,
        key: String,
        prompt: String,
        requester: RunContext,
    ) -> BoxFuture<'_, Result<AgentSnapshot>>;
    fn get(&self, id: String, requester: RunContext) -> BoxFuture<'_, Result<AgentSnapshot>>;
    fn wait(&self, id: String, requester: RunContext) -> BoxFuture<'_, Result<AgentSnapshot>>;
    fn cancel(&self, id: String, requester: RunContext) -> BoxFuture<'_, Result<AgentSnapshot>>;
}
struct Record {
    owner: String,
    prompt: String,
    snapshot: watch::Receiver<AgentSnapshot>,
    control: ControlHandle,
}
struct StateStore {
    agents: HashMap<String, Record>,
    keys: HashMap<(String, String), String>,
}
pub struct InMemoryAgentBackend {
    runtime: Runtime,
    system_prompt: String,
    state: Mutex<StateStore>,
}
impl InMemoryAgentBackend {
    pub fn new(runtime: Runtime, system_prompt: impl Into<String>) -> Self {
        Self {
            runtime,
            system_prompt: system_prompt.into(),
            state: Mutex::new(StateStore {
                agents: HashMap::new(),
                keys: HashMap::new(),
            }),
        }
    }
    fn record(
        &self,
        id: &str,
        requester: &RunContext,
    ) -> Result<(watch::Receiver<AgentSnapshot>, ControlHandle)> {
        let state = self.state.lock().expect("agent state lock");
        let record = state
            .agents
            .get(id)
            .ok_or_else(|| Error::Invalid("unknown agent".into()))?;
        if record.owner != requester.run_id {
            return Err(Error::Invalid("agent belongs to another parent run".into()));
        }
        Ok((record.snapshot.clone(), record.control.clone()))
    }
}
impl Drop for InMemoryAgentBackend {
    fn drop(&mut self) {
        for record in self
            .state
            .get_mut()
            .expect("agent state lock")
            .agents
            .values()
        {
            record.control.cancel();
        }
    }
}
impl AgentBackend for InMemoryAgentBackend {
    fn start_or_get(
        &self,
        key: String,
        prompt: String,
        requester: RunContext,
    ) -> BoxFuture<'_, Result<AgentSnapshot>> {
        Box::pin(async move {
            if key.is_empty() || prompt.is_empty() {
                return Err(Error::Invalid("agent key and prompt are required".into()));
            }
            let mut state = self.state.lock().expect("agent state lock");
            let identity = (requester.run_id.clone(), key.clone());
            if let Some(id) = state.keys.get(&identity) {
                let record = state.agents.get(id).expect("indexed agent");
                if record.prompt != prompt {
                    return Err(Error::Invalid(
                        "agent key reused with different prompt".into(),
                    ));
                }
                return Ok(record.snapshot.borrow().clone());
            }
            let context = RunContext {
                parent_run_id: Some(requester.run_id.clone()),
                parent_runtime_tool_call_id: Some(key),
                deadline_at_ms: requester.deadline_at_ms,
                ..zhir_kernel::defaults::context()
            };
            let id = context.run_id.clone();
            let mut messages = Vec::new();
            if !self.system_prompt.is_empty() {
                messages.push(Message::system(&self.system_prompt));
            }
            messages.push(Message::user(&prompt));
            let mut invocation = self
                .runtime
                .start(zhir_kernel::RunRequest::new(messages).context(context))?;
            let control = invocation.control();
            let snapshot = AgentSnapshot {
                id: id.clone(),
                status: "running".into(),
                content: vec![],
                error: None,
            };
            let (sender, receiver) = watch::channel(snapshot.clone());
            state.keys.insert(identity, id.clone());
            state.agents.insert(
                id.clone(),
                Record {
                    owner: requester.run_id,
                    prompt,
                    snapshot: receiver,
                    control,
                },
            );
            tokio::spawn(async move {
                let completed = match invocation.result().await {
                    Ok(checkpoint) => AgentSnapshot {
                        id,
                        status: checkpoint.checkpoint().state.kind().into(),
                        content: if let State::Completed { content } =
                            &checkpoint.checkpoint().state
                        {
                            content.clone()
                        } else {
                            vec![]
                        },
                        error: if let State::Failed { error } = &checkpoint.checkpoint().state {
                            Some(error.message.clone())
                        } else {
                            None
                        },
                    },
                    Err(error) => AgentSnapshot {
                        id,
                        status: if matches!(error.error, Error::Cancelled) {
                            "cancelled"
                        } else {
                            "failed"
                        }
                        .into(),
                        content: vec![],
                        error: Some(error.to_string()),
                    },
                };
                sender.send_replace(completed);
            });
            Ok(snapshot)
        })
    }
    fn get(&self, id: String, requester: RunContext) -> BoxFuture<'_, Result<AgentSnapshot>> {
        Box::pin(async move { Ok(self.record(&id, &requester)?.0.borrow().clone()) })
    }
    fn wait(&self, id: String, requester: RunContext) -> BoxFuture<'_, Result<AgentSnapshot>> {
        Box::pin(async move {
            let (mut receiver, _) = self.record(&id, &requester)?;
            loop {
                let current = receiver.borrow().clone();
                if current.terminal() {
                    return Ok(current);
                }
                receiver.changed().await.map_err(|_| {
                    Error::Protocol("agent worker closed without final result".into())
                })?;
            }
        })
    }
    fn cancel(&self, id: String, requester: RunContext) -> BoxFuture<'_, Result<AgentSnapshot>> {
        Box::pin(async move {
            let (_, control) = self.record(&id, &requester)?;
            control.cancel();
            self.wait(id, requester).await
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    key: String,
    prompt: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Id {
    id: String,
}
pub fn tools(backend: Arc<dyn AgentBackend>) -> Result<Vec<Arc<dyn RuntimeTool>>> {
    let mut runtime_tools: Vec<Arc<dyn RuntimeTool>> = Vec::new();
    let start_backend = backend.clone();
    runtime_tools.push(Arc::new(zhir_tools::function::structured(spec("agent_start","Start an idempotent child Agent task.",json!({"type":"object","required":["key","prompt"],"properties":{"key":{"type":"string","minLength":1},"prompt":{"type":"string","minLength":1}},"additionalProperties":false}),false),move |a:Start,context| {let backend=start_backend.clone();async move {let snapshot=backend.start_or_get(a.key,a.prompt,context.run).await?;Ok(RuntimeToolResult::json(serde_json::to_value(snapshot).map_err(|e|Error::Protocol(e.to_string()))?))}})?));
    for name in ["agent_get", "agent_wait", "agent_cancel"] {
        let backend = backend.clone();
        runtime_tools.push(Arc::new(zhir_tools::function::structured(spec(name,"Inspect, wait for or cancel a child Agent.",json!({"type":"object","required":["id"],"properties":{"id":{"type":"string","minLength":1}},"additionalProperties":false}),false),move |a:Id,context| {let backend=backend.clone();async move {
            let snapshot=if name=="agent_cancel" {backend.cancel(a.id,context.run).await?} else {backend.get(a.id,context.run).await?};
            let value=serde_json::to_value(&snapshot).map_err(|e|Error::Protocol(e.to_string()))?;
            if name=="agent_wait" && !snapshot.terminal() {Ok(RuntimeToolResult::waiting(snapshot.id,value,"agent_wait"))} else {Ok(RuntimeToolResult::json(value))}
        }})?));
    }
    Ok(runtime_tools)
}
pub fn response(checkpoint: &Checkpoint, snapshot: AgentSnapshot) -> Result<Message> {
    let State::Suspended { suspension, .. } = &checkpoint.state else {
        return Err(Error::Invalid("agent response requires suspension".into()));
    };
    if suspension.source != "agent_wait" || suspension.wait_id.as_deref() != Some(&snapshot.id) {
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
