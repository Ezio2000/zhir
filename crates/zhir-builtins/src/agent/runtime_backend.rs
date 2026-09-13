//! Bounded in-process child tasks, each executed by the existing kernel.
use super::{AgentBackend, AgentSnapshot, AgentStatus};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::{Semaphore, watch};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    message::Message,
    run::{RunContext, State},
};
use zhir_kernel::{Runtime, control::ControlHandle};

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
    permits: Arc<Semaphore>,
    system_prompt: String,
    state: Mutex<StateStore>,
}
impl InMemoryAgentBackend {
    pub fn new(
        runtime: Runtime,
        system_prompt: impl Into<String>,
        max_running: usize,
    ) -> Result<Self> {
        if max_running == 0 || max_running > Semaphore::MAX_PERMITS {
            return Err(Error::Invalid(
                "invalid child agent concurrency limit".into(),
            ));
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(max_running)),
            runtime,
            system_prompt: system_prompt.into(),
            state: Mutex::new(StateStore {
                agents: HashMap::new(),
                keys: HashMap::new(),
            }),
        })
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
            // Admission is bounded; idempotent lookups above consume no new slot.
            let permit = self.permits.clone().try_acquire_owned().map_err(|_| {
                Error::RuntimeTool(zhir_core::error::Failure::new(
                    "agent_capacity",
                    "child agent concurrency limit reached",
                ))
            })?;
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
                status: AgentStatus::Running,
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
                let _permit = permit;
                let completed = match invocation.result().await {
                    Ok(checkpoint) => AgentSnapshot {
                        id,
                        status: AgentStatus::from(checkpoint.checkpoint().state.kind()),
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
                            AgentStatus::Cancelled
                        } else {
                            AgentStatus::Failed
                        },
                        content: vec![],
                        error: Some(error.to_string()),
                    },
                };
                drop(_permit);
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
