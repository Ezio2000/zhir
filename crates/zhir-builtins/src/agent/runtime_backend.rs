//! Bounded child invocations; durable state belongs exclusively to the kernel.
use super::AgentBackend;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};
use zhir_core::{
    BoxFuture, Result,
    error::{Error, Failure},
    message::Message,
    operation::*,
    run::{RunContext, State},
    tool::{RuntimeToolContext, RuntimeToolOutcome},
};
use zhir_kernel::{Invocation, ResumeRequest, RunRequest, Runtime, control::ControlHandle};

pub struct RuntimeAgentBackend {
    runtime: Runtime,
    permits: Arc<Semaphore>,
    system_prompt: String,
}
impl RuntimeAgentBackend {
    pub fn new(
        runtime: Runtime,
        system_prompt: impl Into<String>,
        max_running: usize,
    ) -> Result<Self> {
        if max_running == 0 || max_running > Semaphore::MAX_PERMITS {
            return Err(Error::Invalid("invalid child concurrency".into()));
        }
        Ok(Self {
            runtime,
            permits: Arc::new(Semaphore::new(max_running)),
            system_prompt: system_prompt.into(),
        })
    }
    fn child_id(context: &RuntimeToolContext) -> String {
        format!("{}:child:{}", context.run.run_id, context.operation_id)
    }
    fn attach(&self, invocation: Invocation, id: String, sequence: u64) -> Result<OperationHandle> {
        let permit = self.permits.clone().try_acquire_owned().map_err(|_| {
            Error::RuntimeTool(Failure::new(
                "agent_capacity",
                "child concurrency limit reached",
            ))
        })?;
        let reference = RecoveryRef {
            adapter: "agent_run".into(),
            data: serde_json::json!({"run_id":id}),
        };
        let (events, receiver) = mpsc::channel(16);
        let (commands, mut command_rx) = mpsc::channel(16);
        let control = Arc::new(ChildControl {
            commands,
            current: std::sync::Mutex::new(invocation.control()),
        });
        let worker_control = control.clone();
        let runtime = self.runtime.clone();
        let recovery = reference.clone();
        tokio::spawn(async move {
            let mut permit = Some(permit);
            let mut invocation = invocation;
            let mut sequence = sequence;
            loop {
                let result = tokio::select! {
                    _ = events.closed() => {
                        let control = invocation.control();
                        let _ = tokio::join!(control.pause(zhir_core::run::Suspension { reason: "parent detached".into(), source: "agent_run".into(), wait_id: None, metadata: Default::default() }), invocation.result());
                        break;
                    }
                    result = invocation.result() => result,
                };
                let update = match result {
                    Err(error) => OperationUpdate::Unknown {
                        reason: error.to_string(),
                    },
                    Ok(completion) => match &completion.checkpoint().state {
                        State::Completed { content } => OperationUpdate::Finished {
                            outcome: RuntimeToolOutcome::Success {
                                content: content.clone(),
                                structured: serde_json::Value::Null,
                            },
                        },
                        State::Failed { error } => OperationUpdate::Finished {
                            outcome: RuntimeToolOutcome::Failure {
                                error: error.clone(),
                            },
                        },
                        State::Cancelled => OperationUpdate::Finished {
                            outcome: RuntimeToolOutcome::Cancelled {
                                reason: "child cancelled".into(),
                            },
                        },
                        State::Limited { reason } => OperationUpdate::Finished {
                            outcome: RuntimeToolOutcome::Failure {
                                error: Failure::new("child_limit", format!("{reason:?}")),
                            },
                        },
                        State::Suspended { suspension } => {
                            let waiting = OperationUpdate::Waiting {
                                prompt: serde_json::json!({"run_id":id,"suspension":suspension}),
                                recovery: Some(recovery.clone()),
                            };
                            if events
                                .send(OperationEvent {
                                    sequence,
                                    update: waiting,
                                })
                                .await
                                .is_err()
                            {
                                break;
                            }
                            sequence += 1;
                            let command = tokio::select! { _ = events.closed() => break, command = command_rx.recv() => command };
                            match command {
                                Some(ChildCommand::Reply(reply)) => {
                                    let mut request = ResumeRequest::from_checkpoint(
                                        completion.into_checkpoint(),
                                    )
                                    .messages(reply.messages);
                                    for resolution in reply.resolutions {
                                        request = request.resolve(resolution);
                                    }
                                    match runtime.resume(request).await {
                                        Ok(next) => {
                                            *worker_control
                                                .current
                                                .lock()
                                                .expect("child control") = next.control();
                                            invocation = next;
                                            if events
                                                .send(OperationEvent {
                                                    sequence,
                                                    update: OperationUpdate::Running {
                                                        recovery: Some(recovery.clone()),
                                                    },
                                                })
                                                .await
                                                .is_err()
                                            {
                                                break;
                                            }
                                            sequence += 1;
                                            continue;
                                        }
                                        Err(error) => OperationUpdate::Unknown {
                                            reason: error.to_string(),
                                        },
                                    }
                                }
                                Some(ChildCommand::Cancel) => {
                                    match runtime
                                        .resume(ResumeRequest::from_checkpoint(
                                            completion.into_checkpoint(),
                                        ))
                                        .await
                                    {
                                        Ok(next) => {
                                            next.control().cancel();
                                            invocation = next;
                                            continue;
                                        }
                                        Err(error) => OperationUpdate::Unknown {
                                            reason: error.to_string(),
                                        },
                                    }
                                }
                                None => break,
                            }
                        }
                        State::Running => unreachable!("completion is settled"),
                    },
                };
                drop(permit.take());
                let _ = events.send(OperationEvent { sequence, update }).await;
                break;
            }
        });
        Ok(OperationHandle {
            recovery: Some(reference),
            control,
            events: Box::new(ChildEvents { receiver }),
        })
    }
}
impl AgentBackend for RuntimeAgentBackend {
    fn start(
        &self,
        prompt: String,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<OperationHandle>> {
        Box::pin(async move {
            context.cancellation.check()?;
            let id = Self::child_id(&context);
            let child = RunContext {
                run_id: id.clone(),
                parent_run_id: Some(context.run.run_id),
                parent_runtime_tool_call_id: Some(context.operation_id),
                deadline_at_ms: context.run.deadline_at_ms,
                ..zhir_kernel::defaults::context()
            };
            let mut messages = vec![];
            if !self.system_prompt.is_empty() {
                messages.push(Message::system(&self.system_prompt));
            }
            messages.push(Message::user(prompt));
            self.attach(
                self.runtime
                    .start(RunRequest::new(messages).context(child))?,
                id,
                0,
            )
        })
    }
    fn recover(
        &self,
        operation: OperationRecord,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<OperationHandle>> {
        Box::pin(async move {
            let id = Self::child_id(&context);
            if let Some(reference) = &operation.recovery
                && (reference.adapter != "agent_run"
                    || reference.data.get("run_id").and_then(|v| v.as_str()) != Some(&id))
            {
                return Err(Error::Invalid("child recovery identity mismatch".into()));
            }
            let checkpoint = self.runtime.load_checkpoint(&id).await?.ok_or_else(|| {
                Error::Invalid(
                    "child checkpoint missing; explicit recovery resolution required".into(),
                )
            })?;
            let invocation = if checkpoint.state.active() {
                self.runtime.continue_from(checkpoint)?
            } else if matches!(checkpoint.state, State::Suspended { .. }) {
                self.runtime
                    .resume(ResumeRequest::from_checkpoint(checkpoint))
                    .await?
            } else {
                let outcome = match &checkpoint.state {
                    State::Completed { content } => RuntimeToolOutcome::Success {
                        content: content.clone(),
                        structured: serde_json::Value::Null,
                    },
                    State::Failed { error } => RuntimeToolOutcome::Failure {
                        error: error.clone(),
                    },
                    State::Cancelled => RuntimeToolOutcome::Cancelled {
                        reason: "child cancelled".into(),
                    },
                    State::Limited { reason } => RuntimeToolOutcome::Failure {
                        error: Failure::new("child_limit", format!("{reason:?}")),
                    },
                    _ => unreachable!(),
                };
                let (sender, receiver) = mpsc::channel(1);
                let _ = sender
                    .send(OperationEvent {
                        sequence: operation.last_sequence.map_or(0, |s| s + 1),
                        update: OperationUpdate::Finished { outcome },
                    })
                    .await;
                return Ok(OperationHandle {
                    recovery: operation.recovery,
                    control: Arc::new(SettledControl),
                    events: Box::new(ChildEvents { receiver }),
                });
            };
            self.attach(invocation, id, operation.last_sequence.map_or(0, |s| s + 1))
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildReply {
    #[serde(default)]
    messages: Vec<Message>,
    #[serde(default)]
    resolutions: Vec<RecoveryResolution>,
}
enum ChildCommand {
    Reply(ChildReply),
    Cancel,
}
struct ChildControl {
    commands: mpsc::Sender<ChildCommand>,
    current: std::sync::Mutex<ControlHandle>,
}
impl OperationControl for ChildControl {
    fn cancel(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.current.lock().expect("child control").cancel();
            let _ = self.commands.try_send(ChildCommand::Cancel);
            Ok(())
        })
    }
    fn reply(&self, value: serde_json::Value) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let reply = serde_json::from_value(value).map_err(|e| Error::Invalid(e.to_string()))?;
            self.commands
                .send(ChildCommand::Reply(reply))
                .await
                .map_err(|_| Error::Cancelled)
        })
    }
}
struct SettledControl;
impl OperationControl for SettledControl {
    fn cancel(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn reply(&self, _: serde_json::Value) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Err(Error::Invalid("child already settled".into())) })
    }
}
struct ChildEvents {
    receiver: mpsc::Receiver<OperationEvent>,
}
impl OperationEvents for ChildEvents {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<OperationEvent>>> {
        Box::pin(async move { Ok(self.receiver.recv().await) })
    }
}
