use super::*;
use zhir_core::run::{Checkpoint, RunCompletion, Suspension};

pub(super) fn attach(
    runtime: Runtime,
    permits: Arc<Semaphore>,
    invocation: Invocation,
    id: String,
    sequence: u64,
) -> Result<OperationHandle> {
    let permit = permits.try_acquire_owned().map_err(|_| {
        Error::RuntimeTool(Failure::new(
            "agent_capacity",
            "child concurrency limit reached",
        ))
    })?;
    let recovery = RecoveryRef {
        adapter: "agent_run".into(),
        data: serde_json::json!({"run_id":id}),
    };
    let (events, receiver) = mpsc::channel(16);
    let (commands, command_rx) = mpsc::channel(16);
    let control = Arc::new(ChildControl {
        commands,
        current: std::sync::Mutex::new(invocation.control()),
    });
    let worker = ChildWorker {
        runtime,
        invocation,
        id,
        sequence,
        events,
        commands: command_rx,
        control: control.clone(),
        recovery: recovery.clone(),
        permit: Some(permit),
    };
    tokio::spawn(worker.run());
    Ok(OperationHandle {
        recovery: Some(recovery),
        control,
        events: Box::new(ChildEvents { receiver }),
    })
}

struct ChildWorker {
    runtime: Runtime,
    invocation: Invocation,
    id: String,
    sequence: u64,
    recovery: RecoveryRef,
    events: mpsc::Sender<OperationEvent>,
    commands: mpsc::Receiver<ChildCommand>,
    control: Arc<ChildControl>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl ChildWorker {
    async fn run(mut self) {
        let update = loop {
            let Some(result) = self.wait_completion().await else {
                return;
            };
            let completion = match result {
                Ok(completion) => completion,
                Err(error) => {
                    break OperationUpdate::Unknown {
                        reason: error.to_string(),
                    };
                }
            };
            if completion.checkpoint().state.terminal() {
                break OperationUpdate::Finished {
                    outcome: settled_outcome(&completion.checkpoint().state),
                };
            }
            match self.resume_after_input(completion.into_checkpoint()).await {
                Ok(true) => (),
                Ok(false) => return,
                Err(error) => {
                    break OperationUpdate::Unknown {
                        reason: error.to_string(),
                    };
                }
            }
        };
        drop(self.permit.take());
        let _ = self.emit(update).await;
    }

    async fn wait_completion(
        &mut self,
    ) -> Option<std::result::Result<RunCompletion, zhir_kernel::RunError>> {
        tokio::select! {
            result = self.invocation.result() => Some(result),
            _ = self.events.closed() => {
                let control = self.invocation.control();
                let suspension = Suspension {
                    reason: "parent detached".into(), source: "agent_run".into(),
                    wait_id: None, metadata: Default::default(),
                };
                let _ = tokio::join!(control.pause(suspension), self.invocation.result());
                None
            }
        }
    }

    async fn resume_after_input(&mut self, checkpoint: Arc<Checkpoint>) -> Result<bool> {
        let State::Suspended { suspension } = &checkpoint.state else {
            unreachable!("settled child is terminal or suspended")
        };
        self.emit(OperationUpdate::Waiting {
            prompt: serde_json::json!({"run_id":self.id,"suspension":suspension}),
            recovery: Some(self.recovery.clone()),
        })
        .await?;
        let command = tokio::select! {
            _ = self.events.closed() => return Ok(false),
            command = self.commands.recv() => command,
        };
        let Some(command) = command else {
            return Ok(false);
        };
        let cancel = matches!(command, ChildCommand::Cancel);
        let mut request = ResumeRequest::from_checkpoint(checkpoint);
        if let ChildCommand::Reply(reply) = command {
            request = request.messages(reply.messages);
            for resolution in reply.resolutions {
                request = request.resolve(resolution);
            }
        }
        let next = self.runtime.resume(request).await?;
        *self.control.current.lock().expect("child control") = next.control();
        if cancel {
            next.control().cancel();
        }
        self.invocation = next;
        if !cancel {
            self.emit(OperationUpdate::Running {
                recovery: Some(self.recovery.clone()),
            })
            .await?;
        }
        Ok(true)
    }

    async fn emit(&mut self, update: OperationUpdate) -> Result<()> {
        self.events
            .send(OperationEvent {
                sequence: self.sequence,
                update,
            })
            .await
            .map_err(|_| Error::Cancelled)?;
        self.sequence += 1;
        Ok(())
    }
}
