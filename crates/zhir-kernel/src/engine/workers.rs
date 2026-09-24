use super::*;
use zhir_core::operation::OperationOutcome;

impl Engine {
    pub(super) async fn handle(&mut self, work: Work) -> Result<()> {
        if let Work::Started(id, _) = &work {
            self.pending_starts.remove(id);
        }
        match work {
            Work::Model(Ok(Some(event))) => self.model_event(event).await,
            Work::Model(Ok(None)) => {
                if self.current.active.session.closure.is_none() {
                    self.suspend(WaitReason::Recovery).await
                } else {
                    Ok(())
                }
            }
            Work::Model(Err(error)) => {
                if matches!(error, Error::Uncertain(_))
                    || self.current.active.session.recovery.is_some()
                    || self
                        .current
                        .active
                        .operations
                        .values()
                        .any(|op| !op.state.terminal())
                {
                    self.suspend(WaitReason::Recovery).await
                } else {
                    Err(error)
                }
            }
            Work::Started(id, Ok(ToolExecution::Finished(outcome))) => {
                self.finish(&id, outcome).await
            }
            Work::Started(id, Ok(ToolExecution::Active(handle))) => {
                self.install_operation(id, handle).await
            }
            Work::Started(id, Err(error)) => self.start_failed(&id, error).await,
            Work::CancelFailed(id, error) => {
                if self
                    .current
                    .active
                    .operations
                    .get(&id)
                    .is_some_and(|op| !op.state.terminal())
                {
                    self.unknown(&id, format!("cancel failed: {error}")).await
                } else {
                    Ok(())
                }
            }
            Work::Operation(id, Ok(Some(event))) => self.operation_event(&id, event).await,
            Work::Operation(id, _) => {
                if self
                    .current
                    .active
                    .operations
                    .get(&id)
                    .is_some_and(|op| !op.state.terminal())
                {
                    self.unknown(&id, "operation stream ended without a final result".into())
                        .await
                } else {
                    Ok(())
                }
            }
            Work::Admitted(bindings, decisions) => self.admitted(bindings, decisions).await,
            Work::MediaReady(chunk, reference, reply) => {
                let valid = chunk.epoch == self.current.active.session.output_epoch
                    && self.current.active.session.id == chunk.session_id;
                if valid {
                    self.seal_cursor("output", &chunk, reference).await?;
                }
                let _ = reply.send(valid);
                Ok(())
            }
            Work::InputReady(chunk, reference, reply) => {
                let valid = chunk.epoch == 0 && self.current.active.session.id == chunk.session_id;
                if valid {
                    self.seal_cursor("input", &chunk, reference).await?;
                }
                let _ = reply.send(valid);
                Ok(())
            }
            Work::InputSent(result) => {
                self.input_sending = false;
                if result.is_err() {
                    self.suspend(WaitReason::Recovery).await
                } else {
                    Ok(())
                }
            }
            Work::MediaDone => {
                self.media_pending = false;
                Ok(())
            }
            Work::ReplyDone(id, result) => {
                self.pending_replies.remove(&id);
                if self
                    .current
                    .active
                    .operations
                    .get(&id)
                    .is_some_and(|op| op.state == OperationState::Unknown)
                {
                    if let Err(error) = result {
                        self.unknown(&id, error.to_string()).await?;
                    } else {
                        self.local_operation_update(&id, OperationState::Running, None)
                            .await?;
                    }
                }
                Ok(())
            }
            Work::Progress(id, value) => {
                self.emitter.emit(EventData::OperationProgress {
                    operation_id: id,
                    value,
                });
                Ok(())
            }
            Work::MediaError(error) => Err(error),
            Work::CommandSent(result) => {
                self.sending = false;
                if result.is_err() {
                    self.suspend(WaitReason::Recovery).await
                } else {
                    Ok(())
                }
            }
        }
    }
}

impl Engine {
    /// A start error settles the operation unless the external effect is left unknown.
    async fn start_failed(&mut self, id: &str, error: Error) -> Result<()> {
        let cancelling = self
            .current
            .active
            .operations
            .get(id)
            .is_some_and(|op| op.state == OperationState::Cancelling);
        match error {
            Error::Cancelled if cancelling => {
                self.finish(
                    id,
                    OperationOutcome::Cancelled {
                        reason: "operation cancelled".into(),
                    },
                )
                .await
            }
            Error::Cancelled
            | Error::Uncertain(_)
            | Error::Storage(_)
            | Error::Conflict { .. }
            | Error::Protocol(_)
            | Error::Deadline => self.unknown(id, error.to_string()).await,
            error => {
                self.finish(
                    id,
                    OperationOutcome::Failure {
                        error: crate::failure::failure(&error),
                    },
                )
                .await
            }
        }
    }
}
