use super::{Checkpoint, LimitReason, State, SuspensionTicket};
use crate::{
    Result,
    error::{Error, Failure},
    message::Content,
};
use std::sync::Arc;

/// A validated settled invocation, backed by its single committed checkpoint.
#[derive(Debug, Clone)]
pub struct RunCompletion {
    checkpoint: Arc<Checkpoint>,
}
#[derive(Debug)]
pub enum RunOutcome<'a> {
    Completed(&'a [Content]),
    Suspended(SuspensionTicket),
    Failed(&'a Failure),
    Limited(&'a LimitReason),
    Cancelled,
}
impl RunCompletion {
    pub fn new(checkpoint: Arc<Checkpoint>) -> Result<Self> {
        checkpoint.validate()?;
        if checkpoint.state.active() {
            return Err(Error::Protocol(
                "invocation settled with an active checkpoint".into(),
            ));
        }
        Ok(Self { checkpoint })
    }
    pub fn checkpoint(&self) -> &Arc<Checkpoint> {
        &self.checkpoint
    }
    pub fn into_checkpoint(self) -> Arc<Checkpoint> {
        self.checkpoint
    }
    pub fn outcome(&self) -> RunOutcome<'_> {
        match &self.checkpoint.state {
            State::Cancelled => RunOutcome::Cancelled,
            State::Completed { content } => RunOutcome::Completed(content),
            State::Failed { error } => RunOutcome::Failed(error),
            State::Limited { reason } => RunOutcome::Limited(reason),
            State::Suspended { suspension, .. } => RunOutcome::Suspended(SuspensionTicket {
                run_id: self.checkpoint.context.run_id.clone(),
                checkpoint_id: self.checkpoint.id.clone(),
                revision: self.checkpoint.revision,
                suspension: suspension.clone(),
            }),
            _ => unreachable!("validated settled checkpoint"),
        }
    }
}
