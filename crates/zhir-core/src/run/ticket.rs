use super::{Checkpoint, State, Suspension};
use crate::{Result, error::ResumeError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuspensionTicket {
    pub run_id: String,
    pub checkpoint_id: String,
    pub revision: u64,
    pub suspension: Suspension,
}
impl SuspensionTicket {
    pub fn from_checkpoint(checkpoint: &Checkpoint) -> Result<Self> {
        checkpoint.validate()?;
        let State::Suspended { suspension, .. } = &checkpoint.state else {
            return Err(ResumeError::NotSuspended.into());
        };
        Ok(Self {
            run_id: checkpoint.context.run_id.clone(),
            checkpoint_id: checkpoint.id.clone(),
            revision: checkpoint.revision,
            suspension: suspension.clone(),
        })
    }
    pub fn validate(&self) -> Result<()> {
        if self.run_id.is_empty() || self.checkpoint_id.is_empty() {
            return Err(ResumeError::InvalidTicketIdentity.into());
        }
        self.suspension.validate()
    }
    pub fn check(&self, checkpoint: &Checkpoint) -> Result<()> {
        self.validate()?;
        checkpoint.validate()?;
        if Self::from_checkpoint(checkpoint).as_ref().ok() != Some(self) {
            return Err(ResumeError::StaleTicket {
                run_id: self.run_id.clone(),
                ticket_revision: self.revision,
                head_revision: checkpoint.revision,
            }
            .into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub enum ResumeTarget {
    Checkpoint(Arc<Checkpoint>),
    Ticket(SuspensionTicket),
}
