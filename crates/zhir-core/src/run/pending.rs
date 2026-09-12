use crate::{Result, error::Error};
use serde::{Deserialize, Serialize};

/// A remaining range of runtime calls in one assistant history message.
/// Call payloads live in history once; checkpoints persist only this cursor.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingCalls {
    pub message_index: usize,
    pub next: usize,
    pub end: usize,
}
impl PendingCalls {
    pub fn len(&self) -> usize {
        self.end.saturating_sub(self.next)
    }
    pub fn is_empty(&self) -> bool {
        self.next >= self.end
    }
    pub fn validate(&self) -> Result<()> {
        if self.is_empty() {
            return Err(Error::Invalid("pending call range must be nonempty".into()));
        }
        Ok(())
    }
    pub fn advance(self, count: usize) -> Result<Option<Self>> {
        self.validate()?;
        if count > self.len() {
            return Err(Error::Invalid("consumed more than pending calls".into()));
        }
        Ok((count < self.len()).then_some(Self {
            next: self.next + count,
            ..self
        }))
    }
}
