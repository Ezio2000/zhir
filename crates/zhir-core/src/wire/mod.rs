//! Explicit native v1 envelopes. Provider payloads and storage layouts are separate.
use crate::{
    Result,
    error::Error,
    message::Message,
    run::{Checkpoint, Fact, History, Metrics, RunContext, State},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const VERSION: u32 = 1;
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointCore {
    pub options: crate::run::RunOptions,
    pub id: String,
    pub parent_id: Option<String>,
    pub revision: u64,
    pub context: RunContext,
    pub state: State,
    pub metrics: Metrics,
    pub fact: Fact,
    pub history_count: usize,
    pub history_digest: [u8; 32],
}
impl From<&Checkpoint> for CheckpointCore {
    fn from(c: &Checkpoint) -> Self {
        Self {
            options: c.options.clone(),
            id: c.id.clone(),
            parent_id: c.parent_id.clone(),
            revision: c.revision,
            context: c.context.clone(),
            state: c.state.clone(),
            metrics: c.metrics.clone(),
            fact: c.fact.clone(),
            history_count: c.history.len(),
            history_digest: c.history.digest(),
        }
    }
}
impl CheckpointCore {
    pub fn digest(&self) -> Result<String> {
        let bytes = serde_json::to_vec(self).map_err(|e| Error::Invalid(e.to_string()))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
    pub fn with_history(self, history: History) -> Result<Checkpoint> {
        if self.history_count != history.len() || self.history_digest != history.digest() {
            return Err(Error::Storage("history integrity mismatch".into()));
        }
        let c = Checkpoint {
            options: self.options,
            id: self.id,
            parent_id: self.parent_id,
            revision: self.revision,
            context: self.context,
            history,
            state: self.state,
            metrics: self.metrics,
            fact: self.fact,
        };
        c.validate()?;
        Ok(c)
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointEnvelope {
    #[cfg_attr(feature = "schema", schemars(range(min = 1, max = 1)))]
    version: u32,
    checkpoint: CheckpointCore,
    history: Vec<Message>,
}
pub fn encode_checkpoint(checkpoint: &Checkpoint) -> Result<Vec<u8>> {
    checkpoint.validate()?;
    serde_json::to_vec(&CheckpointEnvelope {
        version: VERSION,
        checkpoint: checkpoint.into(),
        history: checkpoint.history.messages(),
    })
    .map_err(|e| Error::Invalid(e.to_string()))
}
pub fn decode_checkpoint(bytes: &[u8]) -> Result<Checkpoint> {
    let e: CheckpointEnvelope =
        serde_json::from_slice(bytes).map_err(|e| Error::Invalid(e.to_string()))?;
    if e.version != VERSION {
        return Err(Error::Invalid("unsupported checkpoint version".into()));
    }
    e.checkpoint.with_history(History::new(e.history)?)
}

/// Schemas are generated from explicitly tagged native DTOs and checked into contracts/v1.
#[cfg(feature = "schema")]
pub fn schemas() -> std::collections::BTreeMap<&'static str, serde_json::Value> {
    use schemars::schema_for;
    let mut schemas = std::collections::BTreeMap::new();
    macro_rules! add {
        ($name:literal,$ty:ty) => {
            schemas.insert(
                $name,
                serde_json::to_value(schema_for!($ty)).expect("schema serialization"),
            );
        };
    }
    add!("checkpoint", CheckpointEnvelope);
    add!("message", crate::message::Message);
    add!("state", crate::run::State);
    add!("event", crate::run::Event);
    add!("limits", crate::run::Limits);
    add!("run-options", crate::run::RunOptions);
    add!("suspension-ticket", crate::run::SuspensionTicket);
    add!("model-response", crate::model::ModelResponse);
    add!("tool-spec", crate::tool::RuntimeToolSpec);
    add!("tool-outcome", crate::tool::RuntimeToolOutcome);
    schemas
}
