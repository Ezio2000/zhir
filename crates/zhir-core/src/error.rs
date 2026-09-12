use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
}
impl Failure {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("model: {0:?}")]
    Model(Failure),
    #[error("tool: {0:?}")]
    RuntimeTool(Failure),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("storage: {0}")]
    Storage(String),
    #[error("revision conflict: expected {expected:?}, actual {actual:?}")]
    Conflict {
        expected: Option<u64>,
        actual: Option<u64>,
    },
    #[error("operation deadline reached")]
    Deadline,
    #[error("operation cancelled")]
    Cancelled,
}
impl Error {
    pub fn failure(&self) -> Failure {
        match self {
            Self::Model(f) | Self::RuntimeTool(f) => f.clone(),
            Self::Invalid(s) => Failure::new("invalid_arguments", s),
            Self::Protocol(s) => Failure::new("protocol", s),
            Self::Deadline => Failure::new("deadline", "operation deadline reached"),
            Self::Cancelled => Failure::new("cancelled", "operation cancelled"),
            _ => Failure::new("infrastructure", self.to_string()),
        }
    }
}
