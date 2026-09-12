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
    #[error(transparent)]
    Resume(#[from] ResumeError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
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
            Self::Validation(_) | Self::Catalog(_) | Self::Context(_) => {
                Failure::new("invalid_arguments", self.to_string())
            }
            Self::Resume(_) => Failure::new("resume", self.to_string()),
            Self::Protocol(s) => Failure::new("protocol", s),
            Self::Deadline => Failure::new("deadline", "operation deadline reached"),
            Self::Cancelled => Failure::new("cancelled", "operation cancelled"),
            _ => Failure::new("infrastructure", self.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResumeError {
    #[error("ticket resume requires a configured store")]
    StoreRequired,
    #[error("run not found: {run_id}")]
    RunNotFound { run_id: String },
    #[error("suspension ticket no longer matches the stored checkpoint")]
    StaleTicket {
        run_id: String,
        ticket_revision: u64,
        head_revision: u64,
    },
    #[error("empty suspension ticket identity")]
    InvalidTicketIdentity,
    #[error("resume requires a suspended checkpoint")]
    NotSuspended,
    #[error("continue requires an active checkpoint")]
    NotActive,
    #[error("suspension selector mismatch")]
    SelectorMismatch,
    #[error("resume messages require idle planning")]
    MessagesNotAllowed,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("empty runtime tool name")]
    EmptyName,
    #[error("duplicate runtime tool {name} in {sources:?}")]
    Duplicate { name: String, sources: Vec<String> },
    #[error("runtime tool not found: {name}")]
    NotFound { name: String },
    #[error("runtime tool is not selected: {name}")]
    NotSelected { name: String },
    #[error("catalog binding changed specification: {name}")]
    BindingChanged { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    #[error("invalid schema at {path}: {message}")]
    Schema { path: String, message: String },
    #[error("schema validation at {path}: {message}")]
    Value {
        path: String,
        schema_path: String,
        message: String,
    },
    #[error("tool input kind mismatch")]
    InputKind,
    #[error("JSON decoding at {path}: {message}")]
    Decode { path: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ContextError {
    #[error("empty context key")]
    EmptyKey,
    #[error("missing context value: {key}")]
    Missing { key: String },
    #[error("context value {key}: {message}")]
    Decode { key: String, message: String },
    #[error("context serialization {key}: {message}")]
    Encode { key: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArtifactError {
    #[error("artifact not found: {id}")]
    NotFound { id: String },
    #[error("artifact key has different contents: {id}")]
    Conflict { id: String },
    #[error("invalid artifact {id}: {message}")]
    Invalid { id: String, message: String },
    #[error("artifact {operation} failed: {message}")]
    Io { operation: String, message: String },
}
