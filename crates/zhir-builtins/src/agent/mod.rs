//! Child-agent tool contracts and adapters; the local runtime backend is optional.
mod tools;
mod types;
pub use tools::{response, tools};
pub use types::{AgentBackend, AgentSnapshot, AgentStatus};
#[cfg(feature = "agent-runtime")]
pub mod runtime_backend;
