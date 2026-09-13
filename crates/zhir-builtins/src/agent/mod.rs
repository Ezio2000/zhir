//! Child agents use the common operation lifecycle and the existing kernel.
mod tools;
mod types;
pub use tools::tools;
pub use types::AgentBackend;
#[cfg(feature = "agent-runtime")]
pub mod runtime_backend;
