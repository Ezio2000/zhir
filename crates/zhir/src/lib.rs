//! Composable Rust Agent SDK. Components are enabled explicitly with Cargo features.
#[cfg(any(
    feature = "filesystem",
    feature = "shell",
    feature = "interaction",
    feature = "agent"
))]
pub use zhir_builtins as builtins;
pub use zhir_core as core;
pub use zhir_core::run::{RunCompletion, RunOutcome};
pub use zhir_core::{BoxFuture, Result, error, message, model, run, storage, tool, wire};
pub use zhir_kernel as kernel;
pub use zhir_kernel::{
    Invocation, ResumeRequest, ResumeTarget, RunError, RunOptions, RunRequest, Runtime,
    RuntimeBuilder, SuspensionSelector, SuspensionTicket,
};
#[cfg(feature = "models")]
pub use zhir_models as models;
#[cfg(feature = "policies")]
pub use zhir_policies as policies;
pub mod output;
pub mod runs;
#[cfg(any(
    feature = "memory",
    feature = "artifacts-filesystem",
    feature = "sqlite",
    feature = "mysql",
    feature = "redis"
))]
pub use zhir_storage as stores;
#[cfg(feature = "tools")]
pub use zhir_tools as runtime_tools;
