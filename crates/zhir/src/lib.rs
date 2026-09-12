//! Composable Rust Agent SDK. Components are enabled explicitly with Cargo features.
#[cfg(any(
    feature = "filesystem",
    feature = "shell",
    feature = "interaction",
    feature = "agent"
))]
pub use zhir_builtins as builtins;
pub use zhir_core as core;
pub use zhir_core::{BoxFuture, Result, error, message, model, run, storage, tool, wire};
pub use zhir_kernel as kernel;
pub use zhir_kernel::{
    Invocation, ResumeRequest, RunError, RunOptions, RunRequest, Runtime, RuntimeBuilder,
    SuspensionTicket,
};
#[cfg(feature = "models")]
pub use zhir_models as models;
pub mod history;
pub mod output;
pub mod runs;
#[cfg(any(
    feature = "memory",
    feature = "sqlite",
    feature = "mysql",
    feature = "redis"
))]
pub use zhir_storage as stores;
#[cfg(feature = "tools")]
pub use zhir_tools as runtime_tools;
