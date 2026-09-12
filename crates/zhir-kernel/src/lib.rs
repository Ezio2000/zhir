//! The single zhir execution engine, hosted by the caller's Tokio runtime.
mod catalog;
pub mod control;
pub mod defaults;
mod environment;
mod failure;
mod request;
pub use request::{ResumeRequest, RunRequest};
pub mod diagnostics;
mod engine;
pub mod invocation;
pub mod runtime;
pub use invocation::{EventStream, Invocation, RunError};
pub use runtime::{RunOptions, Runtime, RuntimeBuilder, SuspensionTicket};

pub use zhir_core::run::{RunCompletion, RunOutcome};
