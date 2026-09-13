//! Shared, executor-independent strategy implementations.
mod retry;
pub use retry::{Backoff, RetryPolicy};

pub mod history;
