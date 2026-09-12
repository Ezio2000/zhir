//! Executor-independent domain values and extension contracts.
pub mod artifact;
pub mod error;
pub mod message;
pub mod model;
pub mod run;
pub mod storage;
pub mod tool;
pub mod wire;

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type Result<T> = std::result::Result<T, error::Error>;

/// Cooperative cancellation state; the executor owns interruption and cleanup.
#[derive(Clone, Default, Debug)]
pub struct Cancellation(Arc<AtomicBool>);
impl Cancellation {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(error::Error::Cancelled)
        } else {
            Ok(())
        }
    }
}
