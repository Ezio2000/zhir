//! Executor-independent domain values and extension contracts.
pub mod credential;
pub mod error;
pub mod message;
pub mod model;
pub mod operation;
pub mod profile;
pub mod resource;
pub mod run;
pub mod storage;
pub mod tool;
pub mod wire;

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type Result<T> = std::result::Result<T, error::Error>;

/// Cooperative cancellation state; the executor owns interruption and cleanup.
///
/// `cancelled` completes when `cancel` is called, so waiters need no polling timer.
#[derive(Clone, Default, Debug)]
pub struct Cancellation(Arc<CancellationState>);
#[derive(Default, Debug)]
struct CancellationState {
    cancelled: AtomicBool,
    waiters: Mutex<Waiters>,
}
#[derive(Default, Debug)]
struct Waiters {
    next: u64,
    wakers: BTreeMap<u64, Waker>,
}
impl Cancellation {
    pub fn cancel(&self) {
        // The flag is set before the waiter lock, and waiters test it under that lock.
        if !self.0.cancelled.swap(true, Ordering::AcqRel) {
            let wakers = std::mem::take(&mut self.waiters().wakers);
            for waker in wakers.into_values() {
                waker.wake();
            }
        }
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }
    /// A future that completes once this cancellation is requested.
    pub fn cancelled(&self) -> Cancelled {
        Cancelled {
            cancellation: self.clone(),
            key: None,
        }
    }
    fn waiters(&self) -> std::sync::MutexGuard<'_, Waiters> {
        self.0
            .waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(error::Error::Cancelled)
        } else {
            Ok(())
        }
    }
}
/// Completes when its [`Cancellation`] is requested; dropping it unregisters the waiter.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct Cancelled {
    cancellation: Cancellation,
    key: Option<u64>,
}
impl Future for Cancelled {
    type Output = ();
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.cancellation.is_cancelled() {
            return Poll::Ready(());
        }
        let mut waiters = this.cancellation.waiters();
        if this.cancellation.is_cancelled() {
            return Poll::Ready(());
        }
        let key = *this.key.get_or_insert_with(|| {
            waiters.next += 1;
            waiters.next
        });
        waiters.wakers.insert(key, context.waker().clone());
        Poll::Pending
    }
}
impl Drop for Cancelled {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            self.cancellation.waiters().wakers.remove(&key);
        }
    }
}
