//! Reservations follow media from transport receipt to the public receiver.
//! Empty end markers cost a slot, but no payload bytes.
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zhir_core::{Result, error::Error};

/// Independent of control-event capacity and the maximum individual chunk size.
/// The slot bound also covers zero-byte markers and small-packet allocation overhead.
pub(crate) const MAX_BUFFERED_CHUNKS: usize = 4096;

#[derive(Clone)]
pub(crate) struct MediaBudget {
    bytes: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    limit: usize,
}
pub(crate) struct Reservation {
    _bytes: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
}
pub(crate) struct Buffered<T> {
    pub value: T,
    pub reservation: Reservation,
}
impl<T> Buffered<T> {
    pub fn into_inner(self) -> T {
        let Self { value, reservation } = self;
        drop(reservation);
        value
    }
    #[cfg(feature = "openai-live")]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Buffered<U> {
        Buffered {
            value: f(self.value),
            reservation: self.reservation,
        }
    }
}
impl MediaBudget {
    pub fn new(limit: usize) -> Self {
        Self {
            bytes: Arc::new(Semaphore::new(limit)),
            slots: Arc::new(Semaphore::new(MAX_BUFFERED_CHUNKS)),
            limit,
        }
    }
    fn size(&self, size: usize) -> Result<u32> {
        if size > self.limit {
            return Err(Error::Invalid("media payload exceeds buffer budget".into()));
        }
        u32::try_from(size).map_err(|_| Error::Invalid("media payload too large".into()))
    }
    pub async fn reserve(&self, size: usize) -> Result<Reservation> {
        let size = self.size(size)?;
        let slot = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Cancelled)?;
        let bytes = self
            .bytes
            .clone()
            .acquire_many_owned(size)
            .await
            .map_err(|_| Error::Cancelled)?;
        Ok(Reservation {
            _bytes: bytes,
            _slot: slot,
        })
    }
    /// Datagram ingress cannot promise backpressure to the remote sender.
    /// Exhaustion is explicit, and never silently drops an accepted media packet.
    #[cfg(feature = "openai-live")]
    pub fn try_reserve(&self, size: usize) -> Result<Reservation> {
        let size = self.size(size)?;
        let exhausted = |_| Error::Uncertain("native media receive budget exceeded".into());
        let slot = self.slots.clone().try_acquire_owned().map_err(exhausted)?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(size)
            .map_err(exhausted)?;
        Ok(Reservation {
            _bytes: bytes,
            _slot: slot,
        })
    }
}
