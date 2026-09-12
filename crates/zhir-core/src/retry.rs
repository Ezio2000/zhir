//! Pure per-call retry budgets and caller-owned backoff calculations.
use crate::{Result, error::Error};
use std::{sync::Arc, time::Duration};

#[derive(Clone)]
pub struct Backoff(Arc<dyn Fn(usize) -> Duration + Send + Sync>);
impl Backoff {
    pub fn fixed(delay: Duration) -> Self {
        Self::custom(move |_| delay)
    }
    pub fn custom(callback: impl Fn(usize) -> Duration + Send + Sync + 'static) -> Self {
        Self(Arc::new(callback))
    }
    pub fn exponential(initial: Duration, maximum: Duration) -> Result<Self> {
        if initial > maximum {
            return Err(Error::Invalid("initial retry delay exceeds maximum".into()));
        }
        Ok(Self::custom(move |failed_attempt| {
            if initial.is_zero() {
                return Duration::ZERO;
            }
            let shift = u32::try_from(failed_attempt.saturating_sub(1)).unwrap_or(u32::MAX);
            let nanos = 1u128
                .checked_shl(shift)
                .and_then(|factor| initial.as_nanos().checked_mul(factor))
                .unwrap_or(maximum.as_nanos())
                .min(maximum.as_nanos());
            Duration::new(
                (nanos / 1_000_000_000) as u64,
                (nanos % 1_000_000_000) as u32,
            )
        }))
    }
}
#[derive(Clone)]
pub struct RetryPolicy {
    max_attempts: usize,
    backoff: Backoff,
}
impl RetryPolicy {
    /// Includes the initial attempt. Calculations receive one-based failed attempts.
    pub fn new(max_attempts: usize) -> Result<Self> {
        if max_attempts == 0 {
            return Err(Error::Invalid("retry attempts must be positive".into()));
        }
        Ok(Self {
            max_attempts,
            backoff: Backoff::fixed(Duration::ZERO),
        })
    }
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }
    pub fn max_attempts(&self) -> usize {
        self.max_attempts
    }
    pub fn delay_after(&self, failed_attempt: usize) -> Option<Duration> {
        (failed_attempt > 0 && failed_attempt < self.max_attempts)
            .then(|| (self.backoff.0)(failed_attempt))
    }
}
