use std::time::Duration;
use zhir_core::{Result, error::Error};

/// One serialized remote barrier. The adapter owns the expected protocol event
/// and its payload; this slot owns admission and the monotonic acknowledgement
/// deadline. Observations and media never extend that deadline.
pub(crate) struct Confirmation<T> {
    pending: Option<(T, tokio::time::Instant, &'static str)>,
}
impl<T> Default for Confirmation<T> {
    fn default() -> Self {
        Self { pending: None }
    }
}
impl<T: std::fmt::Debug> Confirmation<T> {
    pub fn begin(&mut self, value: T, expected: &'static str, timeout: Duration) -> Result<()> {
        if self.pending.is_some() {
            return Err(Error::Protocol(
                "remote confirmation already pending".into(),
            ));
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .filter(|_| !timeout.is_zero())
            .ok_or_else(|| Error::Invalid("invalid confirmation timeout".into()))?;
        self.pending = Some((value, deadline, expected));
        Ok(())
    }
    pub fn pending(&self) -> Option<&T> {
        self.pending.as_ref().map(|(value, _, _)| value)
    }
    pub fn deadline(&self) -> Option<tokio::time::Instant> {
        self.pending.as_ref().map(|(_, deadline, _)| *deadline)
    }
    pub fn check(&self) -> Result<()> {
        if let Some((value, deadline, expected)) = &self.pending
            && tokio::time::Instant::now() >= *deadline
        {
            return Err(Error::Uncertain(format!(
                "remote confirmation timed out: expected {expected}, pending {value:?}"
            )));
        }
        Ok(())
    }
    /// Call only after matching the provider event against `pending()`.
    pub fn complete(&mut self) -> Result<T> {
        self.check()?;
        self.pending
            .take()
            .map(|(value, _, _)| value)
            .ok_or_else(|| Error::Protocol("unsolicited remote confirmation".into()))
    }
}
