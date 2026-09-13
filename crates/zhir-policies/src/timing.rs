//! Shared monotonic retry deadlines; the caller supplies its executor timer.
use std::time::Duration;
use std::{future::Future, time::Instant};
use zhir_core::{Cancellation, Result, error::Error, run::RunContext};
pub fn deadline(run: &RunContext) -> Result<Option<Instant>> {
    run.deadline_at_ms
        .map(|at| {
            Instant::now()
                .checked_add(Duration::from_millis(at.saturating_sub(now_ms())))
                .ok_or_else(|| Error::Invalid("retry deadline exceeds clock range".into()))
        })
        .transpose()
}
pub fn check(cancellation: &Cancellation, deadline: Option<Instant>) -> Result<()> {
    cancellation.check()?;
    if deadline.is_some_and(|at| Instant::now() >= at) {
        return Err(Error::Deadline);
    }
    Ok(())
}
pub async fn wait<F, Fut>(
    delay: Duration,
    cancellation: &Cancellation,
    deadline: Option<Instant>,
    mut sleep_until: F,
) -> Result<()>
where
    F: FnMut(Instant) -> Fut,
    Fut: Future<Output = ()>,
{
    check(cancellation, deadline)?;
    let until = Instant::now()
        .checked_add(delay)
        .ok_or_else(|| Error::Invalid("retry delay exceeds clock range".into()))?;
    loop {
        check(cancellation, deadline)?;
        if Instant::now() >= until {
            return Ok(());
        }
        let wake = deadline
            .map_or(until, |d| d.min(until))
            .min(Instant::now() + Duration::from_millis(10));
        sleep_until(wake).await;
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
