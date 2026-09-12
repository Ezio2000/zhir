use std::time::Duration;
use tokio::time::Instant;
use zhir_core::{Cancellation, Result, error::Error, run::RunContext};
pub(crate) fn deadline(run: &RunContext) -> Result<Option<Instant>> {
    run.deadline_at_ms
        .map(|at| {
            Instant::now()
                .checked_add(Duration::from_millis(at.saturating_sub(now_ms())))
                .ok_or_else(|| Error::Invalid("retry deadline exceeds clock range".into()))
        })
        .transpose()
}
pub(crate) fn check(cancellation: &Cancellation, deadline: Option<Instant>) -> Result<()> {
    cancellation.check()?;
    if deadline.is_some_and(|at| Instant::now() >= at) {
        return Err(Error::Deadline);
    }
    Ok(())
}
pub(crate) async fn wait(
    delay: Duration,
    cancellation: &Cancellation,
    deadline: Option<Instant>,
) -> Result<()> {
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
        tokio::time::sleep_until(wake).await;
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
