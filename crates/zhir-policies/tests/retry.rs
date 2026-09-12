use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use zhir_policies::{Backoff, RetryPolicy};

#[test]
fn retry_budget_includes_first_call_and_bounds_custom_callbacks() {
    assert!(RetryPolicy::new(0).is_err());
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let observed = attempts.clone();
    let policy = RetryPolicy::new(4)
        .unwrap()
        .backoff(Backoff::custom(move |attempt| {
            observed.lock().unwrap().push(attempt);
            Duration::from_millis(attempt as u64)
        }));
    assert_eq!(policy.max_attempts(), 4);
    assert_eq!(policy.delay_after(0), None);
    assert_eq!(policy.delay_after(1), Some(Duration::from_millis(1)));
    assert_eq!(
        policy.clone().delay_after(3),
        Some(Duration::from_millis(3))
    );
    assert_eq!(policy.delay_after(4), None);
    assert_eq!(policy.delay_after(usize::MAX), None);
    assert_eq!(*attempts.lock().unwrap(), [1, 3]);
    assert_eq!(RetryPolicy::new(1).unwrap().delay_after(1), None);
}

#[test]
fn exponential_backoff_caps_without_integer_or_duration_overflow() {
    let policy = RetryPolicy::new(4)
        .unwrap()
        .backoff(Backoff::exponential(Duration::from_millis(1), Duration::from_millis(3)).unwrap());
    assert_eq!(
        (1..4)
            .map(|n| policy.delay_after(n).unwrap().as_millis())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert!(Backoff::exponential(Duration::from_secs(2), Duration::from_secs(1)).is_err());
    let policy = RetryPolicy::new(usize::MAX)
        .unwrap()
        .backoff(Backoff::exponential(Duration::from_nanos(1), Duration::MAX).unwrap());
    assert_eq!(
        policy.delay_after(34),
        Some(Duration::from_nanos(1u64 << 33))
    );
    assert_eq!(policy.delay_after(usize::MAX - 1), Some(Duration::MAX));
    let zero = RetryPolicy::new(usize::MAX)
        .unwrap()
        .backoff(Backoff::exponential(Duration::ZERO, Duration::MAX).unwrap());
    assert_eq!(zero.delay_after(usize::MAX - 1), Some(Duration::ZERO));
}
