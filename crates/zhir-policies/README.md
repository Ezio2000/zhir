# zhir-policies

Shared strategy implementations over core values. This crate has no executor,
HTTP client, database or dependency on another implementation crate.

`RetryPolicy` bounds attempts, including the initial call. `Backoff::fixed`,
`exponential` and `custom` calculate delays. Models and tools own actual retry
execution, cancellation, deadlines and eligibility checks.

```rust
use std::time::Duration;
use zhir_policies::{Backoff, RetryPolicy};

let policy = RetryPolicy::new(4)?.backoff(Backoff::exponential(
    Duration::from_millis(100), Duration::from_secs(2),
)?);
# Ok::<(), zhir_core::error::Error>(())
```

The SDK facade exposes `zhir::policies` with the `policies` feature; `models` and
`tools` enable it automatically. Approval and batch policy traits remain in core.

history::HistoryWindow implements HistoryReducer over core snapshots. It retains
complete user turns and system-message order, and supports explicit dependencies
that expand the retained window. It returns a rewrite proposal; kernel commits it.
