# zhir-kernel

The only zhir execution state machine and checkpoint commit path. Drives sessions,
bounded tool operations, command outboxes, media cursors, cancellation and explicit
recovery. Adapters and policies are injected through core traits. Kernel depends
only on zhir-core among production SDK crates; it has no provider/auth/storage
implementation. Trace acceptance and benchmarks live in zhir-testing.

Part of the zhir workspace, version 0.3.0.
