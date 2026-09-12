# zhir v1 runtime behavior

1. Lifecycle is exactly Planning, RuntimeToolsPending, Suspended, Completed, Failed or
   Limited. Suspended stores an active resume target; terminal states cannot resume.
2. Start rejects empty or unresolved-tool history before execution. Continue accepts
   active checkpoints; resume accepts suspended checkpoints and checks any supplied selector.
   Start resolves RunRequest overrides against runtime defaults and persists effective
   RunOptions. Continue/resume retain those options. Ticket-based resume loads the
   configured store and matches run, checkpoint id, revision and full Suspension;
   reused wait ids cannot make an older ticket current.
   Appended resume messages require Planning with no pending provider continuation.
3. One committed change advances revision once and records one fact. The returned
   checkpoint is authoritative only after its atomic storage commit succeeds.
   Invocation returns RunCompletion; its outcome exposes Completed, Suspended, Failed
   or Limited. Infrastructure errors retain the last authoritative checkpoint.
4. A complete model response commits ordered content, runtime calls, provider calls,
   usage and one planning step together. Partial stream observations never enter history.
5. Only runtime calls are bound and scheduled. Provider-only responses may complete
   or continue planning without creating runtime tool work.
6. One catalog snapshot is opened per invocation using run context and cancellation.
   Each composite source opens once; duplicate names fail. Frozen RuntimeToolSelection
   applies to both model declarations and binding. Missing selected tools fail on recovery.
   Binding and input validation precede
   approval. A suspended approval executes none of the selected batch.
7. Batch policies select a nonempty bounded prefix. Parallel calls must be explicitly
   parallel, read-only and idempotent. Concurrency is bounded and results commit in
   model order in one atomic batch, independent of physical completion order.
8. RuntimeTool failures become model-visible outcomes. Waiting has one model-visible outcome
   and a separate host suspension. The first waiting result in model order wins.
9. At a boundary the precedence is deadline, completed terminal result, tool waiting,
   pause, legal inserted messages, then normal continuation. A tool-boundary pause
   is folded into the batch checkpoint.
10. A planning pause discards partial model output. A tool pause waits for the batch.
    Inserts interrupt ordinary planning, wait until planning after tools, and defer
    throughout an unfinished provider turn. Uncommitted controls are not durable.
11. Cancellation of a tool is cooperative and active-call scoped. Unknown or stale
    call ids are no-ops. Observation stream abandonment cancels its invocation.
12. Model deltas and observational progress have a bounded lossy allowance. The
    producer-side tool progress channel is independently bounded; producer overflow
    becomes a tool failure. Durable checkpoint events are retained.
13. Planning/tool/token limits use committed metrics. Usage includes only reported
    values, and a complete model response may cross the token threshold before
    becoming Limited. One monotonic deadline bounds work. Deadline cleanup may only
    settle owned work and attempt Limited(deadline), never start more model/tool work.
14. Stores atomically check revision, parent, history delta, frozen options and checkpoint identity.
    Exact retries are idempotent. Failures leave the previous checkpoint authoritative.
    Normal appends do not read, encode or rewrite existing history.
15. Portable DTOs use native v1 shapes with explicit tags. Unknown versions/shapes and
    invalid discriminators fail. Unsigned integer fields reject fractional lexical
    representations; floating options reject nonfinite or unsafe integer conversions.
    Decode applies bounded JSON nesting and domain validation. Trace verification
    performs durable transition checks without re-executing effects.

The original behavior matrix has 77 cases under `conformance/cases/`; parameter
freezing and ticket races are additionally covered by Rust integration tests.
The SDK's Rust APIs, wire format and database layout have no legacy compatibility path.
