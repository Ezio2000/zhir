# zhir v2 runtime behavior

These rules describe the current Rust values, kernel and v2 schemas. Acceptance
implementations belong to testing modules and are not exported by production crates.

## State and identity

1. Run state is Running, Suspended, Completed, Failed, Cancelled or Limited.
   ActiveState independently stores session state, operations, command outbox and
   media cursors. Suspended is resumable; terminal runs cannot advance.
2. Start validates a nonempty history without unresolved calls and freezes complete
   RunOptions. Continue accepts Running; resume accepts Suspended and validates any
   selector/ticket. A ticket matches run, checkpoint ID, revision and suspension.
   Recovery preserves initial options and immutable parent/deadline context.
3. Each successful commit advances revision once, links the previous checkpoint and
   records a Fact. Checkpoints become authoritative only after the store succeeds.
   A CAS loser opens no model/catalog/tool recovery work. Commit timeout returns an
   infrastructure error and the last known checkpoint; callers reload the durable head.
4. History entries have unique stable IDs and optional CallRef causality. Runtime
   call/result pairs match session, turn, caller and call, including tool name.
   Immutable history chunks and prefix digests allow incremental append validation.
   A rewrite cannot cross active operations, pending commands or media cursors.

## Sessions and commands

5. Model exposes capabilities, negotiate and open_session. ModelSession has command
   and event ports plus optional independent media ports. StartTurn initiates model
   work. Its outbox record freezes turn ID, history count, profile and tool specs.
6. The kernel commits intent before dispatch and commits sent=true before the
   external send. An acknowledgement retires the matching command. Recovery does
   not resend an entry marked sent; the adapter must reconcile it using its recovery
   reference. Loss of a session before completion, or an uncertain external result,
   suspends with RecoveryRequired when its outcome cannot be established.
7. Complete output items are durable immediately; deltas are observations. The
   kernel may dispatch local tools before TurnFinished. Asynchronous ToolResult and
   running Input/Profile updates require the bound session's relevant capabilities.
   Turn adapters deliver results/input at the next legal turn boundary.
8. Session event sequences increase. Events at/before the saved recovery cursor are
   ignored. Unknown acknowledgements, conflicting output identities and duplicate or
   foreign turn completions are protocol errors. Each output requires a nonempty
   item/caller identity. Provider continuation preserves the initiating call identity.
9. TurnFinished records disposition, usage, response metadata and actual profile
   confirmations. An unfinished provider call requires continuation in a turn
   exchange. Usage totals contain only reported values. Final output includes results
   produced during the final turn even when their operation began earlier.
10. History::messages retains arrival order. The model conversation projection
    groups assistant items and completion metadata by causal session/turn, replacing
    intermediate provider output with its later update. It does not mutate history.

## Operations

11. RuntimeTool is the only executable tool trait. start and recover return Finished
    or Active. OperationRecord identity and Running admission are committed before
    start. recover attaches to existing work; inability to recover is not permission
    to repeat start. Queued work has not been dispatched.
12. RuntimeToolOutcome contains only Success, Failure and Cancelled. Running,
    Waiting, Cancelling and Unknown are unfinished operation states. Waiting prompt
    data is not a model-visible final tool result. Only Finished appends a result.
13. A local final result, its terminal operation state and delivery command share one
    commit. Output schema failure becomes a final invalid_tool_output failure. An
    identical completion is idempotent; conflicting completion or reuse of an adapter
    event sequence with another update fails. The persisted sequence/update pair is
    not advanced by kernel-local control transitions.
14. Catalog binding, input validation and approval precede external start. Scheduling
    is bounded by inflight and concurrent limits and explicit tool execution facts.
    A batch approval suspension starts none of that admission group. Independent
    operations commit in completion order with causal identities, not batch order.
15. ProviderToolCall reports provider-owned work. It never invokes a local tool.
    Provider Operation events refer to an introduced call; a model cannot report the
    outcome of a local tool. Provider failed/incomplete/cancelled statuses remain
    failures/cancellation rather than successful results.
16. CancelOperation records cancellation intent. Waiting work is completed only by
    an operation event or explicit recovery resolution. Reply records an uncertain
    boundary before the external call. Attach, Complete and Abandon are explicit
    recovery resolutions; Unknown is never silently converted into a new start.
17. Task runs suspend when all unfinished operations are Waiting/Unknown and the
    model turn has yielded. Native async sessions can continue while work remains.
    Completed requires finished turn disposition, no pending commands and no
    unfinished operations. Cancelling/limiting a run does not claim that external
    effects have been undone; unresolved records retain their actual uncertainty.

## Control, media and bounds

18. A control receipt confirms the committed intent revision, not remote execution.
    Running profile changes require ProfileUpdates; idle turn-protocol changes apply
    locally for the next turn. Interrupt commits the new epoch with its command.
    EndInput closes media admission, drains accepted chunks, then sends the command.
    Task and Interactive modes have explicit completion boundaries.
19. ResourceRef carries Inline bytes, URL, Stored key or Provider reference, separately
    from ResourceUsage fidelity/transforms. Resource adapters resolve inputs within a
    byte budget and seal outputs before history. Native replay media positions are
    declared explicitly; unbound inline copies of sealed media are rejected.
20. MediaChunk carries stream, turn, epoch, sequence, timestamp, media type and end.
    Media input/output are separate bounded channels. Payload and manifest resources
    are sealed, and the cursor is committed, before delivery. Old epochs are not
    delivered. Within a stream/epoch the cursor increases; checkpoints retain only
    the latest immutable manifest reference. EndInput cannot overtake accepted input.
21. Defaults bound inflight operations to 64, command and session-event queues to 256,
    concurrent runtime tools to 8, retained media streams to 64, individual media chunks to 1 MiB and buffered media
    bytes to 16 MiB per direction. Observer capacity defaults to 256. Configured
    limits are validated before channel/worker creation.
22. Observer events can be lost; ObservationGap reports loss when capacity returns.
    Committed checkpoint state is the durable source of truth. Media uses awaited
    backpressure and cannot be dropped as observation overflow.
23. One monotonic execution deadline includes catalog/model establishment and active
    work. Commit timeout is a separate bounded write budget. Model-turn/tool-call/
    token limits use committed metrics. Drop of an invocation or unfinished observer
    stream requests cancellation. Kernel-owned workers are stopped at settlement.

## Profiles, credentials and formats

24. Required profile/resource semantics must be supported. Preferred semantics can
    choose only explicit alternatives; unmet preferences remain visible. Negotiated
    and effective profiles are distinct. Effective values are Provider/Verified or
    Unknown; absence of confirmation never becomes inferred success.
25. Model retry/fallback applies only to establishment before command dispatch.
    Fallback recovery binds a stable candidate identity and the original recovery
    reference. Endpoint capabilities, account credentials and profile mappings belong
    to adapters. Credential resolution/refresh is injected, not implemented by kernel.
26. Run stores use one Commit validator, optimistic revisions and immutable history
    deltas. Same checkpoint ID/digest is idempotent; conflicting identity/revision is
    rejected. Wire envelope version is exactly 2. SQL and Redis format markers are 2;
    unversioned or other-version layouts require a fresh database/namespace. There
    are no aliases, v1 readers, migration paths or compatibility execution modes.


Provider operation completion preserves `ProviderToolCall.data` exactly. The kernel
records `ProviderToolOutcome` metadata in `ProviderToolCall.outcome` and the visible
result in `output`. The outcome carries success structured data, a failure, or a
cancellation reason; it never duplicates resource content. A populated outcome must
agree with status, and failure/cancellation settlements have no success content.
Duplicate completion compares this typed outcome and canonical content, including after
checkpoint encoding/decoding. Adapter replay continues to read its original top-level
payload on subsequent turns.
