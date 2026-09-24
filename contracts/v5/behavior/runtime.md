# zhir v5 runtime behavior

These rules describe the current Rust values, kernel and v5 schemas. Acceptance
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
   call/result pairs match session, original item, optional generation, caller and call, including tool name.
   Immutable history chunks and prefix digests allow incremental append validation.
   A rewrite cannot cross active operations, pending commands or media cursors.

## Sessions and commands

5. Model exposes capabilities, negotiate and open_session. ModelSession exposes
   a SessionControl through control, SessionEvents through events, and independently
   optional media.input/media.output endpoints grouped in MediaPorts. control.submit
   admits a command; acknowledgement arrives through events.receive. Grouping media
   adds no buffering, execution or ownership coupling between endpoints.
   Open seeds context/configuration;
   Ready confirms establishment. Generate freezes generation ID, context/profile revisions
   and input position, not a full request. At most one generation is active.
6. The kernel commits intent before dispatch and commits sent=true before the
   external send. An acknowledgement retires the matching command. Recovery does
   not resend an entry marked sent; the adapter must reconcile it using its recovery
   reference. Loss of a session before completion, or an uncertain external result,
   suspends with RecoveryRequired when its outcome cannot be established. A
   LocalProjection session is rebuilt from committed history, so only Generate
   persists sent=true; its other commands are dispatched again into the rebuilt
   projection.
7. Complete output items are durable immediately; deltas are observations. The
   kernel may dispatch local tools before ResponseFinished. Append carries submitted inputs,
   host results or kernel-accepted canonical outputs. Ordinary adapters maintain their own
   incremental projection; received updates do not mutate a running exchange's snapshot.
8. Session event sequences increase. Events at/before the saved recovery cursor are
   ignored. Unknown acknowledgements, conflicting output identities and duplicate or
   foreign response completions are protocol errors. Each output requires a nonempty
   item/caller identity. Provider continuation preserves the initiating call identity.
   Session events already queued behind one another, up to `max_session_events`, are
   applied in order and committed as one `Session` fact carrying the last applied
   sequence. Deltas, provider operation events, input sealing, suspension, provider
   tool output and non-running states end a batch; their effects follow its commit.
9. ResponseFinished records typed status, verified covered input position, usage,
   metadata and profile confirmations. Failed, Cancelled and Incomplete are not successful
   run completion. A response started before a later input cannot claim to cover it without
   a real protocol boundary. Usage totals contain only reported values. Live emits no
   synthetic response events; it supports Interactive mode and real session closure only.
10. History::messages retains arrival order. The model conversation projection
    groups assistant items and completion metadata by real causal generation, replacing
    intermediate provider output with its later update. It does not mutate history.

## Operations

11. RuntimeTool is the only executable tool trait. start and recover return Finished
    or Active. OperationRecord identity and Running admission are committed before
    start. recover attaches to existing work; inability to recover is not permission
    to repeat start. Queued work has not been dispatched. A start error other than
    Uncertain, Storage, Conflict, Protocol, Deadline or Cancelled settles as a Failure
    with the error's code; those errors leave the operation Unknown, except Cancelled
    after a cancellation request, which settles as Cancelled.
12. OperationOutcome contains only Success, Failure and Cancelled. Running,
    Waiting, Cancelling and Unknown are unfinished operation states. Waiting prompt
    data is not a model-visible final tool result. Only Finished appends a result.
13. A local final result, its terminal operation state and delivery command share one
    commit. Output schema failure becomes a final invalid_tool_output failure. An
    identical completion is idempotent; conflicting completion or reuse of an adapter
    event sequence with another update fails; an earlier sequence is an already applied
    replay and is ignored. The persisted sequence/update pair is
    not advanced by kernel-local control transitions. Every committed operation state
    change is observed once as OperationChanged after its checkpoint.
14. Catalog binding, input validation and approval precede external start. Scheduling
    is bounded by inflight and concurrent limits and explicit tool execution facts.
    Queued calls are admitted in emission order: a serial call runs alone after all
    earlier calls, and consecutive parallel calls form one group. No new group is
    admitted while an approval decision is pending. A batch approval suspension
    starts none of that admission group. Independent
    operations commit in completion order with causal identities, not batch order.
15. ProviderToolCall reports provider-owned work. It never invokes a local tool.
    Provider Operation events refer to an introduced call; a model cannot report the
    outcome of a local tool. Provider failed/incomplete/cancelled statuses remain
    failures/cancellation rather than successful results.
16. CancelOperation records cancellation intent. Waiting work is completed only by
    an operation event or explicit recovery resolution. Reply records an uncertain
    boundary before the external call. Attach, Complete and Abandon are explicit
    recovery resolutions; Unknown is never silently converted into a new start.
    AbandonGeneration gives up the unfinished generation of a persisted
    LocalProjection session: its Generate command is removed, the response is
    Incomplete and generation is required again, committed as GenerationAbandoned.
17. Task runs suspend when all unfinished operations are Waiting/Unknown and the
    response has yielded and pending deliveries are acknowledged. Native async sessions
    can continue while work remains. Completed requires the mode's response/input boundary,
    Closed, drained media, no pending commands and no unfinished operations. EOF or silence
    never substitutes for Closed. Cancelling/limiting a run does not claim that external
    effects have been undone; unresolved records retain their actual uncertainty.

## Control, media and bounds

18. A control receipt confirms the committed intent revision, not remote execution.
    Profile changes require ProfileUpdates and explicit acknowledgement. InterruptOutput commits and carries the new output epoch
    with its command. The adapter uses that epoch after its verified remote boundary.
    SealUserInput closes media admission, drains accepted chunks, then sends the command.
    Task and Interactive modes have explicit completion boundaries.
19. ResourceRef carries Inline bytes, URL, Stored key or Provider reference, separately
    from ResourceUsage fidelity/transforms. Resource adapters resolve inputs within a
    byte budget and seal outputs before history. Native replay media positions are
    declared explicitly; unbound inline copies of sealed media are rejected.
20. MediaChunk carries session, stream, epoch, sequence, timestamp, media type and end.
    Media input/output are separate bounded channels. Payload and manifest resources
    are sealed, and the cursor is committed, before delivery. Old output epochs are not
    delivered; input epoch is zero. Within a stream/epoch the cursor increases; checkpoints retain only
    the latest immutable segment reference. Chunks of one stream, epoch and media type
    already queued behind one another are sealed as one SealedMedia segment: one
    resource holds their bytes and `chunks` records each sequence, timestamp, offset,
    length and end. The segment commits once and its chunks are then delivered in
    order; an idle stream seals single chunks, so segmenting adds no wait. Only the
    last chunk of a segment can end the stream. SealUserInput cannot overtake accepted input.
21. Defaults bound inflight operations to 64, command and session-event queues to 256,
    concurrent host operations to 8, simultaneously active media streams to 64, individual media chunks to 1 MiB and buffered media
    bytes to 16 MiB and buffered media packets to 256 per direction. Observer capacity defaults to 256. Configured
    limits are validated before channel/worker creation.
22. Observer events can be lost; ObservationGap reports loss when capacity returns.
    Committed checkpoint state is the durable source of truth. SDK media sends use
    awaited backpressure and cannot be dropped as observation overflow. Adapters
    document additional transport packet limits. Datagram ingress that cannot apply
    remote backpressure reports an uncertain failure when its receive budget is
    exhausted; it does not silently discard accepted media as observation overflow.
23. One monotonic execution deadline includes catalog/model establishment and active
    work. Commit timeout is a separate bounded write budget. Generation-request/tool-call/
    token limits use committed metrics. Observed responses are counted separately and
    remain unknown for protocols without response events. Drop of an invocation or unfinished observer
    stream requests cancellation. Kernel-owned workers are stopped at settlement.

## Profiles, credentials and formats

24. Required profile/resource semantics must be supported. Preferred semantics can
    choose only explicit alternatives; unmet preferences remain visible. Negotiated
    and effective profiles are distinct. Effective values are Provider/Verified or
    Unknown; absence of confirmation never becomes inferred success.
25. Model retry/fallback decorators apply only to establishment before command dispatch.
    An HTTP adapter retries a generation request only for a retryable rejection received
    before its response body: connection failure, 429 or 5xx, within the deadline.
    Fallback recovery binds a stable candidate identity and the original recovery
    reference. Endpoint capabilities, account credentials and profile mappings belong
    to adapters. Credential resolution/refresh is injected, not implemented by kernel.
26. Run stores use one Commit validator, optimistic revisions and immutable history
    deltas. Same checkpoint ID/digest is idempotent; conflicting identity/revision is
    rejected. Wire envelope version is exactly 5. SQL, Redis and resource format markers are 5;
    unversioned or other-version layouts require a fresh database/namespace. There
    are no old aliases, old-format readers, migration paths, transitional fields or
    compatibility execution modes. API, callers, tests, schemas and docs change together.
    A history rewrite removes the replaced generation in the same commit, so stores keep
    only the current history. `RunStore::delete` removes a run and
    `ResourceStore::delete` removes a resource; unknown targets succeed. Stores never
    delete on their own; `resources::reachable` lists what a checkpoint still references.
    The commit deadline bounds SQL connection acquisition and is checked on the local
    monotonic clock before a Redis write; server clocks do not decide it.


Provider operation completion preserves `ProviderToolCall.data` exactly. The kernel
records `ProviderToolOutcome` metadata in `ProviderToolCall.outcome` and the visible
result in `output`. The outcome carries success structured data, a failure, or a
cancellation reason; it never duplicates resource content. A populated outcome must
agree with status, and failure/cancellation settlements have no success content.
Duplicate completion compares this typed outcome and canonical content, including after
checkpoint encoding/decoding. Adapter replay continues to read its original top-level
payload on subsequent turns.

27. ConversationItems admits complete User/Assistant content messages with independent
    identities. Exact duplicates are idempotent; conflicts are protocol errors.
    Recording a provider-observed user message never echoes it back as submitted input.
28. Delegation requires its own capability and a native Delegation operation owner.
    DelegationHandler executes host work using the existing operation state machine.
    Durable Context updates and final results use committed outbox commands. Orphan
    results, duplicate calls and mismatched command owners are rejected.
29. InterruptOutput requires InterruptOutput capability and affects output only.
    Input epoch is zero. Ended streams leave active.media and remain reachable through
    immutable ArchivedMedia nodes; output interruption archives its unfinished streams
    with complete=false. A stream identity cannot resume after end in the same epoch.
    The kernel reads the archive chain once per execution and tracks later archives in
    memory, so opening a stream does not rescan the chain.

30. FlushInput requires FlushInput capability. It keeps input admission open, drains
    accepted media input before dispatch, and asks the adapter to materialize buffered
    input. Acknowledgement confirms its remote flush boundary, not completed playback.
31. SetInputAudio requires InputAudioControl capability. Kernel commits the requested
    input_audio_enabled value and outbox intent together. The adapter acknowledges the
    remote mode transition. Audio admission stays open; output epochs are unchanged.
32. Native adapters share bounded ports and independent terminal settlement in models.
    Output pressure must not block command dispatch or acknowledgement timers. Retired
    output epochs are filtered at both adapter and kernel queues; rejected late chunks
    cannot remove the accepted epoch’s media-segment predecessor. A segment never spans
    epochs, so a rejected segment contains only retired chunks.

33. ReplaceContext is an acknowledged context revision, atomic with the history rewrite
    and its outbox intent. It cannot cross active generation, operations, media or commands.
    Unsupported live replacement/history reduction fails before external establishment.
    No close/reopen simulation is permitted.
34. Opening is a durable uncertain boundary for remote sessions. Recovery never replays
    uncertain remote creation or commands. A local projection is reconstructed without a
    recovery attachment unless a sent or started generation has no response; that
    generation needs AbandonGeneration or an attachment. Retry/fallback is restricted to
    certain establishment rejection.
35. MiniMax Task explicitly generates, seals its input after ResponseStarted, waits for
    task_finished, then closes and drains. Interactive synthesis waits for SealUserInput.
    Live opens directly, reports independent conversation/delegation items, and closes
    only after host sealing and operation-result delivery. Closure with unresolved work
    is a recovery condition, never fabricated success.

36. Response-capable sessions may complete only when the latest verified response
    covers the current input position and no generation remains required. This applies
    equally to explicit and automatic generation; a submitted-input acknowledgement
    cannot settle that input. Checkpoint validation enforces the same invariant.
37. ModelBinding persists adapter selection separately from RecoveryRef. Local
    projection reconstruction must reopen that binding, even after fallback candidates
    recover or change order. Missing or conflicting bindings fail rather than select
    another model. Transparent control decorators preserve the binding unchanged.
38. Exchange command acknowledgements, generated deltas and final output are polled
    by one scheduler. A blocked emission cannot stop the generation or cancellation
    needed to release its capacity. Control staging is bounded; terminal settlement
    uses an independent port and never waits for output consumption. The exchange
    callback is invoked only after Generate acknowledgement and ResponseStarted are
    delivered to the event port, so even eagerly produced deltas follow both events.
