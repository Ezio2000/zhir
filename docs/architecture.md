# Architecture

zhir is a Rust SDK with one execution state machine. Public contracts describe
sessions, operations and resources independently of an endpoint, executor or product.
The 0.2.0 API and v3 wire/storage formats are the only supported contracts.

## Ownership

Production dependencies (`A -> B` means A depends on B):

```text
zhir       -> core, kernel, [policies, models, tools, builtins, storage]
policies   -> core
kernel     -> core
models     -> core, policies
tools      -> core, policies
builtins   -> core, tools, [kernel: agent-runtime only]
storage    -> core
core       -> no other zhir crate
```

- **Core** owns values, extension traits, validation and explicit wire DTOs. It has
  no HTTP, database, executor, environment-derived identities, runtime presets or
  concrete port adapters. Its history projection is a pure value transformation.
- **Policies** implements capability negotiation, retry decisions and history
  windows using core values. Waiting and scheduling effects belong to adapters/kernel.
- **Kernel** owns the only run state machine and checkpoint commit path. It receives
  model sessions, tool bindings, policies and stores through core ports. It does not
  decode provider wire formats, refresh credentials or own provider model catalogs.
- **Models** implements turn-protocol, native WebSocket and WebRTC sessions, transformations, concurrency,
  establishment retry/fallback, credential providers and resource normalization.
  **Tools** owns immutable catalog snapshots, binding, schema validation and tool
  decorators. Neither depends on storage or the other adapter crate.
- **Storage** implements atomic commits and immutable resources. It does not execute
  models/tools or decide how a run advances.
- **Builtins** supplies concrete tools. `agent` exposes `AgentBackend` and `agent_run`;
  `agent-runtime` additionally drives ordinary child kernel invocations with bounded
  admission. It has no second child execution state machine or independent job store.
- **The facade** selects components through features and adds composition helpers.
- **Testing** may depend on core, kernel, models and policies. It is a development
  dependency only. All acceptance runners, recordings, synthetic providers, fault
  injection, trace validation and benchmarks live in the unpublished `zhir-testing` or
  `conformance` packages. Production packages contain no acceptance sources or dependencies; release archives use an explicit allowlist.

## Model session boundary

`Model` exposes `capabilities`, `negotiate` and `open_session`. A `ModelSession`
contains `SessionSender`, `SessionReceiver` and optional media input/output ports.
A session sender exposes the capabilities of the bound model, so a fallback's
aggregate advertisement cannot authorize an unsupported command after selection.

Commands carry identities. StartTurn freezes the history prefix, tool declarations
and profile used for that turn. Input, ToolResult, UpdateProfile, InterruptOutput,
EndInput and Close are explicit commands. Events acknowledge commands and report
incremental deltas, complete output items, operation updates, turn completion,
recovery references and closure. Complete output items are committed as they arrive;
tools can start before TurnFinished when the session permits it.

FunctionModel and the HTTP Chat/Responses/Messages adapters implement the same
session interface over turn exchanges. They reject unsupported duplex, steering,
resume and asynchronous-result capabilities. Native transports implement the core
session ports directly. The `minimax` feature provides text-input/audio-output TTS:
websocket.rs owns bounded ports and connection lifetime, transport/websocket.rs owns
framing and handshake authentication, and codec/streaming retain MiniMax task and
sentence semantics. Provider protocol phases do not advance runs or commit storage.
HTTP-only codec/streaming helpers remain feature-isolated. No second `invoke` or stream runtime is retained.

RetryingModel retries establishment only. FallbackModel negotiates candidates
independently, binds the selected session and wraps recovery references with a
stable candidate ID. Recovery works after candidate reordering and fails explicitly
if that candidate is missing. A sent command never switches models or transparently
replays because a network request failed.

## Execution and durability

Run state is Running, Suspended, Completed, Failed, Cancelled or Limited. Concurrent
work lives in ActiveState: a session snapshot, operation records, command outbox and
sealed media cursors. Waiting is an unfinished operation, not a tool result.

The kernel commits operation identity and Running state before calling start. It
commits command intent and marks the uncertain send boundary before calling send.
A recovered invocation wins an Attached CAS before opening catalogs, model sessions
or recovering tools. Already-sent outbox entries wait for recovered acknowledgements;
they are not dispatched again. Unknown work requires reconciliation through adapter
recovery or explicit operation Attach/Complete/Abandon resolutions.

Only final Success/Failure/Cancelled outcomes produce runtime-tool history results.
The result, operation state and result-delivery command share one commit. Adapter
sequence/update pairs detect conflicting duplicates. Provider operations retain the
original call identity across continuation turns and stay outside local tool
scheduling. Model history groups causal assistant output by session/turn, replacing
intermediate provider results; raw history retains arrival order and stable IDs.
Final content comes from outputs produced since the current turn started, including
completion of an operation initiated in an earlier turn.

History uses immutable chunks and incremental digests. Storage persists a compact
CheckpointCore plus a history delta. Rewrites are allowed only without active
operations, pending commands or media cursors. Commit validation enforces revision,
parent, frozen options, immutable context, history integrity and active-state bounds.
A commit timeout returns the last known checkpoint and requires a durable-head reload;
the kernel does not pretend that the write was rolled back.

Execution deadlines use monotonic time, including catalog/model establishment.
Commit timeout is a separate bounded settlement budget. Queue limits, tool
concurrency and media byte budgets are explicit run options. Observer events may be
dropped with ObservationGap; they are not the durable source of truth.

## Profiles, resources and credentials

RequestProfile separates semantic intent from endpoint field names. Required values
must be supported; preferred values can use only explicitly listed alternatives.
NegotiatedProfile records selected values and unmet preferences. EffectiveProfile
records Provider/Verified confirmations or Unknown. ResourceUsage attaches fidelity
and transforms to a particular ResourceRef. Extensions have named namespaces;
endpoint mappings cannot silently replace controlled request fields.

ResourceRef supports inline bytes, URLs, stored keys and provider-owned references.
ResourceReader/Writer use bounded chunks. ResourceModel resolves stored inputs for
opening turns and live Input/ToolResult commands, and seals normalized output before
kernel history. Native replay positions are explicit adapter bindings; unbound
copies of sealed media data are rejected.

MediaChunk carries stream, turn, epoch, sequence, timestamp and end metadata. Separate
byte-bounded channels apply backpressure. Payload and linked stream-manifest nodes
are sealed before committing cursors and before external input/output delivery.
InterruptOutput advances the output epoch in the same commit as its outbox command. EndInput
closes admission and drains accepted input before sending the endpoint command.
Older output epochs cannot advance a stream; input epoch is zero. Checkpoints keep the latest sealed reference,
not an ever-growing media transcript. Resource retention/collection belongs to the
host; the SDK does not delete resources automatically.

CredentialProvider is injected into adapters. StaticCredential and
RefreshingCredential implement static values and refresh with generation-aware
invalidation, audience-scoped caching and shared refresh coordination. The kernel
has no account, OAuth callback, browser login, subscription or token-plan logic.

## Product and language boundaries

A future development platform builds on these contracts: account/OAuth login,
endpoint/model discovery, additional video/task transports, native realtime clients, media
rendering, IDE surfaces, billing, remote workers and artifact retention belong in
product or adapter packages. Adding a video tool to an OpenAI-driven agent uses a
RuntimeTool operation; a model service's own video job uses a provider operation.
Neither requires changing core solely to recognize a provider name.

The five motivating cases are covered as extension seams: rich model sessions and
provider tools; cross-provider video/voice operations; injected OAuth-style refreshed
credentials; explicit low-latency/original-fidelity intent; and native duplex media.
Synthetic fixtures establish these seams, not provider entitlement or online behavior.
The explicit MiniMax live test separately checks TTS drain and interrupt/continue;
it does not establish video support, audio input, transport recovery or load guarantees.

A language binding wraps core values and the SDK invocation/control interfaces. It
must use v3 DTOs and preserve identities, revisions, cancellation and backpressure.
There is no Python wire compatibility layer, alternate scheduler or migration reader.
SQL stores require format 3 in a new database; Redis requires a new format-3 namespace.
Filesystem resources use their current format in a new directory. Older layouts are
rejected rather than translated.

## Execution and protocol implementation boundaries

Kernel's private engine modules separate lifecycle decisions, checkpoint persistence,
commands, session setup and events, admission, operations, controls, and media. They
share one Engine snapshot and its single commit path. Shutdown cancellation uses the
configured tool concurrency with one 50 ms cleanup budget for all external handles;
handles still pending when the budget expires are dropped before local tasks are aborted.
Suspension detaches without cancelling external operation handles.

Conversation projection indexes each turn's provider calls by `(provider, call_id)`;
updates retain their first output position and do not scan accumulated output. Provider
operation settlement metadata lives in `ProviderToolCall.outcome`, separately from
adapter-owned `data`. Success content is stored once in `output`; settlement preserves
structured results, failures and cancellation reasons for duplicate completion checks.
No wrapper is inserted into adapter JSON when operations finish.

Built-in HTTP protocols implement one internal ProtocolAdapter per protocol. Each owns
endpoint/authentication, capabilities, encoding/decoding, usage normalization, replay
shape and stream state construction. Transport uses this interface without its own
protocol branches. External protocols still implement core Model; ProtocolExtension and
ProviderToolAdapter customize the built-ins. Retry deadlines and the waiting loop are
shared in policies::timing; adapters supply their executor's timer. Core profile::keys
owns construction and recognition of built-in negotiation dimension names.

## Independent conversation items and delegated work

Execution turns are scheduling units. `ConversationItem` carries a complete message
and provider item identity independently of a turn: the kernel deduplicates identical
items, rejects identity conflicts, and appends user and assistant messages without
sending them back as `Input`. These entries have their own history IDs and no call
origin, so conversation projection preserves each speaker boundary. Incremental
execution outputs continue to use causal turn grouping.

`Output::Delegation` and `Message::DelegationResult` describe host-owned delegated
work. Core owns `DelegationHandler`, `DelegationRequest`, `DelegationContext` and the
shared `OperationOutcome`; kernel uses its existing operation admission, cancellation,
recovery and atomic result/outbox commits. `OperationUpdate::Context` is durable model
context, unlike lossy observer progress. Tools and delegations share
`max_operation_concurrency`; models never invoke a backend Runtime. A missing handler
produces an explicit failure result, and recovery cannot silently restart work.

`InterruptOutput` requires its own capability. Its atomic commit advances only
`SessionSnapshot.output_epoch`, archives incomplete output cursors, and invalidates
queued output packets. Input epoch stays zero and accepted microphone audio remains
valid. A completed stream leaves the active cursor map and appends an immutable
`ArchivedMedia` node to `SessionSnapshot.media_archive`; it cannot reuse the same
stream identity in that epoch. Thus `max_media_streams` bounds simultaneous active
streams. Checkpoints retain constant-size archive references; enumerating archived
streams (including checking a new identity) reads their linked resource nodes.

`openai-live` supplies subscription WebRTC signaling, DataChannel events and Opus
ports through these native contracts. It has no public-Realtime, HTTP/SSE or WebSocket
fallback and advertises no session resume or manual output-interruption guarantee.
See the model crate README for credential, audio and protocol limits.
