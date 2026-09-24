# Architecture

zhir is a Rust SDK with one execution state machine. Public contracts describe
sessions, operations and resources independently of an endpoint, executor or product.
The 0.4.0 API and v5 wire/storage formats are the only supported contracts.

## Ownership

Production dependencies (`A -> B` means A depends on B):

```text
zhir       -> core, kernel, [policies, models, tools, builtins, storage]
policies   -> core
kernel     -> core
models     -> core, policies
minimax    -> core, policies, models
openai     -> core, policies, models
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
- **Models** implements incremental exchange, native WebSocket and WebRTC sessions, transformations, concurrency,
  establishment retry/fallback, credential providers and resource normalization.
  Transport-dependent adapter contracts live here; queues and reservations remain private.
- **Integrations** (`zhir-minimax`, `zhir-openai`) own service configuration, endpoints,
  handshake formats, protocol phases and media interpretation. They depend on core,
  policies and models, never kernel/tools/storage or one another. Adding an integration
  does not add a provider dependency or feature to models or the SDK facade.
  Packages are scoped to a service provider; capabilities live in modules such as
  `zhir_minimax::tts` and `zhir_openai::live`. Reusable Chat/Responses protocols stay
  in models even when an integration uses them for a provider's endpoint.
- **Tools** owns immutable catalog snapshots, binding, schema validation and tool
  decorators. Models and tools depend on neither storage nor each other.
- **Storage** implements atomic commits and immutable resources. It does not execute
  models/tools or decide how a run advances.
- **Builtins** supplies concrete tools. `agent` exposes `AgentBackend` and `agent_run`;
  `agent-runtime` additionally drives ordinary child kernel invocations with bounded
  admission. It has no second child execution state machine or independent job store.
- **The facade** selects SDK components through features and adds composition helpers.
  Applications depend directly on independent service packages and pass their Model to Runtime.
- **Testing** may depend on core, kernel, models, policies and integration packages. It is a development
  dependency only. All acceptance runners, recordings, synthetic providers, fault
  injection, trace validation and benchmarks live in the unpublished `zhir-testing` or
  `conformance` packages. Production packages contain no acceptance sources or dependencies; release archives use an explicit allowlist.

## Model session boundary

`Model` exposes `capabilities`, `negotiate` and `open_session`. A `ModelSession`
exposes `control: Arc<dyn SessionControl>`, `events: Box<dyn SessionEvents>` and
`media: MediaPorts`. The control handle exposes binding, capabilities, negotiation
and `submit(SessionCommand)`; successful submission is not provider confirmation.
Events carry acknowledgements, content deltas, complete outputs and lifecycle facts,
followed by any terminal error. MediaPorts groups independently optional `input`
and `output` endpoints without introducing a queue, task or lifecycle of its own.
All endpoints can be moved independently; the control handle can be shared.
A session control exposes the capabilities of the bound model, so a fallback's
aggregate advertisement cannot authorize an unsupported command after selection.

Opening seeds the context and configuration, and emits Ready after establishment.
Append carries one identified history entry and a context revision. Submitted entries
are host inputs/results; Accepted entries synchronize the kernel's canonical, transformed
and resource-sealed outputs without echoing them back to a native provider. Generate
carries only a generation identity, context revision, input position and profile revision;
it never carries another full history. ReplaceContext requires an explicit capability,
an idle session and acknowledgement before generation. Unsupported reducers fail before
remote work; the kernel never closes and reopens to simulate context replacement.

One generation may be active, independently of concurrent inputs, tools and media.
ResponseStarted/ResponseFinished describe actual protocol boundaries, not a session's
lifetime. Completion reports the verified input position it covers: response A cannot
complete a run after unprocessed input B. Acknowledgements explicitly distinguish local
projection, transport acceptance and provider confirmation. SealUserInput stops user
and media admission but permits host operation results. Close is graceful; only Closed
and drained media permit run completion. EOF, silence and empty queues are not success.

FunctionModel and the HTTP Chat/Responses/Messages adapters implement the same
session interface over individual exchanges and maintain an incremental context projection.
They accept updates while an exchange runs without changing its frozen request. They reject unsupported duplex, steering,
resume and asynchronous-result capabilities. Native transports implement the core
session ports directly. Independent integration packages implement provider-specific
protocols over models' public WebSocket/WebRTC adapter contracts. MiniMax's TTS
configuration, task protocol and sentence decoder live in `zhir-minimax/src/tts`;
Live's signaling, protocol, media mapping and connection policy live in `zhir-openai/src/live`.
The models crate owns bounded ports, output scheduling, epoch filtering, terminal
delivery and monotonic confirmation deadlines. Its transport layer owns socket,
PeerConnection and RTP mechanics. Core owns provider-neutral commands and capabilities;
kernel owns durable command intent and output epochs; policies owns runtime-independent
strategy. Provider protocol phases do not advance runs or commit storage.
Session IDs and sideband attachments alone do not
satisfy recovery: an adapter must also restore media and reconcile pending commands.
HTTP-only codec/streaming helpers remain feature-isolated. No second `invoke` or stream runtime is retained.

EstablishmentRetryModel retries establishment only. FallbackModel negotiates candidates
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
A run has at most one live executor, and the host guarantees it: revision CAS rejects a
stale checkpoint but cannot stop an executor that is still running. The SDK has no lease
port; a multi-worker host provides the lease.

Only final Success/Failure/Cancelled outcomes produce runtime-tool history results.
The result, operation state and result-delivery command share one commit. Adapter
sequence/update pairs detect conflicting duplicates. Provider operations retain the
original call identity across continuation responses and stay outside local tool
scheduling. Model history groups causal assistant output by session/generation, replacing
intermediate provider results; raw history retains arrival order and stable IDs.
CallRef identifies the original item; its generation identity is optional. Live does
not manufacture a generation for its session or delegations. Media is keyed by session,
stream, epoch and sequence. HTTP Task output comes from the final response; Interactive
output includes assistant content from the run. Both retain full committed history.

The establishment state and all decisions about context, input coverage and generation
are persisted. An uncertain open or send cannot be retried as a fresh remote session.
Local projection reconstruction requires the bound LocalProjection capability. Such a
session commits no boundary before opening, records Generate with its send boundary in
one checkpoint, and replays its other commands into the rebuilt projection, submitting
consecutive ones together. A sent or started generation without a response stays uncertain
until the host abandons it with `AbandonGeneration` or recovery proves attachment.
HttpModel retries retryable rejections (connection failure, 429, 5xx) before reading a
response body, within the run deadline; a shared `RequestLimit` bounds whole requests. `max_generation_requests` counts explicit host requests;
`observed_responses` is unknown until real response events are received.

History uses immutable chunks and incremental digests. Chunks share their entries, so appending
or cloning a history never copies existing entries. Storage persists a compact
CheckpointCore plus a history delta. A Commit derives that core and its digest once;
validation and every store reuse them. Session events queued behind one another
share one checkpoint, so a local text turn commits single-digit checkpoints rather
than one per event. Rewrites are allowed only without active
operations, pending commands or media cursors. Commit validation enforces revision,
parent, frozen options, immutable context, history integrity and active-state bounds.
A commit timeout returns the last known checkpoint and requires a durable-head reload;
the kernel does not pretend that the write was rolled back.

Execution deadlines use monotonic time, including catalog/model establishment.
`Cancellation::cancelled` wakes waiters when cancellation is requested; the kernel,
session schedulers and built-in tools wait on it together with a deadline timer, so
an idle run performs no periodic work.
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
opening context, ReplaceContext and Append commands, and seals normalized output before
kernel history. Native replay positions are explicit adapter bindings; unbound
copies of sealed media data are rejected.

MediaChunk carries session, stream, epoch, sequence, timestamp and end metadata. Separate
channels bounded by bytes and packets (`max_buffered_media_packets`) apply backpressure.
Chunks of one stream already queued behind one another are sealed as one segment: one
data resource plus a SealedMedia node listing each chunk's offset and length, committed
once and then delivered in order. Slow storage therefore costs fewer commits instead of
more latency; an idle stream still seals each chunk alone. The kernel reads the archive
chain once and tracks ended streams in memory. Payload and linked segment nodes
are sealed before committing cursors and before external input/output delivery.
InterruptOutput advances the output epoch in the same commit as its outbox command;
the command carries that exact epoch to the adapter. Providers do not allocate a
second independent epoch. Late rejected media cannot mutate the current segment chain. SealUserInput
closes admission and drains accepted input before sending the endpoint command.
Older output epochs cannot advance a stream; input epoch is zero. Checkpoints keep the latest sealed reference,
not an ever-growing media transcript. Resource retention/collection belongs to the
host; the SDK does not delete resources automatically. `resources::reachable` walks a
checkpoint, its active cursors and the archive chain to list every stored resource it
still references.

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
The explicit MiniMax live test separately checks TTS drain, flush/continue and interrupt/continue;
it does not establish video support, audio input, transport recovery or load guarantees.

A language binding wraps core values and the SDK invocation/control interfaces. It
must use v5 DTOs and preserve identities, revisions, cancellation and backpressure.
There is no Python wire compatibility layer, alternate scheduler or migration reader.
SQL stores require format 5 in a new database; Redis requires a new format-5 namespace.
SQLite uses one writer connection with WAL and a busy timeout; MySQL uses a pool
(8 connections by default, `MysqlRunStore::connect_with` to choose) whose acquisition is
bounded by the commit deadline. Redis shares one multiplexed connection and reconnects
after an error; a failed commit remains uncertain and is not replayed. A history rewrite
removes the previous generation in the same write. `RunStore::delete`,
`ResourceStore::delete` and `zhir_storage::resources::reachable` let the host implement
retention; nothing is collected automatically.
FilesystemResourceStore marks its root with format 5 and shards resources by the first
two hex digits of their id; a root with another marker or unsharded resource files is
rejected. Older layouts are
rejected rather than translated.

## Execution and protocol implementation boundaries

Kernel's private engine modules separate lifecycle decisions, checkpoint persistence,
commands, session setup and events, admission, operations, controls, and media. They
share one Engine snapshot and its single commit path. Shutdown cancellation uses the
configured tool concurrency with one 50 ms cleanup budget for all external handles;
handles still pending when the budget expires are dropped before local tasks are aborted.
Suspension detaches without cancelling external operation handles.

Conversation projection indexes each response's provider calls by `(provider, call_id)`;
updates retain their first output position and do not scan accumulated output. Provider
operation settlement metadata lives in `ProviderToolCall.outcome`, separately from
adapter-owned `data`. Success content is stored once in `output`; settlement preserves
structured results, failures and cancellation reasons for duplicate completion checks.
No wrapper is inserted into adapter JSON when operations finish.

Built-in HTTP protocols implement one internal ProtocolAdapter per protocol. Each owns
endpoint/authentication, capabilities, encoding/decoding, usage normalization, replay
shape and stream state construction. Transport uses this interface without its own
protocol branches. External protocols still implement core Model; ProtocolExtension and
ProviderToolAdapter customize the built-ins. Retry deadlines, cancellable waits and
interruption (`timing::wait`, `timing::interrupted`) are shared in policies::timing;
adapters supply their executor's timer. Core profile::keys
owns construction and recognition of built-in negotiation dimension names.

## Independent conversation items and delegated work

Generations are optional protocol events. `ConversationItem` carries a complete message
and provider item identity independently of a generation: the kernel deduplicates identical
items, rejects identity conflicts, and appends user and assistant messages without
sending them back as submitted Append. These entries have their own history IDs and no call
origin, so conversation projection preserves each speaker boundary. Incremental
generation outputs continue to use real causal response grouping.

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

`zhir_openai::live` supplies subscription WebRTC signaling, DataChannel events and Opus
ports through these native contracts. It has no public-Live, HTTP/SSE or WebSocket
fallback and advertises no session resume or manual output-interruption guarantee.
Verified sideband attachment belongs to Live's control transport; it neither restores
a destroyed media peer nor establishes command replay or deduplication. It is not a
generic recovery capability. See the integration crate README for credential, audio and
protocol limits.

The shared native output scheduler transfers remaining bounded control events and
the worker outcome through one terminal port. Receivers first drain channel events,
then the transferred events, then receive the terminal error once. Producer teardown
never waits for output consumption. This is shared by WebRTC Live and WebSocket TTS;
remote confirmation matching and close-reason interpretation stay in each adapter.


`FlushInput` and `SetInputAudio` are provider-neutral core commands, each with an
explicit capability. Kernel commits their outbox intents; the latter also records
`SessionSnapshot.input_audio_enabled`. A control receipt establishes intent durability,
while a session acknowledgement establishes the adapter’s documented remote boundary.
Flush leaves input admission open. Audio input gating leaves the media port open and
does not advance output epochs. Provider event names and confirmation matching stay
in the integration packages; neither core nor kernel knows MiniMax tasks or Live DataChannel messages.

Models' private native machinery owns the shared executor-dependent port implementation, bounded
pending output queues, event sequencing, cancellation/deadline guard and terminal
channel. The public `Confirmation<T>` owns a pending remote barrier and its
fixed monotonic deadline; protocol adapters own the payload, matching and phases.
A timeout never settles a command, and new observations never extend the deadline.
WebSocket and WebRTC drivers keep commands and deadlines runnable while output is
waiting for capacity. WebRTC has separate event/audio receive queues; closure drains
already received data through the output scheduler. This is adapter machinery, not
a second execution/checkpoint state machine, and does not belong in core.

Live's factory returns `WebRtcModel`. `zhir-openai/src/live/adapter.rs` supplies synchronous
protocol transitions (`protocol.rs`), media interpretation (`audio.rs`), signaling
(`signaling.rs`) and original-peer connection policy (`connection.rs`). The generic
`webrtc/driver.rs` depends on these contracts, never on Live types. Protocols emit
initialization effects and command/event effects; only the driver performs I/O.
A protocol may connect immediately or process commands before requesting a peer.
The current transport scope is a text DataChannel and an independent RTP audio queue.
Peer channel labels, codec parameters and input admission come from the adapter;
queue capacities and shared media reservations come from session Limits. Signaling
payloads remain opaque to the driver. Independent service factories select adapters through models' transport-specific
contracts. They return core Models; kernel sees no transport-specific types.
Media adapters map raw AudioPacket values into MediaChunk values without expanding
the payload. The driver alone retains and releases the private ingress reservation.

Input draining closes admission and preserves the ordering of subsequent command
effects. In-flight writes retain their reservations and are polled alongside remote
control events, output flushes, cancellation and fixed confirmation deadlines.
Received control effects may progress while a command's drain/write is pending;
local transport acknowledgements stay behind their corresponding writes. Finalization
requires stopped ingress producers and drained accepted queues, then adapter-defined
media termination and protocol completion. Queue emptiness alone is not completion.
The models `websocket` and `webrtc` features select transport infrastructure.
MiniMax TTS selects WebSocket; OpenAI Live selects WebRTC. Neither transport feature
selects a service implementation, model, endpoint or credentials.
Peer, codec and connection-state values are models-owned types; service packages do
not depend on `webrtc` or `tokio-tungstenite` directly, which the dependency test checks.
Control receive processing continues under public event backpressure; bounded native
staging reserves room for command receipts, and overflow is an explicit capacity
failure. It cannot turn an already returned receipt into a remote timeout merely
because the caller has not yet read earlier observations.

`native/media.rs` attaches byte and slot reservations to queued packets. Live output
retains the same reservation from RTP receipt through native scheduling until public
consumption. Input has a separate budget. The packet bound is 4096 per direction,
independent of event limits and maximum chunk size; empty end markers consume slots
without consuming payload bytes. Datagram exhaustion is uncertain because RTP cannot
promise remote backpressure. WebSocket decoded output awaits budget before delivery,
with at most one bounded wire frame's decoded batch staged separately. This distinction
is transport behavior; it does not introduce a second media or execution contract.

WebRTC transport reports connection state independently of terminal receive errors
and preserves RTP source, sequence and timestamp identity. The transport records
connection state and its monotonic transition time without imposing a grace policy.
Live applies bounded `Disconnected` grace to the existing peer and gates new writes during that interval;
no signaling retry or command replay is implied. RTP ordering and clock extension
are transport mechanics; correlating a remote output boundary with a kernel epoch
remains the adapter's responsibility.

No additional public recovery fields are introduced without a verified protocol
consumer. `RecoveryRef` is an adapter-owned handle, while `after_sequence` is the
local committed event watermark, not a provider cursor. Provider replay/correlation
and uncertain-command reconciliation must be established before advertising Resume.


## Binding and completion invariants

`ModelBinding` records stable adapter selection independently of a remote
`RecoveryRef`. The kernel persists the bound control identity before consuming
session events and supplies it when rebuilding a local projection.
Fallback unwraps each binding layer and reopens only the original candidate;
reordering, temporary availability changes or missing candidates cannot cause
silent reselection. Transparent control decorators forward the binding.

Response-capable sessions cannot complete over an older input position, whether
generation is explicit or automatic. Acknowledging Append is not a claim that
its input has been answered. Kernel advancement and checkpoint validation both
require current response coverage. Protocols without response events continue
to use their real closure boundary, not a fabricated response or idle timeout.

Ordinary exchanges use one scheduler to poll active generation, bounded pending
emissions, command admission and cancellation/deadlines. Command handlers do
not await public output capacity. Sequence allocation is serialized with event
delivery. Generate freezes the exchange request, but invokes the callback only
after its acknowledgement and ResponseStarted reach the event port. This orders
even deltas emitted during callback invocation after the response start.
Terminal settlement uses an independent port and drains admitted
events before returning an error once. No second kernel or execution path is
introduced.

Persistent history indexes share immutable call identities and values through
Arc references, keeping B-tree node rebalancing stack-bounded during bulk
operation settlement. Public history references and snapshot semantics do not
change; regression coverage also exercises a 512 KiB worker stack.
