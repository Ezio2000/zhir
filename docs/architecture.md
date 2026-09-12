# Architecture

zhir is one Cargo workspace with eight SDK crates and one consumer test-support
crate. Core defines values and asynchronous ports; kernel owns the execution state
machine. Models, tools and storage implement those ports independently.

```text
zhir/
├── crates/
│   ├── zhir-core/       # Values, model/tool/storage ports and native wire DTOs
│   ├── zhir-policies/   # Shared retry budgets and backoff calculations
│   ├── zhir-kernel/     # Runtime, scheduling, controls, commits and trace checks
│   ├── zhir-models/     # Model composition, protocol codecs and extension sessions
│   ├── zhir-tools/      # Tool registration, binding, schemas and decorators
│   ├── zhir-builtins/   # Filesystem, shell, interaction and child-agent tools
│   ├── zhir-storage/    # Memory, SQLite, MySQL and Redis implementations
│   ├── zhir-testing/    # Scripted models, recordings and optional HTTP/SSE fixtures
│   └── zhir/            # SDK facade, output/history/run helpers and examples
├── contracts/v1/        # Runtime behavior and generated JSON Schemas
├── conformance/         # Native behavior fixtures and their runner
├── docs/                # Architecture, developer guide and test instructions
└── .github/workflows/   # SDK, feature and database CI
```

## Ownership and dependencies

- Core has no Tokio, HTTP client, database or other zhir dependency. Its optional
  schema feature generates contract documents during development. Core retains
  value validation and consistency rules, not retry implementations, catalog
  wrappers, error presentation or runtime presets.
- Policies depends only on core and owns shared strategy implementations, without an executor.
- Kernel and storage depend only on core among the zhir crates. Models and tools
  depend on core and policies. These are production boundaries; development
  dependencies may use kernel to exercise runtime-produced values.
- Builtins depend on core and tools; the optional agent feature also uses kernel.
- The facade selects components through Cargo features and adds convenience APIs
  over existing ports. It introduces no additional scheduler or commit path.
- Consumer test support depends on core/kernel and never becomes a normal SDK
  dependency. Its optional HTTP server exists only for the lifetime of a fixture.

The only executable tool trait is RuntimeTool in core. Tools handles registration,
immutable catalog snapshots, binding and schema validation. Builtins supplies
concrete implementations. Kernel handles approval, scheduling, cancellation,
failure normalization and committing the complete tool batch.

ProviderToolSpec declares a service capability; ProviderToolCall records service
execution. These calls remain model output and never enter RuntimeTool scheduling.
Consumer-owned ProviderToolAdapter and ProtocolExtension sessions define native
fields, statuses, choice and replay. Unknown output items require explicit mapping;
the SDK does not infer execution ownership from names or status strings.

## Execution and recovery

Kernel owns RunRequest/ResumeRequest builders, run identity/time creation, default
limits and default RunOptions. Core RunContext takes explicit identity and time.
Persisted numeric limits are required; deserialization does not fill runtime defaults.
Runtime holds shared resources and defaults for new runs. Start resolves explicit
RunRequest overrides into RunOptions and freezes them in the initial Checkpoint.
Continue and resume retain those options even when resources are rebuilt with
different defaults. RuntimeToolSelection is part of these frozen options. CatalogContext
passes run metadata and cancellation to each source. CompositeRuntimeTools opens source
snapshots once; kernel owns the selected catalog used for declarations and binding.
Stores and trace validation reject parameter drift.

Each start, continue or resume creates one lazy, single-use Invocation. Controls
can be shared independently. Only complete model responses and complete selected
tool batches enter durable history; progress events can be bounded and lossy.
Full model-delta observation is caller-owned and does not share checkpoint atomicity.
Invocation returns RunCompletion, a settled view of the same committed checkpoint.
RunOutcome exposes completion, suspension tickets, failures and limits. Structured
resume/catalog/validation/context/artifact errors retain actionable causes.
ContextKey<T> provides typed access to serialized metadata. RetryPolicy in policies
holds backoff calculations; models/tools own waits and execution eligibility.
Core error values retain structured causes; kernel maps execution failures into
model-visible tool results or terminal checkpoints.

ResumeRequest accepts a snapshot or a SuspensionTicket. A ticket loads the
configured store and matches the exact run, checkpoint, revision and suspension.
The existing commit path arbitrates competing resumptions through revision checks.
Work deadlines use a monotonic clock, reconstructed from persisted wall-clock
context on recovery. Owned writes settle before reporting their outcome.

State transitions, control precedence and failure semantics are specified in the
[runtime contract](../contracts/v1/behavior/runtime.md).

## Persistence and artifacts

History uses immutable chunks shared through Arc. Append copies at most one partial
64-message chunk and updates cached prefix digests and tool-order validation.
PendingCalls is a cursor into the runtime calls of an assistant history message:
message_index identifies the message, next is the first unresolved call ordinal,
and end is the total number of runtime calls in that message. Call payloads are
shared in the immutable history; states and checkpoint cores store only the cursor.
History::resolve_pending borrows the remaining calls after checking the cursor.
History::appended_since verifies the cached prefix digest and materializes only the
suffix, including for independently reconstructed histories. Offline trace checks
use this suffix and cached order validation. Full materialization is reserved for
model requests and explicit recovery/export.

SQL and Redis persist compact checkpoint cores plus accepted history deltas.
Replacements start a new generation; Memory retains shared histories in-process.
SQL writes use transactions and revision checks. Redis uses same-slot keys and an
atomic Lua commit. Exact retries are idempotent; conflicting identity reuse fails.
Applications own history retention and external-effect idempotency.

ArtifactStore is a core port. Storage provides MemoryArtifactStore and the optional
FilesystemArtifactStore; consumers may supply other implementations. ArtifactModel saves media
before returning the complete response to kernel. ProviderOutput keeps canonical
native replay and decoder-local media bindings with the normalized call; the raw
response position references its call id. Reordering normalized calls retains the
association. Input resolution restores artifact contents before protocol encoding.
Consumers configure artifact resources and own collection of uncommitted artifacts.

## Protocol and wire boundaries

Wire DTOs are independent of database layouts. Generated schemas validate shape;
core/kernel additionally validate history, fields and transitions.

Capabilities are explicit core values without an assumed default model. Models
owns named capability presets and protocol-specific declarations; testing owns
fixture capabilities. Selected endpoints may override protocol presets.

Models owns protocol envelopes and per-invocation extension sessions. A fallible
factory receives read-only protocol, request and run context. HTTP/SSE transport,
validation and retry composition stay in models; concrete capability policies and
auxiliary service clients stay with consumers. Entirely new protocols implement Model.

Protocol encoding, decoding and stream accumulation are split into private Chat,
Responses and Messages modules. Shared replay code indexes normalized provider calls
by id while preserving native replay order. Stream text grows in place; media binding
and payload membership use sets. Kernel indexes the selected catalog once, and passes
its name-keyed specifications to BatchPolicy. Binding, approval, execution and commit
preparation remain phases of the same kernel engine.

StateKind, RuntimeToolOutcomeKind, ControlAction and ApprovalDecisionKind describe
closed runtime classifications. Facts and events use these values directly. Builtins
use AgentStatus and a typed grep mode, and derive input schemas from their Serde input
types, including defaults, required nullable fields and numerical bounds.

Normalized input usage includes cache reads and writes; cache counters are a
breakdown and must not be added again. Protocol codecs normalize their native usage
representation before it reaches the runtime.

API examples and detailed extension behavior belong in the
[developer guide](developer-api.md). Reproducible checks belong in
[testing instructions](testing.md).

## Future products and bindings

Independently delivered products belong under products/<product-id>/ and own UI,
configuration, deployment and process lifecycle. Products consume the SDK without
importing each other's internals; shared crates require a concrete shared need.

Language bindings belong under bindings/<language>/ and own FFI, value conversion
and packaging. Execution and persistence continue to use the same Rust SDK.
These directories are created when implementations exist.
