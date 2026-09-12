# Architecture

zhir is one Cargo workspace with seven SDK crates and one consumer test-support
crate. Core defines values and asynchronous ports; kernel owns the execution state
machine. Models, tools and storage implement those ports independently.

```text
zhir/
├── crates/
│   ├── zhir-core/       # Values, model/tool/storage ports and native wire DTOs
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
  schema feature generates contract documents during development.
- Kernel, models, tools and storage each depend only on core among the zhir crates.
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

Runtime holds shared resources and defaults for new runs. Start resolves explicit
RunRequest overrides into RunOptions and freezes them in the initial Checkpoint.
Continue and resume retain those options even when resources are rebuilt with
different defaults. Stores and trace validation reject parameter drift.

Each start, continue or resume creates one lazy, single-use Invocation. Controls
can be shared independently. Only complete model responses and complete selected
tool batches enter durable history; progress events can be bounded and lossy.
Full model-delta observation is caller-owned and does not share checkpoint atomicity.

ResumeRequest accepts a snapshot or a SuspensionTicket. A ticket loads the
configured store and matches the exact run, checkpoint, revision and suspension.
The existing commit path arbitrates competing resumptions through revision checks.
Work deadlines use a monotonic clock, reconstructed from persisted wall-clock
context on recovery. Owned writes settle before reporting their outcome.

State transitions, control precedence and failure semantics are specified in the
[runtime contract](../contracts/v1/behavior/runtime.md).

## Persistence and artifacts

History uses immutable chunks shared through Arc. Append copies at most one partial
64-message chunk and updates an incremental digest. Full materialization is reserved
for model requests and explicit recovery/export.

SQL and Redis persist compact checkpoint cores plus accepted history deltas.
Replacements start a new generation; Memory retains shared histories in-process.
SQL writes use transactions and revision checks. Redis uses same-slot keys and an
atomic Lua commit. Exact retries are idempotent; conflicting identity reuse fails.
Applications own history retention and external-effect idempotency.

ArtifactStore is a core port implemented by consumers. ArtifactModel saves media
before returning the complete response to kernel. ProviderOutput keeps canonical
native replay and decoder-local media bindings with the normalized call; the raw
response position references its call id. Reordering normalized calls retains the
association. Input resolution restores artifact contents before protocol encoding.
Consumers own durable artifact storage and collection of uncommitted artifacts.

## Protocol and wire boundaries

Wire DTOs are independent of database layouts. Generated schemas validate shape;
core/kernel additionally validate history, fields and transitions.

Models owns protocol envelopes and per-invocation extension sessions. A fallible
factory receives read-only protocol, request and run context. HTTP/SSE transport,
validation and retry composition stay in models; concrete capability policies and
auxiliary service clients stay with consumers. Entirely new protocols implement Model.

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
