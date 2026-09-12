# zhir-core

Public values, extension traits, immutable history, checkpoints and native v1 wire
contracts. Core contains shared validation and consistency rules. It has no executor,
HTTP client, database, generated identities, runtime presets or zhir dependency.
Enable `schema` when generating JSON Schema contracts.

RunOptions stores fully resolved parameters; numeric limits are required on decode.
RunContext::new takes explicit identity and start time. Capabilities must be supplied
explicitly. SuspensionTicket identifies a paused revision and ResumeTarget names its
source. Runtime request builders and environment defaults belong to kernel.

ContextKey<T> provides typed JSON metadata access; it can insert metadata without
creating a run. RunCompletion/RunOutcome project settled checkpoints. CatalogContext
and RuntimeToolSelection carry catalog parameters; kernel implements selection.
Structured errors, ApprovalPolicy and BatchPolicy contracts stay here; concrete
retry strategies belong to zhir-policies.

PendingCalls identifies an unresolved call range in an assistant history message.
Use History::resolve_pending to borrow its payloads; advancing the cursor does not
copy the remaining calls. History caches order validation and prefix digests during
append. History::appended_since validates a prefix and returns only added messages.
StateKind, RuntimeToolOutcomeKind, ControlAction and ApprovalDecisionKind provide
closed classifications for facts and events. BatchPolicy takes specifications in a
BTreeMap keyed by tool name.
