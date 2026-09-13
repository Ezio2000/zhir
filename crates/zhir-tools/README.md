# zhir-tools

Provides RuntimeToolRegistry, typed structured/freeform function adapters, JSON Schema validation and retry/circuit-breaker decorators. The only RuntimeTool trait lives in zhir-core.

Enable `typed` (`typed-tools` through the zhir facade) for `TypedTool<A, O>`.
Inputs require Deserialize + JsonSchema, outputs Serialize + JsonSchema. Generated
Draft 2020-12 schemas follow the distinct Serde deserialization/serialization
contracts, including renames, defaults and nested definitions. Execution facts
remain explicit. The registry validates input and output; the adapter handles
Serde conversion and checks cancellation before and after the callback.

Structured tool arguments must be JSON objects. Typed callbacks return
`Result<ToolReply<O>>`; `success`, `accepted` and `waiting` preserve the generated
output schema, with optional media `content`. `suspended` accepts custom Suspension
metadata. Waiting includes a model-visible outcome and a suspension; Accepted does
not suspend. Errors still use Result. FunctionTool and structured/freeform adapters
accept caller-provided schemas. No provider policy is inferred from Rust types.

Part of the zhir Cargo workspace, version 0.1.0.

CompositeRuntimeTools combines catalog sources without changing execution ownership.
FunctionApprovalPolicy adapts per-call or batch functions. RetryingTool uses
zhir_policies::RetryPolicy while retaining idempotency checks and interruptible backoff.

reply::json and reply::waiting construct untyped JSON replies using the same text
conversion as ToolReply. Core owns the result values and their consistency checks.
