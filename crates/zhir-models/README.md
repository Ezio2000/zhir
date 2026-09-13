# zhir-models

Model implementations and composition over the zhir-core Model port. Shared retry
strategies come from zhir-policies; tools, storage and kernel are not dependencies.

The default feature set provides FunctionModel, TransformModel, decorators,
ConcurrencyLimitedModel, ArtifactModel and extension composition without an HTTP client.
Enable openai-chat, openai-responses or anthropic for the corresponding HTTP/SSE
protocol. Supply endpoint, model, capabilities and credentials through ModelConfig
and the model builders.

ProviderTools composes consumer-owned capability adapters. ProviderOutput builds
normalized results with canonical replay and local media bindings. Extension
factories receive ExtensionContext and return a fresh Result<ProtocolExtension>
session for each invocation or retry attempt.

Chat, Responses and Messages have separate codec and stream accumulator modules.
Text fragments append in place. Provider replay uses an id index and retains native
item order even when normalized calls are reordered; media pointers are deduplicated
with a set while their ordered bindings remain part of canonical replay.

```toml
[dependencies]
zhir-models = { path = "../zhir/crates/zhir-models", features = ["openai-responses"] }
```

[Developer guide](../../docs/developer-api.md) · [Public API source](src/lib.rs)

TransformModel supports ordered asynchronous response maps. RetryingModel uses
zhir_policies::RetryPolicy for fixed, exponential or custom backoff, bounded by cancellation and
deadlines. Already emitted deltas prevent retries.

Capabilities have no core default. `capabilities::text_tool_calling()` is an explicit
text/structured-tool preset. HTTP adapters own protocol presets; use
`with_capabilities` to describe the actual selected model.

Artifact input resolution, output persistence and replay validation are separate
modules. Replay errors are Protocol errors, invalid artifact references/content use
Artifact errors, and underlying storage failures are forwarded. Tool failure text
is produced by protocol encoding; Content::source comes from core.
