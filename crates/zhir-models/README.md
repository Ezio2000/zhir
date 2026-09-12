# zhir-models

Model implementations and composition over the zhir-core Model port. No other
zhir implementation crate is required.

The default feature set provides FunctionModel, TransformModel, decorators,
ConcurrencyLimitedModel, ArtifactModel and extension composition without an HTTP client.
Enable openai-chat, openai-responses or anthropic for the corresponding HTTP/SSE
protocol. Supply endpoint, model, capabilities and credentials through ModelConfig
and the model builders.

ProviderTools composes consumer-owned capability adapters. ProviderOutput builds
normalized results with canonical replay and local media bindings. Extension
factories receive ExtensionContext and return a fresh Result<ProtocolExtension>
session for each invocation or retry attempt.

```toml
[dependencies]
zhir-models = { path = "../zhir/crates/zhir-models", features = ["openai-responses"] }
```

[Developer guide](../../docs/developer-api.md) · [Public API source](src/lib.rs)
