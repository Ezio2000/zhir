# zhir-models

Model-session adapters and composition over core/policies. FunctionModel wraps
turn exchanges; optional `openai-chat`, `openai-responses` and `anthropic` features
add HTTP/SSE protocols. Native realtime transports implement core session ports.

Includes TransformModel, ResourceModel, session concurrency, establishment-only
retry, stable-ID fallback recovery, static/refreshing credential providers and
explicit endpoint profile mappings. Consumer-owned ProtocolExtension and
ProviderToolAdapter implement endpoint-specific requests, outputs and replay.
Protocol defaults are not live model capability discovery. No OAuth login flow or
built-in MiniMax task client is supplied.

Part of the zhir workspace, version 0.2.0.
