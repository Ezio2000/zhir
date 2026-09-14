# zhir

The public composable Rust SDK facade. Default features are empty and include core
and kernel. Opt into models, tools, policies, typed tools/output, builtins, protocols
and storage as needed. APIs use ModelSession, ToolExecution/OperationHandle,
ResourceRef, RequestProfile and v3 checkpoints. This crate adds convenience methods
over the same kernel, not another runtime.

See the workspace README and docs/developer-api.md for composition, explicit
recovery, credential injection and media streams. Offline examples: custom_tool
(features models,typed-tools) and resume (features models,interaction,memory).
The minimax_tts example (features minimax,memory) performs a live synthesis with
MINIMAX_API_KEY and consumes quota.

Part of the zhir workspace, version 0.2.0.
