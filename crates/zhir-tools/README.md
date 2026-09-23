# zhir-tools

RuntimeTool catalog assembly, immutable binding, structured/freeform input
validation and function/typed adapters. Enable `typed` for Rust-derived schemas.
Tools start or recover work through the one core RuntimeTool trait, returning
ToolExecution::Finished or Active(OperationHandle). Typed ToolReply contains final
success payloads only. Active final results also pass output validation. Retry and
circuit-breaking decorators preserve the operation lifecycle. Production
dependencies are core and policies.

Part of the zhir workspace, version 0.4.0.
