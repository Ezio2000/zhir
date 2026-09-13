# zhir-builtins

Enable `filesystem`, `shell`, `interaction` or `agent` explicitly. Builtins use the same RuntimeTool trait and registration path as application tools. The agent feature exposes backend-independent tools. Only agent-runtime depends on zhir-kernel.

agent::runtime_backend::InMemoryAgentBackend::new(runtime, system_prompt, max_running)
requires a positive concurrency bound and returns Result. Admission of new keys fails
with agent_capacity while full; idempotent lookups consume no slot. Cancellation waits
for settlement, and every settled child releases its slot before publishing its result.
Agent contracts, tool adapters and runtime backend are separate modules. Filesystem
workspace primitives, read/list, search and write/edit implementations are separate modules.

Part of the zhir Cargo workspace, version 0.1.0.
