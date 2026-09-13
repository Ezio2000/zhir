# zhir-builtins

Concrete tools over core and tools. Enable `filesystem`, `shell`, `interaction`,
`agent` or `agent-runtime` explicitly. ask_question is a waiting operation with
explicit response validation. agent_run uses AgentBackend start/recover and the
same operation control/events as other tools.

Only `agent-runtime` depends on kernel, driving child invocations with bounded
admission, persistent recovery and cancellation. Parent detach suspends the child.
There is no independent child runtime or job-state store. Acceptance code lives in
crates/zhir-testing/tests/builtins_*.rs.

Part of the zhir workspace, version 0.2.0.
