# zhir-storage

MemoryRunStore and MemoryResourceStore are always available. Enable `sqlite`,
`mysql`, `redis` or `resources-filesystem` explicitly. Run stores atomically commit
CheckpointCore and history deltas with optimistic revisions, identity idempotency
and shared core validation. They never drive operations.

Wire and run-storage formats are v5. Use fresh SQL databases and Redis namespaces;
unversioned/other-format layouts are rejected without translation. Redis connect
requires an explicit namespace. FilesystemResourceStore publishes immutable chunk
resources with atomic finish, explicit same-key conflicts and reconstruction reads.
Resource retention/collection is host-owned. Integration acceptance lives in
crates/zhir-testing/tests/storage_stores.rs.

Part of the zhir workspace, version 0.4.0.
