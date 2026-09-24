# zhir-storage

MemoryRunStore and MemoryResourceStore are always available. Enable `sqlite`,
`mysql`, `redis` or `resources-filesystem` explicitly. Run stores atomically commit
CheckpointCore and history deltas with optimistic revisions, identity idempotency
and shared core validation. They never drive operations.

Wire and run-storage formats are v5. Use fresh SQL databases and Redis namespaces;
unversioned/other-format layouts are rejected without translation. Redis connect
requires an explicit namespace. FilesystemResourceStore publishes immutable chunk
resources with atomic finish, explicit same-key conflicts and reconstruction reads.
History rewrites drop the previous generation in the same write. `RunStore::delete`
and `ResourceStore::delete` remove a run or resource; `resources::reachable` lists the
stored resources a checkpoint references. Retention/collection policy is host-owned.
SQLite uses one WAL writer connection; `MysqlRunStore::connect_with` sets the pool size
(default 8); Redis reuses one multiplexed connection and reconnects after errors.
Integration acceptance lives in crates/zhir-testing/tests/storage_stores.rs.

Part of the zhir workspace, version 0.4.0.
