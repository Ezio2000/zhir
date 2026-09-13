# zhir-storage

MemoryRunStore is always available. Enable `sqlite`, `mysql` or `redis` explicitly. Stores atomically commit checkpoint cores and history deltas, with optimistic revisions and run-scoped idempotency. They use native zhir layouts only.

Part of the zhir Cargo workspace, version 0.1.0.

MemoryArtifactStore is always available. Enable artifacts-filesystem for
FilesystemArtifactStore: immutable, atomically published MIME/base64 records,
idempotent keys, explicit conflicts and reads across process reconstruction.

Every adapter uses core Commit::validate_against with a compact CheckpointCore.
Adapters own clock reads, deadline checks at write boundaries and atomic I/O.
