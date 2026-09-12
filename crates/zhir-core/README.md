# zhir-core

Owns messages, model/tool ports, immutable history, checkpoints and native v1 serialization. It has no async executor, HTTP client or database dependency. Enable `schema` only when generating JSON Schema contracts.

Part of the zhir Cargo workspace, version 0.1.0.

RunRequest/RunOptions describe per-run parameters; checkpoints persist effective
options. SuspensionTicket identifies one paused revision; ResumeRequest carries a
snapshot or ticket plus external messages/metadata. These are values only: loading,
scheduling and resuming remain kernel responsibilities.
