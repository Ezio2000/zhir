# zhir engineering rules

zhir is a Rust Agent SDK and the foundation for future products. Use native Rust
APIs and contracts; do not add legacy API, wire, database, or Python compatibility layers.

- `zhir-core` owns public values, extension traits, and explicit wire DTOs. It has
  no executor, HTTP, database, or dependency on another zhir crate.
- `zhir-kernel` owns the only execution state machine and checkpoint commit path.
- Models, tools, and storage depend on core, not on one another or the kernel.
- Builtins use core and tools; only the optional agent backend depends on kernel.
- `zhir` is the public SDK facade, not another runtime.
- Keep one RuntimeTool trait, explicit structured/freeform inputs, immutable snapshots,
  bounded concurrency, monotonic execution deadlines, and incremental history.
- Schemas, implementation, callers, tests, and documentation describe the same
  current concepts. Do not add old aliases, fallbacks, or migration readers.
- Future product and language-binding boundaries are documented in docs/architecture.md.
- Maintain current usage, architecture, contracts and test instructions in their
  existing documents. Keep generated reports/logs in ignored test-results/ or CI artifacts.
- Python reference checks and development scripts use uv exclusively.

Validate with cargo fmt --check, cargo clippy --workspace --all-targets
--all-features -- -D warnings, cargo test --workspace --all-features, standalone
feature builds, conformance, storage integration tests, and package verification.
Report remaining gaps honestly; passing checks do not establish missing coverage.
