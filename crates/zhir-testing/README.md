# zhir-testing

Development-only models, recordings, native session peers, HTTP/SSE fixtures,
waiting-operation fixtures and trace validation. Use as a dev-dependency, never a
production SDK dependency. Depends on core, kernel, models and policies. Enable
`http` for the local HTTP fixture. The history and trace benchmarks live here.

SessionModel enables synthetic native protocol and fault tests. RecordingModel
records opening requests, commands and events; completed_turns projects completed
exchanges for assertions. RecordingStore verifies committed histories through
verify_trace. None of these fixtures establish an external provider's live
capabilities, entitlement, authentication or media quality.

Part of the zhir workspace, version 0.2.0.

This workspace-only package is not published. Consumer acceptance tests live in tests/;
SDK feature names forward to zhir for feature-specific tests. Release verification uses
`uv run conformance/package.py` and packages only the eight production crates.
