# zhir-core

Public Rust values and extension ports for model sessions, asynchronous operations,
resources, credentials, profiles, tool catalogs and atomic run storage. Explicit v5
wire DTOs are separate from provider payloads and database layouts. Core has no
executor, I/O adapters, environment-derived defaults or dependency on another zhir
crate. Enable `schema` to generate JSON Schemas during development.

ModelSession exposes SessionControl (`control.submit`), SessionEvents
(`events.receive`) and resource::MediaPorts (`media.input` / `media.output`).
Media directions are independently optional; grouping does not add execution or queues.

Part of the zhir workspace, version 0.4.0.
