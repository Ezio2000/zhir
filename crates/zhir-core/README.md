# zhir-core

Public Rust values and extension ports for model sessions, asynchronous operations,
resources, credentials, profiles, tool catalogs and atomic run storage. Explicit v3
wire DTOs are separate from provider payloads and database layouts. Core has no
executor, I/O adapters, environment-derived defaults or dependency on another zhir
crate. Enable `schema` to generate JSON Schemas during development.

Part of the zhir workspace, version 0.2.0.
