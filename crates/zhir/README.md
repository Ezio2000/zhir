# zhir

The public Rust Agent SDK facade. Select model protocols, tool components and
stores through Cargo features; core and kernel form the default execution foundation.

The models feature provides model composition without an HTTP client. typed-tools
adds type-derived tool schemas; typed-output adds JsonOutput<T>. Protocol, builtin
and storage features enable their respective crates.

RunRequest configures one run; ResumeRequest resumes a checkpoint or suspension
ticket. Output decoding, history windows and event consumption compose the same
runtime and extension ports. Applications own configuration and resource lifecycle.

```toml
[dependencies]
zhir = { path = "../zhir/crates/zhir", features = ["openai-chat", "typed-tools", "sqlite"] }
```

[Quick start and feature list](../../README.md) · [Developer guide](../../docs/developer-api.md)
