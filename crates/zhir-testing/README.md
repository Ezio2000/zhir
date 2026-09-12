# zhir-testing

Consumer test support over zhir-core and zhir-kernel. Use only as a dev-dependency;
production SDK components do not depend on this crate.

ScriptedModel supplies finite response/error scripts. `model_capabilities()` declares
the text/tool fixture capabilities explicitly; it does not inherit a core preset. RecordingModel, RecordingSink
and RecordingStore retain requests, deltas and successful commits for assertions.
Records are in memory and unbounded; scope them to a test.

The optional http feature adds HttpFixture, HttpReply and HttpRequest for scripted
JSON/SSE responses, request capture, byte fragmentation, delays and disconnects.
Each exchange has a timeout. Dropping a fixture or pending finish future aborts its
task. The server accepts fixed-length HTTP/1.1 requests and closes each connection.

```toml
[dev-dependencies]
zhir-testing = { path = "../zhir/crates/zhir-testing", features = ["http"] }
```

[Developer guide](../../docs/developer-api.md) · [Test instructions](../../docs/testing.md)

ScriptedModel::matching accepts named ModelCase matchers with independent step
queues. verify checks unconsumed expectations and unexpected/ambiguous calls.
