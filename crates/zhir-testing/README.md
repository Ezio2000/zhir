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

## MiniMax TTS integration tests

`tests/minimax_tts.rs` exercises the production `models::minimax::tts` adapter through
the public SDK facade. The unchanged kernel owns execution, interrupt epochs, media
backpressure, resource sealing and checkpoint commits. Only text Input and audio
output are exposed; microphone input, model tools, profile updates and transport
recovery are not advertised. The test observer records events without implementing
any provider protocol.

Run deterministic local WebSocket tests without credentials:

```sh
cargo test -p zhir-testing --no-default-features --features minimax --locked --test minimax_tts
```

They check fragment `is_final` versus session completion, flushing trailing text on
EndInput, interruption without another remote start, old-epoch frames arriving
before cancellation acknowledgement, uncertain disconnects and invalid audio. A
slow consumer drains a 76-byte burst through an 8-byte kernel media budget.
Delivered chunks are checked against already committed manifests and stored bytes;
completed checkpoints also undergo wire roundtrip and trace validation. Additional
contracts cover handshake credential refresh and metadata headers, the fixed 401
retry budget, heartbeat, cancellation, consumer drop, connection timeout, unsupported
requests and rejected recovery cursors. Review regressions cover both API-key
forms through the same handshake, terminal errors with a full event queue, malformed
field types and missing/duplicate sentence boundaries.
Each sentence is a separate media stream with its own sequence and end marker:
MiniMax returns independent MP3 containers, which must not be concatenated into
one MP3 file. Fragment `is_final` does not close the sentence.

The ignored live test runs two short syntheses (drain, then interrupt/continue),
consuming the caller's MiniMax quota. It defaults to `speech-2.8-hd` and the domestic
`/ws/v1/t2a_v2_bidi` endpoint. With an environment-provided `MINIMAX_API_KEY`:

```sh
cargo test -p zhir-testing --no-default-features --features minimax --locked --test minimax_tts live_minimax_tts_session -- --ignored --exact --nocapture
```

Or explicitly read the `MiniMax` Claude provider from the local cc-switch database:

```sh
uv run --managed-python crates/zhir-testing/scripts/minimax_tts.py
```

The launcher opens cc-switch read-only and passes the credential only in the child
environment. It does not switch providers, copy configuration or log the key.
`MINIMAX_TTS_MODEL` overrides the voice model. Direct test invocation also accepts
`MINIMAX_TTS_URL`. Audio and JSON evidence go to ignored `test-results/minimax-tts/`.
The JSON lists one MP3 per sentence and whether it completed; an interrupted
sentence can contain a partial container. Decode complete files individually
with `ffmpeg -xerror -v error -i <file.mp3> -f null -`.
Timing is observational, with no latency/SLA assertion. These tests do not prove
audio-input duplex, reconnect/resume, real OAuth login/refresh endpoints, high-load behavior or perceptual
speech quality. Input acknowledgement means adapter acceptance: the service has no
per-text command acknowledgement or persisted replay cursor.

Protocol reference: [MiniMax bidirectional streaming TTS](https://platform.minimax.cn/docs/api-reference/speech-t2a-websocket-bidi).
