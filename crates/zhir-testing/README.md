# zhir-testing

Development-only models, recordings, native session peers, HTTP/SSE fixtures,
waiting-operation fixtures and trace validation. Use as a dev-dependency, never a
production SDK dependency. Depends on core, kernel, models and policies. Enable
`http` for the local HTTP fixture. The history and trace benchmarks live here.

SessionModel enables synthetic native protocol and fault tests. RecordingModel
records opening requests, commands and events; completed_responses projects completed
exchanges for assertions. RecordingStore verifies committed histories through
verify_trace. None of these fixtures establish an external provider's live
capabilities, entitlement, authentication or media quality.

Part of the zhir workspace, version 0.3.0.

This workspace-only package is not published. Consumer acceptance tests live in tests/;
SDK feature names forward to zhir for feature-specific tests. Release verification uses
`uv run conformance/package.py` and packages only the eight production crates.

## MiniMax TTS integration tests

`tests/minimax_tts.rs` exercises the production `zhir_minimax::tts` adapter through
the public SDK facade. The kernel owns execution, interrupt epochs, media
backpressure, resource sealing and checkpoint commits. Only text Input and audio
output are exposed; microphone input, model tools, profile updates and transport
recovery are not advertised. The test observer records events without implementing
any provider protocol, consuming the native SessionReceiver rather than installing a DeltaSink.

Run deterministic local WebSocket tests without credentials:

```sh
cargo test -p zhir-testing --no-default-features --features minimax --locked --test minimax_tts
```

They check fragment `is_final` versus session completion, flushing trailing text on
SealUserInput and explicit FlushInput, preserved whitespace fragments, protocol receipt
deadlines, interruption without another remote start, old-epoch frames arriving
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
MiniMax returns independent sentence containers, which must be decoded separately. Fragment `is_final` does not close the sentence.

The ignored live test runs 18 short syntheses: drain, interrupt/continue and
flush/continue for each of six formats (MP3, PCM, WAV, FLAC, raw/WAV μ-law),
consuming the caller's MiniMax quota. It defaults to `speech-2.8-hd` and the domestic
`/ws/v1/t2a_v2_bidi` endpoint. It requires host `ffmpeg` and decodes every complete
stream with strict error handling. MP3 cases also send mixed voices, emotion,
normalization, formula reading, effects, subtitles and continuous inference.
Parameter acceptance and decodability do not establish perceptual effects or
subtitle availability when the provider omits subtitle data.
Case starts are spaced 15 seconds apart to limit RPM, so the matrix takes about
four and a half minutes. WAV exports finalize the stream header's unknown lengths; original
wire bytes remain alongside each export as `.stream` files.
With an environment-provided `MINIMAX_API_KEY`:

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
The JSON lists one file per sentence, its format and whether it completed; an
interrupted sentence can contain a partial container. The test automatically
decodes complete files, supplying raw PCM/μ-law parameters when required.
Timing is observational, with no latency/SLA assertion. These tests do not prove
audio-input duplex, reconnect/resume, real OAuth login/refresh endpoints, high-load behavior or perceptual
speech quality. Input acknowledgement means adapter acceptance: the service has no
per-text command acknowledgement or persisted replay cursor.

Protocol reference: [MiniMax bidirectional streaming TTS](https://platform.minimax.io/docs/api-reference/speech-t2a-websocket-bidi).

The documented Ogg/Opus format is not advertised by the production configuration:
the tested subscription returned incomplete containers. Reproduce the boundary
with the explicit diagnostic (four synthesis requests; not a feature acceptance test):

```sh
uv run crates/zhir-testing/scripts/minimax_opus_probe.py --output test-results/minimax-opus-probe
```

It preserves sentence files, Ogg page flags/granules, provider duration metadata
and errors. A successful task_finished or decodable prefix is insufficient: an
absent end page or header-only sentence cannot establish complete audio.

## Live subscription verification

`tests/gpt_live.rs` exercises the adapter through native ports and the Runtime,
including control confirmation, media pressure, provider rejection details and
uncertain closure. Its ignored OAuth cases cover real spoken input and an eight
second blackout of the original WebRTC connection.

`scripts/live_probe.py` orchestrates `tests/live_subscription_probe.rs` as a separate
media process. It probes output controls during speech, sideband reattachment,
explicit peer destruction, SIGKILL and fork access using a saved remote call ID.
See [testing instructions](../../docs/testing.md) for the invocation and evidence
format. Probe completion does not assert support for interruption or checkpoint
recovery; successful controls and rejected operations remain distinct in the report.
