# zhir-minimax

MiniMax service integrations, organized by capability. The current `tts` module
implements native text-to-speech; shared protocols and transport drivers remain
in `zhir-models`.

## TTS

`zhir_minimax::tts::model(TtsConfig::new(model, voice_id, credentials))` returns a
WebSocketModel. TtsConfig exposes typed VoiceSettings, AudioSettings and a
WebSocketConfig (URL, connection/write/command timeout, heartbeat and wire message limit),
plus language/pronunciation hints, emotion, normalization, formula reading, weighted
voice mixtures, voice effects, subtitle granularity and continuous inference settings.
These are creation settings; the current bidi protocol has no settings-update command.
The default endpoint is `wss://api.minimaxi.com/ws/v1/t2a_v2_bidi`.
Both pay-as-you-go API keys and Token Plan keys use the same Bearer handshake.
The SDK does not inspect key prefixes, select a billing plan or switch credentials
when quota is exhausted. The service determines entitlement and billing. The live
acceptance evidence currently uses a Token Plan key; ordinary API keys have local
handshake coverage, not live billing verification. The cc-switch launcher in testing
is deliberately subscription-only and is not part of production authentication.

Credentials use the existing CredentialProvider; `header:` metadata injects account
headers. A rejected handshake gets at most one generation-aware refresh on 401,
before any synthesis command is sent. The connection timeout includes credential
resolution. No commands are replayed after a transport failure.

The initial Generate references the acknowledged context and synthesizes its latest user text; history is
not concatenated into speech. Task mode automatically seals after ResponseStarted and
waits for task_finished, Close and drained media. In Interactive mode, append user text through Append,
and call `flush_input()` to synthesize buffered text while keeping the session open.
`seal_user_input()` flushes and finishes the remote task. Consume media concurrently
with the invocation result. Interrupt cancels the current synthesis without starting
a second remote task. Input acknowledgement means adapter acceptance, because the
service has no per-text acknowledgement. An uncorrelated queue rejection (2205)
is reported as uncertain instead of automatically resending text. Whitespace-only
fragments are preserved. Startup, flush, cancel and finish acknowledgements each have
a configurable monotonic command deadline, separate from heartbeat activity.

Every sentence is an independent media stream. Supported wire formats are MP3,
signed 16-bit little-endian PCM, FLAC, WAV and raw/WAV G.711 μ-law.
`AudioSettings.format` selects the format and every chunk declares its media type.
μ-law requires 8 kHz. MP3 remains the default.
The documented Ogg/Opus option is excluded from the production configuration:
2026-09-14 Token Plan probes at 24 kHz returned header-only short sentences and
truncated longer audio (2310 ms metadata versus a 1000 ms Ogg granule position,
without an end-of-stream page). Continuous inference also reproduced missing
payloads. 32 kHz failed with a provider error. These results describe this route,
not a general absence of Opus support at MiniMax.
Group chunks by stream_id and epoch, respecting end markers; an interrupted
sentence may be incomplete. Decode complete containers independently.
Streaming WAV uses unknown RIFF/data lengths. A host exporting a seekable WAV file
must finalize those lengths after `end`; the adapter preserves the original stream.
Provider format/rate/channel metadata is checked against the requested settings.
Voice mixtures require an empty voice_id and one to four weighted voices;
streaming voice effects require MP3. Formula reading requires explicit Chinese.
Subtitle fields, when returned, remain provider observations; the SDK does not
invent subtitle timestamps. Microphone input, tools, profile updates and
transport recovery are not exposed.
Voice settings belong in TtsConfig; generation/extension overrides are rejected.
Malformed field types and missing/duplicate sentence boundaries fail explicitly.
Terminal errors use a separate completion channel so a full event queue cannot
turn failure into an apparently clean EOF.
Validated JSON observations flow through `SessionEventBody::Delta`, even when
`ModelContext.deltas` is absent. Completion retains the latest provider `extra_info`;
character/audio accounting is not fabricated into token usage. Provider rejections
preserve the original status message and trace event before terminal failure.
The kernel session ID is sent as the provider correlation ID; its echo is not a
recovery cursor.

The protocol and sentence decoder live in `src/tts/protocol.rs` and `src/tts/audio.rs`.
Shared WebSocket transport, session ports, bounded delivery and confirmation deadlines
come from `zhir-models`. This crate has no executor loop, tool scheduler or storage.
It does not enable reqwest. Acceptance and live tests remain in `zhir-testing`.

Use direct dependencies on `zhir` and `zhir-minimax`; the SDK facade has no provider feature.
See [the example](examples/minimax_tts.rs), then run with `MINIMAX_API_KEY`:

```sh
cargo run -p zhir-minimax --example minimax_tts
```
