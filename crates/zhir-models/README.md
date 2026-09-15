# zhir-models

Model-session adapters and composition over core/policies. FunctionModel wraps
turn exchanges; optional `openai-chat`, `openai-responses` and `anthropic` features
add HTTP/SSE protocols. The optional `minimax` feature adds bidirectional TTS over
WebSocket; `openai-live` adds Codex subscription voice sessions over WebRTC.
Both use the same core session ports and native output scheduling.

Includes TransformModel, ResourceModel, session concurrency, establishment-only
retry, stable-ID fallback recovery, static/refreshing credential providers and
explicit endpoint profile mappings. Consumer-owned ProtocolExtension and
ProviderToolAdapter implement endpoint-specific requests, outputs and replay.
Protocol defaults are not live model capability discovery. OAuth login remains the host
application's responsibility.

Part of the zhir workspace, version 0.2.0.

## MiniMax TTS

`minimax::tts::model(TtsConfig::new(model, voice_id, credentials))` returns a
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

The initial StartTurn synthesizes the latest user text in the request; history is
not concatenated into speech. Use Interactive mode, append user text through Input,
and call `flush_input()` to synthesize buffered text while keeping the session open.
`end_input()` flushes and finishes the remote task. Consume media concurrently
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

Both native adapters use `native.rs` for bounded session ports, event sequencing,
terminal settlement, output scheduling and a serialized `Confirmation<T>` slot.
The slot owns command admission and a fixed monotonic deadline; timeout errors
identify the expected remote event and pending command. Adapters own
expected provider events and acknowledgement matching. The WebSocket driver owns connection lifetime, codec/minimax_tts.rs for provider task semantics, and
streaming/minimax_tts.rs for audio boundaries. transport/websocket.rs owns socket
mechanics. Existing HTTP helpers are isolated in codec/http.rs and streaming/http.rs;
`minimax` alone does not enable reqwest. Acceptance and live tests remain in
zhir-testing. See [the SDK example](../zhir/examples/minimax_tts.rs).

## GPT-Live

`openai-live` exposes `openai::live::{model, LiveConfig, AUDIO_TYPE}`.
`openai::live::model(config)` returns the shared `WebRtcModel`. It uses
Codex subscription signaling at
`https://chatgpt.com/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas`,
then WebRTC Opus media and the `oai-events` DataChannel. Defaults are
`gpt-live-1-codex` and voice `cove`. This subscription endpoint is distinct from the
public OpenAI Realtime API; a ChatGPT/Codex entitlement is required. The SDK does not
implement login or exchange a Codex token for a public API key.

Inject `CredentialProvider` with Bearer credentials and
`header:ChatGPT-Account-Id` metadata. Hosts may inject a `reqwest::Client` for their
proxy/TLS policy and configure ICE servers. A rejected HTTP 401 invalidates only the
rejected credential generation and permits one new authorization attempt. Accepted
or uncertain creates, DataChannel commands and audio are never automatically replayed.

Audio ports carry one raw Opus packet per `MediaChunk`, with media type `AUDIO_TYPE`
(`audio/opus;rate=48000;channels=2`). Input uses epoch zero, the current execution turn,
and caller-owned stream/sequence IDs. Packet duration is derived from its Opus TOC.
The host paces capture, supplies a continuous audio track, and owns encoding, decoding,
jitter buffering and playback. Output timestamps use the RTP 48 kHz clock; packets
are delivered in arrival order. This is not an Ogg file or PCM byte stream. The smoke
example supplies paced Opus silence while exercising spoken text context.

`turn.done` commits an independent user/assistant `ConversationItem`.
`delegation.created` introduces a native delegation operation. Register a host
`DelegationHandler` with `Runtime::builder(...).delegation(...)`; it may use an
ordinary SDK Runtime for backend work. Durable `OperationUpdate::Context` maps to
`delegation.context.append` on `commentary`; a final `OperationOutcome` maps to
`speakable`. UTF-8 fragments are at most 500 bytes. These appends acknowledge local
transport acceptance: the subscription protocol does not echo a correlating command
ID or confirm that the speech has finished. There is no function-tool impersonation.

`EndInput` drains accepted audio, requests native closure and waits for session usage
and media finalization. `LiveConfig.reconnect_timeout` gives the original peer a
bounded grace after `Disconnected` (10 seconds by default). New commands and audio
wait while disconnected; confirmation deadlines continue to run. Reconnection
does not replay sent commands or create a new session. Failure, control-channel
closure or grace expiry is uncertain and requires caller reconciliation.
`set_input_audio_enabled(false/true)` pauses/resumes remote audio processing through
`input_audio.pause/resume` and waits for `input_audio.paused/resumed` before acknowledging
the command. The microphone port remains open; this is not output interruption. The
remote session identity, startup metadata and native usage are retained at completion.
DataChannel events and RTP packets have separate bounded transport queues, so audio
backpressure does not prevent control acknowledgements; closure drains received media
before its final marker. RTP source/sequence/timestamp identities are preserved;
the timeline handles counter wrap and drops late or duplicate packets. An unexplained
source change is uncertain rather than silently merged into the current stream.

Live's `protocol.rs` owns remote phases, command ordering, delegation identities and
confirmation matching. `webrtc.rs` defines the shared model and internal adapter,
protocol, media and connection-policy contracts; `webrtc/driver.rs` executes I/O.
`openai/live/adapter.rs` supplies per-session components and signaling, and
`openai/live/connection.rs` owns the original-peer grace policy. `audio.rs` owns the
negotiated Opus format and RTP-to-MediaChunk boundaries. Channel labels and codec
parameters come from Live, while `transport/webrtc.rs` owns the peer and wire mechanics.
The driver supports initialization effects and commands before a peer exists. Its
current scope is one text DataChannel and an independent audio queue, not video or
multi-track orchestration. `websocket` and `webrtc` select transport infrastructure;
`minimax` and `openai-live` select their provider adapters. Internal adapter traits
are crate-private; external implementations use core's `Model` contract.
Public event backpressure does not stop reading remote confirmations: native staging
remains bounded, with capacity reserved before admitting more commands. Exhausting
that staging reports a capacity failure instead of a misleading confirmation timeout.

Media reservations use actual payload bytes. For Live output, one
`max_buffered_media_bytes` budget spans the RTP receive queue, native pending output
and public media queue; consumption releases the reservation. Input has its own
budget. Each direction also has an independent 4096-chunk bound, including zero-byte
end markers; the maximum individual chunk and event queue sizes do not determine
audio packet capacity. RTP cannot backpressure a remote sender: exhausting either
receive bound fails explicitly as uncertain. Media must be consumed concurrently.
`max_event_bytes` bounds incoming/outgoing control frames and the signaling response;
audio packets use `Limits.max_media_chunk_bytes`.
An explicit `Close` drains received media, acknowledges once and ends the event port
without requiring another Close. `EndInput` also closes direct session media-input
admission and drains accepted packets before sending the remote close request.
Drain writes are polled alongside control receives, output delivery, cancellation and
fixed confirmation deadlines; a stalled packet cannot suspend confirmation expiry.

Session recovery and manual `InterruptOutput` are not implemented. Their required
remote prerequisites remain unverified or inaccessible on the tested OAuth route;
this is not an established adapter-only omission. The real subscription probes
established these boundaries:

- During generated speech, `response.cancel` and `output_audio_buffer.clear` were
  rejected. `output_audio.playback.play` with nonempty PCM audio returned
  `output_audio_playback_unsupported` (protected playback was not enabled).
- OAuth can attach and reattach `wss://api.openai.com/v1/live/{call_id}` using the
  creation response's `Location`. Sideband pause/resume receipts work. This is
  control attachment, not replacement of the primary media connection.
  Reattachment replayed some observed events, but no complete replay cursor or
  command reconciliation contract was established; `SessionOpen.after_sequence`
  is a local event sequence, not a server cursor.
- After the media process exits, sideband can briefly remain accessible. After
  explicit peer destruction, later joins returned `session_id_not_found`. After
  SIGKILL, a new sideband still received controls but rejected audio input. Neither
  experiment restored a duplex media session or reconciled the durable outbox.
- Responses delegation passed session creation, but executing `response.create`
  failed inside the service: its Responses backend hostname could not resolve.
  Direct RuntimeTools and backend profile updates therefore remain unverified for
  this OAuth route; client delegation remains the implemented mode.
- Subscription creation rejected `store`. The public fork WebSocket accepted its
  handshake but rejected `session.start` with `forbidden: Voice session access denied`.
- The public primary WebSocket at `wss://api.openai.com/v1/live/sessions` also
  accepted the OAuth handshake but rejected session startup with the same
  `forbidden` error for both `gpt-live-1-codex` and `gpt-live-1`. A successful
  sideband join therefore does not establish access to a replacement media transport.

These results describe the tested account, session configuration and entry points.
They do not establish that GPT-Live generally lacks storage, fork or natural voice
interruption. Public [Live fork](https://developers.openai.com/api/reference/typescript/resources/live/subresources/sessions/methods/fork)
derives a new session; it does not establish exact continuation of a zhir outbox.
It also requires a completed stored recording and storage enabled for the project,
as described in [session management](https://developers.openai.com/api/docs/guides/live-conversations#store-and-fork-a-session).
The [playback guide](https://developers.openai.com/api/docs/guides/voice-server-controls?api=live#control-playback-when-needed)
also distinguishes application playback control from instruction acceptance; an
instruction receipt is not an audio cutoff. No local mute, prompt injection, new
session or history replay is presented as `InterruptOutput` or `Resume`.
Implementing these capabilities requires an accessible media continuation protocol
with pending-command reconciliation, and an output stop boundary that identifies
which delayed audio belongs to the canceled output. More core fields cannot supply
these remote guarantees.

Structured signaling and control rejections retain the remote code and message;
control errors also preserve the original protocol event and correlation fields.
Already queued events precede terminal failure even under output pressure.
Remote closure while waiting for a client delegation is uncertain unless the
caller explicitly requested Close. In particular, expiry during EndInput cannot
acknowledge a completed drain while delegated work remains unfinished.
An abnormal close cannot acknowledge a pending drain; `connection_lost` is uncertain.
Generation/profile overrides, structured output, declared runtime tools and non-text
initial history are also not implemented by this adapter. Acoustic
barge-in remains provider behavior; it does not claim an SDK output-epoch barrier.

Run the native example with host-provided environment credentials:

```sh
cargo run -p zhir --example gpt_live --features openai-live,memory
```

Protocol reference: the OpenAI Codex source's
[`methods_frameless_bidi.rs`](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/endpoint/realtime_websocket/methods_frameless_bidi.rs)
and [`protocol_frameless_bidi.rs`](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/endpoint/realtime_websocket/protocol_frameless_bidi.rs).
