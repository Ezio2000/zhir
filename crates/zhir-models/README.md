# zhir-models

Model-session adapters and composition over core/policies. FunctionModel wraps
turn exchanges; optional `openai-chat`, `openai-responses` and `anthropic` features
add HTTP/SSE protocols. The optional `minimax` feature adds bidirectional TTS over
WebSocket, using the same core session ports.

Includes TransformModel, ResourceModel, session concurrency, establishment-only
retry, stable-ID fallback recovery, static/refreshing credential providers and
explicit endpoint profile mappings. Consumer-owned ProtocolExtension and
ProviderToolAdapter implement endpoint-specific requests, outputs and replay.
Protocol defaults are not live model capability discovery. OAuth login remains the host
application's responsibility.

Part of the zhir workspace, version 0.2.0.

## MiniMax TTS

`minimax::tts::model(TtsConfig::new(model, voice_id, credentials))` returns a
WebSocketModel. TtsConfig exposes VoiceSettings, MP3 AudioSettings and a
WebSocketConfig (URL, connection/write timeout, heartbeat and wire message limit).
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
and call EndInput to flush and finish the remote task. Consume media concurrently
with the invocation result. Interrupt cancels the current synthesis without starting
a second remote task. Input acknowledgement means adapter acceptance, because the
service has no per-text acknowledgement. An uncorrelated queue rejection (2205)
is reported as uncertain instead of automatically resending text.

Every sentence is an independent MP3 media stream. Group chunks by stream_id and
epoch, respecting end markers; an interrupted sentence may be incomplete. Do not
concatenate independent MP3 containers. Microphone input, tools, profile updates,
transport recovery, explicit task_flush and other audio formats are not exposed.
Voice settings belong in TtsConfig; generation/extension overrides are rejected.
Malformed field types and missing/duplicate sentence boundaries fail explicitly.
Terminal errors use a separate completion channel so a full event queue cannot
turn failure into an apparently clean EOF.
Optional ModelContext.deltas observers receive validated JSON protocol events without
an adapter-owned transcript.

The public provider entry delegates to websocket.rs for bounded Session ports and
connection lifetime, codec/minimax_tts.rs for provider task semantics, and
streaming/minimax_tts.rs for audio boundaries. transport/websocket.rs owns socket
mechanics. Existing HTTP helpers are isolated in codec/http.rs and streaming/http.rs;
`minimax` alone does not enable reqwest. Acceptance and live tests remain in
zhir-testing. See [the SDK example](../zhir/examples/minimax_tts.rs).

## GPT-Live

`openai-live` exposes `openai::live::{LiveConfig, LiveModel, AUDIO_TYPE}`. It uses
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
and media finalization. Disconnects are uncertain and require caller reconciliation.
Session resume, manual `InterruptOutput`, generation/profile overrides, structured
output, declared runtime tools and non-text initial history are rejected. Acoustic
barge-in remains provider behavior; it does not claim an SDK output-epoch barrier.

Run the native example with host-provided environment credentials:

```sh
cargo run -p zhir --example gpt_live --features openai-live,memory
```

Protocol reference: the OpenAI Codex source's
[`methods_frameless_bidi.rs`](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/endpoint/realtime_websocket/methods_frameless_bidi.rs)
and [`protocol_frameless_bidi.rs`](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/endpoint/realtime_websocket/protocol_frameless_bidi.rs).
