# zhir-openai

OpenAI service integrations, organized by capability. The current `live` module
implements GPT-Live through the Codex subscription endpoint. Shared Chat/Responses
protocols and transport drivers remain in `zhir-models`; this package owns OpenAI
endpoints, service configuration and protocol-specific behavior.

## Live

Use the independent `zhir-openai` package and `zhir_openai::live::{model, LiveConfig, AUDIO_TYPE}`.
`zhir_openai::live::model(config)` returns the shared `WebRtcModel`. It uses
Codex subscription signaling at
`https://chatgpt.com/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas`,
then WebRTC Opus media and the `oai-events` DataChannel. Defaults are
`gpt-live-1-codex` and voice `cove`. This subscription endpoint is distinct from the
public OpenAI Live API; a ChatGPT/Codex entitlement is required. The SDK does not
implement login or exchange a Codex token for a public API key.

Inject `CredentialProvider` with Bearer credentials and
`header:ChatGPT-Account-Id` metadata. Hosts may inject a `reqwest::Client` for their
proxy/TLS policy and configure ICE servers. A rejected HTTP 401 invalidates only the
rejected credential generation and permits one new authorization attempt. Accepted
or uncertain creates, DataChannel commands and audio are never automatically replayed.

Audio ports carry one raw Opus packet per `MediaChunk`, with media type `AUDIO_TYPE`
(`audio/opus;rate=48000;channels=2`). Input uses epoch zero, the stable session ID,
and caller-owned stream/sequence IDs. Packet duration is derived from its Opus TOC.
The host paces capture, supplies a continuous audio track, and owns encoding, decoding,
jitter buffering and playback. Output timestamps use the RTP 48 kHz clock; packets
are delivered in arrival order. This is not an Ogg file or PCM byte stream. The smoke
example supplies paced Opus silence while exercising spoken text context.

`turn.done` commits an independent user/assistant `ConversationItem`.
Opening establishes the remote session and emits Ready. Live supports Interactive only:
it has no verified per-generation boundary and emits neither ResponseStarted nor
ResponseFinished. Generate, ReplaceContext, history reducers, Task mode, resume and
manual interruption are rejected rather than simulated. SealUserInput drains input;
host results remain deliverable, then Close requests remote closure and tail draining.
`delegation.created` introduces a native delegation operation. Register a host
`DelegationHandler` with `Runtime::builder(...).delegation(...)`; it may use an
ordinary SDK Runtime for backend work. Durable `OperationUpdate::Context` maps to
`delegation.context.append` on `commentary`; a final `OperationOutcome` maps to
`speakable`. UTF-8 fragments are at most 500 bytes. These appends acknowledge local
transport acceptance: the subscription protocol does not echo a correlating command
ID or confirm that the speech has finished. There is no function-tool impersonation.

`SealUserInput` drains accepted audio, requests native closure and waits for session usage
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
confirmation matching. `zhir-models::webrtc` defines the shared model and public adapter,
protocol, media and connection-policy contracts; `webrtc/driver.rs` executes I/O.
`src/adapter.rs` supplies per-session components and signaling, and
`src/connection.rs` owns the original-peer grace policy. `audio.rs` owns the
negotiated Opus format and RTP-to-MediaChunk boundaries. Channel labels and codec
parameters come from Live, while `transport/webrtc.rs` owns the peer and wire mechanics.
The driver supports initialization effects and commands before a peer exists. Its
current scope is one text DataChannel and an independent audio queue, not video or
multi-track orchestration. `zhir-models` owns the reusable WebRTC driver and keeps its queues and reservations private.
This package supplies the protocol, signaling, media mapping and connection policy through
public driver contracts. Media mapping cannot expand the reserved packet payload.
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
without requiring another Close. `SealUserInput` also closes direct session media-input
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
caller explicitly requested Close. In particular, expiry during SealUserInput cannot
acknowledge a completed drain while delegated work remains unfinished.
An abnormal close cannot acknowledge a pending drain; `connection_lost` is uncertain.
Generation/profile overrides, structured output, declared runtime tools and non-text
initial history are also not implemented by this adapter. Acoustic
barge-in remains provider behavior; it does not claim an SDK output-epoch barrier.

Run the native example with host-provided environment credentials:

```sh
cargo run -p zhir-openai --example gpt_live
```

Protocol reference: the OpenAI Codex source's
[`methods_frameless_bidi.rs`](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/endpoint/realtime_websocket/methods_frameless_bidi.rs)
and [`protocol_frameless_bidi.rs`](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/endpoint/realtime_websocket/protocol_frameless_bidi.rs).

The public Live API creation protocol is not implemented by this package. Changing
credentials or the endpoint URL does not change the subscription request format.
The SDK facade has no dependency on this package. See [the example](examples/gpt_live.rs).
