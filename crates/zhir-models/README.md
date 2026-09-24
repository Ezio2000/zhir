# zhir-models

Protocol-level model adapters and composition over core/policies. FunctionModel wraps
generation exchanges; optional `openai-chat`, `openai-responses` and `anthropic` features
add HTTP/SSE protocols. `websocket` and `webrtc` enable reusable native-session drivers.
Service endpoints, model/voice defaults and provider task semantics belong to independent
integration crates: [zhir-minimax](../zhir-minimax/README.md) and
[zhir-openai](../zhir-openai/README.md).

Includes TransformModel, ResourceModel, session and request concurrency limits,
EstablishmentRetryModel, HTTP request retry before the response body, stable-ID
fallback recovery, static/refreshing credential providers and
explicit endpoint profile mappings. ProtocolExtension and ProviderToolAdapter
implement endpoint-specific requests, outputs and replay. Protocol defaults are not
live model capability discovery. OAuth login remains the host's responsibility.

Every model opens a ModelSession with a shareable `control` handle, a single-consumer
`events` stream and independently optional `media.input` / `media.output` ports.
Use `control.submit(command)` to request operations; confirmations and final session
errors arrive through `events.receive()`. Decorators preserve each endpoint's ownership.

## Native protocol adapters

`websocket::{WebSocketAdapter, WebSocketProtocol, Action, WireMessage}` and
`WebSocketModel::new` let independent packages supply capabilities, negotiation,
per-session protocol state and command/event mappings. The driver owns connection
lifetime, handshake authentication, heartbeat, bounded delivery and cancellation.
`WebSocketConfig` includes credential resolution in its connection timeout; an HTTP
401 permits one generation-aware credential invalidation before a new handshake.

`webrtc::{WebRtcAdapter, WebRtcSession, WebRtcProtocol, WebRtcMedia,
WebRtcConnectionPolicy, PeerSettings, Action}` and `WebRtcModel::new` support a text
DataChannel and an independent RTP audio queue. The adapter supplies signaling,
codec settings, connection policy and synchronous protocol transitions. It may
request connection at initialization or after receiving session commands.

`WebRtcMedia::receive` consumes an `AudioPacket` and returns an optional `MediaChunk`.
The chunk payload cannot be larger than the packet payload. The driver preserves
its byte/slot reservation through mapping until public consumption; a discarded or
rejected packet releases it. Adapters cannot access queues, permits or driver tasks.
`webrtc::RtpTimeline` handles source identity, packet ordering and timestamp wrap.
`Confirmation<T>` provides a serialized remote barrier with a fixed monotonic deadline;
adapters own expected events and acknowledgement matching.

Private native machinery owns bounded ports, output epochs, event sequencing and
terminal delivery. Accepted events precede terminal failure, even under backpressure.
The shared driver owns only connection/session I/O, never Agent execution or checkpoint
commits. External integrations may instead implement core's Model contract directly.

Features have no provider dependencies. Enabling WebSocket does not enable HTTP or
WebRTC, and enabling WebRTC does not enable a service's signaling client.

Part of the zhir workspace, version 0.4.0.
