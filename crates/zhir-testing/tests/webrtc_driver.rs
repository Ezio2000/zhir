//! Compile the production internal driver with a deterministic transport. The
//! fixture uses plain control text and PCMU, without a Live adapter or constants.
#![cfg(feature = "webrtc")]
#[path = "webrtc_driver/transport.rs"]
mod fixture;
#[allow(dead_code, unused_imports)]
#[path = "../../zhir-models/src/native.rs"]
mod native;
mod transport {
    pub(crate) use crate::fixture as webrtc;
}
#[path = "../../zhir-models/src/webrtc.rs"]
mod webrtc;
use crate::webrtc::*;
use native::Confirmation;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;
use zhir_core::{
    BoxFuture, Result, error::Error, model::*, profile::NegotiatedProfile, resource::MediaChunk,
};

struct Adapter {
    label: &'static str,
    eager: bool,
    audio: bool,
    signals: Arc<AtomicUsize>,
    grow_media: bool,
}
impl WebRtcAdapter for Adapter {
    fn capabilities(&self) -> CapabilitySet {
        CapabilitySet {
            input_modalities: vec!["text".into(), "audio".into()],
            output_modalities: vec!["audio".into()],
            features: Default::default(),
            tool_choices: vec!["auto".into(), "none".into()],
            constraints: Default::default(),
            extensions: Default::default(),
        }
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        zhir_policies::negotiation::negotiate(request, &self.capabilities())
    }
    fn open(&self, _: &SessionOpen) -> Result<WebRtcSession> {
        Ok(WebRtcSession {
            protocol: Box::new(Protocol {
                eager: self.eager,
                pending: Confirmation::default(),
                draining: false,
                closed: false,
            }),
            media: Box::new(Media {
                timeline: Default::default(),
                sequence: 0,
                grow: self.grow_media,
            }),
            connection: Box::new(Connected),
            peer: PeerSettings {
                ice_servers: vec![],
                channel_label: self.label,
                audio_codec: AudioCodec {
                    mime_type: "audio/PCMU".into(),
                    clock_rate: 8000,
                    channels: 1,
                    fmtp: String::new(),
                    payload_type: 0,
                },
                max_event_bytes: 1024,
                audio_input: self.audio,
            },
            write_timeout: Duration::from_secs(10),
            command_headroom: 2,
        })
    }
    fn signal<'a>(
        &'a self,
        session_id: &'a str,
        offer: String,
        payload: serde_json::Value,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            assert_eq!(session_id, "fixture");
            assert_eq!(offer, "fixture-offer");
            assert_eq!(payload, serde_json::json!({"fixture": true}));
            self.signals.fetch_add(1, Ordering::SeqCst);
            Ok("fixture-answer".into())
        })
    }
}
struct Connected;
impl WebRtcConnectionPolicy for Connected {
    fn evaluate(
        &self,
        connection: fixture::Connection,
        queued_events: bool,
    ) -> Result<ConnectionStatus> {
        use ConnectionState as State;
        if matches!(connection.state, State::Failed | State::Closed) && !queued_events {
            return Err(Error::Uncertain("fixture connection failed".into()));
        }
        assert!(connection.since <= Instant::now());
        let connected = connection.state == State::Connected;
        Ok(ConnectionStatus {
            commands_allowed: connected,
            input_allowed: connected,
            deadline: None,
        })
    }
}
struct Protocol {
    eager: bool,
    pending: Confirmation<String>,
    draining: bool,
    closed: bool,
}
fn ack(id: &str) -> Action {
    Action::Event(Box::new(SessionEventBody::Acknowledged {
        level: zhir_core::model::Acknowledgement::Provider,
        command_id: id.into(),
        recovery: None,
    }))
}
fn connect() -> Action {
    Action::Connect {
        payload: serde_json::json!({"fixture":true}),
        deadline: Instant::now() + Duration::from_secs(2),
        timeout: Error::Uncertain("fixture startup".into()),
    }
}
impl WebRtcProtocol for Protocol {
    fn initial(&mut self) -> Result<Vec<Action>> {
        Ok(if self.eager { vec![connect()] } else { vec![] })
    }
    fn commands_allowed(&self) -> bool {
        self.pending.pending().is_none() && !self.closed
    }
    fn input_allowed(&self) -> bool {
        false
    }
    fn draining(&self) -> bool {
        self.draining
    }
    fn finished(&self) -> bool {
        self.closed
    }
    fn closed(&self) -> bool {
        self.closed
    }
    fn deadline(&self) -> Option<Instant> {
        self.pending.deadline()
    }
    fn check_deadline(&self) -> Result<()> {
        self.pending.check()
    }
    fn command(&mut self, command: SessionCommand) -> Result<Vec<Action>> {
        match command.body {
            SessionCommandBody::Generate { .. } => Ok(vec![ack(&command.id)]),
            SessionCommandBody::Append { .. } => Ok(vec![connect()]),
            SessionCommandBody::SetInputAudio { .. } => Ok(vec![
                Action::Send("first".into()),
                Action::Send("second".into()),
                ack(&command.id),
            ]),
            SessionCommandBody::SealUserInput => {
                self.pending
                    .begin(command.id, "fixture drain", Duration::from_secs(1))?;
                Ok(vec![Action::DrainInput, Action::Send("finish".into())])
            }
            SessionCommandBody::Close => {
                self.closed = true;
                Ok(vec![Action::Disconnect, ack(&command.id)])
            }
            _ => Err(Error::Invalid("fixture command".into())),
        }
    }
    fn receive(&mut self, text: &str) -> Result<Vec<Action>> {
        match text {
            "ready" | "receipt" => Ok(vec![ack(text)]),
            "done" => {
                self.pending.complete()?;
                self.draining = true;
                Ok(vec![])
            }
            "reject" => Ok(vec![
                ack("diagnostic"),
                Action::Fail(Error::Uncertain("fixture rejection".into())),
            ]),
            _ => Err(Error::Protocol("fixture frame".into())),
        }
    }
    fn finalize(&mut self) -> Result<Vec<Action>> {
        self.draining = false;
        self.closed = true;
        Ok(vec![
            ack("end"),
            Action::Event(Box::new(SessionEventBody::Closed {
                reason: "host_request".into(),
                provider_data: serde_json::Value::Null,
            })),
        ])
    }
}
struct Media {
    timeline: RtpTimeline,
    sequence: u64,
    grow: bool,
}
impl WebRtcMedia for Media {
    fn input(&mut self, turn: &str, chunk: &MediaChunk) -> Result<Option<Duration>> {
        assert_eq!(turn, chunk.session_id);
        assert_eq!(chunk.media_type, "audio/PCMU");
        assert_eq!(chunk.epoch, 0);
        Ok(Some(Duration::from_millis(20)))
    }
    fn receive(&mut self, turn: &str, packet: fixture::AudioPacket) -> Result<Option<MediaChunk>> {
        let Some(ticks) = self
            .timeline
            .accept(packet.ssrc, packet.sequence, packet.timestamp)?
        else {
            return Ok(None);
        };
        let sequence = self.sequence;
        self.sequence += 1;
        let mut payload = packet.payload;
        if self.grow {
            payload.push(0);
        }
        Ok(Some(MediaChunk {
            stream_id: "fixture-stream".into(),
            session_id: turn.into(),
            epoch: 3,
            sequence,
            timestamp_us: ticks * 1_000_000 / 8000,
            media_type: "audio/PCMU".into(),
            bytes: payload,
            end: false,
        }))
    }
    fn finish(&mut self, turn: &str) -> Option<MediaChunk> {
        (self.sequence > 0).then(|| MediaChunk {
            stream_id: "fixture-stream".into(),
            session_id: turn.into(),
            epoch: 3,
            sequence: self.sequence,
            timestamp_us: self.timeline.ticks() * 1_000_000 / 8000,
            media_type: "audio/PCMU".into(),
            bytes: vec![],
            end: true,
        })
    }
}
fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![],
        runtime_tools: vec![],
        provider_tools: vec![],
        profile: Default::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream: false,
    }
}
async fn setup(
    label: &'static str,
    eager: bool,
    audio: bool,
) -> (ModelSession, fixture::Harness, Arc<AtomicUsize>) {
    setup_mapping(label, eager, audio, false).await
}
async fn setup_mapping(
    label: &'static str,
    eager: bool,
    audio: bool,
    grow_media: bool,
) -> (ModelSession, fixture::Harness, Arc<AtomicUsize>) {
    let harness = fixture::Harness::new(label);
    let signals = Arc::new(AtomicUsize::new(0));
    let model = WebRtcModel::new(Arc::new(Adapter {
        label,
        eager,
        audio,
        signals: signals.clone(),
        grow_media,
    }));
    let mut limits = zhir_kernel::defaults::limits();
    limits.max_media_chunk_bytes = 3;
    limits.max_buffered_media_bytes = 3;
    let session = model
        .open_session(SessionOpen {
            binding: None,
            context_revision: 0,
            input_position: 0,
            profile_revision: 0,
            mode: zhir_core::run::RunMode::Interactive,
            session_id: "fixture".into(),
            output_epoch: 3,
            after_sequence: None,
            recovery: None,
            limits,
            request: request(),
            context: ModelContext {
                run: zhir_core::run::RunContext::new("fixture", 0),
                cancellation: Default::default(),
                deltas: None,
            },
        })
        .await
        .unwrap();
    (session, harness, signals)
}
async fn command(session: &ModelSession, id: &str, body: SessionCommandBody) {
    session
        .control
        .submit(SessionCommand {
            id: id.into(),
            body,
        })
        .await
        .unwrap();
}
async fn receipt(session: &mut ModelSession, expected: &str) {
    assert!(
        matches!(session.events.receive().await.unwrap().unwrap().body, SessionEventBody::Acknowledged { command_id, .. } if command_id == expected)
    );
}
fn chunk(sequence: u64) -> MediaChunk {
    MediaChunk {
        stream_id: "mic".into(),
        session_id: "fixture".into(),
        epoch: 0,
        sequence,
        timestamp_us: sequence * 20_000,
        media_type: "audio/PCMU".into(),
        bytes: vec![sequence as u8],
        end: false,
    }
}

#[tokio::test]
async fn adapter_can_connect_initially_without_audio_input() {
    let (mut session, harness, signals) = setup("eager", true, false).await;
    assert!(session.media.input.is_none());
    receipt(&mut session, "ready").await;
    assert_eq!(signals.load(Ordering::SeqCst), 1);
    command(&session, "close", SessionCommandBody::Close).await;
    receipt(&mut session, "close").await;
    assert!(session.events.receive().await.unwrap().is_none());
    assert!(harness.state.closed.load(Ordering::SeqCst));
}
#[tokio::test]
async fn commands_before_connect_do_not_require_a_peer() {
    let (mut session, _harness, signals) = setup("deferred", false, false).await;
    command(
        &session,
        "start",
        SessionCommandBody::Generate {
            generation_id: "fixture-turn".into(),
            context_revision: 0,
            input_position: 0,
            profile_revision: 0,
        },
    )
    .await;
    receipt(&mut session, "start").await;
    assert_eq!(signals.load(Ordering::SeqCst), 0);
    command(
        &session,
        "connect",
        SessionCommandBody::Append {
            entry: zhir_core::run::HistoryEntry {
                id: "connect".into(),
                origin: None,
                message: zhir_core::message::Message::user("connect"),
            },
            context_revision: 1,
            input_position: 1,
            source: AppendSource::Submitted,
        },
    )
    .await;
    receipt(&mut session, "ready").await;
    assert_eq!(signals.load(Ordering::SeqCst), 1);
    command(&session, "close", SessionCommandBody::Close).await;
    receipt(&mut session, "close").await;
    assert!(session.events.receive().await.unwrap().is_none());
}
#[tokio::test]
async fn draining_keeps_receipts_runnable_and_preserves_write_and_media_order() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (mut session, mut harness, _) = setup("drain", true, true).await;
        receipt(&mut session, "ready").await;
        for i in 0..3 {
            session
                .media
                .input
                .as_ref()
                .unwrap()
                .send(chunk(i))
                .await
                .unwrap();
        }
        command(&session, "end", SessionCommandBody::SealUserInput).await;
        let fixture::Write::Audio(bytes, duration, accept) = harness.writes.recv().await.unwrap()
        else {
            panic!("close overtook audio");
        };
        assert_eq!(bytes, [0]);
        assert_eq!(duration, Duration::from_millis(20));
        harness.event("receipt");
        receipt(&mut session, "receipt").await;
        assert!(
            session
                .media
                .input
                .as_ref()
                .unwrap()
                .send(chunk(3))
                .await
                .is_err()
        );
        accept.send(()).unwrap();
        for i in 1..3 {
            let fixture::Write::Audio(bytes, _, accept) = harness.writes.recv().await.unwrap()
            else {
                panic!("close overtook audio");
            };
            assert_eq!(bytes, [i]);
            accept.send(()).unwrap();
        }
        let fixture::Write::Control(text, accept) = harness.writes.recv().await.unwrap() else {
            panic!("missing finish");
        };
        assert_eq!(text, "finish");
        accept.send(()).unwrap();
        // Queue final media and closure together; accepted packets must precede
        // the adapter's end marker even if the control event wins the select.
        for i in 0..3 {
            harness.audio(i);
        }
        harness.event("done");
        receipt(&mut session, "end").await;
        assert!(matches!(
            session.events.receive().await.unwrap().unwrap().body,
            SessionEventBody::Closed { .. }
        ));
        assert!(session.events.receive().await.unwrap().is_none());
        let budget = harness
            .state
            .config
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .media_budget
            .clone();
        assert!(matches!(budget.try_reserve(1), Err(Error::Uncertain(_))));
        let media = session.media.output.as_mut().unwrap();
        for i in 0..3 {
            let packet = media.receive().await.unwrap().unwrap();
            assert_eq!(
                (
                    packet.sequence,
                    packet.timestamp_us,
                    packet.bytes,
                    packet.end
                ),
                (i, i * 20_000, vec![i as u8], false)
            );
        }
        let end = media.receive().await.unwrap().unwrap();
        assert!(end.end);
        assert_eq!(end.sequence, 3);
        assert!(media.receive().await.unwrap().is_none());
        budget.try_reserve(3).unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test(start_paused = true)]
async fn input_drain_cannot_hide_confirmation_expiry_behind_a_stalled_write() {
    let (mut session, mut harness, _) = setup("deadline", true, true).await;
    receipt(&mut session, "ready").await;
    for i in 0..3 {
        session
            .media
            .input
            .as_ref()
            .unwrap()
            .send(chunk(i))
            .await
            .unwrap();
    }
    command(&session, "end", SessionCommandBody::SealUserInput).await;
    let fixture::Write::Audio(_, _, accept) = harness.writes.recv().await.unwrap() else {
        panic!("missing audio");
    };
    harness.event("receipt");
    receipt(&mut session, "receipt").await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        matches!(session.events.receive().await, Err(Error::Uncertain(message)) if message.contains("fixture drain"))
    );
    assert!(accept.is_closed(), "deadline must cancel the pending write");
    assert!(harness.state.closed.load(Ordering::SeqCst));
    assert!(
        harness.writes.try_recv().is_err(),
        "no close or remaining input after expiry"
    );
    assert!(session.events.receive().await.unwrap().is_none());
}
#[tokio::test]
async fn rejection_preserves_diagnostic_and_cancels_pending_write() {
    let (mut session, mut harness, _) = setup("rejection", true, true).await;
    receipt(&mut session, "ready").await;
    session
        .media
        .input
        .as_ref()
        .unwrap()
        .send(chunk(0))
        .await
        .unwrap();
    command(&session, "end", SessionCommandBody::SealUserInput).await;
    let fixture::Write::Audio(_, _, accept) = harness.writes.recv().await.unwrap() else {
        panic!("missing audio");
    };
    harness.event("reject");
    receipt(&mut session, "diagnostic").await;
    assert!(
        matches!(session.events.receive().await, Err(Error::Uncertain(message)) if message == "fixture rejection")
    );
    assert!(accept.is_closed());
    assert!(session.events.receive().await.unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn connection_policy_gates_queued_writes_without_blocking_observations() {
    use ConnectionState as State;
    let (mut session, mut harness, _) = setup("connection", true, false).await;
    receipt(&mut session, "ready").await;
    command(
        &session,
        "gate",
        SessionCommandBody::SetInputAudio { enabled: false },
    )
    .await;
    let fixture::Write::Control(first, accept) = harness.writes.recv().await.unwrap() else {
        panic!("expected control");
    };
    assert_eq!(first, "first");
    harness.connection(State::Disconnected);
    accept.send(()).unwrap();
    harness.event("receipt");
    receipt(&mut session, "receipt").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), harness.writes.recv())
            .await
            .is_err()
    );
    harness.connection(State::Connected);
    let fixture::Write::Control(second, accept) = harness.writes.recv().await.unwrap() else {
        panic!("expected second control");
    };
    assert_eq!(second, "second");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), session.events.receive())
            .await
            .is_err(),
        "local acknowledgement overtook its write"
    );
    accept.send(()).unwrap();
    receipt(&mut session, "gate").await;
    // Queued diagnostics must be interpreted before a connection failure wins.
    harness.event("receipt");
    harness.event("reject");
    harness.connection(State::Failed);
    receipt(&mut session, "receipt").await;
    receipt(&mut session, "diagnostic").await;
    assert!(
        matches!(session.events.receive().await, Err(Error::Uncertain(message)) if message == "fixture rejection")
    );
}

#[tokio::test]
async fn media_mapping_cannot_exceed_its_ingress_reservation() {
    let (mut session, harness, _) = setup_mapping("growing-media", true, false, true).await;
    receipt(&mut session, "ready").await;
    let budget = harness
        .state
        .config
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .media_budget
        .clone();
    harness.audio(0);
    let error = tokio::time::timeout(Duration::from_secs(2), session.events.receive())
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, Error::Invalid(message) if message == "media mapping grew the reserved payload")
    );
    assert!(
        session
            .media
            .output
            .as_mut()
            .unwrap()
            .receive()
            .await
            .unwrap()
            .is_none()
    );
    budget
        .try_reserve(3)
        .expect("rejected mapping must release its reservation");
    assert!(harness.state.closed.load(Ordering::SeqCst));
}
