//! Deterministic transport for the production driver: writes complete only when
//! the test accepts them. No network timing is needed to exercise stalled I/O.
use crate::native::{Buffered, MediaBudget};
use ::webrtc::{
    peer_connection::configuration::RTCConfiguration,
    rtp_transceiver::rtp_codec::RTCRtpCodecParameters,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};
use zhir_core::{BoxFuture, Result, error::Error};
#[path = "../../../zhir-models/src/transport/webrtc/connection.rs"]
mod connection;
pub use connection::Connection;
#[path = "../../../zhir-models/src/transport/rtp.rs"]
mod rtp;
pub use rtp::RtpTimeline;

pub struct AudioPacket {
    pub payload: Vec<u8>,
    pub timestamp: u32,
    pub sequence: u16,
    pub ssrc: u32,
}
pub(crate) struct PeerConfig {
    pub connection: RTCConfiguration,
    pub channel_label: &'static str,
    pub audio_codec: RTCRtpCodecParameters,
    pub event_capacity: usize,
    pub max_event_bytes: usize,
    pub max_audio_bytes: usize,
    pub media_budget: MediaBudget,
}
pub enum Write {
    Control(String, oneshot::Sender<()>),
    Audio(Vec<u8>, Duration, oneshot::Sender<()>),
}
struct Ingress {
    events: mpsc::Sender<String>,
    audio: mpsc::UnboundedSender<Buffered<AudioPacket>>,
}
#[derive(Default)]
pub struct State {
    ingress: Mutex<Option<Ingress>>,
    connection: Mutex<Option<watch::Sender<Connection>>>,
    pub config: Mutex<Option<PeerConfig>>,
    pub closed: std::sync::atomic::AtomicBool,
}
pub struct Harness {
    pub state: Arc<State>,
    pub writes: mpsc::UnboundedReceiver<Write>,
}
struct Setup {
    state: Arc<State>,
    writes: mpsc::UnboundedSender<Write>,
}
fn setups() -> &'static Mutex<BTreeMap<&'static str, Setup>> {
    static SETUPS: OnceLock<Mutex<BTreeMap<&'static str, Setup>>> = OnceLock::new();
    SETUPS.get_or_init(Default::default)
}
impl Harness {
    pub fn new(label: &'static str) -> Self {
        let state = Arc::new(State::default());
        let (writes, receiver) = mpsc::unbounded_channel();
        assert!(
            setups()
                .lock()
                .unwrap()
                .insert(
                    label,
                    Setup {
                        state: state.clone(),
                        writes
                    }
                )
                .is_none()
        );
        Self {
            state,
            writes: receiver,
        }
    }
    pub fn event(&self, text: &str) {
        self.state
            .ingress
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .events
            .try_send(text.into())
            .unwrap();
    }
    pub fn connection(
        &self,
        state: ::webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState,
    ) {
        self.state
            .connection
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .send_replace(Connection::new(state));
    }
    pub fn audio(&self, sequence: u16) {
        let config = self.state.config.lock().unwrap();
        let reservation = config
            .as_ref()
            .unwrap()
            .media_budget
            .try_reserve(1)
            .unwrap();
        self.state
            .ingress
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .audio
            .send(Buffered {
                value: AudioPacket {
                    payload: vec![sequence as u8],
                    timestamp: u32::from(sequence) * 160,
                    sequence,
                    ssrc: 7,
                },
                reservation,
            })
            .unwrap_or_else(|_| panic!("closed media receiver"));
    }
}
pub(crate) struct Peer {
    pub events: mpsc::Receiver<String>,
    pub audio: mpsc::UnboundedReceiver<Buffered<AudioPacket>>,
    pub failure: watch::Receiver<Option<Error>>,
    pub connection: watch::Receiver<Connection>,
    _failure: watch::Sender<Option<Error>>,
    _connection: watch::Sender<Connection>,
    state: Arc<State>,
    writes: mpsc::UnboundedSender<Write>,
}
impl Peer {
    pub async fn new(config: PeerConfig) -> Result<Self> {
        let setup = setups()
            .lock()
            .unwrap()
            .remove(config.channel_label)
            .expect("registered fixture");
        assert!(config.connection.ice_servers.is_empty());
        assert_eq!(config.audio_codec.capability.mime_type, "audio/PCMU");
        assert_eq!(config.audio_codec.capability.clock_rate, 8000);
        assert_eq!(config.max_event_bytes, 1024);
        assert_eq!(config.max_audio_bytes, 3);
        let (event_tx, events) = mpsc::channel(config.event_capacity);
        let (audio_tx, audio) = mpsc::unbounded_channel();
        let (failure_tx, failure) = watch::channel(None);
        let (connection_tx, connection) = watch::channel(Connection::new(
            ::webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState::Connected,
        ));
        *setup.state.connection.lock().unwrap() = Some(connection_tx.clone());
        *setup.state.ingress.lock().unwrap() = Some(Ingress {
            events: event_tx,
            audio: audio_tx,
        });
        *setup.state.config.lock().unwrap() = Some(config);
        Ok(Self {
            events,
            audio,
            failure,
            connection,
            _failure: failure_tx,
            _connection: connection_tx,
            state: setup.state,
            writes: setup.writes,
        })
    }
    pub async fn offer(&self) -> Result<String> {
        Ok("fixture-offer".into())
    }
    pub async fn answer(&mut self, answer: String) -> Result<()> {
        assert_eq!(answer, "fixture-answer");
        self.state
            .ingress
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .events
            .try_send("ready".into())
            .unwrap();
        Ok(())
    }
    pub fn send(&self, text: &str) -> BoxFuture<'static, Result<()>> {
        let tx = self.writes.clone();
        let text = text.to_owned();
        Box::pin(async move {
            let (done, accepted) = oneshot::channel();
            tx.send(Write::Control(text, done))
                .map_err(|_| Error::Cancelled)?;
            accepted.await.map_err(|_| Error::Cancelled)
        })
    }
    pub fn audio(&self, bytes: Vec<u8>, duration: Duration) -> BoxFuture<'static, Result<()>> {
        let tx = self.writes.clone();
        Box::pin(async move {
            let (done, accepted) = oneshot::channel();
            tx.send(Write::Audio(bytes, duration, done))
                .map_err(|_| Error::Cancelled)?;
            accepted.await.map_err(|_| Error::Cancelled)
        })
    }
    pub async fn close(&self) {
        self.state.ingress.lock().unwrap().take();
        self.state
            .closed
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
