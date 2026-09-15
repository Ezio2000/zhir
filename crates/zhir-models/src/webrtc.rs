//! Session orchestration for a text DataChannel and an independent RTP audio queue.
//! Provider phases, signaling, media interpretation and connection policy are injected.
use crate::{
    native::Buffered,
    transport::webrtc::{AudioPacket, Connection},
};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use webrtc::{
    peer_connection::configuration::RTCConfiguration,
    rtp_transceiver::rtp_codec::RTCRtpCodecParameters,
};
use zhir_core::{
    BoxFuture, Result, error::Error, model::*, profile::NegotiatedProfile, resource::MediaChunk,
};

#[path = "webrtc/driver.rs"]
mod driver;

/// Built-in WebRTC models share ports and I/O orchestration. Provider factories
/// select the protocol; custom public model implementations use core's Model trait.
#[derive(Clone)]
pub struct WebRtcModel {
    adapter: Arc<dyn WebRtcAdapter>,
    capabilities: CapabilitySet,
}
impl WebRtcModel {
    pub(crate) fn new(adapter: Arc<dyn WebRtcAdapter>) -> Self {
        Self {
            capabilities: adapter.capabilities(),
            adapter,
        }
    }
}
impl Model for WebRtcModel {
    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.adapter.negotiate(request)
    }
    fn open_session(&self, open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            open.limits.validate()?;
            self.negotiate(&open.request)?;
            let session = self.adapter.open(&open)?;
            driver::open(self.clone(), open, session)
        })
    }
}

pub(crate) trait WebRtcAdapter: Send + Sync {
    fn capabilities(&self) -> CapabilitySet;
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile>;
    fn open(&self, open: &SessionOpen) -> Result<WebRtcSession>;
    fn signal<'a>(
        &'a self,
        session_id: &'a str,
        offer: String,
        payload: Value,
    ) -> BoxFuture<'a, Result<String>>;
}

pub(crate) struct WebRtcSession {
    pub protocol: Box<dyn WebRtcProtocol>,
    pub media: Box<dyn WebRtcMedia>,
    pub connection: Box<dyn WebRtcConnectionPolicy>,
    pub peer: PeerSettings,
    pub write_timeout: Duration,
    /// Space retained for receipts and finalization when admitting commands.
    pub command_headroom: usize,
}
pub(crate) struct PeerSettings {
    pub connection: RTCConfiguration,
    pub channel_label: &'static str,
    pub audio_codec: RTCRtpCodecParameters,
    pub max_event_bytes: usize,
    pub audio_input: bool,
}

pub(crate) trait WebRtcProtocol: Send {
    /// Empty for protocols that wait for a command before connecting.
    fn initial(&mut self) -> Result<Vec<Action>>;
    fn turn(&self) -> Result<&str>;
    fn commands_allowed(&self) -> bool;
    fn input_allowed(&self) -> bool;
    /// Remote completion has been accepted; the driver must stop ingress and
    /// drain accepted queues before calling finalize, exactly once.
    fn draining(&self) -> bool;
    fn finished(&self) -> bool;
    fn closed(&self) -> bool;
    fn deadline(&self) -> Option<Instant>;
    fn check_deadline(&self) -> Result<()>;
    fn command(&mut self, command: SessionCommand) -> Result<Vec<Action>>;
    fn receive(&mut self, text: &str) -> Result<Vec<Action>>;
    fn finalize(&mut self) -> Result<Vec<Action>>;
}

pub(crate) trait WebRtcMedia: Send {
    fn input(&mut self, turn: &str, chunk: &MediaChunk) -> Result<Option<Duration>>;
    /// Mapping must retain the ingress reservation until public consumption.
    fn receive(
        &mut self,
        turn: &str,
        packet: Buffered<AudioPacket>,
    ) -> Result<Option<Buffered<MediaChunk>>>;
    fn finish(&mut self, turn: &str) -> Option<MediaChunk>;
}

pub(crate) struct ConnectionStatus {
    pub commands_allowed: bool,
    pub input_allowed: bool,
    pub deadline: Option<Instant>,
}
pub(crate) trait WebRtcConnectionPolicy: Send {
    /// The provider decides whether queued confirmations defer connection loss.
    fn evaluate(&self, connection: Connection, queued_events: bool) -> Result<ConnectionStatus>;
}

pub(crate) enum Action {
    Connect {
        payload: Value,
        deadline: Instant,
        timeout: Error,
    },
    Send(String),
    DrainInput,
    Event(Box<SessionEventBody>),
    Disconnect,
    Fail(Error),
}
