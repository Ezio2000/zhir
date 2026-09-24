//! Transport-owned peer values. The driver converts them to and from the WebRTC stack,
//! so adapters and hosts never depend on it.
use webrtc::{
    ice_transport::ice_server::RTCIceServer,
    peer_connection::{
        configuration::RTCConfiguration, peer_connection_state::RTCPeerConnectionState,
    },
    rtp_transceiver::rtp_codec::{RTCRtpCodecCapability, RTCRtpCodecParameters},
};

/// Peer connection state as reported by the WebRTC stack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    New,
    Connecting,
    Connected,
    Disconnected,
    Failed,
    Closed,
}
impl From<RTCPeerConnectionState> for ConnectionState {
    fn from(state: RTCPeerConnectionState) -> Self {
        match state {
            RTCPeerConnectionState::Unspecified | RTCPeerConnectionState::New => Self::New,
            RTCPeerConnectionState::Connecting => Self::Connecting,
            RTCPeerConnectionState::Connected => Self::Connected,
            RTCPeerConnectionState::Disconnected => Self::Disconnected,
            RTCPeerConnectionState::Failed => Self::Failed,
            RTCPeerConnectionState::Closed => Self::Closed,
        }
    }
}

#[derive(Clone, Copy)]
pub struct Connection {
    pub state: ConnectionState,
    pub since: tokio::time::Instant,
}
impl Connection {
    pub(crate) fn new(state: ConnectionState) -> Self {
        Self {
            state,
            since: tokio::time::Instant::now(),
        }
    }
}

/// One STUN or TURN server, the URLs that reach it and, for TURN, its credentials.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IceServer {
    pub urls: Vec<String>,
    pub username: String,
    pub credential: String,
}

/// The single audio codec a peer registers, sends and receives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AudioCodec {
    pub mime_type: String,
    pub clock_rate: u32,
    pub channels: u16,
    pub fmtp: String,
    pub payload_type: u8,
}

pub(crate) fn configuration(ice_servers: &[IceServer]) -> RTCConfiguration {
    RTCConfiguration {
        ice_servers: ice_servers
            .iter()
            .map(|server| RTCIceServer {
                urls: server.urls.clone(),
                username: server.username.clone(),
                credential: server.credential.clone(),
            })
            .collect(),
        ..Default::default()
    }
}
pub(crate) fn capability(codec: &AudioCodec) -> RTCRtpCodecCapability {
    RTCRtpCodecCapability {
        mime_type: codec.mime_type.clone(),
        clock_rate: codec.clock_rate,
        channels: codec.channels,
        sdp_fmtp_line: codec.fmtp.clone(),
        rtcp_feedback: vec![],
    }
}
pub(crate) fn parameters(codec: &AudioCodec) -> RTCRtpCodecParameters {
    RTCRtpCodecParameters {
        capability: capability(codec),
        payload_type: codec.payload_type,
        ..Default::default()
    }
}
