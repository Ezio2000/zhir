use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use zhir_core::{Result, error::Error};

#[derive(Clone, Copy)]
pub(crate) struct Connection {
    pub state: RTCPeerConnectionState,
    since: tokio::time::Instant,
}
impl Connection {
    pub(crate) fn new(state: RTCPeerConnectionState) -> Self {
        Self {
            state,
            since: tokio::time::Instant::now(),
        }
    }
    pub fn deadline(&self, grace: std::time::Duration) -> Option<tokio::time::Instant> {
        (self.state == RTCPeerConnectionState::Disconnected).then(|| self.since + grace)
    }
    pub fn connected(&self, grace: std::time::Duration) -> Result<bool> {
        if matches!(
            self.state,
            RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed
        ) || self
            .deadline(grace)
            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
        {
            return Err(Error::Uncertain("WebRTC connection lost".into()));
        }
        Ok(self.state == RTCPeerConnectionState::Connected)
    }
}
