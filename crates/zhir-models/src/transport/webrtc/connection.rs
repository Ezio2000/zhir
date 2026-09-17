use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;

#[derive(Clone, Copy)]
pub struct Connection {
    pub state: RTCPeerConnectionState,
    pub since: tokio::time::Instant,
}
impl Connection {
    pub(crate) fn new(state: RTCPeerConnectionState) -> Self {
        Self {
            state,
            since: tokio::time::Instant::now(),
        }
    }
}
