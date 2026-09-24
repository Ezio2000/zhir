//! Live keeps the original peer during a bounded Disconnected interval.
//! Pending control frames are interpreted before reporting connection loss.
use std::time::Duration;
use tokio::time::Instant;
use zhir_core::{Result, error::Error};
use zhir_models::webrtc::{
    Connection, ConnectionState as State, ConnectionStatus, WebRtcConnectionPolicy,
};

pub(crate) struct LiveConnection {
    pub grace: Duration,
}
impl WebRtcConnectionPolicy for LiveConnection {
    fn evaluate(&self, connection: Connection, queued_events: bool) -> Result<ConnectionStatus> {
        let deadline =
            (connection.state == State::Disconnected).then(|| connection.since + self.grace);
        let expired = deadline.is_some_and(|deadline| Instant::now() >= deadline);
        let failed = matches!(connection.state, State::Failed | State::Closed) || expired;
        if failed && !queued_events {
            return Err(Error::Uncertain("WebRTC connection lost".into()));
        }
        let connected = connection.state == State::Connected;
        Ok(ConnectionStatus {
            commands_allowed: connected,
            input_allowed: connected,
            deadline: deadline.filter(|deadline| *deadline > Instant::now()),
        })
    }
}
