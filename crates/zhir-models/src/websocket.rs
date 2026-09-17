//! Persistent model-session orchestration, independent of provider wire semantics.
use crate::native::{self, Outputs, guarded};
use crate::transport::websocket::Socket;
pub use crate::transport::websocket::WireMessage;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use zhir_core::{
    BoxFuture, Result, credential::CredentialProvider, error::Error, model::*,
    profile::NegotiatedProfile, resource::MediaChunk,
};

#[derive(Clone)]
pub struct WebSocketConfig {
    pub url: String,
    pub credentials: Arc<dyn CredentialProvider>,
    /// Includes credential resolution and the optional single 401 refresh.
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    /// Maximum wait for protocol startup, flush, cancellation or completion.
    pub command_timeout: Duration,
    /// Ping interval for idle connections. This does not replay model commands.
    pub heartbeat_interval: Duration,
    pub max_message_bytes: usize,
}
impl WebSocketConfig {
    pub fn new(url: impl Into<String>, credentials: Arc<dyn CredentialProvider>) -> Self {
        Self {
            url: url.into(),
            credentials,
            connect_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(15),
            command_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(30),
            max_message_bytes: 4 * 1024 * 1024,
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let request = self
            .url
            .clone()
            .into_client_request()
            .map_err(|_| Error::Invalid("invalid WebSocket URL".into()))?;
        if !matches!(request.uri().scheme_str(), Some("ws" | "wss"))
            || [
                self.connect_timeout,
                self.write_timeout,
                self.command_timeout,
                self.heartbeat_interval,
            ]
            .iter()
            .any(|duration| {
                duration.is_zero() || std::time::Instant::now().checked_add(*duration).is_none()
            })
            || self.max_message_bytes == 0
        {
            return Err(Error::Invalid(
                "invalid WebSocket connection settings".into(),
            ));
        }
        Ok(())
    }
}

/// WebSocket session driver. External adapters own protocol state and wire messages;
/// this model owns transport, bounded delivery, cancellation and deadlines.
#[derive(Clone)]
pub struct WebSocketModel {
    config: WebSocketConfig,
    adapter: Arc<dyn WebSocketAdapter>,
    capabilities: CapabilitySet,
}
pub trait WebSocketAdapter: Send + Sync {
    fn capabilities(&self) -> CapabilitySet;
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile>;
    fn open(&self, open: &SessionOpen) -> Result<Box<dyn WebSocketProtocol>>;
}
pub trait WebSocketProtocol: Send {
    fn connected(&mut self) -> Result<()>;
    fn commands_allowed(&self) -> bool;
    fn finished(&self) -> bool;
    fn generation_id(&self) -> Option<&str>;
    fn deadline(&self) -> Option<tokio::time::Instant>;
    fn check_deadline(&self) -> Result<()>;
    fn command(&mut self, command: SessionCommand) -> Result<Vec<Action>>;
    fn receive(&mut self, message: WireMessage) -> Result<Vec<Action>>;
}
pub enum Action {
    Send(WireMessage),
    Event(SessionEventBody),
    Observe(ModelDelta),
    Media(MediaChunk),
    Fail(Error),
    Close,
}
impl WebSocketModel {
    pub fn new(config: WebSocketConfig, adapter: Arc<dyn WebSocketAdapter>) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            capabilities: adapter.capabilities(),
            config,
            adapter,
        })
    }
}
impl Model for WebSocketModel {
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
            let deadline = zhir_policies::timing::deadline(&open.context.run)?;
            zhir_policies::timing::check(&open.context.cancellation, deadline)?;
            let mut protocol = self.adapter.open(&open)?;
            let connect = async {
                tokio::time::timeout(self.config.connect_timeout, Socket::connect(&self.config))
                    .await
                    .map_err(|_| Error::Deadline)?
            };
            let socket = guarded(connect, &open.context.cancellation, deadline).await?;
            protocol.connected()?;
            let (session, ports) = native::ports(
                Arc::new(self.clone()),
                &open.limits,
                false,
                open.output_epoch,
            );
            drop(ports.audio_input);
            let event_port = ports.outputs.events.clone();
            let mut driver = Driver {
                socket,
                protocol,
                commands: ports.commands,
                outputs: ports.outputs,
                heartbeat_interval: self.config.heartbeat_interval,
            };
            tokio::spawn(async move {
                let result = tokio::select! {
                    result = guarded(driver.run(), &open.context.cancellation, deadline) => result,
                    _ = event_port.closed() => Err(Error::Cancelled),
                };
                let _ = ports.terminal.send(driver.outputs.settle(result));
            });
            Ok(session)
        })
    }
}
struct Driver {
    socket: Socket,
    protocol: Box<dyn WebSocketProtocol>,
    commands: mpsc::Receiver<SessionCommand>,
    outputs: Outputs,
    heartbeat_interval: Duration,
}
impl Driver {
    async fn run(&mut self) -> Result<()> {
        let mut socket_closed = false;
        let mut closing = false;
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + self.heartbeat_interval,
            self.heartbeat_interval,
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if closing && !self.outputs.pending() {
                return Ok(());
            }
            let actions = tokio::select! {
                result = self.outputs.flush_one(), if self.outputs.pending() => { result?; vec![] }
                _ = native::confirmation_deadline(self.protocol.deadline()) => {
                    self.protocol.check_deadline()?;
                    vec![]
                }
                command = self.commands.recv(), if !closing && self.protocol.commands_allowed() => {
                    let Some(command) = command else { return Ok(()); };
                    let epoch = match &command.body { SessionCommandBody::InterruptOutput { output_epoch, .. } => Some(*output_epoch), _ => None };
                    let actions = self.protocol.command(command)?;
                    if let Some(epoch) = epoch { self.outputs.invalidate_before(epoch); }
                    actions
                }
                message = self.socket.receive(), if !socket_closed && !closing && self.outputs.can_receive() => {
                    match message? {
                        Some(message) => {
                            self.protocol.receive(message)?
                        }
                        None if self.protocol.finished() => { socket_closed = true; vec![] }
                        None => return Err(Error::Uncertain("WebSocket ended before protocol completion".into())),
                    }
                }
                _ = heartbeat.tick(), if !socket_closed && !self.protocol.finished() => {
                    self.socket.ping().await?;
                    vec![]
                }
            };
            for action in actions {
                match action {
                    Action::Send(message) => self.socket.send(message).await?,
                    Action::Observe(delta) => {
                        self.outputs.event(SessionEventBody::Delta {
                            generation_id: self.protocol.generation_id().map(str::to_owned),
                            delta,
                        })?;
                    }
                    Action::Event(body) => self.outputs.event(body)?,
                    Action::Media(chunk) => self.outputs.media(chunk)?,
                    Action::Fail(error) => return Err(error),
                    Action::Close => {
                        if !socket_closed {
                            let _ = self.socket.close().await;
                        }
                        closing = true;
                    }
                }
            }
            if self.protocol.finished() {
                self.outputs.end_media();
            }
        }
    }
}
