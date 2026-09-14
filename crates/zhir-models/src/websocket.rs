//! Persistent model-session orchestration, independent of provider wire semantics.
use crate::transport::websocket::{Socket, WireMessage};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};
use zhir_core::{
    BoxFuture, Result,
    credential::CredentialProvider,
    error::Error,
    model::*,
    profile::NegotiatedProfile,
    resource::{MediaChunk, MediaReceiver},
};

#[derive(Clone)]
pub struct WebSocketConfig {
    pub url: String,
    pub credentials: Arc<dyn CredentialProvider>,
    /// Includes credential resolution and the optional single 401 refresh.
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
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

/// Built-in WebSocket models share transport and Session ports. Provider-specific
/// constructors select the wire protocol; custom models can implement core::Model.
#[derive(Clone)]
pub struct WebSocketModel {
    config: WebSocketConfig,
    adapter: Arc<dyn WebSocketAdapter>,
    capabilities: CapabilitySet,
}
pub(crate) trait WebSocketAdapter: Send + Sync {
    fn capabilities(&self) -> CapabilitySet;
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile>;
    fn open(&self, open: &SessionOpen) -> Result<Box<dyn WebSocketProtocol>>;
}
pub(crate) trait WebSocketProtocol: Send {
    fn commands_allowed(&self) -> bool;
    fn finished(&self) -> bool;
    fn command(&mut self, command: SessionCommand) -> Result<Vec<Action>>;
    fn receive(&mut self, message: WireMessage) -> Result<Vec<Action>>;
}
pub(crate) enum Action {
    Send(WireMessage),
    Event(SessionEventBody),
    Observe(ModelDelta),
    Media(MediaChunk),
    Close,
}
impl WebSocketModel {
    pub(crate) fn new(config: WebSocketConfig, adapter: Arc<dyn WebSocketAdapter>) -> Result<Self> {
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
            let protocol = self.adapter.open(&open)?;
            let connect = async {
                tokio::time::timeout(self.config.connect_timeout, Socket::connect(&self.config))
                    .await
                    .map_err(|_| Error::Deadline)?
            };
            let socket = guarded(connect, &open.context.cancellation, deadline).await?;
            let (sender, commands) = mpsc::channel(open.limits.max_control_commands);
            let (events, receiver) = mpsc::channel(open.limits.max_session_events);
            let (completion, terminal) = oneshot::channel();
            let limit = open.limits.max_media_chunk_bytes;
            let capacity = (open.limits.max_buffered_media_bytes / limit).max(1);
            let (media, media_receiver) = mpsc::channel(capacity);
            let driver = Driver {
                socket,
                protocol,
                commands,
                events: events.clone(),
                media,
                sequence: 0,
                limit,
                heartbeat_interval: self.config.heartbeat_interval,
                deltas: open.context.deltas.clone(),
            };
            tokio::spawn(async move {
                let result = tokio::select! {
                    result = guarded(driver.run(), &open.context.cancellation, deadline) => result,
                    _ = events.closed() => Err(Error::Cancelled),
                };
                // Terminal delivery is independent of the bounded event queue.
                // It neither blocks actor cleanup nor loses errors under backpressure.
                let _ = completion.send(result);
            });
            Ok(ModelSession {
                input: Arc::new(Input {
                    sender,
                    model: self.clone(),
                }),
                output: Box::new(Events {
                    receiver,
                    terminal: Some(terminal),
                }),
                media_input: None,
                media_output: Some(Box::new(Media(media_receiver))),
            })
        })
    }
}
struct Input {
    sender: mpsc::Sender<SessionCommand>,
    model: WebSocketModel,
}
impl SessionSender for Input {
    fn capabilities(&self) -> &CapabilitySet {
        self.model.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.model.negotiate(request)
    }
    fn send(&self, command: SessionCommand) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.sender
                .send(command)
                .await
                .map_err(|_| Error::Cancelled)
        })
    }
}
struct Events {
    receiver: mpsc::Receiver<SessionEvent>,
    terminal: Option<oneshot::Receiver<Result<()>>>,
}
impl SessionReceiver for Events {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        Box::pin(async move {
            if let Some(event) = self.receiver.recv().await {
                return Ok(Some(event));
            }
            if let Some(terminal) = self.terminal.take() {
                terminal.await.map_err(|_| {
                    Error::Uncertain("WebSocket session task stopped without a result".into())
                })??;
            }
            Ok(None)
        })
    }
}
struct Media(mpsc::Receiver<MediaChunk>);
impl MediaReceiver for Media {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<MediaChunk>>> {
        Box::pin(async move { Ok(self.0.recv().await) })
    }
}
struct Driver {
    socket: Socket,
    protocol: Box<dyn WebSocketProtocol>,
    commands: mpsc::Receiver<SessionCommand>,
    events: mpsc::Sender<SessionEvent>,
    media: mpsc::Sender<MediaChunk>,
    sequence: u64,
    limit: usize,
    heartbeat_interval: Duration,
    deltas: Option<Arc<dyn DeltaSink>>,
}
impl Driver {
    async fn run(mut self) -> Result<()> {
        let mut socket_closed = false;
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + self.heartbeat_interval,
            self.heartbeat_interval,
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let actions = tokio::select! {
                command = self.commands.recv(), if self.protocol.commands_allowed() => {
                    let Some(command) = command else { return Ok(()); };
                    self.protocol.command(command)?
                }
                message = self.socket.receive(), if !socket_closed => {
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
                        if let Some(sink) = &self.deltas {
                            sink.emit(delta).await?;
                        }
                    }
                    Action::Event(body) => {
                        self.events
                            .send(SessionEvent {
                                sequence: self.sequence,
                                body,
                            })
                            .await
                            .map_err(|_| Error::Cancelled)?;
                        self.sequence = self.sequence.checked_add(1).ok_or_else(|| {
                            Error::Protocol("session event sequence overflow".into())
                        })?;
                    }
                    Action::Media(chunk) => {
                        chunk.validate(self.limit)?;
                        self.media.send(chunk).await.map_err(|_| Error::Cancelled)?;
                    }
                    Action::Close => {
                        if !socket_closed {
                            let _ = self.socket.close().await;
                        }
                        return Ok(());
                    }
                }
            }
        }
    }
}
async fn guarded<T>(
    work: impl std::future::Future<Output = Result<T>>,
    cancellation: &zhir_core::Cancellation,
    deadline: Option<std::time::Instant>,
) -> Result<T> {
    tokio::pin!(work);
    loop {
        zhir_policies::timing::check(cancellation, deadline)?;
        tokio::select! {
            result = &mut work => return result,
            _ = tokio::time::sleep(Duration::from_millis(10)) => (),
        }
    }
}
