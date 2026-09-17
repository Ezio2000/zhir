//! Bounded WebSocket transport. No model commands or provider task phases live here.
use crate::WebSocketConfig;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{
        self, Message, client::IntoClientRequest, http::HeaderValue,
        protocol::WebSocketConfig as WireConfig,
    },
};
use zhir_core::{
    Result,
    credential::CredentialContext,
    error::{Error, Failure},
};

pub enum WireMessage {
    Text(String),
    Binary(Vec<u8>),
}
pub(crate) struct Socket {
    wire: WebSocketStream<MaybeTlsStream<TcpStream>>,
    write_timeout: std::time::Duration,
    max_message_bytes: usize,
}
impl Socket {
    pub async fn connect(config: &WebSocketConfig) -> Result<Self> {
        // Retry only a rejected handshake, before any model command can be sent.
        for attempt in 0..2 {
            let credential = config
                .credentials
                .resolve(CredentialContext {
                    audience: config.url.clone(),
                    now_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                })
                .await?;
            let mut request = config
                .url
                .clone()
                .into_client_request()
                .map_err(|_| Error::Invalid("invalid WebSocket URL".into()))?;
            let mut authorization =
                HeaderValue::from_str(&format!("{} {}", credential.scheme, credential.value))
                    .map_err(|_| Error::Invalid("invalid WebSocket credential header".into()))?;
            authorization.set_sensitive(true);
            request.headers_mut().insert("Authorization", authorization);
            for (key, value) in &credential.metadata {
                let Some(key) = key.strip_prefix("header:") else {
                    continue;
                };
                let name = tungstenite::http::HeaderName::from_bytes(key.as_bytes())
                    .map_err(|_| Error::Invalid("invalid credential metadata header".into()))?;
                if request.headers().contains_key(&name)
                    || key.to_ascii_lowercase().starts_with("sec-websocket-")
                {
                    return Err(Error::Invalid(
                        "credential metadata overrides a controlled WebSocket header".into(),
                    ));
                }
                let mut value = HeaderValue::from_str(value)
                    .map_err(|_| Error::Invalid("invalid credential metadata value".into()))?;
                value.set_sensitive(true);
                request.headers_mut().insert(name, value);
            }
            let limits = WireConfig::default()
                .max_message_size(Some(config.max_message_bytes))
                .max_frame_size(Some(config.max_message_bytes));
            match connect_async_with_config(request, Some(limits), false).await {
                Ok((wire, _)) => {
                    return Ok(Self {
                        wire,
                        write_timeout: config.write_timeout,
                        max_message_bytes: config.max_message_bytes,
                    });
                }
                Err(tungstenite::Error::Http(response)) => {
                    let status = response.status().as_u16();
                    if status == 401 && attempt == 0 {
                        config
                            .credentials
                            .invalidate(&credential.generation)
                            .await?;
                        continue;
                    }
                    return Err(Error::Model(Failure {
                        code: format!("http_{status}"),
                        message: "WebSocket handshake rejected".into(),
                        retryable: status == 429 || status >= 500,
                    }));
                }
                Err(_) => return Err(Error::Protocol("WebSocket handshake failed".into())),
            }
        }
        unreachable!()
    }
    pub async fn receive(&mut self) -> Result<Option<WireMessage>> {
        loop {
            match self.wire.next().await {
                Some(Ok(Message::Text(text))) => {
                    return Ok(Some(WireMessage::Text(text.to_string())));
                }
                Some(Ok(Message::Binary(bytes))) => {
                    return Ok(Some(WireMessage::Binary(bytes.to_vec())));
                }
                Some(Ok(Message::Ping(_))) => {
                    tokio::time::timeout(self.write_timeout, self.wire.flush())
                        .await
                        .map_err(|_| Error::Uncertain("WebSocket pong timed out".into()))?
                        .map_err(|_| Error::Uncertain("WebSocket pong failed".into()))?;
                }
                Some(Ok(Message::Pong(_))) => (),
                Some(Ok(Message::Close(_))) | None => return Ok(None),
                Some(Err(tungstenite::Error::Capacity(_) | tungstenite::Error::Protocol(_))) => {
                    return Err(Error::Protocol(
                        "invalid or oversized WebSocket frame".into(),
                    ));
                }
                _ => return Err(Error::Uncertain("WebSocket connection lost".into())),
            }
        }
    }
    async fn frame(&mut self, frame: Message) -> Result<()> {
        tokio::time::timeout(self.write_timeout, self.wire.send(frame))
            .await
            .map_err(|_| Error::Uncertain("WebSocket write timed out".into()))?
            .map_err(|_| Error::Uncertain("WebSocket write failed".into()))
    }
    pub async fn send(&mut self, message: WireMessage) -> Result<()> {
        let length = match &message {
            WireMessage::Text(text) => text.len(),
            WireMessage::Binary(bytes) => bytes.len(),
        };
        if length > self.max_message_bytes {
            return Err(Error::Invalid(
                "outgoing WebSocket message exceeds the configured limit".into(),
            ));
        }
        self.frame(match message {
            WireMessage::Text(text) => Message::Text(text.into()),
            WireMessage::Binary(bytes) => Message::Binary(bytes.into()),
        })
        .await
    }
    pub async fn ping(&mut self) -> Result<()> {
        self.frame(Message::Ping(Vec::new().into())).await
    }
    pub async fn close(&mut self) -> Result<()> {
        self.frame(Message::Close(None)).await
    }
}
