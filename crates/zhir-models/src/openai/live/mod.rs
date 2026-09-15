//! GPT-Live through the native Codex subscription WebRTC endpoint.
//! Credentials and account login are supplied by the host. Audio chunks contain
//! one Opus packet (48 kHz clock, negotiated stereo); devices/codecs are host-owned.
mod adapter;
mod audio;
mod codec;
mod protocol;
mod signaling;
use std::{sync::Arc, time::Duration};
use zhir_core::{Result, credential::CredentialProvider, error::Error};

pub const AUDIO_TYPE: &str = "audio/opus;rate=48000;channels=2";
#[derive(Clone)]
pub struct LiveConfig {
    pub model: String,
    pub voice: String,
    pub instructions: String,
    pub endpoint: String,
    pub credentials: Arc<dyn CredentialProvider>,
    pub http_client: reqwest::Client,
    pub connect_timeout: Duration,
    pub command_timeout: Duration,
    /// Grace for the original WebRTC connection to recover after Disconnected.
    /// This does not create a new peer or resume a session after process restart.
    pub reconnect_timeout: Duration,
    pub max_event_bytes: usize,
    pub ice_servers: Vec<String>,
}
impl LiveConfig {
    pub fn new(credentials: Arc<dyn CredentialProvider>) -> Self {
        Self {
            model: "gpt-live-1-codex".into(), voice: "cove".into(), instructions: String::new(),
            endpoint: "https://chatgpt.com/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas".into(),
            credentials, http_client: reqwest::Client::new(), connect_timeout: Duration::from_secs(30),
            command_timeout: Duration::from_secs(15), reconnect_timeout: Duration::from_secs(10),
            max_event_bytes: 1024 * 1024, ice_servers: vec![],
        }
    }
}
/// Construct the Live provider adapter over the shared WebRTC session driver.
pub fn model(config: LiveConfig) -> Result<crate::WebRtcModel> {
    let url = reqwest::Url::parse(&config.endpoint)
        .map_err(|_| Error::Invalid("invalid Live endpoint".into()))?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.host_str().is_none()
        || url.fragment().is_some()
        || std::time::Instant::now()
            .checked_add(config.connect_timeout)
            .is_none()
        || std::time::Instant::now()
            .checked_add(config.command_timeout)
            .is_none()
        || std::time::Instant::now()
            .checked_add(config.reconnect_timeout)
            .is_none()
        || config.model.trim().is_empty()
        || config.voice.trim().is_empty()
        || config.connect_timeout.is_zero()
        || config.command_timeout.is_zero()
        || config.reconnect_timeout.is_zero()
        || config.max_event_bytes == 0
    {
        return Err(Error::Invalid("invalid Live configuration".into()));
    }
    Ok(crate::WebRtcModel::new(Arc::new(adapter::Adapter::new(
        config,
    ))))
}
