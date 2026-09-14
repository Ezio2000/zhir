//! GPT-Live through the native Codex subscription WebRTC endpoint.
//! Credentials and account login are supplied by the host. Audio chunks contain
//! one Opus packet (48 kHz clock, negotiated stereo); devices/codecs are host-owned.
mod codec;
mod session;
mod signaling;
use std::{sync::Arc, time::Duration};
use zhir_core::{
    BoxFuture, Result, credential::CredentialProvider, error::Error, model::*,
    profile::NegotiatedProfile,
};

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
    pub max_event_bytes: usize,
    pub ice_servers: Vec<String>,
}
impl LiveConfig {
    pub fn new(credentials: Arc<dyn CredentialProvider>) -> Self {
        Self {
            model: "gpt-live-1-codex".into(), voice: "cove".into(), instructions: String::new(),
            endpoint: "https://chatgpt.com/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas".into(),
            credentials, http_client: reqwest::Client::new(), connect_timeout: Duration::from_secs(30),
            command_timeout: Duration::from_secs(15), max_event_bytes: 1024 * 1024, ice_servers: vec![],
        }
    }
}
#[derive(Clone)]
pub struct LiveModel {
    config: Arc<LiveConfig>,
    capabilities: CapabilitySet,
}
impl LiveModel {
    pub fn new(config: LiveConfig) -> Result<Self> {
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
            || config.model.trim().is_empty()
            || config.voice.trim().is_empty()
            || config.connect_timeout.is_zero()
            || config.command_timeout.is_zero()
            || config.max_event_bytes == 0
        {
            return Err(Error::Invalid("invalid Live configuration".into()));
        }
        Ok(Self {
            config: Arc::new(config),
            capabilities: CapabilitySet {
                input_modalities: vec!["text".into(), "audio".into()],
                output_modalities: vec!["text".into(), "audio".into()],
                features: [
                    Capability::Streaming,
                    Capability::Duplex,
                    Capability::Steering,
                    Capability::AsyncResults,
                    Capability::ConversationItems,
                    Capability::Delegation,
                ]
                .into(),
                tool_choices: vec!["auto".into(), "none".into()],
                constraints: Default::default(),
                extensions: Default::default(),
            },
        })
    }
}
impl Model for LiveModel {
    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        if request.profile.generation != GenerationProfile::default()
            || !request.profile.extensions.is_empty()
        {
            return Err(Error::Invalid(
                "Live configuration uses LiveConfig; generation overrides are unsupported".into(),
            ));
        }
        // Initial history must be text. Native audio goes through the media port.
        codec::initial_items(request)?;
        zhir_policies::negotiation::negotiate(request, &self.capabilities)
    }
    fn open_session(&self, open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            open.limits.validate()?;
            self.negotiate(&open.request)?;
            if open.recovery.is_some() || open.after_sequence.is_some() {
                return Err(Error::Protocol(
                    "Live session recovery is unsupported; no input is replayed".into(),
                ));
            }
            session::open(self.clone(), open)
        })
    }
}
