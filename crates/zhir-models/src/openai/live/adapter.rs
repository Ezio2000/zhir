use super::{
    LiveConfig,
    audio::{self, Audio},
    codec,
    protocol::Protocol,
    signaling,
};
use crate::webrtc::{PeerSettings, WebRtcAdapter, WebRtcSession};
use std::sync::Arc;
use zhir_core::{BoxFuture, Result, error::Error, model::*, profile::NegotiatedProfile};

#[path = "connection.rs"]
mod connection;

#[derive(Clone)]
pub(super) struct Adapter {
    pub(super) config: Arc<LiveConfig>,
}
impl Adapter {
    pub(super) fn new(config: LiveConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}
impl WebRtcAdapter for Adapter {
    fn capabilities(&self) -> CapabilitySet {
        CapabilitySet {
            input_modalities: vec!["text".into(), "audio".into()],
            output_modalities: vec!["text".into(), "audio".into()],
            features: [
                Capability::Streaming,
                Capability::Duplex,
                Capability::Steering,
                Capability::InputAudioControl,
                Capability::AsyncResults,
                Capability::ConversationItems,
                Capability::Delegation,
            ]
            .into(),
            tool_choices: vec!["auto".into(), "none".into()],
            constraints: Default::default(),
            extensions: Default::default(),
        }
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        if request.profile.generation != GenerationProfile::default()
            || !request.profile.extensions.is_empty()
        {
            return Err(Error::Invalid(
                "Live configuration uses LiveConfig; generation overrides are unsupported".into(),
            ));
        }
        codec::initial_items(request)?;
        zhir_policies::negotiation::negotiate(request, &self.capabilities())
    }
    fn open(&self, open: &SessionOpen) -> Result<WebRtcSession> {
        if open.recovery.is_some() || open.after_sequence.is_some() {
            return Err(Error::Protocol(
                "Live adapter has no verified subscription session recovery mechanism".into(),
            ));
        }
        Ok(WebRtcSession {
            protocol: Box::new(Protocol::new(
                self.clone(),
                open.limits.max_inflight_operations,
            )),
            media: Box::new(Audio::new(
                open.output_epoch,
                open.limits.max_media_chunk_bytes,
            )),
            connection: Box::new(connection::LiveConnection {
                grace: self.config.reconnect_timeout,
            }),
            peer: PeerSettings {
                connection: webrtc::peer_connection::configuration::RTCConfiguration {
                    ice_servers: self
                        .config
                        .ice_servers
                        .iter()
                        .map(|url| webrtc::ice_transport::ice_server::RTCIceServer {
                            urls: vec![url.clone()],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                channel_label: "oai-events",
                audio_codec: audio::codec(),
                max_event_bytes: self.config.max_event_bytes,
                audio_input: true,
            },
            write_timeout: self.config.command_timeout,
            command_headroom: 8,
        })
    }
    fn signal<'a>(
        &'a self,
        session_id: &'a str,
        offer: String,
        payload: serde_json::Value,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(signaling::create(&self.config, session_id, offer, payload))
    }
}
