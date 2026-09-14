//! Bidirectional text-to-speech over MiniMax's `/ws/v1/t2a_v2_bidi` protocol.
//! Text is supplied through Session commands; each output sentence is an MP3 stream.
use crate::{WebSocketConfig, WebSocketModel};
use std::sync::Arc;
use zhir_core::{Result, credential::CredentialProvider, error::Error};

#[derive(Clone, Debug)]
pub struct VoiceSettings {
    pub voice_id: String,
    pub speed: f64,
    pub volume: f64,
    pub pitch: i32,
}
impl VoiceSettings {
    pub fn new(voice_id: impl Into<String>) -> Self {
        Self {
            voice_id: voice_id.into(),
            speed: 1.0,
            volume: 1.0,
            pitch: 0,
        }
    }
}

/// MP3 output settings. Other audio containers are not supported by this adapter.
#[derive(Clone, Debug)]
pub struct AudioSettings {
    pub sample_rate: u32,
    pub bitrate: u32,
    pub channels: u8,
}
impl Default for AudioSettings {
    fn default() -> Self {
        Self {
            sample_rate: 32000,
            bitrate: 128000,
            channels: 1,
        }
    }
}

#[derive(Clone)]
pub struct TtsConfig {
    pub connection: WebSocketConfig,
    pub model: String,
    pub voice: VoiceSettings,
    pub audio: AudioSettings,
}
impl TtsConfig {
    pub fn new(
        model: impl Into<String>,
        voice_id: impl Into<String>,
        credentials: Arc<dyn CredentialProvider>,
    ) -> Self {
        Self {
            connection: WebSocketConfig::new(
                "wss://api.minimaxi.com/ws/v1/t2a_v2_bidi",
                credentials,
            ),
            model: model.into(),
            voice: VoiceSettings::new(voice_id),
            audio: AudioSettings::default(),
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.model.trim().is_empty()
            || self.voice.voice_id.trim().is_empty()
            || !self.voice.speed.is_finite()
            || !(0.5..=2.0).contains(&self.voice.speed)
            || !self.voice.volume.is_finite()
            || !(0.0..=10.0).contains(&self.voice.volume)
            || !(-12..=12).contains(&self.voice.pitch)
            || ![8000, 16000, 22050, 24000, 32000, 44100].contains(&self.audio.sample_rate)
            || ![32000, 64000, 128000, 256000].contains(&self.audio.bitrate)
            || ![1, 2].contains(&self.audio.channels)
        {
            return Err(Error::Invalid(
                "invalid MiniMax TTS model, voice or audio settings".into(),
            ));
        }
        self.connection.validate()
    }
}

pub fn model(config: TtsConfig) -> Result<WebSocketModel> {
    config.validate()?;
    WebSocketModel::new(
        config.connection.clone(),
        Arc::new(crate::codec::minimax_tts::Adapter::new(config)),
    )
}
