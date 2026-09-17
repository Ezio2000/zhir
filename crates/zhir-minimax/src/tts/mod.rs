//! Bidirectional text-to-speech over MiniMax's `/ws/v1/t2a_v2_bidi` protocol.
//! Text is supplied through Session commands; each output sentence is a media stream.
mod audio;
mod protocol;
mod settings;
pub use settings::*;
use std::sync::Arc;
use zhir_core::{Result, credential::CredentialProvider, error::Error};
use zhir_models::{WebSocketConfig, WebSocketModel};

#[derive(Clone)]
pub struct TtsConfig {
    pub connection: WebSocketConfig,
    pub model: String,
    pub voice: VoiceSettings,
    pub audio: AudioSettings,
    /// Optional language hint accepted by MiniMax (for example Chinese or English).
    pub language_boost: Option<String>,
    /// Pronunciation entries in MiniMax’s word/(phonetic) notation.
    pub pronunciation_dictionary: Vec<String>,
    pub timbre_weights: Vec<TimbreWeight>,
    pub voice_effects: Option<VoiceEffects>,
    /// Subtitle payloads are retained in ProtocolEvent observations.
    pub subtitles: Option<SubtitleGranularity>,
    /// Continuous inference on speech-2.8 models; false selects sentence splitting.
    pub continuous_sound: bool,
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
            language_boost: None,
            pronunciation_dictionary: vec![],
            timbre_weights: vec![],
            voice_effects: None,
            subtitles: None,
            continuous_sound: false,
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.model.trim().is_empty()
            || (self.timbre_weights.is_empty() == self.voice.voice_id.trim().is_empty())
            || self.timbre_weights.len() > 4
            || self
                .timbre_weights
                .iter()
                .any(|v| v.voice_id.trim().is_empty() || !(1..=100).contains(&v.weight))
            || !self.voice.speed.is_finite()
            || !(0.5..=2.0).contains(&self.voice.speed)
            || !self.voice.volume.is_finite()
            || self.voice.volume <= 0.0
            || self.voice.volume > 10.0
            || !(-12..=12).contains(&self.voice.pitch)
            || ![8000, 16000, 22050, 24000, 32000, 44100].contains(&self.audio.sample_rate)
            || (self.audio.format == AudioFormat::Mp3
                && ![32000, 64000, 128000, 256000].contains(&self.audio.bitrate))
            || ![1, 2].contains(&self.audio.channels)
            || (matches!(
                self.audio.format,
                AudioFormat::PcmuRaw | AudioFormat::PcmuWav
            ) && self.audio.sample_rate != 8000)
            || self.voice_effects.as_ref().is_some_and(|v| {
                self.audio.format != AudioFormat::Mp3
                    || [v.pitch, v.intensity, v.timbre]
                        .iter()
                        .any(|v| !(-100..=100).contains(v))
            })
            || (self.voice.latex_read && self.language_boost.as_deref() != Some("Chinese"))
            || self
                .language_boost
                .as_ref()
                .is_some_and(|s| s.trim().is_empty())
            || self
                .pronunciation_dictionary
                .iter()
                .any(|s| s.trim().is_empty())
        {
            return Err(Error::Invalid(
                "invalid MiniMax TTS model, voice or audio settings".into(),
            ));
        }
        Ok(())
    }
}

pub fn model(config: TtsConfig) -> Result<WebSocketModel> {
    config.validate()?;
    WebSocketModel::new(
        config.connection.clone(),
        Arc::new(protocol::Adapter::new(config)),
    )
}
