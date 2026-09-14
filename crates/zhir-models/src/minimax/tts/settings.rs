use serde::Serialize;

/// MiniMax wire formats with complete synthesis and decoding acceptance coverage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioFormat {
    #[default]
    Mp3,
    Pcm,
    Flac,
    /// Streaming WAV has unknown RIFF/data lengths; a seekable file export must finalize them.
    Wav,
    PcmuRaw,
    /// G.711 μ-law in streaming WAV, with the same export requirement as `Wav`.
    PcmuWav,
}
impl AudioFormat {
    pub(crate) fn media_type(self, sample_rate: u32, channels: u8) -> String {
        match self {
            Self::Mp3 => "audio/mpeg".into(),
            Self::Pcm => format!("audio/pcm;encoding=s16le;rate={sample_rate};channels={channels}"),
            Self::Flac => "audio/flac".into(),
            Self::Wav | Self::PcmuWav => "audio/wav".into(),
            Self::PcmuRaw => format!("audio/PCMU;rate=8000;channels={channels}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Emotion {
    Happy,
    Sad,
    Angry,
    Fearful,
    Disgusted,
    Surprised,
    Calm,
    Fluent,
    Whisper,
}

#[derive(Clone, Debug, Serialize)]
pub struct VoiceSettings {
    /// Empty only when `TtsConfig::timbre_weights` selects a mixture.
    pub voice_id: String,
    pub speed: f64,
    #[serde(rename = "vol")]
    pub volume: f64,
    pub pitch: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emotion: Option<Emotion>,
    pub english_normalization: bool,
    /// MiniMax formula reading requires Chinese language selection.
    pub latex_read: bool,
}
impl VoiceSettings {
    pub fn new(voice_id: impl Into<String>) -> Self {
        Self {
            voice_id: voice_id.into(),
            speed: 1.0,
            volume: 1.0,
            pitch: 0,
            emotion: None,
            english_normalization: false,
            latex_read: false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AudioSettings {
    pub format: AudioFormat,
    pub sample_rate: u32,
    /// Used only for MP3. Other formats do not send a bitrate parameter.
    pub bitrate: u32,
    pub channels: u8,
}
impl Default for AudioSettings {
    fn default() -> Self {
        Self {
            format: AudioFormat::Mp3,
            sample_rate: 32000,
            bitrate: 128000,
            channels: 1,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TimbreWeight {
    pub voice_id: String,
    /// Relative weight from 1 to 100; up to four voices can be mixed.
    pub weight: u8,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SoundEffect {
    SpaciousEcho,
    AuditoriumEcho,
    LofiTelephone,
    Robotic,
}

/// MiniMax post-processing effects, available on streaming MP3 only.
#[derive(Clone, Debug, Default, Serialize)]
pub struct VoiceEffects {
    pub pitch: i32,
    pub intensity: i32,
    pub timbre: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sound_effects: Option<SoundEffect>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubtitleGranularity {
    Sentence,
    Word,
    WordStreaming,
}
