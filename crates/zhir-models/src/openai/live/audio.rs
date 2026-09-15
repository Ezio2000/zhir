//! Live's negotiated Opus format and mapping between RTP and SDK media streams.
use super::AUDIO_TYPE;
use crate::webrtc::WebRtcMedia;
use crate::{
    native::Buffered,
    transport::webrtc::{AudioPacket, RtpTimeline},
};
use zhir_core::{Result, error::Error, resource::MediaChunk};

const CLOCK_RATE: u32 = 48000;
pub(super) fn codec() -> webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecParameters {
    use webrtc::rtp_transceiver::rtp_codec::{RTCRtpCodecCapability, RTCRtpCodecParameters};
    RTCRtpCodecParameters {
        capability: RTCRtpCodecCapability {
            mime_type: "audio/opus".into(),
            clock_rate: CLOCK_RATE,
            channels: 2,
            sdp_fmtp_line: "minptime=10;useinbandfec=1".into(),
            rtcp_feedback: vec![],
        },
        payload_type: 111,
        ..Default::default()
    }
}
pub(super) struct Audio {
    timeline: RtpTimeline,
    sequence: u64,
    epoch: u64,
    limit: usize,
}
impl Audio {
    pub fn new(epoch: u64, limit: usize) -> Self {
        Self {
            timeline: RtpTimeline::default(),
            sequence: 0,
            epoch,
            limit,
        }
    }
}
impl WebRtcMedia for Audio {
    fn receive(
        &mut self,
        turn: &str,
        packet: Buffered<AudioPacket>,
    ) -> Result<Option<Buffered<MediaChunk>>> {
        let value = &packet.value;
        let Some(ticks) = self
            .timeline
            .accept(value.ssrc, value.sequence, value.timestamp)?
        else {
            return Ok(None);
        };
        let sequence = self.sequence;
        self.sequence = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("audio sequence overflow".into()))?;
        let packet = packet.map(|value| MediaChunk {
            stream_id: "live-audio".into(),
            turn_id: turn.into(),
            epoch: self.epoch,
            sequence,
            timestamp_us: ticks.saturating_mul(1_000_000) / u64::from(CLOCK_RATE),
            media_type: AUDIO_TYPE.into(),
            bytes: value.payload,
            end: false,
        });
        packet.value.validate(self.limit)?;
        Ok(Some(packet))
    }
    fn finish(&mut self, turn: &str) -> Option<MediaChunk> {
        (self.sequence > 0).then(|| MediaChunk {
            stream_id: "live-audio".into(),
            turn_id: turn.into(),
            epoch: self.epoch,
            sequence: self.sequence,
            timestamp_us: self.timeline.ticks().saturating_mul(1_000_000) / u64::from(CLOCK_RATE),
            media_type: AUDIO_TYPE.into(),
            bytes: vec![],
            end: true,
        })
    }
    fn input(&mut self, turn: &str, chunk: &MediaChunk) -> Result<Option<std::time::Duration>> {
        chunk.validate(self.limit)?;
        if chunk.media_type != AUDIO_TYPE || chunk.epoch != 0 || turn != chunk.turn_id {
            return Err(Error::Invalid(
                "Live requires current-turn Opus input with epoch zero".into(),
            ));
        }
        if chunk.bytes.is_empty() {
            return Ok(None);
        }
        opus_duration(&chunk.bytes).map(Some)
    }
}

// RFC 6716 section 3: duration derives from TOC, never host wall-clock timing.
fn opus_duration(packet: &[u8]) -> Result<std::time::Duration> {
    let toc = *packet
        .first()
        .ok_or_else(|| Error::Invalid("empty Opus packet".into()))?;
    let config = toc >> 3;
    let micros = if config >= 16 {
        2500_u64 << (config & 3)
    } else if config >= 12 {
        10000_u64 << (config & 1)
    } else {
        [10000, 20000, 40000, 60000][(config & 3) as usize]
    };
    let frames = match toc & 3 {
        0 => 1,
        1 | 2 => 2,
        _ => u64::from(
            *packet
                .get(1)
                .ok_or_else(|| Error::Invalid("truncated Opus TOC".into()))?
                & 63,
        ),
    };
    if frames == 0 || micros * frames > 120000 {
        return Err(Error::Invalid("invalid Opus duration".into()));
    }
    Ok(std::time::Duration::from_micros(micros * frames))
}
