//! MiniMax sentence containers and bounded incremental audio decoding.
use std::time::Instant;
use zhir_core::{Result, error::Error, resource::MediaChunk};

pub(crate) struct Audio {
    epoch: u64,
    sentence: u64,
    sequence: u64,
    open: bool,
    limit: usize,
    started_at: Instant,
    media_type: String,
}
impl Audio {
    pub fn new(epoch: u64, limit: usize, media_type: String) -> Self {
        Self {
            epoch,
            sentence: 0,
            sequence: 0,
            open: false,
            limit,
            started_at: Instant::now(),
            media_type,
        }
    }
    pub fn start(&mut self) -> Result<()> {
        if self.open {
            return Err(protocol("sentence started before previous sentence ended"));
        }
        self.sentence = self
            .sentence
            .checked_add(1)
            .ok_or_else(|| protocol("sentence counter overflow"))?;
        self.sequence = 0;
        self.open = true;
        Ok(())
    }
    pub fn validate_epoch(&self, epoch: u64) -> Result<()> {
        if epoch <= self.epoch {
            return Err(protocol("interrupt must advance the kernel output epoch"));
        }
        Ok(())
    }
    pub fn cancel(&mut self, epoch: u64) -> Result<()> {
        self.validate_epoch(epoch)?;
        self.epoch = epoch;
        self.open = false;
        self.sequence = 0;
        Ok(())
    }
    pub fn push(&mut self, turn: &str, hex: &str) -> Result<MediaChunk> {
        if !self.open {
            return Err(protocol("audio outside a sentence"));
        }
        self.chunk(turn, decode_audio(hex, self.limit)?, false)
    }
    pub fn end(&mut self, turn: &str) -> Result<MediaChunk> {
        if !self.open {
            return Err(protocol("sentence ended without an open sentence"));
        }
        let chunk = self.chunk(turn, vec![], true)?;
        self.open = false;
        Ok(chunk)
    }
    pub fn finish(&self) -> Result<()> {
        if self.open {
            return Err(protocol("task finished before sentence_end"));
        }
        Ok(())
    }
    fn chunk(&mut self, turn: &str, bytes: Vec<u8>, end: bool) -> Result<MediaChunk> {
        let chunk = MediaChunk {
            stream_id: format!("minimax-tts:{}", self.sentence),
            turn_id: turn.into(),
            epoch: self.epoch,
            sequence: self.sequence,
            timestamp_us: self.started_at.elapsed().as_micros().min(u64::MAX as u128) as u64,
            media_type: self.media_type.clone(),
            bytes,
            end,
        };
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| protocol("audio sequence overflow"))?;
        Ok(chunk)
    }
}
fn decode_audio(hex: &str, limit: usize) -> Result<Vec<u8>> {
    if !hex.len().is_multiple_of(2) || hex.len() / 2 > limit {
        return Err(protocol("invalid audio length"));
    }
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digit = |b: u8| {
                (b as char)
                    .to_digit(16)
                    .ok_or_else(|| protocol("invalid audio hex"))
            };
            Ok(((digit(pair[0])? << 4) | digit(pair[1])?) as u8)
        })
        .collect()
}
pub(crate) fn protocol(message: &str) -> Error {
    Error::Protocol(format!("MiniMax TTS: {message}"))
}
