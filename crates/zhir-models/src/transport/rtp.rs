use zhir_core::{Result, error::Error};

/// A single RTP source's accepted packet timeline. Late/duplicate packets are
/// discarded instead of turning a backwards timestamp into a multi-hour jump.
/// Changing sources requires the adapter to establish a new media stream.
#[derive(Default)]
pub struct RtpTimeline {
    last: Option<(u32, u16, u32)>,
    ticks: u64,
}
impl RtpTimeline {
    pub fn accept(&mut self, ssrc: u32, sequence: u16, timestamp: u32) -> Result<Option<u64>> {
        if let Some((source, previous_sequence, previous_timestamp)) = self.last {
            if source != ssrc {
                return Err(Error::Uncertain(
                    "RTP source changed without a media boundary".into(),
                ));
            }
            let distance = sequence.wrapping_sub(previous_sequence);
            if distance == 0 || distance >= 1 << 15 {
                return Ok(None);
            }
            let elapsed = timestamp.wrapping_sub(previous_timestamp);
            if elapsed >= 1 << 31 {
                return Err(Error::Protocol("RTP timestamp regressed".into()));
            }
            self.ticks = self
                .ticks
                .checked_add(u64::from(elapsed))
                .ok_or_else(|| Error::Protocol("RTP timestamp overflow".into()))?;
        }
        self.last = Some((ssrc, sequence, timestamp));
        Ok(Some(self.ticks))
    }
    pub fn ticks(&self) -> u64 {
        self.ticks
    }
}
