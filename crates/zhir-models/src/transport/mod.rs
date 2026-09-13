use zhir_core::error::Error;
#[cfg(any(
    feature = "openai-chat",
    feature = "openai-responses",
    feature = "anthropic"
))]
use zhir_core::error::Failure;
#[cfg(any(
    feature = "openai-chat",
    feature = "openai-responses",
    feature = "anthropic"
))]
pub(crate) fn request_error(error: reqwest::Error) -> Error {
    Error::Uncertain(error.to_string())
}
#[cfg(any(
    feature = "openai-chat",
    feature = "openai-responses",
    feature = "anthropic"
))]
pub(crate) async fn http_error(response: reqwest::Response) -> Error {
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    Error::Model(Failure {
        code: format!("http_{}", status.as_u16()),
        message: text,
        retryable: status.as_u16() == 429 || status.is_server_error(),
    })
}
/// One SSE frame. Fields are those explicitly present in this frame; the decoder
/// does not reconnect or apply a persistent last-event ID.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SseEvent {
    pub data: String,
    pub event: Option<String>,
    pub id: Option<String>,
    pub retry: Option<u64>,
}

/// Incremental UTF-8-safe SSE framing with LF, CRLF, CR and multiline data.
#[derive(Default)]
pub struct SseDecoder {
    line: Vec<u8>,
    data: Vec<String>,
    frame: SseEvent,
    skip_lf: bool,
}
impl SseDecoder {
    pub fn push(&mut self, bytes: &[u8]) -> zhir_core::Result<Vec<SseEvent>> {
        let mut events = Vec::new();
        for &byte in bytes {
            if std::mem::take(&mut self.skip_lf) && byte == b'\n' {
                continue;
            }
            if byte != b'\n' && byte != b'\r' {
                self.line.push(byte);
                continue;
            }
            self.skip_lf = byte == b'\r';
            let bytes = std::mem::take(&mut self.line);
            let line = std::str::from_utf8(&bytes).map_err(|e| Error::Protocol(e.to_string()))?;
            if line.is_empty() {
                if !self.data.is_empty() {
                    self.frame.data = self.data.join("\n");
                    events.push(std::mem::take(&mut self.frame));
                    self.data.clear();
                } else {
                    self.frame = SseEvent::default();
                }
                continue;
            }
            let (field, value) = line.split_once(':').unwrap_or((line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "data" => self.data.push(value.into()),
                "event" => self.frame.event = Some(value.into()),
                "id" if !value.contains('\0') => self.frame.id = Some(value.into()),
                "retry" if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) => {
                    self.frame.retry = value.parse().ok();
                }
                _ => {}
            }
        }
        Ok(events)
    }
    /// An unfinished frame at EOF is not an event. Protocol assemblers must still
    /// require their terminal marker to distinguish success from truncation.
    pub fn finish(&mut self) -> zhir_core::Result<Vec<SseEvent>> {
        *self = Self::default();
        Ok(Vec::new())
    }
}
