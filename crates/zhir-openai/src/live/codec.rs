use serde::Deserialize;
use serde_json::{Value, json};
use zhir_core::{
    Result,
    error::Error,
    message::{Content, Message, Output},
    model::ModelRequest,
};
pub(super) fn text(message: &Message) -> Result<String> {
    let parts = match message {
        Message::User { content } | Message::System { content } | Message::External { content } => {
            content.clone()
        }
        Message::Assistant { output, .. } => output
            .iter()
            .map(|o| match o {
                Output::Content { content } => Ok(content.clone()),
                _ => Err(Error::Invalid(
                    "Live history accepts conversation text only".into(),
                )),
            })
            .collect::<Result<Vec<_>>>()?,
        _ => {
            return Err(Error::Invalid(
                "Live history accepts conversation text only".into(),
            ));
        }
    };
    parts
        .into_iter()
        .map(|part| match part {
            Content::Text { text } => Ok(text),
            _ => Err(Error::Invalid("Live history accepts text only".into())),
        })
        .collect()
}
pub(super) fn initial_items(request: &ModelRequest) -> Result<Vec<Value>> {
    request.messages.iter().map(|message| {
        let role = match message { Message::System { .. } => "developer", Message::User { .. } | Message::External { .. } => "user", Message::Assistant { .. } => "assistant", _ => return Err(Error::Invalid("unsupported Live initial message".into())) };
        Ok(json!({"type":"message","role":role,"content":[{"type":if role=="assistant" {"output_text"}else{"input_text"},"text":text(message)?}]}))
    }).collect()
}
pub(super) fn context(kind: &str, id: &str, text: &str, delegation: Option<&str>) -> Vec<Value> {
    // Current subscription endpoint limits context fragments to 500 UTF-8 bytes.
    let mut result = Vec::new();
    let mut start = 0;
    loop {
        let mut end = (start + 500).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let mut value = json!({"type":kind,"event_id":format!("{id}:{}",result.len()),"channel":"commentary","content":[{"type":"input_text","text":&text[start..end]}]});
        if let Some(id) = delegation {
            value["delegation_item_id"] = json!(id);
        }
        result.push(value);
        start = end;
        if start == text.len() {
            break;
        }
    }
    result
}
#[derive(Deserialize)]
#[serde(tag = "type")]
pub(super) enum ServerEvent {
    #[serde(rename = "session.started")]
    Started { session: Value },
    #[serde(rename = "input_audio.paused")]
    AudioPaused,
    #[serde(rename = "input_audio.resumed")]
    AudioResumed,
    #[serde(rename = "turn.done")]
    Turn { turn: Turn },
    #[serde(rename = "delegation.created")]
    Delegation { item: Delegation },
    #[serde(rename = "session.closed")]
    Closed { reason: String, usage: Value },
    #[serde(rename = "error")]
    Error { error: ServerError },
    #[serde(other)]
    Observation,
}
#[derive(Deserialize)]
pub(super) struct ServerError {
    pub code: String,
    pub message: String,
}
#[derive(Deserialize)]
pub(super) struct Turn {
    pub id: String,
    pub role: String,
    pub transcript: String,
}
#[derive(Deserialize)]
pub(super) struct Delegation {
    #[serde(rename = "type")]
    pub kind: String,
    pub id: String,
    pub target: String,
    pub content: Vec<TextPart>,
}
#[derive(Deserialize)]
pub(super) struct TextPart {
    #[serde(rename = "type")]
    pub kind: String,
    pub text: String,
}

pub(super) fn context_text(content: Vec<Content>) -> Result<String> {
    content
        .into_iter()
        .map(|part| match part {
            Content::Text { text } => Ok(text),
            _ => Err(Error::Invalid(
                "Live delegation context requires text".into(),
            )),
        })
        .collect()
}
pub(super) fn outcome_text(outcome: zhir_core::operation::OperationOutcome) -> Result<String> {
    use zhir_core::operation::OperationOutcome;
    Ok(match outcome {
        OperationOutcome::Success {
            content,
            structured,
        } => {
            let mut text = context_text(content)?;
            if !structured.is_null() {
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(&structured.to_string());
            }
            text
        }
        OperationOutcome::Failure { error } => format!("Task failed: {}", error.message),
        OperationOutcome::Cancelled { reason } => format!("Task cancelled: {reason}"),
    })
}
