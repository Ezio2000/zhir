use crate::{
    Protocol, ProtocolExtension,
    transport::{SseDecoder, SseEvent, request_error},
};
use futures::StreamExt;
use serde_json::{Value, json};
use zhir_core::{
    Result,
    error::Error,
    model::{ModelContext, ModelDelta},
};
pub(crate) async fn receive(
    protocol: Protocol,
    response: reqwest::Response,
    context: &ModelContext,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Value> {
    let mut bytes = response.bytes_stream();
    let mut parser = SseDecoder::default();
    let mut state = Accumulator::new(protocol);
    while let Some(chunk) = bytes.next().await {
        context.cancellation.check()?;
        for event in parser.push(&chunk.map_err(request_error)?)? {
            dispatch(&mut state, event, context, extension).await?;
        }
    }
    for event in parser.finish()? {
        dispatch(&mut state, event, context, extension).await?;
    }
    state.finish()
}
async fn dispatch(
    state: &mut Accumulator,
    mut event: SseEvent,
    context: &ModelContext,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<()> {
    context.cancellation.check()?;
    let payload = serde_json::from_str::<Value>(&event.data).unwrap_or_else(|_| json!(event.data));
    let index = payload
        .get("output_index")
        .or_else(|| payload.get("index"))
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;
    let original = json!({"event":event.event,"id":event.id,"retry":event.retry,"data":payload});
    let extra = if let Some(extension) = extension {
        extension.decode_event(state.protocol(), &mut event)?
    } else {
        Vec::new()
    };
    let deltas = state.push(&event.data)?;
    if let Some(sink) = &context.deltas {
        // Every wire frame is observable, including unknown fields on known events.
        // Do not buffer a transcript: the sink controls backpressure and retention.
        for delta in std::iter::once(ModelDelta::ProtocolEvent {
            output_index: index,
            data: original,
        })
        .chain(extra)
        .chain(deltas)
        {
            context.cancellation.check()?;
            sink.emit(delta).await?;
        }
    }
    Ok(())
}
mod chat;
mod messages;
mod responses;

enum Accumulator {
    Chat(chat::State),
    Responses(responses::State),
    Messages(messages::State),
}
impl Accumulator {
    fn new(protocol: Protocol) -> Self {
        match protocol {
            Protocol::Chat => Self::Chat(chat::State::new()),
            Protocol::Responses => Self::Responses(responses::State::default()),
            Protocol::Messages => Self::Messages(messages::State::new()),
        }
    }
    fn protocol(&self) -> Protocol {
        match self {
            Self::Chat(_) => Protocol::Chat,
            Self::Responses(_) => Protocol::Responses,
            Self::Messages(_) => Protocol::Messages,
        }
    }
    fn push(&mut self, data: &str) -> Result<Vec<ModelDelta>> {
        if data == "[DONE]" {
            if let Self::Chat(state) = self {
                state.done = true;
            }
            return Ok(Vec::new());
        }
        let value: Value = serde_json::from_str(data)
            .map_err(|e| Error::Protocol(format!("invalid SSE JSON: {e}")))?;
        if value.get("error").is_some_and(|e| !e.is_null())
            || value.get("type").and_then(Value::as_str) == Some("error")
        {
            return Err(Error::Protocol(value.to_string()));
        }
        match self {
            Self::Chat(s) => s.push(&value),
            Self::Responses(s) => s.push(&value),
            Self::Messages(s) => s.push(&value),
        }
    }
    fn finish(self) -> Result<Value> {
        match self {
            Self::Chat(s) => s.finish(),
            Self::Responses(s) => s.finish(),
            Self::Messages(s) => s.finish(),
        }
    }
}
fn incomplete() -> Error {
    Error::Protocol("model stream ended before complete response".into())
}
fn append(value: &mut Value, key: &str, text: &str) {
    let slot = &mut value[key];
    match slot {
        Value::String(existing) => existing.push_str(text),
        _ => *slot = Value::String(text.into()),
    }
}
fn copy_fields(target: &mut Value, source: &Value, exclude: &[&str]) {
    if let Some(fields) = source.as_object() {
        for (key, value) in fields {
            if !exclude.contains(&key.as_str()) && !value.is_null() {
                target[key] = value.clone();
            }
        }
    }
}
