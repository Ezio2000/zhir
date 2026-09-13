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
        extension.decode_event(state.protocol, &mut event)?
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
pub(crate) mod chat;
pub(crate) mod messages;
pub(crate) mod responses;
pub(crate) trait StreamState: Send {
    fn push(&mut self, value: &Value) -> Result<Vec<ModelDelta>>;
    fn finish(self: Box<Self>) -> Result<Value>;
    fn done(&mut self) {}
}
struct Accumulator {
    protocol: Protocol,
    state: Box<dyn StreamState>,
}
impl Accumulator {
    fn new(protocol: Protocol) -> Self {
        Self {
            protocol,
            state: protocol.adapter().stream(),
        }
    }
    fn push(&mut self, data: &str) -> Result<Vec<ModelDelta>> {
        if data == "[DONE]" {
            self.state.done();
            return Ok(Vec::new());
        }
        let value: Value = serde_json::from_str(data)
            .map_err(|e| Error::Protocol(format!("invalid SSE JSON: {e}")))?;
        if value.get("error").is_some_and(|e| !e.is_null())
            || value.get("type").and_then(Value::as_str) == Some("error")
        {
            return Err(Error::Protocol(value.to_string()));
        }
        self.state.push(&value)
    }
    fn finish(self) -> Result<Value> {
        self.state.finish()
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
