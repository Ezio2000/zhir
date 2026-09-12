use crate::{
    Protocol, ProtocolExtension, codec,
    transport::{SseDecoder, SseEvent, request_error},
};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::BTreeMap;
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
struct Accumulator {
    protocol: Protocol,
    value: Value,
    calls: BTreeMap<usize, Value>,
    blocks: BTreeMap<usize, Value>,
    partial: BTreeMap<usize, String>,
    done: bool,
}
impl Accumulator {
    fn new(protocol: Protocol) -> Self {
        Self {
            protocol,
            value: match protocol {
                Protocol::Chat => {
                    json!({"choices":[{"message":{"role":"assistant","content":""},"finish_reason":null}]})
                }
                Protocol::Messages => json!({"content":[],"usage":{}}),
                Protocol::Responses => Value::Null,
            },
            calls: BTreeMap::new(),
            blocks: BTreeMap::new(),
            partial: BTreeMap::new(),
            done: false,
        }
    }
    fn push(&mut self, data: &str) -> Result<Vec<ModelDelta>> {
        if data == "[DONE]" {
            if self.protocol == Protocol::Chat {
                self.done = true;
            }
            return Ok(Vec::new());
        }
        let v: Value = serde_json::from_str(data)
            .map_err(|e| Error::Protocol(format!("invalid SSE JSON: {e}")))?;
        if v.get("error").is_some_and(|e| !e.is_null())
            || v.get("type").and_then(Value::as_str) == Some("error")
        {
            return Err(Error::Protocol(v.to_string()));
        }
        let mut deltas = Vec::new();
        match self.protocol {
            Protocol::Chat => {
                if let Some(fields) = v.as_object() {
                    for (key, value) in fields {
                        if key != "choices" && !value.is_null() {
                            self.value[key] = value.clone();
                        }
                    }
                }
                if let Some(usage) = v.get("usage").filter(|v| !v.is_null()) {
                    self.value["usage"] = usage.clone();
                    deltas.push(ModelDelta::Usage {
                        usage: codec::usage(self.protocol, usage),
                    });
                }
                if let Some(choice) = v.pointer("/choices/0") {
                    if let Some(logprobs) = choice.get("logprobs").and_then(Value::as_object) {
                        let accumulated = &mut self.value["choices"][0]["logprobs"];
                        for (key, value) in logprobs {
                            if let Some(tokens) = value.as_array() {
                                if !accumulated[key].is_array() {
                                    accumulated[key] = json!([]);
                                }
                                accumulated[key]
                                    .as_array_mut()
                                    .unwrap()
                                    .extend(tokens.clone());
                            } else if !value.is_null() {
                                accumulated[key] = value.clone();
                            }
                        }
                    }
                    if let Some(fields) = choice.as_object() {
                        for (key, value) in fields {
                            if !["delta", "logprobs"].contains(&key.as_str()) && !value.is_null() {
                                self.value["choices"][0][key] = value.clone();
                            }
                        }
                    }
                    if let Some(delta) = choice.get("delta") {
                        for (key, reasoning) in [("content", false), ("reasoning_content", true)] {
                            if let Some(text) = delta.get(key).and_then(Value::as_str) {
                                append(&mut self.value["choices"][0]["message"], key, text);
                                deltas.push(if reasoning {
                                    ModelDelta::Reasoning {
                                        output_index: 0,
                                        text: text.into(),
                                    }
                                } else {
                                    ModelDelta::Text {
                                        output_index: 0,
                                        text: text.into(),
                                    }
                                });
                            }
                        }
                        if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
                            for c in calls {
                                let index =
                                    c.get("index").and_then(Value::as_u64).ok_or_else(|| {
                                        Error::Protocol("stream tool call missing index".into())
                                    })? as usize;
                                let call=self.calls.entry(index).or_insert_with(||json!({"type":"function","function":{"name":"","arguments":""}}));
                                if let Some(id) = c.get("id") {
                                    call["id"] = id.clone();
                                }
                                if let Some(name) =
                                    c.pointer("/function/name").and_then(Value::as_str)
                                {
                                    append(&mut call["function"], "name", name);
                                }
                                let input = c
                                    .pointer("/function/arguments")
                                    .and_then(Value::as_str)
                                    .unwrap_or("");
                                append(&mut call["function"], "arguments", input);
                                deltas.push(ModelDelta::RuntimeTool {
                                    output_index: index + 1,
                                    id: c.get("id").and_then(Value::as_str).map(str::to_owned),
                                    name: c
                                        .pointer("/function/name")
                                        .and_then(Value::as_str)
                                        .map(str::to_owned),
                                    input: input.into(),
                                });
                            }
                        }
                    }
                    if let Some(reason) = choice.get("finish_reason").filter(|v| !v.is_null()) {
                        self.value["choices"][0]["finish_reason"] = reason.clone();
                    }
                }
            }
            Protocol::Responses => {
                let kind = v.get("type").and_then(Value::as_str).unwrap_or("");
                let index = v.get("output_index").and_then(Value::as_u64).unwrap_or(0) as usize;
                match kind {
                    "response.completed" | "response.incomplete" => {
                        self.value = v.get("response").cloned().ok_or_else(|| {
                            Error::Protocol("terminal response missing full value".into())
                        })?;
                        self.done = true;
                        deltas.push(ModelDelta::Usage {
                            usage: codec::usage(self.protocol, &self.value["usage"]),
                        });
                    }
                    "response.failed" => return Err(Error::Protocol(v.to_string())),
                    "response.output_text.delta" => deltas.push(ModelDelta::Text {
                        output_index: index,
                        text: v.get("delta").and_then(Value::as_str).unwrap_or("").into(),
                    }),
                    "response.reasoning_text.delta" | "response.reasoning_summary_text.delta" => {
                        deltas.push(ModelDelta::Reasoning {
                            output_index: index,
                            text: v.get("delta").and_then(Value::as_str).unwrap_or("").into(),
                        })
                    }
                    "response.function_call_arguments.delta"
                    | "response.custom_tool_call_input.delta" => {
                        deltas.push(ModelDelta::RuntimeTool {
                            output_index: index,
                            id: v.get("item_id").and_then(Value::as_str).map(str::to_owned),
                            name: None,
                            input: v.get("delta").and_then(Value::as_str).unwrap_or("").into(),
                        })
                    }
                    _ => {}
                }
            }
            Protocol::Messages => {
                let index = v.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
                match v.get("type").and_then(Value::as_str).unwrap_or("") {
                    "message_start" => {
                        self.value = v
                            .get("message")
                            .cloned()
                            .ok_or_else(|| Error::Protocol("missing streamed message".into()))?
                    }
                    "content_block_start" => {
                        let block = v
                            .get("content_block")
                            .cloned()
                            .ok_or_else(|| Error::Protocol("missing content block".into()))?;
                        if self.blocks.insert(index, block).is_some() {
                            return Err(Error::Protocol("duplicate content block".into()));
                        }
                    }
                    "content_block_delta" => {
                        let block = self
                            .blocks
                            .get_mut(&index)
                            .ok_or_else(|| Error::Protocol("delta before block start".into()))?;
                        let delta = &v["delta"];
                        match delta.get("type").and_then(Value::as_str).unwrap_or("") {
                            "text_delta" => {
                                let text = delta.get("text").and_then(Value::as_str).unwrap_or("");
                                append(block, "text", text);
                                deltas.push(ModelDelta::Text {
                                    output_index: index,
                                    text: text.into(),
                                });
                            }
                            "thinking_delta" => {
                                let text =
                                    delta.get("thinking").and_then(Value::as_str).unwrap_or("");
                                append(block, "thinking", text);
                                deltas.push(ModelDelta::Reasoning {
                                    output_index: index,
                                    text: text.into(),
                                });
                            }
                            "signature_delta" => append(
                                block,
                                "signature",
                                delta.get("signature").and_then(Value::as_str).unwrap_or(""),
                            ),
                            "input_json_delta" => {
                                let input = delta
                                    .get("partial_json")
                                    .and_then(Value::as_str)
                                    .unwrap_or("");
                                self.partial.entry(index).or_default().push_str(input);
                                deltas.push(ModelDelta::RuntimeTool {
                                    output_index: index,
                                    id: block.get("id").and_then(Value::as_str).map(str::to_owned),
                                    name: block
                                        .get("name")
                                        .and_then(Value::as_str)
                                        .map(str::to_owned),
                                    input: input.into(),
                                });
                            }
                            _ => {}
                        }
                    }
                    "content_block_stop" => {
                        if let Some(raw) = self.partial.remove(&index) {
                            self.blocks
                                .get_mut(&index)
                                .ok_or_else(|| Error::Protocol("unknown block stop".into()))?["input"] =
                                serde_json::from_str(&raw)
                                    .map_err(|e| Error::Protocol(e.to_string()))?;
                        }
                    }
                    "message_delta" => {
                        if let Some(delta) = v.get("delta").and_then(Value::as_object) {
                            for (k, value) in delta {
                                self.value[k] = value.clone();
                            }
                        }
                        if let Some(usage) = v.get("usage").and_then(Value::as_object) {
                            for (k, value) in usage {
                                self.value["usage"][k] = value.clone();
                            }
                            deltas.push(ModelDelta::Usage {
                                usage: codec::usage(self.protocol, &self.value["usage"]),
                            });
                        }
                    }
                    "message_stop" => self.done = true,
                    _ => {}
                }
            }
        }
        Ok(deltas)
    }
    fn finish(mut self) -> Result<Value> {
        if !self.done || !self.partial.is_empty() {
            return Err(Error::Protocol(
                "model stream ended before complete response".into(),
            ));
        }
        if self.protocol == Protocol::Chat && !self.calls.is_empty() {
            self.value["choices"][0]["message"]["tool_calls"] =
                json!(self.calls.into_values().collect::<Vec<_>>());
        }
        if self.protocol == Protocol::Messages {
            self.value["content"] = json!(self.blocks.into_values().collect::<Vec<_>>());
        }
        Ok(self.value)
    }
}
fn append(value: &mut Value, key: &str, text: &str) {
    let mut existing = value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    existing.push_str(text);
    value[key] = json!(existing);
}
