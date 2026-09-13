use super::{append, copy_fields, incomplete};
use crate::{Protocol, codec};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use zhir_core::{Result, error::Error, model::ModelDelta};

pub(crate) struct State {
    value: Value,
    calls: BTreeMap<usize, Value>,
    pub(crate) done: bool,
}
impl State {
    pub(crate) fn new() -> Self {
        Self {
            value: json!({"choices":[{"message":{"role":"assistant","content":""},"finish_reason":null}]}),
            calls: BTreeMap::new(),
            done: false,
        }
    }
    pub(crate) fn push(&mut self, value: &Value) -> Result<Vec<ModelDelta>> {
        copy_fields(&mut self.value, value, &["choices"]);
        let mut deltas = Vec::new();
        if let Some(usage) = value.get("usage").filter(|v| !v.is_null()) {
            deltas.push(ModelDelta::Usage {
                usage: codec::usage(Protocol::Chat, usage),
            });
        }
        let Some(choice) = value.pointer("/choices/0") else {
            return Ok(deltas);
        };
        copy_fields(
            &mut self.value["choices"][0],
            choice,
            &["delta", "logprobs"],
        );
        if let Some(logprobs) = choice.get("logprobs").filter(|v| v.is_object()) {
            merge_logprobs(&mut self.value["choices"][0]["logprobs"], logprobs);
        }
        let Some(delta) = choice.get("delta") else {
            return Ok(deltas);
        };
        self.text(delta, &mut deltas);
        if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                deltas.push(self.call(call)?);
            }
        }
        Ok(deltas)
    }
    fn text(&mut self, delta: &Value, deltas: &mut Vec<ModelDelta>) {
        for (key, reasoning) in [("content", false), ("reasoning_content", true)] {
            let Some(text) = delta.get(key).and_then(Value::as_str) else {
                continue;
            };
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
    fn call(&mut self, value: &Value) -> Result<ModelDelta> {
        let index = value
            .get("index")
            .and_then(Value::as_u64)
            .ok_or_else(|| Error::Protocol("stream tool call missing index".into()))?
            as usize;
        let call = self
            .calls
            .entry(index)
            .or_insert_with(|| json!({"type":"function","function":{"name":"","arguments":""}}));
        if let Some(id) = value.get("id") {
            call["id"] = id.clone();
        }
        let name = value.pointer("/function/name").and_then(Value::as_str);
        if let Some(name) = name {
            append(&mut call["function"], "name", name);
        }
        let input = value
            .pointer("/function/arguments")
            .and_then(Value::as_str)
            .unwrap_or("");
        append(&mut call["function"], "arguments", input);
        Ok(ModelDelta::RuntimeTool {
            output_index: index + 1,
            id: value.get("id").and_then(Value::as_str).map(str::to_owned),
            name: name.map(str::to_owned),
            input: input.into(),
        })
    }
    pub(crate) fn finish(mut self) -> Result<Value> {
        if !self.done {
            return Err(incomplete());
        }
        if !self.calls.is_empty() {
            self.value["choices"][0]["message"]["tool_calls"] =
                json!(self.calls.into_values().collect::<Vec<_>>());
        }
        Ok(self.value)
    }
}
fn merge_logprobs(target: &mut Value, source: &Value) {
    let Some(fields) = source.as_object() else {
        return;
    };
    for (key, value) in fields {
        match value {
            Value::Array(tokens) => {
                if !target[key].is_array() {
                    target[key] = json!([]);
                }
                target[key]
                    .as_array_mut()
                    .expect("initialized tokens")
                    .extend(tokens.iter().cloned());
            }
            Value::Null => {}
            _ => target[key] = value.clone(),
        }
    }
}

impl super::StreamState for State {
    fn push(&mut self, value: &Value) -> Result<Vec<ModelDelta>> {
        State::push(self, value)
    }
    fn finish(self: Box<Self>) -> Result<Value> {
        State::finish(*self)
    }
    fn done(&mut self) {
        self.done = true;
    }
}
