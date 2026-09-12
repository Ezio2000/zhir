use super::{append, incomplete};
use crate::{Protocol, codec};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use zhir_core::{Result, error::Error, model::ModelDelta};

pub(super) struct State {
    value: Value,
    blocks: BTreeMap<usize, Value>,
    partial: BTreeMap<usize, String>,
    done: bool,
}
impl State {
    pub(super) fn new() -> Self {
        Self {
            value: json!({"content":[],"usage":{}}),
            blocks: BTreeMap::new(),
            partial: BTreeMap::new(),
            done: false,
        }
    }
    pub(super) fn push(&mut self, value: &Value) -> Result<Vec<ModelDelta>> {
        let index = value.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
        match value.get("type").and_then(Value::as_str).unwrap_or("") {
            "message_start" => {
                self.value = value
                    .get("message")
                    .cloned()
                    .ok_or_else(|| Error::Protocol("missing streamed message".into()))?
            }
            "content_block_start" => {
                let block = value
                    .get("content_block")
                    .cloned()
                    .ok_or_else(|| Error::Protocol("missing content block".into()))?;
                if self.blocks.insert(index, block).is_some() {
                    return Err(Error::Protocol("duplicate content block".into()));
                }
            }
            "content_block_delta" => return self.block_delta(index, &value["delta"]),
            "content_block_stop" => self.stop_block(index)?,
            "message_delta" => return Ok(self.message_delta(value)),
            "message_stop" => self.done = true,
            _ => {}
        }
        Ok(Vec::new())
    }
    fn block_delta(&mut self, index: usize, delta: &Value) -> Result<Vec<ModelDelta>> {
        let block = self
            .blocks
            .get_mut(&index)
            .ok_or_else(|| Error::Protocol("delta before block start".into()))?;
        let text = |key| delta.get(key).and_then(Value::as_str).unwrap_or("");
        let normalized = match text("type") {
            "text_delta" => {
                append(block, "text", text("text"));
                ModelDelta::Text {
                    output_index: index,
                    text: text("text").into(),
                }
            }
            "thinking_delta" => {
                append(block, "thinking", text("thinking"));
                ModelDelta::Reasoning {
                    output_index: index,
                    text: text("thinking").into(),
                }
            }
            "signature_delta" => {
                append(block, "signature", text("signature"));
                return Ok(Vec::new());
            }
            "input_json_delta" => {
                let input = text("partial_json");
                self.partial.entry(index).or_default().push_str(input);
                ModelDelta::RuntimeTool {
                    output_index: index,
                    id: block.get("id").and_then(Value::as_str).map(str::to_owned),
                    name: block.get("name").and_then(Value::as_str).map(str::to_owned),
                    input: input.into(),
                }
            }
            _ => return Ok(Vec::new()),
        };
        Ok(vec![normalized])
    }
    fn stop_block(&mut self, index: usize) -> Result<()> {
        if let Some(raw) = self.partial.remove(&index) {
            let block = self
                .blocks
                .get_mut(&index)
                .ok_or_else(|| Error::Protocol("unknown block stop".into()))?;
            block["input"] =
                serde_json::from_str(&raw).map_err(|e| Error::Protocol(e.to_string()))?;
        }
        Ok(())
    }
    fn message_delta(&mut self, value: &Value) -> Vec<ModelDelta> {
        if let Some(fields) = value.get("delta").and_then(Value::as_object) {
            for (key, value) in fields {
                self.value[key] = value.clone();
            }
        }
        let Some(usage) = value.get("usage").and_then(Value::as_object) else {
            return Vec::new();
        };
        for (key, value) in usage {
            self.value["usage"][key] = value.clone();
        }
        vec![ModelDelta::Usage {
            usage: codec::usage(Protocol::Messages, &self.value["usage"]),
        }]
    }
    pub(super) fn finish(mut self) -> Result<Value> {
        if !self.done || !self.partial.is_empty() {
            return Err(incomplete());
        }
        self.value["content"] = json!(self.blocks.into_values().collect::<Vec<_>>());
        Ok(self.value)
    }
}
