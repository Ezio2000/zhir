use super::incomplete;
use crate::{Protocol, codec};
use serde_json::Value;
use zhir_core::{Result, error::Error, model::ModelDelta};

#[derive(Default)]
pub(super) struct State {
    completed: Option<Value>,
}
impl State {
    pub(super) fn push(&mut self, value: &Value) -> Result<Vec<ModelDelta>> {
        let index = value
            .get("output_index")
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize;
        let text = || {
            value
                .get("delta")
                .and_then(Value::as_str)
                .unwrap_or("")
                .into()
        };
        let delta = match value.get("type").and_then(Value::as_str).unwrap_or("") {
            "response.completed" | "response.incomplete" => {
                let response = value.get("response").cloned().ok_or_else(|| {
                    Error::Protocol("terminal response missing full value".into())
                })?;
                let usage = codec::usage(Protocol::Responses, &response["usage"]);
                self.completed = Some(response);
                ModelDelta::Usage { usage }
            }
            "response.failed" => return Err(Error::Protocol(value.to_string())),
            "response.output_text.delta" => ModelDelta::Text {
                output_index: index,
                text: text(),
            },
            "response.reasoning_text.delta" | "response.reasoning_summary_text.delta" => {
                ModelDelta::Reasoning {
                    output_index: index,
                    text: text(),
                }
            }
            "response.function_call_arguments.delta" | "response.custom_tool_call_input.delta" => {
                ModelDelta::RuntimeTool {
                    output_index: index,
                    id: value
                        .get("item_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    name: None,
                    input: text(),
                }
            }
            _ => return Ok(Vec::new()),
        };
        Ok(vec![delta])
    }
    pub(super) fn finish(self) -> Result<Value> {
        self.completed.ok_or_else(incomplete)
    }
}
