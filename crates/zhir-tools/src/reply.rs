//! Typed final payloads. Pending work is represented by an OperationHandle.
use serde::Serialize;
use zhir_core::{
    Result, error::Error, message::Content, operation::ToolExecution, tool::RuntimeToolOutcome,
};
pub struct ToolReply<T> {
    payload: T,
    content: Option<Vec<Content>>,
}
impl<T> ToolReply<T> {
    pub fn success(payload: T) -> Self {
        Self {
            payload,
            content: None,
        }
    }
    pub fn content(mut self, content: impl IntoIterator<Item = Content>) -> Self {
        self.content = Some(content.into_iter().collect());
        self
    }
}
impl<T: Serialize> ToolReply<T> {
    pub fn into_execution(self) -> Result<ToolExecution> {
        let structured = serde_json::to_value(self.payload)
            .map_err(|e| Error::Invalid(format!("tool output: {e}")))?;
        let content = self.content.unwrap_or_else(|| text_content(&structured));
        let outcome = RuntimeToolOutcome::Success {
            content,
            structured,
        };
        outcome.validate()?;
        Ok(ToolExecution::Finished(outcome))
    }
}
fn text_content(value: &serde_json::Value) -> Vec<Content> {
    vec![Content::text(
        value
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| value.to_string()),
    )]
}
pub fn json(value: serde_json::Value) -> ToolExecution {
    ToolExecution::Finished(RuntimeToolOutcome::Success {
        content: text_content(&value),
        structured: value,
    })
}
