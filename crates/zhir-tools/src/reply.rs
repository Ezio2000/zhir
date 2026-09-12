//! Typed structured payloads with explicit runtime outcomes and media content.
use serde::Serialize;
use zhir_core::{
    Result,
    error::Error,
    message::Content,
    run::Suspension,
    tool::{RuntimeToolOutcome, RuntimeToolResult},
};

enum State {
    Success,
    Accepted(String),
    Waiting(Suspension),
}
pub struct ToolReply<T> {
    payload: T,
    content: Option<Vec<Content>>,
    state: State,
}
impl<T> ToolReply<T> {
    pub fn success(payload: T) -> Self {
        Self {
            payload,
            content: None,
            state: State::Success,
        }
    }
    pub fn accepted(task_id: impl Into<String>, payload: T) -> Self {
        Self {
            payload,
            content: None,
            state: State::Accepted(task_id.into()),
        }
    }
    pub fn waiting(wait_id: impl Into<String>, payload: T, source: impl Into<String>) -> Self {
        Self::suspended(
            payload,
            Suspension {
                reason: "waiting".into(),
                source: source.into(),
                wait_id: Some(wait_id.into()),
                metadata: Default::default(),
            },
        )
    }
    pub fn suspended(payload: T, suspension: Suspension) -> Self {
        Self {
            payload,
            content: None,
            state: State::Waiting(suspension),
        }
    }
    pub fn content(mut self, content: impl IntoIterator<Item = Content>) -> Self {
        self.content = Some(content.into_iter().collect());
        self
    }
}
impl<T: Serialize> ToolReply<T> {
    pub fn into_result(self) -> Result<RuntimeToolResult> {
        let structured = serde_json::to_value(self.payload)
            .map_err(|e| Error::Invalid(format!("tool output: {e}")))?;
        let content = self.content.unwrap_or_else(|| {
            RuntimeToolResult::json(structured.clone())
                .outcome
                .content()
        });
        let (outcome, suspension) = match self.state {
            State::Success => (
                RuntimeToolOutcome::Success {
                    structured,
                    content,
                },
                None,
            ),
            State::Accepted(task_id) => (
                RuntimeToolOutcome::Accepted {
                    task_id,
                    structured,
                    content,
                },
                None,
            ),
            State::Waiting(suspension) => {
                let wait_id = suspension.wait_id.clone().ok_or_else(|| {
                    Error::Invalid("waiting reply requires a wait identity".into())
                })?;
                (
                    RuntimeToolOutcome::Waiting {
                        wait_id,
                        structured,
                        content,
                    },
                    Some(suspension),
                )
            }
        };
        let result = RuntimeToolResult {
            outcome,
            suspension,
        };
        result.validate()?;
        Ok(result)
    }
}
