use crate::common::spec;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use zhir_core::{
    Result,
    error::Error,
    message::Message,
    run::{Checkpoint, State},
    tool::{RuntimeTool, RuntimeToolResult},
};
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Question {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub options: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Args {
    questions: Vec<Question>,
}
pub fn ask_question() -> Result<Arc<dyn RuntimeTool>> {
    Ok(Arc::new(zhir_tools::function::structured(
        spec(
            "ask_question",
            "Ask the host questions and suspend until it responds.",
            json!({"type":"object","required":["questions"],"properties":{"questions":{"type":"array","minItems":1,"maxItems":3,"items":{"type":"object","required":["id","title"],"properties":{"id":{"type":"string","minLength":1},"title":{"type":"string","minLength":1},"options":{"type":"array","items":{"type":"string"}}},"additionalProperties":false}}},"additionalProperties":false}),
            false,
        ),
        |a: Args, context| async move {
            context.cancellation.check()?;
            let mut ids = std::collections::HashSet::new();
            if a.questions.is_empty()
                || a.questions
                    .iter()
                    .any(|q| q.id.is_empty() || q.title.is_empty() || !ids.insert(&q.id))
            {
                return Err(Error::Invalid(
                    "question ids must be nonempty and unique".into(),
                ));
            }
            let wait = zhir_core::run::new_id();
            Ok(RuntimeToolResult::waiting(
                wait,
                json!({"questions":a.questions}),
                "ask_question",
            ))
        },
    )?))
}
pub fn response(checkpoint: &Checkpoint, answers: Value) -> Result<Message> {
    let State::Suspended { suspension, .. } = &checkpoint.state else {
        return Err(Error::Invalid(
            "question response requires suspension".into(),
        ));
    };
    if suspension.source != "ask_question" {
        return Err(Error::Invalid(
            "checkpoint is not waiting for questions".into(),
        ));
    }
    if !answers.is_object() {
        return Err(Error::Invalid(
            "answers must be keyed by question id".into(),
        ));
    }
    Ok(Message::external(
        json!({"wait_id":suspension.wait_id,"answers":answers}).to_string(),
    ))
}
