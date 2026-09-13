use crate::common::spec;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use zhir_core::{
    Result,
    error::Error,
    message::Message,
    run::{Checkpoint, State},
    tool::RuntimeTool,
};
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Question {
    #[schemars(length(min = 1))]
    pub id: String,
    #[schemars(length(min = 1))]
    pub title: String,
    #[serde(default)]
    pub options: Vec<String>,
}
#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct Args {
    #[schemars(length(min = 1, max = 3))]
    questions: Vec<Question>,
}
const NAME: &str = "ask_question";
pub fn ask_question() -> Result<Arc<dyn RuntimeTool>> {
    Ok(Arc::new(zhir_tools::function::structured(
        spec::<Args>(
            NAME,
            "Ask the host questions and suspend until it responds.",
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
            let wait = uuid::Uuid::new_v4().to_string();
            Ok(zhir_tools::reply::waiting(
                wait,
                json!({"questions":a.questions}),
                NAME,
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
    if suspension.source != NAME {
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
