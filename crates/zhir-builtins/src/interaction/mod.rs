//! Host questions are resumable waiting operations, not completed tool replies.
use crate::common::spec;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::mpsc;
use zhir_core::operation::OperationOutcome;
use zhir_core::{
    BoxFuture, Result, error::Error, message::Content, operation::*, run::Checkpoint, tool::*,
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
struct QuestionTool {
    spec: RuntimeToolSpec,
}
pub fn ask_question() -> Result<Arc<dyn RuntimeTool>> {
    Ok(Arc::new(QuestionTool {
        spec: spec::<Args>(
            NAME,
            "Ask the host questions and wait for its response.",
            false,
        ),
    }))
}
fn answers(questions: &[Question], value: Value) -> Result<OperationOutcome> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::Invalid("answers must be keyed by question identity".into()))?;
    if object.len() != questions.len() || questions.iter().any(|q| !object.contains_key(&q.id)) {
        return Err(Error::Invalid(
            "answers do not match the requested questions".into(),
        ));
    }
    Ok(OperationOutcome::Success {
        content: vec![Content::text(value.to_string())],
        structured: value,
    })
}
struct Input {
    questions: Vec<Question>,
    sender: mpsc::Sender<OperationOutcome>,
}
impl OperationControl for Input {
    fn cancel(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.sender
                .send(OperationOutcome::Cancelled {
                    reason: "question cancelled".into(),
                })
                .await
                .map_err(|_| Error::Cancelled)
        })
    }
    fn reply(&self, value: Value) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.sender
                .send(answers(&self.questions, value)?)
                .await
                .map_err(|_| Error::Cancelled)
        })
    }
}
struct Events {
    initial: Option<OperationUpdate>,
    sequence: u64,
    receiver: mpsc::Receiver<OperationOutcome>,
    finished: bool,
}
impl OperationEvents for Events {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<OperationEvent>>> {
        Box::pin(async move {
            if self.finished {
                return Ok(None);
            }
            let update = if let Some(update) = self.initial.take() {
                update
            } else {
                let Some(outcome) = self.receiver.recv().await else {
                    return Ok(None);
                };
                self.finished = true;
                OperationUpdate::Finished { outcome }
            };
            let sequence = self.sequence;
            self.sequence += 1;
            Ok(Some(OperationEvent { sequence, update }))
        })
    }
}
fn handle(questions: Vec<Question>, sequence: u64) -> Result<ToolExecution> {
    let mut ids = std::collections::HashSet::new();
    if questions.is_empty()
        || questions.len() > 3
        || questions
            .iter()
            .any(|q| q.id.is_empty() || q.title.is_empty() || !ids.insert(&q.id))
    {
        return Err(Error::Invalid(
            "invalid question identities or count".into(),
        ));
    }
    let (sender, receiver) = mpsc::channel(1);
    let recovery = RecoveryRef {
        adapter: NAME.into(),
        data: json!({"questions":questions}),
    };
    Ok(ToolExecution::Active(OperationHandle {
        recovery: Some(recovery.clone()),
        control: Arc::new(Input { questions, sender }),
        events: Box::new(Events {
            initial: Some(OperationUpdate::Waiting {
                prompt: recovery.data.clone(),
                recovery: Some(recovery),
            }),
            sequence,
            receiver,
            finished: false,
        }),
    }))
}
impl RuntimeTool for QuestionTool {
    fn spec(&self) -> &RuntimeToolSpec {
        &self.spec
    }
    fn start(
        &self,
        call: RuntimeToolCall,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<ToolExecution>> {
        Box::pin(async move {
            context.cancellation.check()?;
            let RuntimeToolInput::Structured(value) = call.input else {
                return Err(Error::Invalid("questions require structured input".into()));
            };
            let args: Args =
                serde_json::from_value(value).map_err(|e| Error::Invalid(e.to_string()))?;
            handle(args.questions, 0)
        })
    }
    fn recover(
        &self,
        record: OperationRecord,
        _: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<ToolExecution>> {
        Box::pin(async move {
            let reference = record
                .recovery
                .ok_or_else(|| Error::Invalid("missing question recovery data".into()))?;
            if reference.adapter != NAME {
                return Err(Error::Invalid("question recovery adapter mismatch".into()));
            }
            let args: Args = serde_json::from_value(reference.data)
                .map_err(|e| Error::Invalid(e.to_string()))?;
            handle(
                args.questions,
                record.last_sequence.map_or(0, |seq| seq + 1),
            )
        })
    }
}
pub fn response(
    checkpoint: &Checkpoint,
    operation_id: &str,
    value: Value,
) -> Result<RecoveryResolution> {
    let operation = checkpoint
        .active
        .operations
        .get(operation_id)
        .ok_or_else(|| Error::Invalid("unknown question operation".into()))?;
    if !matches!(&operation.owner, OperationOwner::RuntimeTool { name } if name == NAME)
        || operation.state != OperationState::Waiting
    {
        return Err(Error::Invalid(
            "operation is not waiting for questions".into(),
        ));
    }
    let reference = operation
        .recovery
        .as_ref()
        .ok_or_else(|| Error::Invalid("missing question recovery data".into()))?;
    let args: Args = serde_json::from_value(reference.data.clone())
        .map_err(|e| Error::Invalid(e.to_string()))?;
    Ok(RecoveryResolution::Complete {
        operation_id: operation_id.into(),
        outcome: answers(&args.questions, value)?,
    })
}
