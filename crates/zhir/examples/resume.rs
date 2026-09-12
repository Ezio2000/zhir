//! Offline demonstration of a durable question, native export, and explicit resume.
use serde_json::{Value, json};
use std::sync::Arc;
use zhir::{
    BoxFuture, Result, ResumeRequest, Runtime,
    builtins::interaction,
    message::{Message, Output},
    model::{Capabilities, Model, ModelContext, ModelRequest, ModelResponse},
    run::{State, SuspensionSelector},
    runtime_tools::RuntimeToolRegistry,
    storage::RunStore,
    stores::memory::MemoryRunStore,
    tool::{RuntimeToolCall, RuntimeToolInput},
    wire,
};
struct QuestionModel(Capabilities);
impl Model for QuestionModel {
    fn capabilities(&self) -> &Capabilities {
        &self.0
    }
    fn invoke(
        &self,
        request: ModelRequest,
        _: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            if request
                .messages
                .iter()
                .any(|message| matches!(message, Message::External { .. }))
            {
                return Ok(ModelResponse::text("Proceeding with Rust"));
            }
            let mut response = ModelResponse::text("");
            response.output = vec![Output::RuntimeToolCall {
                call: RuntimeToolCall {
                    id: "question-1".into(),
                    name: "ask_question".into(),
                    input: RuntimeToolInput::Structured(
                        json!({"questions":[{"id":"language","title":"Which language?","options":["Rust","Swift"]}]}),
                    ),
                },
            }];
            Ok(response)
        })
    }
}
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(MemoryRunStore::new());
    let tools = Arc::new(RuntimeToolRegistry::from_tools([
        interaction::ask_question()?,
    ])?);
    let runtime = Runtime::builder(Arc::new(QuestionModel(Capabilities::default())))
        .runtime_tools(tools)
        .store(store.clone())
        .build()?;
    let suspended = runtime
        .start(zhir_core::run::RunRequest::new(vec![Message::user(
            "Start a project",
        )]))?
        .result()
        .await?;
    assert!(matches!(suspended.state, State::Suspended { .. }));
    println!(
        "{} at revision {}",
        suspended.state.kind(),
        suspended.revision
    );

    // A UI may persist/export this native envelope while waiting for a response.
    let bytes = wire::encode_checkpoint(&suspended)?;
    let restored = Arc::new(wire::decode_checkpoint(&bytes)?);
    let head = store
        .load_head(&restored.context.run_id)
        .await?
        .expect("stored head");
    assert_eq!(head.id, restored.id);
    let answers: Value = json!({"language":"Rust"});
    let answer = interaction::response(&restored, answers)?;
    let checkpoint = runtime
        .resume(
            ResumeRequest::from_ticket(zhir::SuspensionTicket::from_checkpoint(&restored)?)
                .matching(SuspensionSelector {
                    source: Some("ask_question".into()),
                    ..Default::default()
                })
                .message(answer),
        )
        .await?
        .result()
        .await?;
    assert!(matches!(checkpoint.state, State::Completed { .. }));
    assert_eq!(checkpoint.metrics.runtime_tool_calls, 1);
    println!(
        "{} at revision {}; question executed once",
        checkpoint.state.kind(),
        checkpoint.revision
    );
    Ok(())
}
