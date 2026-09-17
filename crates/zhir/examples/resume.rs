//! Persist a waiting operation and explicitly resolve it through a suspension ticket.
use serde_json::json;
use std::sync::Arc;
use zhir::{
    ResumeRequest, RunRequest, Runtime, SuspensionTicket,
    builtins::interaction,
    message::{Message, Output},
    model::GenerationOutput,
    models::FunctionModel,
    runtime_tools::RuntimeToolRegistry,
    stores::MemoryRunStore,
    tool::{RuntimeToolCall, RuntimeToolInput},
};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let model = FunctionModel::new(
        zhir::models::capabilities::text_tool_calling(),
        |request, _| async move {
            if request
                .messages
                .iter()
                .any(|m| matches!(m, Message::RuntimeTool { .. }))
            {
                return Ok(GenerationOutput::text("Proceeding with Rust"));
            }
            Ok(GenerationOutput {
                output: vec![Output::RuntimeToolCall {
                    call: RuntimeToolCall {
                        id: "question".into(),
                        name: "ask_question".into(),
                        input: RuntimeToolInput::Structured(
                            json!({"questions":[{"id":"language","title":"Which language?","options":["Rust","Swift"]}]}),
                        ),
                    },
                }],
                ..GenerationOutput::text("")
            })
        },
    );
    let runtime = Runtime::builder(Arc::new(model))
        .runtime_tools(Arc::new(RuntimeToolRegistry::from_tools([
            interaction::ask_question()?,
        ])?))
        .store(Arc::new(MemoryRunStore::new()))
        .build()?;
    let checkpoint = runtime
        .start(RunRequest::new([Message::user("Start a project")]))?
        .result()
        .await?
        .into_checkpoint();
    let bytes = zhir::wire::encode_checkpoint(&checkpoint)?;
    let restored = zhir::wire::decode_checkpoint(&bytes)?;
    let operation_id = restored
        .active
        .operations
        .keys()
        .next()
        .expect("waiting question");
    let resolution = interaction::response(&restored, operation_id, json!({"language":"Rust"}))?;
    let completed = runtime
        .resume(
            ResumeRequest::from_ticket(SuspensionTicket::from_checkpoint(&restored)?)
                .resolve(resolution),
        )
        .await?
        .result()
        .await?;
    println!(
        "{}; {} tool call",
        completed.checkpoint().state.kind(),
        completed.checkpoint().metrics.runtime_tool_calls
    );
    Ok(())
}
