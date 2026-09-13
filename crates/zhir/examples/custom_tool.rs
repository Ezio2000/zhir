//! Type-derived tool schemas and a closure model, without an HTTP protocol feature.
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use zhir::{
    Runtime,
    message::{Message, Output},
    model::TurnOutput,
    models::FunctionModel,
    runtime_tools::{RuntimeToolRegistry, TypedTool},
    tool::{Execution, RuntimeToolCall, RuntimeToolInput},
};
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EchoArgs {
    text: String,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let tool = TypedTool::<EchoArgs, String>::new(
        "echo",
        "Return supplied text",
        Execution::default(),
        |args, _context| async move { Ok(zhir::runtime_tools::ToolReply::success(args.text)) },
    )?;
    let tools = Arc::new(RuntimeToolRegistry::from_tools([Arc::new(tool) as _])?);
    let model = FunctionModel::new(
        zhir::models::capabilities::text_tool_calling(),
        |request, _context| async move {
            if request
                .messages
                .iter()
                .any(|m| matches!(m, Message::RuntimeTool { .. }))
            {
                return Ok(TurnOutput::text("RuntimeTool completed"));
            }
            let mut response = TurnOutput::text("");
            response.output = vec![Output::RuntimeToolCall {
                call: RuntimeToolCall {
                    id: "echo-1".into(),
                    name: "echo".into(),
                    input: RuntimeToolInput::Structured(json!({"text":"hello"})),
                },
            }];
            Ok(response)
        },
    );
    let runtime = Runtime::builder(Arc::new(model))
        .runtime_tools(tools)
        .build()?;
    let checkpoint = runtime
        .start(zhir::RunRequest::new(vec![Message::user("Use echo")]))?
        .result()
        .await?
        .into_checkpoint();
    println!(
        "{}; {} tool call",
        checkpoint.state.kind(),
        checkpoint.metrics.runtime_tool_calls
    );
    Ok(())
}
