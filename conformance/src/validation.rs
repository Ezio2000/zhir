use crate::{
    fixtures::{CaseModel, RecordingStore, decode, seed},
    require, resume_request,
};
use serde_json::{Value, json};
use std::sync::Arc;
use zhir_core::{
    Result,
    error::Error,
    message::Message,
    model::{ModelOptions, ModelResponse},
    run::{Event, Fact, State},
    tool::RuntimeToolSpec,
};
use zhir_kernel::Runtime;
fn parse<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T> {
    serde_json::from_slice(&serde_json::to_vec(value).unwrap())
        .map_err(|e| Error::Invalid(e.to_string()))
}
pub async fn run(case: &Value) -> Result<()> {
    let value = &case["value"];
    let result: Result<()> = match case["target"].as_str() {
        Some("message") => parse::<Message>(value).and_then(|m| m.validate()),
        Some("state") => parse::<State>(value).and_then(|s| s.validate()),
        Some("model_response") => parse::<ModelResponse>(value).and_then(|r| r.validate()),
        Some("model_options") => parse::<ModelOptions>(value).map(|_| ()),
        Some("event") => parse::<Event>(value).map(|_| ()),
        Some("tool_spec") => parse::<RuntimeToolSpec>(value).and_then(|s| s.execution.validate()),
        Some("request") => {
            let runtime = Runtime::builder(Arc::new(CaseModel::new(vec![]))).build()?;
            match value["kind"].as_str() {
                Some("start") => runtime
                    .start(zhir_kernel::RunRequest::new(decode::<Vec<Message>>(
                        &value["messages"],
                    )?))
                    .map(|_| ()),
                Some("continue") => runtime
                    .continue_from(seed(&value["checkpoint"])?)
                    .map(|_| ()),
                Some("resume") => runtime
                    .resume(resume_request(seed(&value["checkpoint"])?, value)?)
                    .await
                    .map(|_| ()),
                _ => Err(Error::Invalid("unknown request".into())),
            }
        }
        Some("trace") => {
            let response = zhir_core::model::ModelResponse::text("done");
            let model = Arc::new(CaseModel::new(vec![
                json!({"outcome":{"kind":"response","response":response}}),
            ]));
            let store = Arc::new(RecordingStore::default());
            Runtime::builder(model)
                .store(store.clone())
                .build()?
                .start(zhir_kernel::RunRequest::new(vec![Message::user("trace")]))?
                .result()
                .await
                .map_err(|e| e.error)?
                .into_checkpoint();
            let mut checkpoints = store.checkpoints();
            let mut after = checkpoints[1].as_ref().clone();
            if value["fault"] == "revision_gap" {
                after.revision += 1;
            } else {
                after.fact = Fact::RuntimeToolBatch {
                    call_ids: vec!["missing".into()],
                    outcomes: vec![],
                    parallel: false,
                };
            }
            checkpoints[1] = Arc::new(after);
            zhir_kernel::diagnostics::verify_trace(&checkpoints)
        }
        _ => return Err(Error::Invalid("unknown validation target".into())),
    };
    require(
        result.is_ok() == case["valid"].as_bool().unwrap(),
        format!("validation {}: {result:?}", case["name"]),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use zhir_core::run::History;

    #[tokio::test]
    async fn trace_rejects_facts_and_content_that_do_not_describe_the_commit() {
        let response = ModelResponse::text("done");
        let store = Arc::new(RecordingStore::default());
        Runtime::builder(Arc::new(CaseModel::new(vec![
            json!({"outcome":{"kind":"response","response":response}}),
        ])))
        .store(store.clone())
        .build()
        .unwrap()
        .start(zhir_kernel::RunRequest::new(vec![Message::user(
            "original",
        )]))
        .unwrap()
        .result()
        .await
        .unwrap()
        .into_checkpoint();
        let trace = store.checkpoints();
        zhir_kernel::diagnostics::verify_trace(&trace).unwrap();
        for fault in [
            "history_prefix",
            "phantom_call",
            "result",
            "completed_content",
            "context",
        ] {
            let mut corrupted = trace.clone();
            let mut after = corrupted[1].as_ref().clone();
            match fault {
                "history_prefix" => {
                    let mut messages = after.history.messages();
                    messages[0] = Message::user("altered");
                    after.history = History::new(messages).unwrap();
                }
                "phantom_call" => {
                    let Fact::ModelTurn {
                        runtime_tool_call_ids,
                        ..
                    } = &mut after.fact
                    else {
                        unreachable!()
                    };
                    runtime_tool_call_ids.push("invented".into());
                }
                "result" => {
                    let Fact::ModelTurn { result, .. } = &mut after.fact else {
                        unreachable!()
                    };
                    *result = zhir_core::run::StateKind::Planning;
                }
                "completed_content" => {
                    after.state = State::Completed {
                        content: vec![zhir_core::message::Content::text("altered")],
                    }
                }
                "context" => after.context.started_at_ms += 1,
                _ => unreachable!(),
            }
            // Each corrupted checkpoint is individually well-formed; the trace is not.
            after.validate().unwrap();
            corrupted[1] = Arc::new(after);
            assert!(
                zhir_kernel::diagnostics::verify_trace(&corrupted).is_err(),
                "{fault}"
            );
        }
    }
}
