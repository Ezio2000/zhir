#![cfg(all(
    feature = "models",
    feature = "typed-tools",
    feature = "typed-output",
    feature = "memory"
))]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use zhir::{
    Result, Runtime,
    error::Error,
    message::{Message, Output},
    model::{CapabilitySet, GenerationOutput, ModelContext, ModelRequest},
    models::{ConcurrencyLimitedModel, FunctionModel},
    output::JsonOutput,
    policies::history::HistoryWindow,
    run::{Checkpoint, History, HistoryReducer, State},
    runtime_tools::{RuntimeToolRegistry, TypedTool},
    tool::{Execution, RuntimeToolCall, RuntimeToolInput},
};
use zhir_testing::ModelTestExt;

fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![Message::user("test")],
        runtime_tools: vec![],
        provider_tools: vec![],
        profile: Default::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream: false,
    }
}
fn context() -> ModelContext {
    ModelContext {
        run: zhir::kernel::defaults::context(),
        cancellation: Default::default(),
        deltas: None,
    }
}
fn checkpoint(messages: Vec<Message>) -> Arc<Checkpoint> {
    let entries = messages
        .into_iter()
        .enumerate()
        .map(|(index, message)| {
            let origin = match &message {
                Message::RuntimeTool { call_id, .. } => Some(zhir_core::operation::CallRef {
                    session_id: "s".into(),
                    item_id: call_id.clone(),
                    generation_id: Some("t".into()),
                    caller_id: "model".into(),
                    call_id: call_id.clone(),
                }),
                Message::Assistant { output, .. }
                    if output
                        .iter()
                        .any(|o| matches!(o, Output::RuntimeToolCall { .. })) =>
                {
                    Some(zhir_core::operation::CallRef {
                        session_id: "s".into(),
                        item_id: "call".into(),
                        generation_id: Some("t".into()),
                        caller_id: "model".into(),
                        call_id: "call".into(),
                    })
                }
                _ => None,
            };
            zhir_core::run::HistoryEntry {
                id: format!("e{index}"),
                origin,
                message,
            }
        })
        .collect();
    let mut checkpoint =
        zhir_testing::checkpoint_with_history(History::from_entries(entries).unwrap());
    checkpoint.active.session.response_status = Some(zhir_core::model::ResponseStatus::Completed);
    Arc::new(checkpoint)
}
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Report {
    #[schemars(range(min = 1, max = 10))]
    count: i64,
}

#[tokio::test]
async fn output_contract_uses_one_schema_and_reports_validation_stage() {
    let output = JsonOutput::<Report>::new("report").unwrap();
    let model = Arc::new(FunctionModel::new(
        CapabilitySet {
            features: [zhir_core::model::Capability::StructuredOutput].into(),

            ..zhir_testing::model_capabilities()
        },
        |request, _| async move {
            assert!(
                matches!(request.response_format, Some(zhir::model::ResponseFormat::Schema {schema, ..}) if schema["properties"]["count"]["maximum"] == 10)
            );
            Ok(GenerationOutput::text(r#"{"count":3}"#))
        },
    ));
    let runtime = Runtime::builder(model)
        .defaults(|run| run.response_format(output.format()))
        .build()
        .unwrap();
    let completed = runtime
        .start(zhir::RunRequest::new(vec![Message::user("report")]))
        .unwrap()
        .result()
        .await
        .unwrap()
        .into_checkpoint();
    assert_eq!(output.decode(&completed).unwrap(), Report { count: 3 });
    for (text, stage) in [
        (r#"{"count":11}"#, "schema validation at /count"),
        (r#"{"count":3,"extra":1}"#, "schema"),
        (r#"{"count":3} trailing"#, "JSON"),
    ] {
        let mut modified = (*completed).clone();
        modified.state = State::Completed {
            content: vec![zhir::message::Content::text(text)],
        };
        let error = output.decode(&modified).unwrap_err();
        match stage {
            "JSON" => assert!(matches!(
                error,
                Error::Validation(zhir::error::ValidationError::Decode { .. })
            )),
            "schema validation at /count" => assert!(
                matches!(error, Error::Validation(zhir::error::ValidationError::Value { path, .. }) if path == "/count")
            ),
            _ => assert!(matches!(
                error,
                Error::Validation(zhir::error::ValidationError::Value { .. })
            )),
        }
    }
    assert!(JsonOutput::<Report>::new("").is_err());
    assert!(
        output
            .decode(&checkpoint(vec![Message::user("pending")]))
            .is_err()
    );
}

#[tokio::test]
async fn history_windows_keep_runtime_results_external_replies_and_dependencies() {
    let call = RuntimeToolCall {
        id: "call".into(),
        name: "echo".into(),
        input: RuntimeToolInput::Structured(json!({})),
    };
    let messages = vec![
        Message::system("system"),
        Message::user("old"),
        Message::Assistant {
            output: vec![Output::text("old reply")],
            provider_data: serde_json::Value::Null,
        },
        Message::user("current"),
        Message::Assistant {
            output: vec![Output::RuntimeToolCall { call: call.clone() }],
            provider_data: serde_json::Value::Null,
        },
        Message::RuntimeTool {
            call_id: call.id,
            name: call.name,
            outcome: zhir::runtime_tools::reply::json(json!("ok"))
                .final_outcome()
                .clone(),
        },
        Message::external("reply"),
    ];
    let original = checkpoint(messages);
    let window = HistoryWindow::last_turns(1).unwrap();
    let rewrite = window.reduce(original.clone()).await.unwrap().unwrap();
    assert_eq!(rewrite.entries.len(), 5);
    assert!(matches!(rewrite.entries[0].message, Message::System { .. }));
    assert!(matches!(
        rewrite.entries[3].message,
        Message::RuntimeTool { .. }
    ));
    assert!(matches!(
        rewrite.entries[4].message,
        Message::External { .. }
    ));
    assert!(
        window
            .reduce(
                zhir_testing::checkpoint_with_history(
                    History::from_entries(rewrite.entries).unwrap()
                )
                .into()
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        HistoryWindow::last_turns(1)
            .unwrap()
            .with_dependencies(|_, _| Ok(1))
            .reduce(original.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        HistoryWindow::last_turns(1)
            .unwrap()
            .with_dependencies(|_, first| Ok(first + 1))
            .reduce(original.clone())
            .await
            .is_err()
    );
    let mut pending = (*original).clone();
    pending.active.session.response_status = None;
    pending.active.session.generation_id = Some("active".into());
    assert!(window.reduce(Arc::new(pending)).await.unwrap().is_none());
    assert!(HistoryWindow::last_turns(0).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_limiter_bounds_128_calls_and_releases_failed_calls() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let model = Arc::new(FunctionModel::new(zhir_testing::model_capabilities(), {
        let active = active.clone();
        let peak = peak.clone();
        move |_, _| {
            let active = active.clone();
            let peak = peak.clone();
            async move {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(2)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Err(Error::Protocol("fixture failure".into()))
            }
        }
    }));
    let limited = Arc::new(ConcurrencyLimitedModel::new(model, 4).unwrap());
    let results =
        futures::future::join_all((0..128).map(|_| limited.generate(request(), context()))).await;
    assert!(results.iter().all(Result::is_err));
    assert!((1..=4).contains(&peak.load(Ordering::SeqCst)));
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn queued_cancellation_deadline_and_dropped_future_release_permits() {
    let entered = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let model = Arc::new(FunctionModel::new(zhir_testing::model_capabilities(), {
        let entered = entered.clone();
        let gate = gate.clone();
        move |_, _| {
            let entered = entered.clone();
            let gate = gate.clone();
            async move {
                entered.fetch_add(1, Ordering::SeqCst);
                let permit = gate.acquire().await.unwrap();
                permit.forget();
                Ok(GenerationOutput::text("done"))
            }
        }
    }));
    let limited = Arc::new(ConcurrencyLimitedModel::new(model, 1).unwrap());
    let first = tokio::spawn({
        let limited = limited.clone();
        async move { limited.generate(request(), context()).await }
    });
    while entered.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let cancelled = context();
    let token = cancelled.cancellation.clone();
    let waiting = tokio::spawn({
        let limited = limited.clone();
        async move { limited.generate(request(), cancelled).await }
    });
    token.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::Cancelled)
    ));
    let mut expired = context();
    expired.run.deadline_at_ms = Some(zhir::kernel::defaults::context().started_at_ms + 15);
    assert!(matches!(
        limited.generate(request(), expired).await,
        Err(Error::Deadline)
    ));
    assert_eq!(entered.load(Ordering::SeqCst), 1);
    first.abort();
    let _ = first.await;
    gate.add_permits(1);
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            limited.generate(request(), context())
        )
        .await
        .unwrap()
        .is_ok()
    );
    assert_eq!(entered.load(Ordering::SeqCst), 2);
}

fn typed(name: &str) -> Arc<dyn zhir::tool::RuntimeTool> {
    Arc::new(
        TypedTool::<Report, Report>::new(
            name,
            "fixture",
            Execution::default(),
            |args, _| async move { Ok(zhir::runtime_tools::ToolReply::success(args)) },
        )
        .unwrap(),
    )
}
#[tokio::test]
async fn selected_catalog_is_frozen_for_the_entire_run() {
    let registry = Arc::new(RuntimeToolRegistry::from_tools([typed("a")]).unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let model = Arc::new(FunctionModel::new(zhir_testing::model_capabilities(), {
        let registry = registry.clone();
        let calls = calls.clone();
        move |request, _| {
            assert_eq!(
                request
                    .runtime_tools
                    .iter()
                    .map(|s| s.name.as_str())
                    .collect::<Vec<_>>(),
                ["a"]
            );
            let turn = calls.fetch_add(1, Ordering::SeqCst);
            if turn == 0 {
                registry.register(typed("b")).unwrap();
            }
            async move {
                if turn == 0 {
                    let mut response = GenerationOutput::text("");
                    response.output = vec![Output::RuntimeToolCall {
                        call: RuntimeToolCall {
                            id: "call".into(),
                            name: "a".into(),
                            input: RuntimeToolInput::Structured(json!({"count": 3})),
                        },
                    }];
                    Ok(response)
                } else {
                    assert!(
                        matches!(request.messages.last(), Some(Message::RuntimeTool {outcome, ..}) if outcome.structured().unwrap()["count"] == 3)
                    );
                    Ok(GenerationOutput::text("done"))
                }
            }
        }
    }));
    let runtime = Runtime::builder(model)
        .runtime_tools(registry)
        .build()
        .unwrap();
    let completion = runtime
        .start(
            zhir::RunRequest::new([Message::user("run")])
                .runtime_tools(zhir::tool::RuntimeToolSelection::only(["a"])),
        )
        .unwrap()
        .result()
        .await
        .unwrap();
    assert!(matches!(
        completion.outcome(),
        zhir::RunOutcome::Completed(_)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn streamed_sink_completion_is_inside_the_concurrency_permit() {
    let entered = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let model = Arc::new(FunctionModel::new(zhir_testing::model_capabilities(), {
        let entered = entered.clone();
        move |_, context| {
            let entered = entered.clone();
            async move {
                entered.fetch_add(1, Ordering::SeqCst);
                context
                    .deltas
                    .unwrap()
                    .emit(zhir::model::ModelDelta::Text {
                        output_index: 0,
                        text: "piece".into(),
                    })
                    .await?;
                Ok(GenerationOutput::text("done"))
            }
        }
    }));
    let limited = Arc::new(ConcurrencyLimitedModel::new(model, 1).unwrap());
    let sink = Arc::new(zhir::models::FunctionDeltaSink::new({
        let gate = gate.clone();
        move |_| {
            let gate = gate.clone();
            async move {
                gate.acquire().await.unwrap().forget();
                Ok(())
            }
        }
    }));
    let start = || {
        let limited = limited.clone();
        let sink = sink.clone();
        tokio::spawn(async move {
            let mut context = context();
            context.deltas = Some(sink);
            let mut request = request();
            request.stream = true;
            limited.generate(request, context).await
        })
    };
    let first = start();
    while entered.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let second = start();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(entered.load(Ordering::SeqCst), 1);
    gate.add_permits(1);
    first.await.unwrap().unwrap();
    while entered.load(Ordering::SeqCst) < 2 {
        tokio::task::yield_now().await;
    }
    assert!(!second.is_finished());
    gate.add_permits(1);
    second.await.unwrap().unwrap();
}

use zhir_testing::FinalExecution;
