#![cfg(all(
    feature = "openai-responses",
    feature = "typed-tools",
    feature = "memory"
))]
mod provider_fixture;
use provider_fixture::*;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use zhir::{
    BoxFuture, Result, Runtime,
    error::Error,
    message::{Content, Message, Output},
    model::{ModelDelta, ToolChoice},
    models::{ModelConfig, ResourceModel, openai},
    run::State,
};
use zhir_core::resource::{
    ResourceReader, ResourceRef, ResourceSource, ResourceStore, ResourceWriter,
};

#[tokio::test]
async fn provider_adapter_matrix_preserves_identity_order_progress_and_replay() {
    let mut scenarios = 0;
    let mut report = Vec::new();
    for repeat in 0..4 {
        for count in [1, 3, 16, 64] {
            for streaming in [false, true] {
                for stage in ["done", "partial", "failed", "queued", "working"] {
                    let raw = frame((0..count).map(|index| item(index, stage)).collect());
                    let body = if streaming {
                        stream(&raw, count)
                    } else {
                        raw.to_string()
                    };
                    let (url, worker) =
                        server(vec![(body, streaming), (frame(vec![]).to_string(), false)]).await;
                    let model = openai::responses::model(ModelConfig::new(
                        url,
                        std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                            "Bearer", "fixture",
                        )),
                        "fixture",
                    ))
                    .unwrap()
                    .with_extension(move |_| extension());
                    let deltas = Arc::new(Deltas::default());
                    let mut first_request = request(streaming);
                    if repeat % 2 == 0 {
                        first_request.tool_choice = ToolChoice::ProviderTool {
                            provider: spec().provider,
                            name: spec().name,
                        };
                    }
                    let response = model
                        .generate(first_request, context(deltas.clone()))
                        .await
                        .unwrap();
                    assert_eq!(response.output.len(), count);
                    assert_eq!(
                        response.status == zhir_core::model::ResponseStatus::Continuation,
                        matches!(stage, "queued" | "working")
                    );
                    for (index, output) in response.output.iter().enumerate() {
                        assert!(
                            matches!(output,Output::ProviderToolCall {call} if call.id==format!("render-{index}") && call.provider=="consumer.example" && zhir::models::provider_tools::ProviderOutput::replay(call).unwrap()==vec![raw["output"][index].clone()])
                        );
                    }
                    let progress = deltas
                        .0
                        .lock()
                        .unwrap()
                        .iter()
                        .filter_map(|delta| {
                            if let ModelDelta::ProviderToolProgress {
                                output_index, data, ..
                            } = delta
                            {
                                Some((*output_index, data["session_event"].as_u64().unwrap()))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(progress.len(), if streaming { count } else { 0 });
                    for (index, sequence) in progress {
                        assert_eq!(sequence, index as u64 + 1);
                    }
                    let mut next = request(false);
                    next.messages.push(Message::Assistant {
                        output: response.output,
                        provider_data: response.provider_data,
                    });
                    model
                        .generate(next, context(Arc::new(Deltas::default())))
                        .await
                        .unwrap();
                    let sent = worker.await.unwrap();
                    assert_eq!(sent[0]["tools"][0]["type"], "consumer_render");
                    if repeat % 2 == 0 {
                        assert_eq!(sent[0]["tool_choice"]["type"], "consumer_render");
                    }
                    let replay = sent[1]["input"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|item| item["type"] == "consumer_render_call")
                        .cloned()
                        .collect::<Vec<_>>();
                    assert_eq!(replay, raw["output"].as_array().unwrap().clone());
                    report.push(json!({"repeat":repeat,"calls":count,"stream":streaming,"stage":stage,"http_requests":2,"passed":true}));
                    scenarios += 1;
                }
            }
        }
    }
    assert_eq!(scenarios, 160);
    if let Ok(path) = std::env::var("ZHIR_PROVIDER_REPORT") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(
                &json!({"scenarios":scenarios,"http_requests":scenarios*2,"rows":report}),
            )
            .unwrap(),
        )
        .unwrap();
    }
    println!(
        "provider matrix: {scenarios} scenarios, {} HTTP requests",
        scenarios * 2
    );
}

#[tokio::test]
async fn unknown_disabled_duplicate_and_unmapped_calls_fail_without_scheduling() {
    for streaming in [false, true] {
        for raw_item in [
            json!({"type":"computer_call","id":"call","call_id":"client-call","status":"completed","actions":[]}),
            json!({"type":"new_remote_call","id":"call","status":"completed"}),
        ] {
            let raw = frame(vec![raw_item]);
            let body = if streaming {
                stream(&raw, 0)
            } else {
                raw.to_string()
            };
            let (url, worker) = server(vec![(body, streaming)]).await;
            let model = openai::responses::model(ModelConfig::new(
                url,
                std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                    "Bearer", "fixture",
                )),
                "fixture",
            ))
            .unwrap();
            let mut input = request(streaming);
            input.provider_tools.clear();
            assert!(
                model
                    .generate(input, context(Arc::new(Deltas::default())))
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("unmapped execution")
            );
            worker.await.unwrap();
        }
    }
    let (url, worker) = server(vec![(frame(vec![item(0, "done")]).to_string(), false)]).await;
    let model = openai::responses::model(ModelConfig::new(
        url,
        std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
            "Bearer", "fixture",
        )),
        "fixture",
    ))
    .unwrap()
    .with_extension(move |_| extension());
    let mut disabled = request(false);
    disabled.provider_tools.clear();
    assert!(
        model
            .generate(disabled, context(Arc::new(Deltas::default())))
            .await
            .unwrap_err()
            .to_string()
            .contains("not enabled")
    );
    worker.await.unwrap();
    let mut adapters = extension().unwrap();
    assert!(adapters.register(Render::default()).is_err());
    let model = openai::responses::model(ModelConfig::new(
        "http://127.0.0.1:1",
        std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
            "Bearer", "fixture",
        )),
        "fixture",
    ))
    .unwrap();
    assert!(
        model
            .generate(request(false), context(Arc::new(Deltas::default())))
            .await
            .unwrap_err()
            .to_string()
            .contains("no provider adapter")
    );
    let (url, worker) = server(vec![(
        frame(vec![item(0, "done"), item(0, "done")]).to_string(),
        false,
    )])
    .await;
    let model = openai::responses::model(ModelConfig::new(
        url,
        std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
            "Bearer", "fixture",
        )),
        "fixture",
    ))
    .unwrap()
    .with_extension(move |_| extension());
    assert!(
        model
            .generate(request(false), context(Arc::new(Deltas::default())))
            .await
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );
    worker.await.unwrap();
}

#[tokio::test]
async fn mixed_runtime_and_provider_calls_have_separate_execution_and_metrics() {
    use zhir::runtime_tools::{RuntimeToolRegistry, TypedTool};
    use zhir::tool::Execution;
    #[derive(serde::Deserialize, schemars::JsonSchema)]
    struct Args {
        value: String,
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let tool = TypedTool::<Args, String>::new("echo", "fixture", Execution::default(), {
        let calls = calls.clone();
        move |args, _| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(zhir::runtime_tools::ToolReply::success(args.value))
            }
        }
    })
    .unwrap();
    let raw = frame(vec![
        item(0, "done"),
        json!({"type":"function_call","call_id":"runtime-1","name":"echo","arguments":"{\"value\":\"ok\"}"}),
        item(1, "done"),
    ]);
    let (url,worker)=server(vec![(raw.to_string(),false),(frame(vec![json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]})]).to_string(),false)]).await;
    let model = openai::responses::model(ModelConfig::new(
        url,
        std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
            "Bearer", "fixture",
        )),
        "fixture",
    ))
    .unwrap()
    .with_extension(move |_| extension());
    let runtime = Runtime::builder(Arc::new(model))
        .runtime_tools(Arc::new(
            RuntimeToolRegistry::from_tools([Arc::new(tool) as Arc<dyn zhir::tool::RuntimeTool>])
                .unwrap(),
        ))
        .defaults(|run| run.provider_tools(vec![spec()]))
        .build()
        .unwrap();
    let completed = runtime
        .start(zhir::RunRequest::new(vec![Message::user("mixed")]))
        .unwrap()
        .result()
        .await
        .unwrap()
        .into_checkpoint();
    assert!(matches!(completed.state, State::Completed { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(completed.metrics.runtime_tool_calls, 1);
    assert_eq!(zhir::output::provider_calls(&completed).len(), 2);
    let sent = worker.await.unwrap();
    assert_eq!(
        sent[1]["input"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "function_call_output")
            .count(),
        1
    );
}

#[derive(Clone)]
struct Files {
    root: std::path::PathBuf,
    puts: Arc<AtomicUsize>,
    gets: Arc<AtomicUsize>,
    fail: bool,
}
impl ResourceStore for Files {
    fn create(
        &self,
        key: String,
        media_type: String,
    ) -> BoxFuture<'_, Result<Box<dyn ResourceWriter>>> {
        Box::pin(async move {
            self.puts.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(Error::Storage("fixture write failed".into()));
            }
            zhir_storage::FilesystemResourceStore::open(&self.root)
                .await?
                .create(key, media_type)
                .await
        })
    }
    fn open(&self, reference: ResourceRef) -> BoxFuture<'_, Result<Box<dyn ResourceReader>>> {
        Box::pin(async move {
            self.gets.fetch_add(1, Ordering::SeqCst);
            ResourceStore::open(
                &zhir_storage::FilesystemResourceStore::open(&self.root).await?,
                reference,
            )
            .await
        })
    }
    fn delete(&self, reference: ResourceRef) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            zhir_storage::FilesystemResourceStore::open(&self.root)
                .await?
                .delete(reference)
                .await
        })
    }
}
fn resource_url(media_type: &str, url: String) -> Content {
    Content::resource(ResourceRef {
        id: url.clone(),
        media_type: media_type.into(),
        name: None,
        source: ResourceSource::Url { url },
        metadata: Default::default(),
    })
}
#[tokio::test]
async fn resources_are_durable_before_commit_and_replay_after_reconstruction() {
    let dir = tempfile::tempdir().unwrap();
    let puts = Arc::new(AtomicUsize::new(0));
    let gets = Arc::new(AtomicUsize::new(0));
    let files = Files {
        root: dir.path().into(),
        puts: puts.clone(),
        gets: gets.clone(),
        fail: false,
    };
    let (url, worker) = server(vec![(frame(vec![item(0, "done")]).to_string(), false)]).await;
    let model = ResourceModel::new(
        Arc::new(
            openai::responses::model(ModelConfig::new(
                url,
                std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                    "Bearer", "fixture",
                )),
                "fixture",
            ))
            .unwrap()
            .with_extension(move |_| extension()),
        ),
        Arc::new(files.clone()),
        16 * 1024 * 1024,
    )
    .unwrap();
    let store = Arc::new(zhir::stores::memory::MemoryRunStore::new());
    let runtime = Runtime::builder(Arc::new(model))
        .defaults(|run| run.provider_tools(vec![spec()]))
        .store(store)
        .build()
        .unwrap();
    let checkpoint = runtime
        .start(zhir::RunRequest::new(vec![Message::user("image")]))
        .unwrap()
        .result()
        .await
        .unwrap()
        .into_checkpoint();
    worker.await.unwrap();
    assert!(
        matches!(&checkpoint.state,State::Completed{content} if matches!(&content[0], Content::Resource { input } if matches!(input.resource.source, ResourceSource::Stored { .. })))
    );
    let bytes = zhir::wire::encode_checkpoint(&checkpoint).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("aGVsbG8="));
    assert_eq!(puts.load(Ordering::SeqCst), 1);
    let restored = zhir::wire::decode_checkpoint(&bytes).unwrap();
    let (url, worker) = server(vec![(frame(vec![]).to_string(), false)]).await;
    let model = ResourceModel::new(
        Arc::new(
            openai::responses::model(ModelConfig::new(
                url,
                std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                    "Bearer", "fixture",
                )),
                "fixture",
            ))
            .unwrap()
            .with_extension(move |_| extension()),
        ),
        Arc::new(Files {
            root: dir.path().into(),
            puts,
            gets: gets.clone(),
            fail: false,
        }),
        16 * 1024 * 1024,
    )
    .unwrap();
    let mut next = request(false);
    next.messages = zhir::model::conversation(restored.history.iter());
    next.messages.push(Message::user("continue"));
    model
        .generate(next, context(Arc::new(Deltas::default())))
        .await
        .unwrap();
    let sent = worker.await.unwrap();
    let image = sent[0]["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "consumer_render_call")
        .unwrap();
    assert_eq!(image["result"], "aGVsbG8=");
    assert_eq!(
        gets.load(Ordering::SeqCst),
        4,
        "canonical accepted outputs are also projected into the active session"
    );
}

#[tokio::test]
async fn resource_failure_or_missing_binding_never_commits_a_partial_provider_result() {
    for failed_store in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let files = Files {
            root: dir.path().into(),
            puts: Arc::new(AtomicUsize::new(0)),
            gets: Arc::new(AtomicUsize::new(0)),
            fail: failed_store,
        };
        let mut raw = item(0, "done");
        if !failed_store {
            raw["unbound_duplicate"] = raw["result"].clone();
        }
        let (url, worker) = server(vec![(frame(vec![raw]).to_string(), false)]).await;
        let model = ResourceModel::new(
            Arc::new(
                openai::responses::model(ModelConfig::new(
                    url,
                    std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                        "Bearer", "fixture",
                    )),
                    "fixture",
                ))
                .unwrap()
                .with_extension(move |_| extension()),
            ),
            Arc::new(files),
            16 * 1024 * 1024,
        )
        .unwrap();
        let runtime = Runtime::builder(Arc::new(model))
            .defaults(|run| run.provider_tools(vec![spec()]))
            .build()
            .unwrap();
        let result = runtime
            .start(zhir::RunRequest::new(vec![Message::user("failure")]))
            .unwrap()
            .result()
            .await;
        worker.await.unwrap();
        let checkpoint = if failed_store {
            let error = result.unwrap_err();
            assert!(matches!(error.error, Error::Storage(_)));
            let checkpoint = error.last_checkpoint.unwrap();
            assert!(checkpoint.active.session.generation_id.is_some());
            checkpoint
        } else {
            let checkpoint = result.unwrap().into_checkpoint();
            assert!(checkpoint.active.session.generation_id.is_some());
            assert!(
                matches!(&checkpoint.state, State::Failed { error } if error.code == "protocol"),
                "{:?}",
                checkpoint.state
            );
            checkpoint
        };
        assert_eq!(checkpoint.history.len(), 1);
        assert!(zhir::output::provider_calls(&checkpoint).is_empty());
    }
}

#[tokio::test]
async fn invocation_sessions_are_isolated_across_concurrent_streams() {
    let raw = frame(vec![item(0, "done")]);
    let (url, worker) = server((0..32).map(|_| (stream(&raw, 1), true)).collect()).await;
    let model = Arc::new(
        openai::responses::model(ModelConfig::new(
            url,
            std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                "Bearer", "fixture",
            )),
            "fixture",
        ))
        .unwrap()
        .with_extension(move |_| extension()),
    );
    let results = futures::future::join_all((0..32).map(|_| {
        let model = model.clone();
        async move {
            let deltas = Arc::new(Deltas::default());
            model
                .generate(request(true), context(deltas.clone()))
                .await
                .unwrap();
            let events = deltas.0.lock().unwrap();
            let progress = events
                .iter()
                .filter_map(|event| {
                    if let ModelDelta::ProviderToolProgress { data, .. } = event {
                        Some(data["session_event"].as_u64().unwrap())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(progress, vec![1]);
        }
    }))
    .await;
    assert_eq!(results.len(), 32);
    assert_eq!(worker.await.unwrap().len(), 32);
}

#[tokio::test]
async fn provider_continuation_never_enters_runtime_tool_execution() {
    for stage in ["queued", "working"] {
        let (url, worker) = server(vec![
            (frame(vec![item(0, stage)]).to_string(), false),
            (frame(vec![item(0, "done")]).to_string(), false),
        ])
        .await;
        let model = openai::responses::model(ModelConfig::new(
            url,
            std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                "Bearer", "fixture",
            )),
            "fixture",
        ))
        .unwrap()
        .with_extension(move |_| extension());
        let runtime = Runtime::builder(Arc::new(model))
            .defaults(|run| run.provider_tools(vec![spec()]))
            .build()
            .unwrap();
        let completed = runtime
            .start(zhir::RunRequest::new(vec![Message::user("continue")]))
            .unwrap()
            .result()
            .await
            .unwrap()
            .into_checkpoint();
        assert!(matches!(completed.state, State::Completed { .. }));
        assert_eq!(completed.metrics.generation_requests, 2);
        assert_eq!(completed.metrics.runtime_tool_calls, 0);
        assert_eq!(zhir::output::provider_calls(&completed).len(), 2);
        assert_eq!(worker.await.unwrap().len(), 2);
    }
}

struct ClientAction;
impl zhir::models::ProtocolExtension for ClientAction {
    fn decode_output_item(
        &mut self,
        _: zhir::models::Protocol,
        item: &Value,
        _: &Value,
    ) -> Result<Option<Vec<Output>>> {
        if item["type"] != "computer_call" {
            return Ok(None);
        }
        Ok(Some(vec![Output::RuntimeToolCall {
            call: zhir::tool::RuntimeToolCall {
                id: item["call_id"].as_str().unwrap().into(),
                name: "browser_action".into(),
                input: zhir::tool::RuntimeToolInput::Structured(json!({"actions":item["actions"]})),
            },
        }]))
    }
    fn encode_request(
        &mut self,
        _: zhir::models::Protocol,
        _: &zhir::model::ModelRequest,
        body: &mut Value,
    ) -> Result<()> {
        body["tools"] = json!([{"type":"computer"}]);
        for item in body["input"].as_array_mut().unwrap() {
            if item["type"] == "function_call_output" {
                item["type"] = json!("computer_call_output");
            }
        }
        Ok(())
    }
}
#[tokio::test]
async fn consumer_maps_native_client_actions_to_the_single_runtime_trait() {
    #[derive(serde::Deserialize, schemars::JsonSchema)]
    struct Args {
        actions: Vec<Value>,
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let tool = zhir::runtime_tools::TypedTool::<Args, bool>::new(
        "browser_action",
        "fixture",
        Default::default(),
        {
            let calls = calls.clone();
            move |args, _| {
                let calls = calls.clone();
                async move {
                    assert_eq!(args.actions.len(), 1);
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(zhir::runtime_tools::ToolReply::success(true))
                }
            }
        },
    )
    .unwrap();
    let (url,worker)=server(vec![(frame(vec![json!({"type":"computer_call","call_id":"client-action","status":"completed","actions":[{"type":"screenshot"}]})]).to_string(),false),(frame(vec![]).to_string(),false)]).await;
    let model = openai::responses::model(ModelConfig::new(
        url,
        std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
            "Bearer", "fixture",
        )),
        "fixture",
    ))
    .unwrap()
    .with_extension(|_| Ok(ClientAction));
    let runtime = Runtime::builder(Arc::new(model))
        .runtime_tools(Arc::new(
            zhir::runtime_tools::RuntimeToolRegistry::from_tools([
                Arc::new(tool) as Arc<dyn zhir::tool::RuntimeTool>
            ])
            .unwrap(),
        ))
        .build()
        .unwrap();
    let completed = runtime
        .start(zhir::RunRequest::new(vec![Message::user("client action")]))
        .unwrap()
        .result()
        .await
        .unwrap()
        .into_checkpoint();
    assert!(matches!(completed.state, State::Completed { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(completed.metrics.runtime_tool_calls, 1);
    assert!(zhir::output::provider_calls(&completed).is_empty());
    let sent = worker.await.unwrap();
    assert!(
        sent[1]["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |item| item["type"] == "computer_call_output" && item["call_id"] == "client-action"
            )
    );
}

#[tokio::test]
async fn conflicting_mapping_and_malformed_provider_status_are_explicit_errors() {
    let model = openai::responses::model(ModelConfig::new(
        "http://127.0.0.1:1",
        std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
            "Bearer", "fixture",
        )),
        "fixture",
    ))
    .unwrap()
    .with_extension(|_| {
        Ok({
            zhir::models::ExtensionChain::new()
                .push(extension()?)
                .push(extension()?)
        })
    });
    assert!(
        model
            .generate(request(false), context(Arc::new(Deltas::default())))
            .await
            .unwrap_err()
            .to_string()
            .contains("multiple extensions")
    );
    for stage in ["unknown", ""] {
        let (url, worker) = server(vec![(stream(&frame(vec![item(0, stage)]), 1), true)]).await;
        let model = openai::responses::model(ModelConfig::new(
            url,
            std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                "Bearer", "fixture",
            )),
            "fixture",
        ))
        .unwrap()
        .with_extension(move |_| extension());
        let deltas = Arc::new(Deltas::default());
        assert!(
            model
                .generate(request(true), context(deltas.clone()))
                .await
                .is_err()
        );
        assert!(
            deltas
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|delta| matches!(delta, ModelDelta::ProviderToolProgress { .. }))
        );
        worker.await.unwrap();
    }
}

#[tokio::test]
async fn resource_resolution_preserves_business_objects_and_missing_references_stop_io() {
    let dir = tempfile::tempdir().unwrap();
    let gets = Arc::new(AtomicUsize::new(0));
    let files = Arc::new(Files {
        root: dir.path().into(),
        gets: gets.clone(),
        puts: Arc::new(AtomicUsize::new(0)),
        fail: false,
    });
    let invoked = Arc::new(AtomicUsize::new(0));
    let inner = Arc::new(zhir::models::FunctionModel::new(
        zhir::model::CapabilitySet {
            input_modalities: vec!["text".into(), "image".into()],
            ..zhir_testing::model_capabilities()
        },
        {
            let invoked = invoked.clone();
            move |request, _| {
                let invoked = invoked.clone();
                async move {
                    invoked.fetch_add(1, Ordering::SeqCst);
                    assert!(
                        matches!(&request.messages[1],Message::Assistant{output,..} if matches!(&output[0],Output::RuntimeToolCall{call} if call.input == zhir::tool::RuntimeToolInput::Structured(json!({"kind":"artifact","id":"business"}))))
                    );
                    Ok(zhir::model::GenerationOutput::text("done"))
                }
            }
        },
    ));
    let model = ResourceModel::new(inner, files, 16 * 1024 * 1024).unwrap();
    let mut first = request(false);
    first.provider_tools.clear();
    first.messages.push(Message::Assistant {
        output: vec![Output::RuntimeToolCall {
            call: zhir::tool::RuntimeToolCall {
                id: "runtime".into(),
                name: "business".into(),
                input: zhir::tool::RuntimeToolInput::Structured(
                    json!({"kind":"artifact","id":"business"}),
                ),
            },
        }],
        provider_data: Value::Null,
    });
    model
        .generate(first, context(Arc::new(Deltas::default())))
        .await
        .unwrap();
    assert_eq!(gets.load(Ordering::SeqCst), 0);
    let mut second = request(false);
    second.provider_tools.clear();
    second.messages.push(Message::User {
        content: vec![Content::resource(ResourceRef {
            id: "missing".into(),
            media_type: "image/png".into(),
            name: None,
            source: ResourceSource::Stored {
                key: "0".repeat(64),
            },
            metadata: Default::default(),
        })],
    });
    let error = model
        .generate(second, context(Arc::new(Deltas::default())))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Resource(_)), "{error:?}");
    assert_eq!(invoked.load(Ordering::SeqCst), 1);
    assert_eq!(gets.load(Ordering::SeqCst), 1);
}

struct TokenAdapter;
impl zhir::models::provider_tools::ProviderToolAdapter for TokenAdapter {
    fn identity(&self) -> (&str, &str) {
        ("consumer.example", "token")
    }
    fn encode(
        &mut self,
        _: zhir::models::Protocol,
        spec: &zhir::model::ProviderToolSpec,
    ) -> Result<Value> {
        Ok(json!({"type":"tokens","options":spec.options}))
    }
    fn decode(
        &mut self,
        _: zhir::models::Protocol,
        item: &Value,
        _: &Value,
    ) -> Result<Option<Vec<Output>>> {
        if item["type"] != "token_result" {
            return Ok(None);
        }
        Ok(Some(vec![Output::ProviderToolCall {
            call: zhir::message::ProviderToolCall {
                outcome: None,
                id: item["token"].as_str().unwrap().into(),
                provider: "consumer.example".into(),
                name: "token".into(),
                status: zhir::message::ProviderToolStatus::Completed,
                output: vec![],
                data: json!({"resume":item["token"]}),
            },
        }]))
    }
    fn replay(
        &mut self,
        _: zhir::models::Protocol,
        call: &zhir::message::ProviderToolCall,
    ) -> Result<Vec<Value>> {
        Ok(vec![
            json!({"type":"token_reference","token":call.data["resume"]}),
        ])
    }
}
#[tokio::test]
async fn arbitrary_native_identity_fields_and_custom_replay_do_not_require_core_changes() {
    let extension = || {
        let mut registry = zhir::models::provider_tools::ProviderTools::new();
        registry.register(TokenAdapter)?;
        Ok(registry)
    };
    let raw = frame(vec![
        json!({"type":"message","content":[{"type":"output_text","text":"before"}]}),
        json!({"type":"token_result","token":"arbitrary"}),
        json!({"type":"message","content":[{"type":"output_text","text":"after"}]}),
    ]);
    let (url, worker) = server(vec![
        (raw.to_string(), false),
        (frame(vec![]).to_string(), false),
    ])
    .await;
    let model = openai::responses::model(ModelConfig::new(
        url,
        std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
            "Bearer", "fixture",
        )),
        "fixture",
    ))
    .unwrap()
    .with_extension(move |_| extension());
    let mut input = request(false);
    input.provider_tools = vec![zhir::model::ProviderToolSpec {
        provider: "consumer.example".into(),
        name: "token".into(),
        options: json!([1, 2]),
    }];
    let response = model
        .generate(input.clone(), context(Arc::new(Deltas::default())))
        .await
        .unwrap();
    assert_eq!(
        response.provider_data["response"]["output"][1]["$zhir_provider_calls"],
        json!(["arbitrary"])
    );
    input.messages.push(Message::Assistant {
        output: response.output,
        provider_data: response.provider_data,
    });
    model
        .generate(input, context(Arc::new(Deltas::default())))
        .await
        .unwrap();
    let sent = worker.await.unwrap();
    assert_eq!(
        sent[1]["input"][2],
        json!({"type":"token_reference","token":"arbitrary"})
    );
    assert_eq!(sent[0]["tools"][0]["options"], json!([1, 2]));
}

#[cfg(feature = "anthropic")]
struct BlockAdapter;
#[cfg(feature = "anthropic")]
impl zhir::models::provider_tools::ProviderToolAdapter for BlockAdapter {
    fn identity(&self) -> (&str, &str) {
        ("consumer.example", "render")
    }
    fn encode(
        &mut self,
        _: zhir::models::Protocol,
        spec: &zhir::model::ProviderToolSpec,
    ) -> Result<Value> {
        Ok(json!({"type":"consumer_renderer","name":"render","config":spec.options}))
    }
    fn decode(
        &mut self,
        _: zhir::models::Protocol,
        item: &Value,
        response: &Value,
    ) -> Result<Option<Vec<Output>>> {
        if item["type"] == "service_receipt" {
            return Ok(Some(vec![]));
        }
        if item["type"] != "service_request" {
            return Ok(None);
        }
        let receipt = response["content"]
            .as_array()
            .unwrap()
            .iter()
            .find(|block| block["type"] == "service_receipt" && block["ticket"] == item["ticket"])
            .unwrap();
        Ok(Some(vec![Output::ProviderToolCall {
            call: zhir::message::ProviderToolCall {
                outcome: None,
                id: item["ticket"].as_str().unwrap().into(),
                provider: "consumer.example".into(),
                name: "render".into(),
                status: zhir::message::ProviderToolStatus::Completed,
                output: vec![Content::text(receipt["answer"].as_str().unwrap())],
                data: json!({"ticket":item["ticket"],"answer":receipt["answer"]}),
            },
        }]))
    }
    fn replay(
        &mut self,
        _: zhir::models::Protocol,
        call: &zhir::message::ProviderToolCall,
    ) -> Result<Vec<Value>> {
        Ok(vec![
            json!({"type":"service_request","ticket":call.data["ticket"]}),
            json!({"type":"service_receipt","ticket":call.data["ticket"],"answer":call.data["answer"]}),
        ])
    }
}
#[cfg(feature = "anthropic")]
#[tokio::test]
async fn paired_native_blocks_replay_once_in_stream_and_nonstream_messages() {
    for streaming in [false, true] {
        let content = vec![
            json!({"type":"service_request","ticket":"ticket-1"}),
            json!({"type":"service_receipt","ticket":"ticket-1","answer":"ok"}),
        ];
        let raw = json!({"id":"message","role":"assistant","content":content,"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}});
        let body = if streaming {
            let mut events = vec![
                json!({"type":"message_start","message":{"id":"message","role":"assistant","content":[],"usage":{"input_tokens":1}}}),
            ];
            for (index, block) in content.iter().enumerate() {
                events.push(
                    json!({"type":"content_block_start","index":index,"content_block":block}),
                );
                events.push(json!({"type":"content_block_stop","index":index}));
            }
            events.push(json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}));
            events.push(json!({"type":"message_stop"}));
            events
                .iter()
                .map(|event| format!("data: {event}\n\n"))
                .collect::<String>()
        } else {
            raw.to_string()
        };
        let (url,worker)=server(vec![(body,streaming),(json!({"content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{}}).to_string(),false)]).await;
        let model = zhir::models::anthropic::messages::model(ModelConfig::new(
            url,
            std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                "Bearer", "fixture",
            )),
            "fixture",
        ))
        .unwrap()
        .with_extension(|_| {
            Ok({
                let mut registry = zhir::models::provider_tools::ProviderTools::new();
                registry.register(BlockAdapter)?;
                registry
            })
        });
        let response = model
            .generate(request(streaming), context(Arc::new(Deltas::default())))
            .await
            .unwrap();
        assert_eq!(response.output.len(), 1);
        assert_eq!(
            response.provider_data["response"]["content"][1]["$zhir_provider_calls"],
            json!([])
        );
        let mut next = request(false);
        next.messages.push(Message::Assistant {
            output: response.output,
            provider_data: response.provider_data,
        });
        model
            .generate(next, context(Arc::new(Deltas::default())))
            .await
            .unwrap();
        let sent = worker.await.unwrap();
        assert_eq!(sent[1]["messages"][1]["content"], json!(content));
    }
}

struct MediaAdapter;
impl zhir::models::provider_tools::ProviderToolAdapter for MediaAdapter {
    fn identity(&self) -> (&str, &str) {
        ("media.example", "compose")
    }
    fn encode(
        &mut self,
        _: zhir::models::Protocol,
        spec: &zhir::model::ProviderToolSpec,
    ) -> Result<Value> {
        Ok(json!({"type":"media_service","settings":spec.options}))
    }
    fn decode(
        &mut self,
        _: zhir::models::Protocol,
        item: &Value,
        _: &Value,
    ) -> Result<Option<Vec<Output>>> {
        if item["type"] != "media_result" {
            return Ok(None);
        }
        Ok(Some(vec![Output::ProviderToolCall {
            call: zhir::message::ProviderToolCall {
                outcome: None,
                id: "media-1".into(),
                provider: "media.example".into(),
                name: "compose".into(),
                status: zhir::message::ProviderToolStatus::Completed,
                output: vec![
                    resource_url("audio/wav", "https://fixtures.invalid/audio".into()),
                    resource_url("video/mp4", "https://fixtures.invalid/video".into()),
                    resource_url(
                        "application/octet-stream",
                        "https://fixtures.invalid/file".into(),
                    ),
                ],
                data: json!({"receipt":item["receipt"]}),
            },
        }]))
    }
    fn replay(
        &mut self,
        _: zhir::models::Protocol,
        call: &zhir::message::ProviderToolCall,
    ) -> Result<Vec<Value>> {
        Ok(vec![
            json!({"type":"media_reference","receipt":call.data["receipt"]}),
        ])
    }
}
#[tokio::test]
async fn consumer_media_outputs_are_independent_of_native_model_input_modalities() {
    let (url, worker) = server(vec![
        (
            frame(vec![json!({"type":"media_result","receipt":"receipt-1"})]).to_string(),
            false,
        ),
        (frame(vec![]).to_string(), false),
    ])
    .await;
    let model = openai::responses::model(ModelConfig::new(
        url,
        std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
            "Bearer", "fixture",
        )),
        "fixture",
    ))
    .unwrap()
    .with_capabilities(zhir::model::CapabilitySet {
        features: [zhir_core::model::Capability::ProviderTools].into(),

        ..zhir_testing::model_capabilities()
    })
    .with_extension(|_| {
        Ok({
            let mut registry = zhir::models::provider_tools::ProviderTools::new();
            registry.register(MediaAdapter)?;
            registry
        })
    });
    let mut input = request(false);
    input.provider_tools = vec![zhir::model::ProviderToolSpec {
        provider: "media.example".into(),
        name: "compose".into(),
        options: json!({}),
    }];
    let response = model
        .generate(input.clone(), context(Arc::new(Deltas::default())))
        .await
        .unwrap();
    assert_eq!(zhir::message::visible_content(&response.output).len(), 3);
    input.messages.push(Message::Assistant {
        output: response.output,
        provider_data: response.provider_data,
    });
    model
        .generate(input.clone(), context(Arc::new(Deltas::default())))
        .await
        .unwrap();
    let sent = worker.await.unwrap();
    assert_eq!(
        sent[1]["input"][1],
        json!({"type":"media_reference","receipt":"receipt-1"})
    );
    input.messages.push(Message::User {
        content: vec![resource_url(
            "video/mp4",
            "https://fixtures.invalid/native-input".into(),
        )],
    });
    assert!(
        model
            .generate(input, context(Arc::new(Deltas::default())))
            .await
            .unwrap_err()
            .to_string()
            .contains("unsupported input modality")
    );
}

struct ReverseNormalized;
impl zhir::models::ProtocolExtension for ReverseNormalized {
    fn decode_response(
        &mut self,
        _: zhir::models::Protocol,
        _: &Value,
        decoded: Result<zhir::model::GenerationOutput>,
    ) -> Result<zhir::model::GenerationOutput> {
        let mut response = decoded?;
        response.output.reverse();
        Ok(response)
    }
}
#[tokio::test]
async fn canonical_media_replay_survives_reordered_outputs_and_multiple_native_positions() {
    for streaming in [false, true] {
        for count in [1, 2, 8, 32] {
            let dir = tempfile::tempdir().unwrap();
            let puts = Arc::new(AtomicUsize::new(0));
            let gets = Arc::new(AtomicUsize::new(0));
            let files = Arc::new(Files {
                root: dir.path().into(),
                puts: puts.clone(),
                gets: gets.clone(),
                fail: false,
            });
            let mut native = vec![
                json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"before"}]}),
            ];
            native.extend((0..count).map(|n| item(n, "done")));
            native.push(json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"after"}]}));
            let raw = frame(native.clone());
            let (url, worker) = server(vec![
                (
                    if streaming {
                        stream(&raw, count)
                    } else {
                        raw.to_string()
                    },
                    streaming,
                ),
                (frame(vec![]).to_string(), false),
            ])
            .await;
            let model = ResourceModel::new(
                Arc::new(
                    openai::responses::model(ModelConfig::new(
                        url,
                        std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                            "Bearer", "fixture",
                        )),
                        "fixture",
                    ))
                    .unwrap()
                    .with_extension(|_| {
                        Ok(zhir::models::ExtensionChain::new()
                            .push(extension()?)
                            .push(ReverseNormalized))
                    }),
                ),
                files,
                16 * 1024 * 1024,
            )
            .unwrap();
            let response = model
                .generate(request(streaming), context(Arc::new(Deltas::default())))
                .await
                .unwrap();
            assert!(
                matches!(&response.output[1],Output::ProviderToolCall{call} if call.id==format!("render-{}",count-1))
            );
            let bytes = serde_json::to_vec(&response).unwrap();
            assert!(!String::from_utf8_lossy(&bytes).contains("aGVsbG8="));
            let restored: zhir::model::GenerationOutput = serde_json::from_slice(&bytes).unwrap();
            let mut next = request(false);
            next.messages.push(Message::Assistant {
                output: restored.output,
                provider_data: restored.provider_data,
            });
            model
                .generate(next, context(Arc::new(Deltas::default())))
                .await
                .unwrap();
            let sent = worker.await.unwrap();
            assert_eq!(sent[1]["input"].as_array().unwrap()[1..], native);
            assert_eq!(puts.load(Ordering::SeqCst), count);
            assert_eq!(gets.load(Ordering::SeqCst), 2 * count);
        }
    }
}
#[tokio::test]
async fn local_media_paths_support_multiple_items_escaped_keys_and_every_media_kind() {
    use zhir::message::ProviderToolStatus;
    use zhir::models::provider_tools::ProviderOutput;
    for kind in 0..4 {
        let first = json!({"a/b~":["aGVsbG8="]});
        let second = json!({"data":{"bytes":"c2Vjb25k"}});
        let output = ProviderOutput::new(
            "consumer.example",
            "render",
            "bound",
            ProviderToolStatus::Completed,
        )
        .native(first.clone())
        .media("/a~1b~0/0", "image/png")
        .unwrap()
        .native(second.clone())
        .media(
            "/data/bytes",
            [
                "image/png",
                "audio/wav",
                "video/mp4",
                "application/octet-stream",
            ][kind],
        )
        .unwrap()
        .content(resource_url(
            "image/png",
            "https://consumer.example/media".into(),
        ))
        .finish()
        .unwrap();
        let Output::ProviderToolCall { call } = &output else {
            panic!()
        };
        assert_eq!(ProviderOutput::replay(call).unwrap(), vec![first, second]);
        let dir = tempfile::tempdir().unwrap();
        let puts = Arc::new(AtomicUsize::new(0));
        let gets = Arc::new(AtomicUsize::new(0));
        let files = Arc::new(Files {
            root: dir.path().into(),
            puts: puts.clone(),
            gets: gets.clone(),
            fail: false,
        });
        let inner = zhir::models::FunctionModel::new(
            zhir::model::CapabilitySet {
                features: [zhir_core::model::Capability::ProviderTools].into(),

                ..zhir_testing::model_capabilities()
            },
            move |_, _| {
                let output = output.clone();
                async move {
                    let mut r = zhir::model::GenerationOutput::text("");
                    r.output = vec![output];
                    Ok(r)
                }
            },
        );
        let model = ResourceModel::new(Arc::new(inner), files.clone(), 16 * 1024 * 1024).unwrap();
        let response = model
            .generate(request(false), context(Arc::new(Deltas::default())))
            .await
            .unwrap();
        let bytes = serde_json::to_vec(&response).unwrap();
        let saved = String::from_utf8_lossy(&bytes);
        assert!(!saved.contains("aGVsbG8=") && !saved.contains("c2Vjb25k"));
        assert!(saved.contains("https://consumer.example/media"));
        let verifier = zhir::models::FunctionModel::new(
            zhir::model::CapabilitySet {
                features: [zhir_core::model::Capability::ProviderTools].into(),

                ..zhir_testing::model_capabilities()
            },
            |request, _| async move {
                let Message::Assistant { output, .. } = &request.messages[1] else {
                    panic!()
                };
                let Output::ProviderToolCall { call } = &output[0] else {
                    panic!()
                };
                let native = ProviderOutput::replay(call)?;
                assert_eq!(native[0]["a/b~"][0], "aGVsbG8=");
                assert_eq!(native[1]["data"]["bytes"], "c2Vjb25k");
                Ok(zhir::model::GenerationOutput::text("done"))
            },
        );
        let mut next = request(false);
        next.messages.push(Message::Assistant {
            output: response.output,
            provider_data: response.provider_data,
        });
        ResourceModel::new(Arc::new(verifier), files, 16 * 1024 * 1024)
            .unwrap()
            .generate(next, context(Arc::new(Deltas::default())))
            .await
            .unwrap();
        assert_eq!(puts.load(Ordering::SeqCst), 2);
        assert_eq!(gets.load(Ordering::SeqCst), 4);
    }
    let builder = || {
        ProviderOutput::new(
            "consumer.example",
            "render",
            "bad",
            ProviderToolStatus::Completed,
        )
        .native(json!({"result":"aGVsbG8="}))
    };
    assert!(builder().media("/missing", "image/png").is_err());
    assert!(
        builder()
            .media("/result", "image/png")
            .unwrap()
            .media("/result", "image/png")
            .is_err()
    );
    assert!(
        ProviderOutput::new(
            "consumer.example",
            "render",
            "invalid",
            ProviderToolStatus::Completed
        )
        .native(json!({"result":"not base64!"}))
        .media("/result", "image/png")
        .is_err()
    );
    assert!(builder().media("/result", "").unwrap().finish().is_err());
}

use zhir_testing::ModelTestExt;
