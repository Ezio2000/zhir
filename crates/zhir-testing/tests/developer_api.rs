#![cfg(all(feature = "models", feature = "typed-tools", feature = "memory"))]
use futures::future::join_all;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use zhir::{
    Result, ResumeRequest, Runtime, SuspensionSelector,
    error::{Error, Failure},
    message::{Content, Message, Output},
    model::{CapabilitySet, ModelContext, ModelDelta, ModelRequest, TurnOutput},
    models::{
        FunctionDeltaSink, FunctionModel, TransformModel,
        decorators::{ObservedModel, RetryingModel},
    },
    run::{EventData, State},
    runs::{DriveError, drive},
    runtime_tools::{RuntimeToolRegistry, TypedTool},
    storage::RunStore,
    stores::memory::MemoryRunStore,
    tool::{Execution, InputSpec, RuntimeTool, RuntimeToolCall, RuntimeToolInput, RuntimeToolSpec},
};
use zhir_testing::{RecordingModel, RecordingSink, RecordingStore, ScriptStep, ScriptedModel};
fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![Message::user("hello")],
        runtime_tools: vec![],
        provider_tools: vec![],
        profile: Default::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream: true,
    }
}
fn context() -> ModelContext {
    ModelContext {
        run: zhir::kernel::defaults::context(),
        cancellation: Default::default(),
        deltas: None,
    }
}
fn failure() -> Error {
    Error::Model(Failure {
        code: "busy".into(),
        message: "fixture".into(),
        retryable: true,
    })
}
fn tool_response(name: &str, input: Value) -> TurnOutput {
    let mut response = TurnOutput::text("");
    response.output = vec![Output::RuntimeToolCall {
        call: RuntimeToolCall {
            id: "call-1".into(),
            name: name.into(),
            input: RuntimeToolInput::Structured(input),
        },
    }];
    response
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Args {
    value: i64,
}
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Report {
    total: i64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_tools_transforms_observers_and_recording_ports_compose_across_64_runs() {
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let tool = TypedTool::<Args, Report>::new(
        "double",
        "Double input",
        Execution {
            parallel: true,
            read_only: true,
            idempotent: true,
        },
        move |args, _| {
            invoked.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(zhir::runtime_tools::ToolReply::success(Report {
                    total: args.value * 2,
                }))
            }
        },
    )
    .unwrap();
    let registry = Arc::new(
        RuntimeToolRegistry::from_tools([Arc::new(tool) as Arc<dyn RuntimeTool>]).unwrap(),
    );
    let inner = Arc::new(FunctionModel::new(
        zhir_testing::model_capabilities(),
        |request, context| async move {
            assert_eq!(
                request.profile.extensions["consumer"]["prepared"],
                context.run.metadata["value"]
            );
            if let Some(sink) = context.deltas {
                sink.emit(ModelDelta::ProtocolEvent {
                    output_index: 0,
                    data: json!({"run_id":context.run.run_id}),
                })
                .await?;
            }
            if let Some(Message::RuntimeTool { outcome, .. }) = request.messages.last() {
                Ok(TurnOutput::text(outcome.structured().unwrap().to_string()))
            } else {
                Ok(tool_response(
                    "double",
                    json!({"value":context.run.metadata["value"]}),
                ))
            }
        },
    ));
    let prepared = TransformModel::new(inner, |mut request, context| async move {
        tokio::task::yield_now().await;
        request
            .profile
            .extensions
            .entry("consumer".into())
            .or_default()
            .insert("prepared".into(), context.run.metadata["value"].clone());
        Ok(request)
    });
    let records = Arc::new(RecordingModel::new(Arc::new(prepared)));
    let observed = Arc::new(AtomicUsize::new(0));
    let captured = observed.clone();
    let model = ObservedModel::new(records.clone(), move |_, run| {
        let run_id = run.run_id.clone();
        let counter = captured.clone();
        Ok(Arc::new(FunctionDeltaSink::new(move |delta| {
            let counter = counter.clone();
            let run_id = run_id.clone();
            async move {
                assert!(
                    matches!(delta,ModelDelta::ProtocolEvent {data,..} if data["run_id"]==run_id)
                );
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })))
    });
    let store = Arc::new(RecordingStore::new(Arc::new(MemoryRunStore::new())));
    let runtime = Runtime::builder(Arc::new(model))
        .runtime_tools(registry)
        .store(store.clone())
        .defaults(|run| run.stream(true))
        .build()
        .unwrap();
    join_all((0..64).map(|value| {
        let runtime = &runtime;
        let store = &store;
        async move {
            let mut run = zhir::kernel::defaults::context();
            run.metadata.insert("value".into(), value.into());
            let mut sequence = 0;
            let checkpoint = drive(
                runtime
                    .start(zhir::RunRequest::new(vec![Message::user("double")]).context(run))
                    .unwrap(),
                |event| {
                    assert!(event.sequence > sequence);
                    sequence = event.sequence;
                    async { Ok(()) }
                },
            )
            .await
            .unwrap()
            .into_checkpoint();
            assert_eq!(
                zhir::output::decode::<Report>(&checkpoint).unwrap(),
                Report { total: value * 2 }
            );
            assert_eq!(checkpoint.metrics.runtime_tool_calls, 1);
            assert_eq!(
                store
                    .load_head(&checkpoint.context.run_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .id,
                checkpoint.id
            );
        }
    }))
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 64);
    assert_eq!(observed.load(Ordering::SeqCst), 128);
    assert_eq!(records.records().len(), 64);
    assert!(records.records().iter().all(|r| {
        r.events
            .iter()
            .filter(|e| matches!(e.body, zhir::model::SessionEventBody::TurnFinished { .. }))
            .count()
            == 2
    }));
    assert_eq!(store.verify_traces().unwrap(), store.commits().len());
}
#[tokio::test]
async fn scripted_failures_sink_failures_and_exhaustion_remain_explicit() {
    for emit in [false, true] {
        let first = ScriptStep::failure(failure()).with_deltas(if emit {
            vec![ModelDelta::Text {
                output_index: 0,
                text: "partial".into(),
            }]
        } else {
            vec![]
        });
        let scripted = Arc::new(ScriptedModel::new([
            first,
            ScriptStep::response(TurnOutput::text("done")),
        ]));
        let model = RetryingModel::new(
            scripted.clone(),
            zhir_policies::RetryPolicy::new(3)
                .unwrap()
                .backoff(zhir_policies::Backoff::fixed(Duration::ZERO)),
        )
        .unwrap();
        let sink = Arc::new(RecordingSink::default());
        let mut ctx = context();
        ctx.deltas = Some(sink.clone());
        assert!(model.turn(request(), ctx).await.is_err());
        assert_eq!(scripted.requests().len(), 1);
        assert_eq!(scripted.remaining(), 1);
        assert_eq!(sink.deltas().len(), usize::from(emit));
        model.turn(request(), context()).await.unwrap();
        {
            assert!(
                matches!(scripted.turn(request(),context()).await,Err(Error::Protocol(e)) if e.contains("exhausted"))
            );
        }
    }
    let scripted = Arc::new(ScriptedModel::new([ScriptStep::response(
        TurnOutput::text("done"),
    )
    .with_deltas([
        ModelDelta::Text {
            output_index: 0,
            text: "first".into(),
        },
        ModelDelta::Text {
            output_index: 0,
            text: "second".into(),
        },
    ])]));
    let sink = Arc::new(RecordingSink::failing_on(1, failure()).unwrap());
    let mut ctx = context();
    ctx.deltas = Some(sink.clone());
    assert!(
        RetryingModel::new(
            scripted.clone(),
            zhir_policies::RetryPolicy::new(3)
                .unwrap()
                .backoff(zhir_policies::Backoff::fixed(Duration::ZERO))
        )
        .unwrap()
        .turn(request(), ctx)
        .await
        .is_err()
    );
    assert_eq!(scripted.requests().len(), 1);
    assert_eq!(sink.deltas().len(), 1);
}
#[tokio::test]
async fn request_preparation_can_resolve_resources_without_changing_context() {
    let inner = Arc::new(RecordingModel::new(Arc::new(FunctionModel::new(
        CapabilitySet {
            input_modalities: vec!["text".into(), "file".into()],
            ..zhir_testing::model_capabilities()
        },
        |request, ctx| async move {
            assert_eq!(request.messages, vec![Message::user("resolved document")]);
            assert_eq!(ctx.run.metadata["tag"], "original");
            Ok(TurnOutput::text("done"))
        },
    ))));
    let model = TransformModel::new(inner.clone(), |mut request, ctx| async move {
        ctx.cancellation.check()?;
        tokio::task::yield_now().await;
        request.messages = vec![Message::user("resolved document")];
        Ok(request)
    });
    let mut req = request();
    req.messages = vec![Message::User {
        content: vec![Content::resource(zhir_core::resource::ResourceRef {
            id: "document".into(),
            media_type: "application/octet-stream".into(),
            name: None,
            source: zhir_core::resource::ResourceSource::Stored {
                key: "document".into(),
            },
            metadata: Default::default(),
        })],
    }];
    let mut ctx = context();
    ctx.run.metadata.insert("tag".into(), json!("original"));
    let run_id = ctx.run.run_id.clone();
    model.turn(req, ctx).await.unwrap();
    assert_eq!(inner.records()[0].opening.run.run_id, run_id);
}
#[tokio::test]
async fn function_and_transform_validation_cancel_before_or_after_callbacks() {
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let inner = Arc::new(FunctionModel::new(
        zhir_testing::model_capabilities(),
        move |_, _| {
            called.fetch_add(1, Ordering::SeqCst);
            async { Ok(TurnOutput::text("ok")) }
        },
    ));
    let ctx = context();
    ctx.cancellation.cancel();
    assert!(matches!(
        inner.turn(request(), ctx).await,
        Err(Error::Cancelled)
    ));
    let mut invalid = request();
    invalid.tool_choice = zhir::model::ToolChoice::RuntimeTool {
        name: "missing".into(),
    };
    assert!(matches!(
        inner.turn(invalid, context()).await,
        Err(Error::Invalid(_))
    ));
    for cancel in [false, true] {
        let model = TransformModel::new(inner.clone(), move |mut req, ctx| async move {
            if cancel {
                ctx.cancellation.cancel();
            } else {
                req.tool_choice = zhir::model::ToolChoice::RuntimeTool {
                    name: "missing".into(),
                };
            }
            Ok(req)
        });
        assert!(model.turn(request(), context()).await.is_err());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let model = FunctionModel::new(zhir_testing::model_capabilities(), |_, ctx| async move {
        ctx.cancellation.cancel();
        Ok(TurnOutput::text("late"))
    });
    assert!(matches!(
        model.turn(request(), context()).await,
        Err(Error::Cancelled)
    ));
    let malformed = FunctionModel::new(zhir_testing::model_capabilities(), |_, _| async {
        let mut response = tool_response("tool", json!({}));
        if let Output::RuntimeToolCall { call } = &mut response.output[0] {
            call.id.clear();
        }
        Ok(response)
    });
    assert!(malformed.turn(request(), context()).await.is_err());
}
#[tokio::test]
async fn drive_preserves_observer_failure_and_actual_settlement() {
    let model = Arc::new(FunctionModel::new(
        zhir_testing::model_capabilities(),
        |_, _| async { std::future::pending::<Result<TurnOutput>>().await },
    ));
    let runtime = Runtime::builder(model).build().unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        drive(
            runtime
                .start(zhir::RunRequest::new(vec![Message::user("wait")]))
                .unwrap(),
            |event| async move {
                if matches!(event.data, EventData::CheckpointCommitted { .. }) {
                    Err(Error::Invalid("writer failed".into()))
                } else {
                    Ok(())
                }
            },
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        matches!(error,DriveError::Observer {error:Error::Invalid(ref message),settled:Ok(ref run)} if message=="writer failed" && matches!(run.checkpoint().state,State::Cancelled))
    );
    let runtime = Runtime::builder(Arc::new(ScriptedModel::new([ScriptStep::failure(
        failure(),
    )])))
    .build()
    .unwrap();
    let failed = drive(
        runtime
            .start(zhir::RunRequest::new(vec![Message::user("fail")]))
            .unwrap(),
        |_| async { Ok(()) },
    )
    .await
    .unwrap();
    assert!(matches!(failed.outcome(), zhir::RunOutcome::Failed(_)));
    let runtime = Runtime::builder(Arc::new(ScriptedModel::responses([TurnOutput::text(
        "done",
    )])))
    .build()
    .unwrap();
    let late=drive(runtime.start(zhir::RunRequest::new(vec![Message::user("finish")])).unwrap(),|event|async move {
        if matches!(event.data,EventData::CheckpointCommitted {ref state,..} if *state==zhir_core::run::StateKind::Completed) {Err(Error::Invalid("late observer".into()))} else {Ok(())}
    }).await.unwrap_err();
    assert!(
        matches!(late,DriveError::Observer {settled:Ok(ref c),..} if matches!(c.outcome(),zhir::RunOutcome::Completed(_)))
    );
}
#[tokio::test]
async fn output_decoding_is_strict_and_leaves_checkpoint_intact() {
    let runtime = Runtime::builder(Arc::new(ScriptedModel::responses([TurnOutput::text(
        "{\"total\":7}",
    )])))
    .build()
    .unwrap();
    let original = runtime
        .start(zhir::RunRequest::new(vec![Message::user("json")]))
        .unwrap()
        .result()
        .await
        .unwrap()
        .into_checkpoint();
    let bytes = zhir::wire::encode_checkpoint(&original).unwrap();
    assert_eq!(zhir::output::decode::<Report>(&original).unwrap().total, 7);
    assert_eq!(zhir::wire::encode_checkpoint(&original).unwrap(), bytes);
    for text in [
        "```json\n{\"total\":7}\n```",
        "{\"total\":7} extra",
        "{\"total\":\"7\"}",
        "{\"total\":7,\"extra\":1}",
        "",
    ] {
        let mut c = original.as_ref().clone();
        c.state = State::Completed {
            content: vec![Content::text(text)],
        };
        assert!(zhir::output::decode::<Report>(&c).is_err());
    }
    let mut c = original.as_ref().clone();
    c.state = State::Completed {
        content: vec![Content::text("{\"total\":"), Content::text("8}")],
    };
    assert_eq!(zhir::output::decode::<Report>(&c).unwrap().total, 8);
    c.state = State::Completed {
        content: vec![
            Content::text("{\"total\":8}"),
            Content::Opaque {
                provider: "fixture".into(),
                data: json!({"attachment":1}),
            },
        ],
    };
    assert!(zhir::output::decode::<Report>(&c).is_err());
    c.state = State::Running;
    assert!(zhir::output::decode::<Report>(&c).is_err());
}
#[tokio::test]
async fn ticket_resume_uses_configured_store_and_does_not_retry_stale_heads() {
    let scripted = Arc::new(ScriptedModel::responses([
        tool_response("wait", json!({})),
        TurnOutput::text("resumed"),
    ]));
    let tool = zhir_testing::WaitingTool::new(RuntimeToolSpec {
        name: "wait".into(),
        description: "Wait".into(),
        input: InputSpec::Structured {
            schema: json!({"type":"object"}),
        },
        output_schema: None,
        execution: Default::default(),
    });
    let store = Arc::new(RecordingStore::new(Arc::new(MemoryRunStore::new())));
    let runtime = Runtime::builder(scripted.clone())
        .runtime_tools(Arc::new(
            RuntimeToolRegistry::from_tools([Arc::new(tool) as _]).unwrap(),
        ))
        .store(store.clone())
        .build()
        .unwrap();
    let checkpoint = runtime
        .start(zhir::RunRequest::new(vec![Message::user("wait")]))
        .unwrap()
        .result()
        .await
        .unwrap()
        .into_checkpoint();
    assert!(matches!(checkpoint.state, State::Suspended { .. }));
    let ticket = zhir::SuspensionTicket::from_checkpoint(&checkpoint).unwrap();
    let mut missing = ticket.clone();
    missing.run_id = "missing".into();
    assert!(
        runtime
            .resume(ResumeRequest::from_ticket(missing))
            .await
            .is_err()
    );
    assert!(
        runtime
            .resume(
                ResumeRequest::from_ticket(ticket.clone()).matching(SuspensionSelector {
                    wait_id: Some("wrong".into()),
                    ..Default::default()
                })
            )
            .await
            .is_err()
    );
    let request = || {
        ResumeRequest::from_ticket(ticket.clone())
            .resolve(complete_wait(&checkpoint, json!({})))
            .message(Message::external("answer"))
    };
    let mut first = runtime.resume(request()).await.unwrap();
    let mut stale = runtime.resume(request()).await.unwrap();
    let done = first.result().await.unwrap().into_checkpoint();
    assert!(matches!(done.state, State::Completed { .. }));
    assert!(matches!(stale.result().await,Err(ref e) if matches!(e.error,Error::Conflict {..})));
    assert_eq!(scripted.requests().len(), 2);
    assert!(runtime.resume(request()).await.is_err());
    let no_store = Runtime::builder(scripted).build().unwrap();
    assert!(no_store.resume(request()).await.is_err());
    let count = store.verify_traces().unwrap();
    let duplicate = store.commits()[0].clone();
    store.commit(duplicate).await.unwrap();
    assert_eq!(store.verify_traces().unwrap(), count);
}
#[tokio::test]
async fn recording_model_keeps_cancelled_calls_without_inventing_an_outcome() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let inner = FunctionModel::new(zhir_testing::model_capabilities(), move |_, _| {
        let signal = signal.clone();
        async move {
            signal.notify_one();
            std::future::pending::<Result<TurnOutput>>().await
        }
    });
    let recorded = Arc::new(RecordingModel::new(Arc::new(inner)));
    let model = recorded.clone();
    let job = tokio::spawn(async move { model.turn(request(), context()).await });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    assert_eq!(recorded.records().len(), 1);
    assert!(
        !recorded.records()[0]
            .events
            .iter()
            .any(|e| matches!(e.body, zhir::model::SessionEventBody::TurnFinished { .. }))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_requests_isolate_all_options_and_persist_effective_values_across_96_runs() {
    use zhir::{
        RunRequest,
        model::{GenerationProfile, ProviderToolSpec, ResponseFormat, ToolChoice},
        run::Limits,
    };
    let model = Arc::new(RecordingModel::new(Arc::new(FunctionModel::new(
        CapabilitySet {
            features: [
                zhir_core::model::Capability::ProviderTools,
                zhir_core::model::Capability::JsonMode,
                zhir_core::model::Capability::Streaming,
                zhir_core::model::Capability::Seed,
            ]
            .into(),

            tool_choices: vec!["auto".into(), "provider_tool".into()],
            ..zhir_testing::model_capabilities()
        },
        |_, _| async { Ok(TurnOutput::text("done")) },
    ))));
    let store = Arc::new(RecordingStore::new(Arc::new(MemoryRunStore::new())));
    let defaults = zhir::kernel::defaults::run_options()
        .stream(true)
        .profile(zhir_core::profile::RequestProfile {
            generation: GenerationProfile {
                temperature: Some(0.2),
                ..Default::default()
            },
            ..Default::default()
        })
        .response_format(ResponseFormat::Json);
    let runtime = Runtime::builder(model.clone())
        .store(store.clone())
        .defaults(|_| defaults.clone())
        .build()
        .unwrap();
    join_all((0..96).map(|index| {
        let runtime = &runtime;
        let defaults = &defaults;
        async move {
            let mut context = zhir::kernel::defaults::context();
            context.metadata.insert("index".into(), json!(index));
            let request = RunRequest::new([Message::user("run")]).context(context);
            let request = match index % 3 {
                0 => request,
                1 => request.stream(false).without_response_format().profile(
                    zhir_core::profile::RequestProfile {
                        generation: GenerationProfile {
                            seed: Some(index),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                ),
                _ => request.run_options(
                    zhir::kernel::defaults::run_options()
                        .limits(Limits {
                            max_model_turns: 2,
                            ..zhir::kernel::defaults::limits()
                        })
                        .provider_tools(vec![ProviderToolSpec {
                            provider: "consumer.example".into(),
                            name: format!("p{index}"),
                            options: json!({"index":index}),
                        }])
                        .tool_choice(ToolChoice::ProviderTool {
                            provider: "consumer.example".into(),
                            name: format!("p{index}"),
                        }),
                ),
            };
            let c = runtime
                .start(request)
                .unwrap()
                .result()
                .await
                .unwrap()
                .into_checkpoint();
            assert!(matches!(c.state, State::Completed { .. }), "{:?}", c.state);
            match index % 3 {
                0 => assert_eq!(&c.options, defaults),
                1 => {
                    assert!(!c.options.stream);
                    assert!(c.options.response_format.is_none());
                    assert_eq!(c.options.profile.generation.seed, Some(index));
                    assert!(c.options.profile.extensions.is_empty());
                }
                _ => {
                    assert_eq!(c.options.provider_tools[0].options["index"], index);
                    assert_eq!(c.options.limits.max_model_turns, 2);
                    assert!(c.options.response_format.is_none());
                }
            }
            let restored =
                zhir::wire::decode_checkpoint(&zhir::wire::encode_checkpoint(&c).unwrap()).unwrap();
            assert_eq!(restored.options, c.options);
            let mut wire =
                serde_json::from_slice::<Value>(&zhir::wire::encode_checkpoint(&c).unwrap())
                    .unwrap();
            wire["checkpoint"]
                .as_object_mut()
                .unwrap()
                .remove("options");
            assert!(zhir::wire::decode_checkpoint(&serde_json::to_vec(&wire).unwrap()).is_err());
        }
    }))
    .await;
    let records = model.records();
    assert_eq!(records.len(), 96);
    for record in records {
        let index = record.opening.run.metadata["index"].as_i64().unwrap();
        let head = store
            .load_head(&record.opening.run.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.opening.request.profile.generation,
            head.options.profile.generation
        );
        assert_eq!(
            record.opening.request.provider_tools,
            head.options.provider_tools
        );
        assert_eq!(record.opening.request.stream, index % 3 == 0);
    }
    assert_eq!(store.verify_traces().unwrap(), store.commits().len());
    let commits = store.commits();
    let first = &commits[0];
    let mut next = commits
        .iter()
        .find(|c| c.checkpoint.parent_id.as_ref() == Some(&first.checkpoint.id))
        .unwrap()
        .clone();
    Arc::make_mut(&mut next.checkpoint).options.stream = !first.checkpoint.options.stream;
    assert!(
        zhir_testing::verify_trace(&[first.checkpoint.clone(), next.checkpoint.clone()]).is_err()
    );
    assert!(
        matches!(next.validate_against(Some(&first.core())),Err(Error::Storage(e)) if e.contains("options changed"))
    );
}

#[tokio::test]
async fn frozen_options_survive_continue_and_ticket_resume_with_different_runtime_defaults() {
    use zhir::{SuspensionTicket, model::GenerationProfile, runtime_tools::ToolReply};
    let mut capabilities = zhir_testing::model_capabilities();
    capabilities.features.insert(zhir::model::Capability::Seed);
    let scripted = Arc::new(
        ScriptedModel::responses([
            tool_response("wait", json!({"value":1})),
            TurnOutput::text("done"),
        ])
        .with_capabilities(capabilities),
    );
    let schema = TypedTool::<Args, Report>::new(
        "wait",
        "confirm",
        Execution::default(),
        |args, _| async move { Ok(ToolReply::success(Report { total: args.value })) },
    )
    .unwrap();
    let tool = Arc::new(zhir_testing::WaitingTool::new(schema.spec().clone()));
    let store = Arc::new(RecordingStore::new(Arc::new(MemoryRunStore::new())));
    let mut seed = zhir_testing::checkpoint(vec![Message::user("wait")]);
    seed.options.profile.generation.seed = Some(42);
    seed.options.stream = true;
    seed.active.session.profile = seed.options.profile.clone();
    let seed = Arc::new(seed);
    store
        .commit(zhir::storage::Commit::new(
            seed.clone(),
            zhir::storage::HistoryDelta::Initial(seed.history.entries()),
        ))
        .await
        .unwrap();
    let runtime = Runtime::builder(scripted.clone())
        .store(store.clone())
        .runtime_tools(Arc::new(
            RuntimeToolRegistry::from_tools([tool.clone() as Arc<dyn RuntimeTool>]).unwrap(),
        ))
        .defaults(|run| {
            run.profile(zhir_core::profile::RequestProfile {
                generation: GenerationProfile {
                    seed: Some(99),
                    ..Default::default()
                },
                ..Default::default()
            })
            .stream(false)
        })
        .build()
        .unwrap();
    let checkpoint = runtime
        .continue_from(seed)
        .unwrap()
        .result()
        .await
        .unwrap()
        .into_checkpoint();
    let ticket = SuspensionTicket::from_checkpoint(&checkpoint).unwrap();
    let ticket: SuspensionTicket =
        serde_json::from_value(serde_json::to_value(ticket).unwrap()).unwrap();
    for variant in 0..5 {
        let mut wrong = ticket.clone();
        match variant {
            0 => wrong.revision += 1,
            1 => wrong.checkpoint_id.push('x'),
            2 => wrong.suspension.source.push('x'),
            3 => wrong.suspension.reason.push('x'),
            _ => {
                wrong
                    .suspension
                    .metadata
                    .insert("other".into(), json!(true));
            }
        }
        assert!(
            runtime
                .resume(ResumeRequest::from_ticket(wrong))
                .await
                .is_err()
        );
    }
    let done = runtime
        .resume(
            ResumeRequest::from_ticket(ticket.clone())
                .resolve(complete_wait(&checkpoint, json!({"total":1})))
                .metadata([("answer".into(), json!(7))].into()),
        )
        .await
        .unwrap()
        .result()
        .await
        .unwrap()
        .into_checkpoint();
    assert_eq!(done.options.profile.generation.seed, Some(42));
    assert!(done.options.stream);
    assert_eq!(done.context.metadata["answer"], 7);
    assert!(
        runtime
            .resume(ResumeRequest::from_ticket(ticket))
            .await
            .is_err()
    );
    assert!(runtime.continue_from(done).is_err());
    assert!(
        scripted
            .requests()
            .iter()
            .all(|r| r.request.profile.generation.seed == Some(42) && r.request.stream)
    );
    assert_eq!(tool.starts(), 1);
    store.verify_traces().unwrap();
}

#[tokio::test]
async fn reused_wait_identity_does_not_accept_a_previous_suspension_ticket() {
    let mut second = tool_response("wait", json!({}));
    if let Output::RuntimeToolCall { call } = &mut second.output[0] {
        call.id = "call-2".into();
    }
    let scripted = Arc::new(ScriptedModel::responses([
        tool_response("wait", json!({})),
        second,
        TurnOutput::text("done"),
    ]));
    let tool = zhir_testing::WaitingTool::new(RuntimeToolSpec {
        name: "wait".into(),
        description: "wait".into(),
        input: InputSpec::Structured {
            schema: json!({"type":"object"}),
        },
        output_schema: None,
        execution: Default::default(),
    });
    let runtime = Runtime::builder(scripted.clone())
        .runtime_tools(Arc::new(
            RuntimeToolRegistry::from_tools([Arc::new(tool) as _]).unwrap(),
        ))
        .store(Arc::new(MemoryRunStore::new()))
        .build()
        .unwrap();
    let first = runtime
        .start(zhir::RunRequest::new([Message::user("wait")]))
        .unwrap()
        .result()
        .await
        .unwrap()
        .into_checkpoint();
    let old = zhir::SuspensionTicket::from_checkpoint(&first).unwrap();
    let second = runtime
        .resume(
            ResumeRequest::from_ticket(old.clone())
                .resolve(complete_wait(&first, json!({})))
                .message(Message::external("first answer")),
        )
        .await
        .unwrap()
        .result()
        .await
        .unwrap()
        .into_checkpoint();
    let current = zhir::SuspensionTicket::from_checkpoint(&second).unwrap();
    assert_eq!(old.suspension, current.suspension);
    assert!(current.revision > old.revision);
    assert!(
        runtime
            .resume(ResumeRequest::from_ticket(old))
            .await
            .is_err()
    );
    assert_eq!(scripted.requests().len(), 2);
    assert!(matches!(
        runtime
            .resume(
                ResumeRequest::from_ticket(current)
                    .resolve(complete_wait(&second, json!({})))
                    .message(Message::external("second answer"))
            )
            .await
            .unwrap()
            .result()
            .await
            .unwrap()
            .into_checkpoint()
            .state,
        State::Completed { .. }
    ));
}

use zhir_testing::ModelTestExt;

fn complete_wait(
    checkpoint: &zhir::run::Checkpoint,
    structured: Value,
) -> zhir_core::operation::RecoveryResolution {
    zhir_core::operation::RecoveryResolution::Complete {
        operation_id: checkpoint
            .active
            .operations
            .values()
            .find(|op| !op.state.terminal())
            .unwrap()
            .id
            .clone(),
        outcome: zhir::tool::RuntimeToolOutcome::Success {
            content: vec![],
            structured,
        },
    }
}
