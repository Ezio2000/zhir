#![cfg(all(feature = "models", feature = "typed-tools", feature = "memory"))]
//! Consumer-owned scenarios exercising the public abstractions together.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use zhir::{
    BoxFuture, Result, ResumeRequest, RunOutcome, RunRequest, Runtime,
    core::{
        Cancellation,
        artifact::{ArtifactContent, ArtifactRef, ArtifactStore},
    },
    error::{
        ArtifactError, CatalogError, ContextError, Error, Failure, ResumeError, ValidationError,
    },
    message::{Message, Output},
    model::{Capabilities, Model, ModelContext, ModelRequest, ModelResponse},
    models::{FunctionModel, TransformModel, decorators::RetryingModel},
    retry::{Backoff, RetryPolicy},
    run::{ContextKey, Limits, RunContext, State, now_ms},
    runtime_tools::{
        CompositeRuntimeTools, FunctionApprovalPolicy, RuntimeToolRegistry, ToolReply, TypedTool,
        decorators::RetryingTool,
    },
    stores::{MemoryArtifactStore, MemoryRunStore},
    tool::{
        ApprovalDecision, ApprovalPolicy, ApprovalRequest, CatalogContext, Execution, RuntimeTool,
        RuntimeToolCall, RuntimeToolCatalog, RuntimeToolCatalogProvider, RuntimeToolContext,
        RuntimeToolInput, RuntimeToolSelection,
    },
};
use zhir_testing::{ModelCase, RecordingStore, ScriptStep, ScriptedModel};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    index: u64,
}
const JOB: ContextKey<Job> = ContextKey::new("consumer.job");
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Args {
    n: u64,
}
fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![Message::user("run")],
        runtime_tools: vec![],
        provider_tools: vec![],
        options: Default::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream: false,
    }
}
fn context() -> ModelContext {
    ModelContext {
        run: RunContext::default(),
        cancellation: Cancellation::default(),
        deltas: None,
    }
}
fn call(name: &str, n: u64) -> RuntimeToolCall {
    RuntimeToolCall {
        id: "call-1".into(),
        name: name.into(),
        input: RuntimeToolInput::Structured(json!({"n": n})),
    }
}
fn tool_response(name: &str, n: u64) -> ModelResponse {
    let mut r = ModelResponse::text("");
    r.output = vec![Output::RuntimeToolCall {
        call: call(name, n),
    }];
    r
}
fn tool(name: &str) -> Arc<dyn RuntimeTool> {
    Arc::new(
        TypedTool::<Args, u64>::new(
            name,
            "echo",
            Execution {
                read_only: true,
                idempotent: true,
                parallel: true,
            },
            |args, _| async move { Ok(ToolReply::success(args.n)) },
        )
        .unwrap(),
    )
}
fn temporary() -> Error {
    Error::Model(Failure {
        code: "busy".into(),
        message: "consumer fixture".into(),
        retryable: true,
    })
}
struct ScopedCatalog {
    registry: Arc<RuntimeToolRegistry>,
    opened: Arc<Mutex<Vec<(String, u64)>>>,
}
impl RuntimeToolCatalogProvider for ScopedCatalog {
    fn open_catalog(
        &self,
        context: CatalogContext,
    ) -> BoxFuture<'_, Result<Arc<dyn RuntimeToolCatalog>>> {
        Box::pin(async move {
            let job = context.run.require(JOB)?;
            context.cancellation.check()?;
            self.opened
                .lock()
                .unwrap()
                .push((context.run.run_id.clone(), job.index));
            self.registry.open_catalog(context).await
        })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_composite_tools_typed_context_and_matching_scripts_across_64_runs() {
    let opened = Arc::new(Mutex::new(vec![]));
    let sources: Vec<Arc<dyn RuntimeToolCatalogProvider>> = ["even", "odd"]
        .into_iter()
        .map(|name| {
            Arc::new(ScopedCatalog {
                registry: Arc::new(RuntimeToolRegistry::from_tools([tool(name)]).unwrap()),
                opened: opened.clone(),
            }) as Arc<dyn RuntimeToolCatalogProvider>
        })
        .collect();
    let script = Arc::new(
        ScriptedModel::matching((0..64).map(|index| {
            let name = if index % 2 == 0 { "even" } else { "odd" };
            ModelCase::new(format!("job-{index}"))
                .when(move |input| {
                    let job = input.run.require(JOB).unwrap();
                    if job.index != index {
                        return false;
                    }
                    assert_eq!(input.request.runtime_tools.len(), 1);
                    assert_eq!(input.request.runtime_tools[0].name, name);
                    true
                })
                .steps([
                    ScriptStep::response(tool_response(name, index)),
                    ScriptStep::response(ModelResponse::text(index.to_string())),
                ])
        }))
        .unwrap(),
    );
    let mapped = Arc::new(AtomicUsize::new(0));
    let count = mapped.clone();
    let model = TransformModel::response(script.clone(), move |mut response, context| {
        count.fetch_add(1, Ordering::SeqCst);
        async move {
            response.provider_data = json!({"job":context.run.require(JOB)?.index});
            Ok(response)
        }
    });
    let approvals = Arc::new(AtomicUsize::new(0));
    let approved = approvals.clone();
    let policy = FunctionApprovalPolicy::per_call(move |request, context| {
        approved.fetch_add(1, Ordering::SeqCst);
        async move {
            let job = context.require(JOB)?;
            assert_eq!(
                request.call.input,
                RuntimeToolInput::Structured(json!({"n":job.index}))
            );
            Ok(ApprovalDecision::Allow)
        }
    });
    let store = Arc::new(RecordingStore::new(Arc::new(MemoryRunStore::new())));
    let runtime = Runtime::builder(Arc::new(model))
        .runtime_tools(Arc::new(CompositeRuntimeTools::new(sources)))
        .approval(Arc::new(policy))
        .store(store.clone())
        .build()
        .unwrap();
    let mut tasks = vec![];
    for index in 0..64 {
        let runtime = runtime.clone();
        tasks.push(tokio::spawn(async move {
            let selection =
                RuntimeToolSelection::only([if index % 2 == 0 { "even" } else { "odd" }]);
            let req = RunRequest::new([Message::user("run")])
                .context_value(JOB, Job { index })
                .unwrap()
                .runtime_tools(selection.clone());
            let result = runtime.start(req).unwrap().result().await.unwrap();
            let RunOutcome::Completed(content) = result.outcome() else {
                panic!("expected completion")
            };
            assert_eq!(content[0].as_text(), Some(index.to_string().as_str()));
            let cp = result.checkpoint();
            assert_eq!(cp.options.runtime_tools, selection);
            let restored =
                zhir::wire::decode_checkpoint(&zhir::wire::encode_checkpoint(cp).unwrap()).unwrap();
            assert_eq!(restored.context.require(JOB).unwrap(), Job { index });
            assert_eq!(restored.options.runtime_tools, selection);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    script.verify().unwrap();
    assert_eq!(script.requests().len(), 128);
    assert_eq!(opened.lock().unwrap().len(), 128);
    assert_eq!(mapped.load(Ordering::SeqCst), 128);
    assert_eq!(approvals.load(Ordering::SeqCst), 64);
    assert_eq!(store.verify_traces().unwrap(), 256);
}

#[tokio::test]
async fn catalog_snapshots_duplicates_missing_selection_and_schema_paths() {
    let a = Arc::new(RuntimeToolRegistry::from_tools([tool("a")]).unwrap());
    let b = Arc::new(RuntimeToolRegistry::from_tools([tool("b")]).unwrap());
    let sources: Vec<Arc<dyn RuntimeToolCatalogProvider>> = vec![a.clone(), b.clone()];
    let combined = CompositeRuntimeTools::new(sources);
    let first = combined.open_catalog(Default::default()).await.unwrap();
    b.register(tool("c")).unwrap();
    assert_eq!(first.specs().len(), 2);
    assert_eq!(
        combined
            .open_catalog(Default::default())
            .await
            .unwrap()
            .specs()
            .len(),
        3
    );
    let selected = RuntimeToolSelection::only(["a"])
        .select(first.clone())
        .unwrap();
    assert!(matches!(
        selected.bind(&call("b", 0)),
        Err(Error::Catalog(CatalogError::NotSelected { .. }))
    ));
    let bad = RuntimeToolCall {
        input: RuntimeToolInput::Structured(json!({"n":"wrong"})),
        ..call("a", 0)
    };
    assert!(
        matches!(selected.bind(&bad), Err(Error::Validation(ValidationError::Value { path, .. })) if path == "/n")
    );
    assert!(matches!(
        RuntimeToolSelection::only(["missing"]).select(first.clone()),
        Err(Error::Catalog(CatalogError::NotFound { .. }))
    ));
    assert!(matches!(
        RuntimeToolSelection::only(["a", "a"]).validate(),
        Err(Error::Catalog(CatalogError::Duplicate { .. }))
    ));
    assert_eq!(
        RuntimeToolSelection::None
            .select(first)
            .unwrap()
            .specs()
            .len(),
        0
    );
    b.register(tool("a")).unwrap();
    assert!(
        matches!(combined.open_catalog(Default::default()).await, Err(Error::Catalog(CatalogError::Duplicate { name, sources })) if name == "a" && sources == ["0", "1"])
    );
    let cancelled = CatalogContext::default();
    cancelled.cancellation.cancel();
    assert!(matches!(
        combined.open_catalog(cancelled).await,
        Err(Error::Cancelled)
    ));
}

#[tokio::test]
async fn selection_and_context_survive_ticket_resume_with_different_defaults() {
    let wait = Arc::new(
        TypedTool::<Args, u64>::new("wait", "wait", Execution::default(), |args, _| async move {
            Ok(ToolReply::waiting("ticket", args.n, "consumer"))
        })
        .unwrap(),
    ) as Arc<dyn RuntimeTool>;
    let registry = Arc::new(RuntimeToolRegistry::from_tools([wait, tool("extra")]).unwrap());
    let store = Arc::new(MemoryRunStore::new());
    let script = Arc::new(ScriptedModel::responses([
        tool_response("wait", 1),
        ModelResponse::text("done"),
    ]));
    let runtime = Runtime::builder(script.clone())
        .runtime_tools(registry.clone())
        .store(store.clone())
        .build()
        .unwrap();
    let result = runtime
        .start(
            RunRequest::new([Message::user("run")])
                .context_value(JOB, Job { index: 1 })
                .unwrap()
                .runtime_tools(RuntimeToolSelection::only(["wait"])),
        )
        .unwrap()
        .result()
        .await
        .unwrap();
    let RunOutcome::Suspended(ticket) = result.outcome() else {
        panic!("expected pause")
    };
    let fresh = Runtime::builder(script.clone())
        .runtime_tools(registry)
        .store(store)
        .defaults(|o| o.runtime_tools(RuntimeToolSelection::None))
        .build()
        .unwrap();
    let resumed = fresh
        .resume(
            ResumeRequest::from_ticket(ticket.clone())
                .context_value(JOB, Job { index: 2 })
                .unwrap(),
        )
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    assert!(matches!(resumed.outcome(), RunOutcome::Completed(_)));
    assert_eq!(
        resumed.checkpoint().options.runtime_tools,
        RuntimeToolSelection::only(["wait"])
    );
    assert_eq!(resumed.checkpoint().context.require(JOB).unwrap().index, 2);
    assert_eq!(script.requests()[1].request.runtime_tools[0].name, "wait");
    assert!(matches!(
        fresh
            .resume(ResumeRequest::from_ticket(ticket.clone()))
            .await,
        Err(Error::Resume(ResumeError::StaleTicket { .. }))
    ));
    let no_store = Runtime::builder(script.clone()).build().unwrap();
    assert!(matches!(
        no_store.resume(ResumeRequest::from_ticket(ticket)).await,
        Err(Error::Resume(ResumeError::StoreRequired))
    ));
    script.verify().unwrap();
    let failed = Runtime::builder(Arc::new(ScriptedModel::new([ScriptStep::failure(
        temporary(),
    )])))
    .build()
    .unwrap()
    .start(RunRequest::new([Message::user("run")]))
    .unwrap()
    .result()
    .await
    .unwrap();
    assert!(matches!(failed.outcome(),RunOutcome::Failed(f) if f.code=="busy"));
    let limited = no_store
        .start(RunRequest::new([Message::user("run")]).limits(Limits {
            max_planning_steps: 0,
            ..Default::default()
        }))
        .unwrap()
        .result()
        .await
        .unwrap();
    assert!(matches!(limited.outcome(), RunOutcome::Limited(_)));
    let mut active = resumed.checkpoint().as_ref().clone();
    active.state = State::Planning {
        provider_turn_pending: false,
    };
    assert!(zhir::RunCompletion::new(Arc::new(active)).is_err());
}

#[test]
fn typed_context_errors_distinguish_missing_null_and_mismatched_types() {
    let mut run = RunContext::default();
    assert!(
        matches!(run.require(JOB), Err(Error::Context(ContextError::Missing { key })) if key==JOB.name())
    );
    assert!(run.get(JOB).unwrap().is_none());
    run.metadata
        .insert(JOB.name().into(), json!({"index":"bad"}));
    assert!(matches!(
        run.require(JOB),
        Err(Error::Context(ContextError::Decode { .. }))
    ));
    const OPTIONAL: ContextKey<Option<u64>> = ContextKey::new("optional");
    run.insert(OPTIONAL, None).unwrap();
    assert_eq!(run.get(OPTIONAL).unwrap(), Some(None));
    assert!(matches!(
        run.insert(ContextKey::new(""), 1),
        Err(Error::Context(ContextError::EmptyKey))
    ));
}

#[tokio::test]
async fn function_approval_preserves_order_and_batch_errors() {
    let requests: Vec<_> = [1, 2, 3]
        .into_iter()
        .map(|n| ApprovalRequest {
            call: call("a", n),
            spec: tool("a").spec().clone(),
        })
        .collect();
    let policy = FunctionApprovalPolicy::per_call(|req, _| async move {
        Ok(ApprovalDecision::Deny(format!("{:?}", req.call.input)))
    });
    let decisions = policy
        .decide(requests.clone(), Default::default())
        .await
        .unwrap();
    for (n, d) in [1, 2, 3].into_iter().zip(decisions) {
        assert!(matches!(d,ApprovalDecision::Deny(s) if s.contains(&n.to_string())));
    }
    let short = FunctionApprovalPolicy::batch(|_, _| async { Ok(vec![]) });
    assert!(matches!(
        short.decide(requests.clone(), Default::default()).await,
        Err(Error::Protocol(_))
    ));
    let failing = FunctionApprovalPolicy::per_call(|_, _| async { Err(Error::Cancelled) });
    assert!(matches!(
        failing.decide(requests, Default::default()).await,
        Err(Error::Cancelled)
    ));
}

#[tokio::test]
async fn response_maps_run_in_order_and_do_not_recover_inner_failures() {
    let script = Arc::new(ScriptedModel::responses([ModelResponse::text("ok")]));
    let model = TransformModel::response(script, |mut r, _| async move {
        r.provider_data = json!([1]);
        Ok(r)
    })
    .map_response(|mut r, _| async move {
        r.provider_data.as_array_mut().unwrap().push(json!(2));
        Ok(r)
    });
    assert_eq!(
        model
            .invoke(request(), context())
            .await
            .unwrap()
            .provider_data,
        json!([1, 2])
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let model = TransformModel::response(
        Arc::new(ScriptedModel::new([ScriptStep::failure(temporary())])),
        move |r, _| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok(r) }
        },
    );
    assert!(model.invoke(request(), context()).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let model = TransformModel::response(
        Arc::new(ScriptedModel::responses([ModelResponse::text("ok")])),
        |_, _| async { Err(Error::Protocol("consumer map failed".into())) },
    );
    assert!(
        matches!(model.invoke(request(),context()).await,Err(Error::Protocol(s)) if s=="consumer map failed")
    );
}

#[tokio::test]
async fn model_and_tool_backoff_are_cancelled_and_deadline_bounded() {
    let policy = RetryPolicy::new(4)
        .unwrap()
        .backoff(Backoff::exponential(Duration::from_millis(1), Duration::from_millis(3)).unwrap());
    assert_eq!(
        (1..4)
            .map(|n| policy.delay_after(n).unwrap().as_millis())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(policy.delay_after(0).is_none());
    assert!(policy.delay_after(4).is_none());
    assert!(RetryPolicy::new(0).is_err());
    let long = RetryPolicy::new(40)
        .unwrap()
        .backoff(Backoff::exponential(Duration::from_nanos(1), Duration::from_secs(100)).unwrap());
    assert_eq!(long.delay_after(34), Some(Duration::from_nanos(1u64 << 33)));
    for deadline in [false, true] {
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let signal = entered.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let model = FunctionModel::new(Capabilities::default(), move |_, _| {
            counted.fetch_add(1, Ordering::SeqCst);
            signal.add_permits(1);
            async { Err(temporary()) }
        });
        let model = RetryingModel::new(
            Arc::new(model),
            RetryPolicy::new(4)
                .unwrap()
                .backoff(Backoff::fixed(Duration::from_secs(5))),
        )
        .unwrap();
        let mut ctx = context();
        if deadline {
            ctx.run.deadline_at_ms = Some(now_ms() + 40);
        }
        let cancel = ctx.cancellation.clone();
        let task = tokio::spawn(async move { model.invoke(request(), ctx).await });
        entered.acquire().await.unwrap().forget();
        if !deadline {
            cancel.cancel();
        }
        let result = tokio::time::timeout(Duration::from_millis(500), task)
            .await
            .unwrap()
            .unwrap();
        if deadline {
            assert!(matches!(result, Err(Error::Deadline)));
        } else {
            assert!(matches!(result, Err(Error::Cancelled)));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let signal = entered.clone();
    let flaky = TypedTool::<Args, u64>::new(
        "flaky",
        "flaky",
        Execution {
            idempotent: true,
            ..Default::default()
        },
        move |_, _| {
            signal.add_permits(1);
            async {
                let Error::Model(f) = temporary() else {
                    unreachable!()
                };
                Err(Error::RuntimeTool(f))
            }
        },
    )
    .unwrap();
    let tool = RetryingTool::new(
        Arc::new(flaky),
        RetryPolicy::new(3)
            .unwrap()
            .backoff(Backoff::fixed(Duration::from_secs(5))),
    )
    .unwrap();
    let cancellation = Cancellation::default();
    let cancel = cancellation.clone();
    let task = tokio::spawn(async move {
        tool.invoke(
            call("flaky", 0),
            RuntimeToolContext {
                run: Default::default(),
                cancellation,
                progress: None,
            },
        )
        .await
    });
    entered.acquire().await.unwrap().forget();
    cancel.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(500), task)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::Cancelled)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matching_retry_cases_have_independent_attempts_and_strict_verification() {
    let script = Arc::new(
        ScriptedModel::matching((0..32).map(|n| {
            ModelCase::new(format!("case-{n}"))
                .when(move |i| i.run.run_id == format!("run-{n}"))
                .steps([
                    ScriptStep::failure(temporary()),
                    ScriptStep::response(ModelResponse::text(n.to_string())),
                ])
        }))
        .unwrap(),
    );
    let model = Arc::new(RetryingModel::new(script.clone(), RetryPolicy::new(2).unwrap()).unwrap());
    let mut tasks = vec![];
    for n in 0..32 {
        let model = model.clone();
        tasks.push(tokio::spawn(async move {
            let mut ctx = context();
            ctx.run.run_id = format!("run-{n}");
            model.invoke(request(), ctx).await.unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    script.verify().unwrap();
    assert_eq!(script.requests().len(), 64);
    assert!(script.invoke(request(), context()).await.is_err());
    assert!(script.verify().is_err());
    let ambiguous = ScriptedModel::matching(["a", "b"].into_iter().map(|n| {
        ModelCase::new(n)
            .when(|_| true)
            .steps([ScriptStep::response(ModelResponse::text("ok"))])
    }))
    .unwrap();
    assert!(ambiguous.invoke(request(), context()).await.is_err());
    assert_eq!(ambiguous.remaining(), 2);
    assert!(ambiguous.verify().is_err());
    let extra = ScriptedModel::matching([ModelCase::new("one")
        .when(|_| true)
        .steps([ScriptStep::response(ModelResponse::text("ok"))])])
    .unwrap();
    extra.invoke(request(), context()).await.unwrap();
    extra.verify().unwrap();
    assert!(extra.invoke(request(), context()).await.is_err());
    assert!(extra.verify().is_err());
}

async fn check_artifacts(store: Arc<dyn ArtifactStore>) -> ArtifactRef {
    let content = ArtifactContent {
        mime_type: "text/plain".into(),
        base64: "aGVsbG8=".into(),
    };
    let mut tasks = vec![];
    for _ in 0..32 {
        let store = store.clone();
        let content = content.clone();
        tasks.push(tokio::spawn(async move {
            store.put("consumer/key".into(), content).await.unwrap()
        }));
    }
    let mut refs = vec![];
    for task in tasks {
        refs.push(task.await.unwrap());
    }
    assert!(refs.iter().all(|r| r == &refs[0]));
    assert_eq!(store.get(refs[0].clone()).await.unwrap(), content);
    assert!(matches!(
        store
            .put(
                "consumer/key".into(),
                ArtifactContent {
                    base64: "different".into(),
                    ..content
                }
            )
            .await,
        Err(Error::Artifact(ArtifactError::Conflict { .. }))
    ));
    assert!(matches!(
        store
            .get(ArtifactRef {
                mime_type: "other".into(),
                ..refs[0].clone()
            })
            .await,
        Err(Error::Artifact(ArtifactError::Invalid { .. }))
    ));
    refs.remove(0)
}
#[tokio::test]
async fn memory_artifacts_are_immutable_and_report_missing_references() {
    let store = Arc::new(MemoryArtifactStore::new());
    check_artifacts(store.clone()).await;
    assert!(matches!(
        store
            .get(ArtifactRef {
                id: "missing".into(),
                mime_type: "text/plain".into()
            })
            .await,
        Err(Error::Artifact(ArtifactError::NotFound { .. }))
    ));
}
#[cfg(feature = "artifacts-filesystem")]
#[tokio::test]
async fn filesystem_artifacts_survive_reopen_and_reject_corruption() {
    use zhir::stores::FilesystemArtifactStore;
    let dir = tempfile::tempdir().unwrap();
    let reference = check_artifacts(Arc::new(
        FilesystemArtifactStore::open(dir.path()).await.unwrap(),
    ))
    .await;
    let fresh = FilesystemArtifactStore::open(dir.path()).await.unwrap();
    assert_eq!(
        fresh.get(reference.clone()).await.unwrap().base64,
        "aGVsbG8="
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    let path = dir.path().join(format!("{}.json", reference.id));
    std::fs::write(path, b"incomplete").unwrap();
    assert!(matches!(
        fresh.get(reference).await,
        Err(Error::Artifact(ArtifactError::Invalid { .. }))
    ));
    let blocked = dir.path().join("not-a-directory");
    std::fs::write(&blocked, b"x").unwrap();
    assert!(matches!(
        FilesystemArtifactStore::open(blocked).await,
        Err(Error::Artifact(ArtifactError::Io { .. }))
    ));
}

#[cfg(feature = "artifacts-filesystem")]
#[tokio::test]
async fn filesystem_artifact_model_commits_references_and_rehydrates_history() {
    use zhir::{
        message::{Content, MediaSource},
        models::ArtifactModel,
        stores::FilesystemArtifactStore,
    };
    let dir = tempfile::tempdir().unwrap();
    let inline = Content::File {
        name: Some("hello.txt".into()),
        source: MediaSource::Inline {
            mime_type: "text/plain".into(),
            base64: "aGVsbG8=".into(),
        },
    };
    let mut response = ModelResponse::text("");
    response.output = vec![Output::Content {
        content: inline.clone(),
    }];
    let capabilities = Capabilities {
        input_modalities: vec!["text".into(), "file".into()],
        output_modalities: vec!["text".into(), "file".into()],
        ..Default::default()
    };
    let model = ArtifactModel::new(
        Arc::new(ScriptedModel::responses([response]).with_capabilities(capabilities.clone())),
        Arc::new(FilesystemArtifactStore::open(dir.path()).await.unwrap()),
    );
    let run = Runtime::builder(Arc::new(model))
        .store(Arc::new(MemoryRunStore::new()))
        .build()
        .unwrap()
        .start(RunRequest::new([Message::user("file")]))
        .unwrap()
        .result()
        .await
        .unwrap();
    let RunOutcome::Completed(parts) = run.outcome() else {
        panic!("expected completion")
    };
    assert!(matches!(
        &parts[0],
        Content::File {
            source: MediaSource::Artifact { .. },
            ..
        }
    ));
    let persisted = zhir::wire::encode_checkpoint(run.checkpoint()).unwrap();
    drop(run);
    let checkpoint = zhir::wire::decode_checkpoint(&persisted).unwrap();
    let mut history = checkpoint.history.messages();
    history.push(Message::user("read this file"));
    let inner = FunctionModel::new(capabilities, move |request, _| {
        let inline = inline.clone();
        async move {
            assert!(request.messages.iter().any(|message| matches!(message, Message::Assistant { output, .. } if output.iter().any(|o| matches!(o, Output::Content { content } if content == &inline)))));
            Ok(ModelResponse::text("hello"))
        }
    });
    let model = ArtifactModel::new(
        Arc::new(inner),
        Arc::new(FilesystemArtifactStore::open(dir.path()).await.unwrap()),
    );
    let run = Runtime::builder(Arc::new(model))
        .build()
        .unwrap()
        .start(RunRequest::new(history))
        .unwrap()
        .result()
        .await
        .unwrap();
    assert!(
        matches!(run.outcome(),RunOutcome::Completed(parts) if parts[0].as_text()==Some("hello"))
    );
}
