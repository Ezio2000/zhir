#![cfg(all(feature = "models", feature = "typed-tools", feature = "memory"))]
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use zhir::{
    BoxFuture, Result, RunRequest, Runtime,
    core::Cancellation,
    error::{ContextError, Error},
    message::{Message, Output},
    model::{CapabilitySet, TurnOutput},
    models::FunctionModel,
    run::{ContextKey, RunContext, State},
    runtime_tools::{RuntimeToolRegistry, ToolReply, TypedTool},
    tool::{
        CatalogContext, Execution, RuntimeTool, RuntimeToolBinding, RuntimeToolCall,
        RuntimeToolCatalog, RuntimeToolCatalogProvider, RuntimeToolInput, RuntimeToolSelection,
        RuntimeToolSpec,
    },
};
use zhir_testing::{ScriptStep, ScriptedModel};

#[derive(serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
struct Args {
    n: u64,
}

fn tool(name: &str, calls: Arc<AtomicUsize>) -> Arc<dyn RuntimeTool> {
    Arc::new(
        TypedTool::<Args, Args>::new(name, "echo", Execution::default(), move |args, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(ToolReply::success(args)) }
        })
        .unwrap(),
    )
}
fn call(name: &str) -> RuntimeToolCall {
    RuntimeToolCall {
        id: "call".into(),
        name: name.into(),
        input: RuntimeToolInput::Structured(json!({"n": 7})),
    }
}
fn response(name: &str) -> TurnOutput {
    let mut response = TurnOutput::text("");
    response.output = vec![Output::RuntimeToolCall { call: call(name) }];
    response
}
struct Source {
    inner: Arc<dyn RuntimeToolCatalog>,
    specs: Vec<RuntimeToolSpec>,
    opens: Arc<AtomicUsize>,
}
impl RuntimeToolCatalog for Source {
    fn specs(&self) -> Vec<RuntimeToolSpec> {
        self.specs.clone()
    }
    fn bind(&self, call: &RuntimeToolCall) -> Result<Arc<dyn RuntimeToolBinding>> {
        self.inner.bind(call)
    }
}
impl RuntimeToolCatalogProvider for Source {
    fn open_catalog(
        &self,
        context: CatalogContext,
    ) -> BoxFuture<'_, Result<Arc<dyn RuntimeToolCatalog>>> {
        Box::pin(async move {
            context.cancellation.check()?;
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(Self {
                inner: self.inner.clone(),
                specs: self.specs.clone(),
                opens: self.opens.clone(),
            }) as Arc<dyn RuntimeToolCatalog>)
        })
    }
}
async fn source() -> (Source, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let registry =
        RuntimeToolRegistry::from_tools([tool("a", calls.clone()), tool("b", calls.clone())])
            .unwrap();
    let inner = registry
        .open_catalog(CatalogContext {
            run: RunContext::new("catalog-fixture", 0),
            cancellation: Cancellation::default(),
        })
        .await
        .unwrap();
    (
        Source {
            specs: inner.specs(),
            inner,
            opens: Arc::new(AtomicUsize::new(0)),
        },
        calls,
    )
}

#[tokio::test]
async fn selection_controls_declaration_and_binding_through_the_kernel() {
    for (selection, names, executed) in [
        (RuntimeToolSelection::All, vec!["a", "b"], true),
        (RuntimeToolSelection::only(["a"]), vec!["a"], false),
        (RuntimeToolSelection::None, vec![], false),
        (
            RuntimeToolSelection::only(Vec::<String>::new()),
            vec![],
            false,
        ),
    ] {
        let (source, calls) = source().await;
        let opens = source.opens.clone();
        let script = Arc::new(ScriptedModel::new([
            ScriptStep::response(response("b")),
            ScriptStep::response(TurnOutput::text("done")),
        ]));
        let runtime = Runtime::builder(script.clone())
            .runtime_tools(Arc::new(source))
            .build()
            .unwrap();
        let result = runtime
            .start(RunRequest::new([Message::user("run")]).runtime_tools(selection))
            .unwrap()
            .result()
            .await
            .unwrap();
        assert!(matches!(result.checkpoint().state, State::Completed { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(executed));
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        for request in script.requests() {
            assert_eq!(
                request
                    .request
                    .runtime_tools
                    .iter()
                    .map(|s| s.name.as_str())
                    .collect::<Vec<_>>(),
                names
            );
        }
        let history = result.checkpoint().history.messages();
        let Some(Message::RuntimeTool { outcome, .. }) = history
            .iter()
            .find(|m| matches!(m, Message::RuntimeTool { .. }))
        else {
            panic!("missing tool outcome")
        };
        if executed {
            assert_eq!(outcome.structured().unwrap()["n"], 7);
        } else {
            assert!(
                matches!(outcome, zhir::tool::RuntimeToolOutcome::Failure { error } if error.code == "invalid_arguments")
            );
        }
        script.verify().unwrap();
    }
}

#[tokio::test]
async fn invalid_catalogs_fail_before_model_execution() {
    for fault in ["missing", "duplicate", "empty"] {
        let (mut source, calls) = source().await;
        match fault {
            "duplicate" => source.specs.push(source.specs[0].clone()),
            "empty" => source.specs[0].name.clear(),
            _ => {}
        }
        let script = Arc::new(ScriptedModel::new([]));
        let runtime = Runtime::builder(script.clone())
            .runtime_tools(Arc::new(source))
            .build()
            .unwrap();
        let selection = if fault == "missing" {
            RuntimeToolSelection::only(["missing"])
        } else {
            RuntimeToolSelection::All
        };
        let result = runtime
            .start(RunRequest::new([Message::user("run")]).runtime_tools(selection))
            .unwrap()
            .result()
            .await
            .unwrap();
        assert!(matches!(result.checkpoint().state, State::Failed { .. }));
        assert!(script.requests().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        script.verify().unwrap();
    }
}

#[tokio::test]
async fn binding_drift_becomes_a_tool_failure_without_invocation() {
    let (mut source, calls) = source().await;
    source
        .specs
        .iter_mut()
        .find(|s| s.name == "a")
        .unwrap()
        .description = "changed".into();
    let script = Arc::new(ScriptedModel::new([
        ScriptStep::response(response("a")),
        ScriptStep::response(TurnOutput::text("recovered")),
    ]));
    let runtime = Runtime::builder(script.clone())
        .runtime_tools(Arc::new(source))
        .build()
        .unwrap();
    let result = runtime
        .start(RunRequest::new([Message::user("run")]))
        .unwrap()
        .result()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(result.checkpoint().state, State::Completed { .. }));
    assert!(
        matches!(result.checkpoint().history.messages().iter().find(|m| matches!(m, Message::RuntimeTool { .. })).unwrap(), Message::RuntimeTool { outcome: zhir::tool::RuntimeToolOutcome::Failure {error}, .. } if error.code == "invalid_arguments" && error.message.contains("binding changed"))
    );
    script.verify().unwrap();
}

#[tokio::test]
async fn runtime_preserves_explicit_context_and_owns_fresh_creation() {
    let first = RunRequest::new([Message::user("first")]);
    let second = RunRequest::new([Message::user("second")]);
    assert_ne!(first.context.run_id, second.context.run_id);
    assert!(first.context.started_at_ms > 0);
    const TENANT: ContextKey<String> = ContextKey::new("tenant");
    let mut supplied = RunContext::new("consumer-run", 1234);
    supplied.parent_run_id = Some("consumer-parent".into());
    supplied.insert(TENANT, "acme".into()).unwrap();
    let runtime = Runtime::builder(Arc::new(ScriptedModel::new([ScriptStep::response(
        TurnOutput::text("done"),
    )])))
    .build()
    .unwrap();
    let result = runtime
        .start(first.context(supplied.clone()))
        .unwrap()
        .result()
        .await
        .unwrap();
    assert_eq!(result.checkpoint().context, supplied);
    let resumed = zhir::ResumeRequest::from_checkpoint(result.into_checkpoint())
        .context_value(TENANT, "next".into())
        .unwrap();
    assert_eq!(resumed.metadata["tenant"], "next");
    assert!(matches!(
        resumed.context_value(ContextKey::<u64>::new(""), 1),
        Err(Error::Context(ContextError::EmptyKey))
    ));
}

#[tokio::test]
async fn capabilities_are_explicit_and_validated_before_model_io() {
    for streaming in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let capabilities = CapabilitySet {
            features: if streaming {
                [zhir_core::model::Capability::Streaming].into()
            } else {
                Default::default()
            },

            input_modalities: vec!["text".into()],
            output_modalities: vec!["text".into()],

            tool_choices: vec!["auto".into()],
            constraints: Default::default(),
            extensions: Default::default(),
        };
        let model = Arc::new(FunctionModel::new(capabilities, move |_, _| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok(TurnOutput::text("done")) }
        }));
        let runtime = Runtime::builder(model).build().unwrap();
        let result = runtime
            .start(RunRequest::new([Message::user("run")]).stream(true))
            .unwrap()
            .result()
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(streaming));
        assert_eq!(
            matches!(result.checkpoint().state, State::Completed { .. }),
            streaming
        );
        if !streaming {
            assert!(
                matches!(&result.checkpoint().state, State::Failed {error} if error.code == "invalid_arguments")
            );
        }
    }
}
