use serde_json::json;
use std::sync::Arc;
use zhir_core::{
    Cancellation,
    run::RunContext,
    tool::{
        Execution, InputSpec, RuntimeTool, RuntimeToolCall, RuntimeToolCatalogProvider,
        RuntimeToolContext, RuntimeToolInput, RuntimeToolSpec,
    },
};
use zhir_tools::{RuntimeToolRegistry, function};
fn spec() -> RuntimeToolSpec {
    RuntimeToolSpec {
        name: "sum".into(),
        description: "add numbers".into(),
        input: InputSpec::Structured {
            schema: json!({"type":"object","properties":{"a":{"type":"integer"}},"required":["a"],"additionalProperties":false}),
        },
        output_schema: Some(json!({"type":"integer"})),
        execution: Execution::default(),
    }
}
#[derive(serde::Deserialize)]
struct Args {
    a: i64,
}
#[tokio::test]
async fn schema_binding_and_snapshot_are_shared_for_custom_tools() {
    let tool = Arc::new(
        function::structured(spec(), |a: Args, _| async move {
            Ok(zhir_tools::reply::json(json!(a.a + 1)))
        })
        .unwrap(),
    ) as Arc<dyn RuntimeTool>;
    let registry = RuntimeToolRegistry::new();
    registry.register(tool.clone()).unwrap();
    assert!(registry.register(tool).is_err());
    let catalog = registry
        .open_catalog(zhir_core::tool::CatalogContext {
            run: zhir_core::run::RunContext::new("port-test", 0),
            cancellation: Default::default(),
        })
        .await
        .unwrap();
    let call = RuntimeToolCall {
        id: "1".into(),
        name: "sum".into(),
        input: RuntimeToolInput::Structured(json!({"a":2})),
    };
    let result = catalog
        .bind(&call)
        .unwrap()
        .invoke(RuntimeToolContext {
            run: RunContext::new("tool-test", 0),
            cancellation: Cancellation::default(),
            progress: None,
        })
        .await
        .unwrap();
    assert_eq!(result.outcome.structured(), Some(&json!(3)));
    let bad = RuntimeToolCall {
        input: RuntimeToolInput::Structured(json!({"a":"2"})),
        ..call
    };
    assert!(catalog.bind(&bad).is_err());
}
#[tokio::test]
async fn function_output_is_validated() {
    let tool = Arc::new(
        function::structured(spec(), |_: Args, _| async move {
            Ok(zhir_tools::reply::json(json!("wrong")))
        })
        .unwrap(),
    ) as Arc<dyn RuntimeTool>;
    let registry = RuntimeToolRegistry::from_tools([tool]).unwrap();
    let catalog = registry
        .open_catalog(zhir_core::tool::CatalogContext {
            run: zhir_core::run::RunContext::new("port-test", 0),
            cancellation: Default::default(),
        })
        .await
        .unwrap();
    let call = RuntimeToolCall {
        id: "1".into(),
        name: "sum".into(),
        input: RuntimeToolInput::Structured(json!({"a":2})),
    };
    assert!(
        catalog
            .bind(&call)
            .unwrap()
            .invoke(RuntimeToolContext {
                run: RunContext::new("tool-test", 0),
                cancellation: Cancellation::default(),
                progress: None
            })
            .await
            .is_err()
    );
}

fn call() -> RuntimeToolCall {
    RuntimeToolCall {
        id: "decorated-1".into(),
        name: "sum".into(),
        input: RuntimeToolInput::Structured(json!({"a":2})),
    }
}
fn context() -> RuntimeToolContext {
    RuntimeToolContext {
        run: RunContext::new("tool-test", 0),
        cancellation: Cancellation::default(),
        progress: None,
    }
}
fn flaky(idempotent: bool) -> (Arc<dyn RuntimeTool>, Arc<std::sync::atomic::AtomicUsize>) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = attempts.clone();
    let mut spec = spec();
    spec.execution.idempotent = idempotent;
    let tool = function::structured(spec, move |_: Args, _| {
        let attempt = count.fetch_add(1, Ordering::SeqCst);
        async move {
            if attempt == 0 {
                Err(zhir_core::error::Error::RuntimeTool(
                    zhir_core::error::Failure {
                        code: "busy".into(),
                        message: "retry".into(),
                        retryable: true,
                    },
                ))
            } else {
                Ok(zhir_tools::reply::json(json!(3)))
            }
        }
    })
    .unwrap();
    (Arc::new(tool), attempts)
}
#[tokio::test]
async fn retry_requires_idempotence_and_retries_transient_errors() {
    use std::{sync::atomic::Ordering, time::Duration};
    use zhir_tools::decorators::RetryingTool;
    assert!(
        RetryingTool::new(
            flaky(false).0,
            zhir_policies::RetryPolicy::new(2)
                .unwrap()
                .backoff(zhir_policies::Backoff::fixed(Duration::ZERO))
        )
        .is_err()
    );
    let (inner, attempts) = flaky(true);
    let tool = RetryingTool::new(
        inner,
        zhir_policies::RetryPolicy::new(2)
            .unwrap()
            .backoff(zhir_policies::Backoff::fixed(Duration::ZERO)),
    )
    .unwrap();
    assert!(tool.invoke(call(), context()).await.is_ok());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn circuit_blocks_during_cooldown_and_recovers_after_a_successful_probe() {
    use std::{sync::atomic::Ordering, time::Duration};
    use zhir_tools::decorators::CircuitBreakingTool;
    let (inner, attempts) = flaky(true);
    let blocked = CircuitBreakingTool::new(inner, 1, Duration::from_secs(60)).unwrap();
    assert!(blocked.invoke(call(), context()).await.is_err());
    assert!(
        matches!(blocked.invoke(call(), context()).await, Err(zhir_core::error::Error::RuntimeTool(e)) if e.code == "circuit_open")
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    let recovering = CircuitBreakingTool::new(flaky(true).0, 1, Duration::ZERO).unwrap();
    assert!(recovering.invoke(call(), context()).await.is_err());
    assert!(recovering.invoke(call(), context()).await.is_ok());
    assert!(recovering.invoke(call(), context()).await.is_ok());
}
