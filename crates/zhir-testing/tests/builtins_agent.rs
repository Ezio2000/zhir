#![cfg(feature = "agent")]
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use zhir_builtins::agent::AgentBackend;
use zhir_core::operation::OperationOutcome;
use zhir_core::{BoxFuture, Result, operation::*, tool::*};
struct Backend(AtomicUsize);
impl AgentBackend for Backend {
    fn start(
        &self,
        prompt: String,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<OperationHandle>> {
        Box::pin(async move {
            assert_eq!(prompt, "work");
            assert_eq!(context.operation_id, "operation");
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(handle())
        })
    }
    fn recover(
        &self,
        _: OperationRecord,
        _: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<OperationHandle>> {
        Box::pin(async { Ok(handle()) })
    }
}
struct Control;
impl OperationControl for Control {
    fn cancel(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn reply(&self, _: serde_json::Value) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
struct Events(bool);
impl OperationEvents for Events {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<OperationEvent>>> {
        Box::pin(async move {
            if self.0 {
                return Ok(None);
            }
            self.0 = true;
            Ok(Some(OperationEvent {
                sequence: 0,
                update: OperationUpdate::Finished {
                    outcome: OperationOutcome::Success {
                        content: vec![],
                        structured: serde_json::json!({"done":true}),
                    },
                },
            }))
        })
    }
}
fn handle() -> OperationHandle {
    OperationHandle {
        recovery: None,
        control: Arc::new(Control),
        events: Box::new(Events(false)),
    }
}
#[tokio::test]
async fn child_tool_uses_operation_contract_and_common_schema_binding() {
    let backend = Arc::new(Backend(AtomicUsize::new(0)));
    let tools = zhir_builtins::agent::tools(backend.clone()).unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].spec().name, "agent_run");
    let registry = zhir_tools::RuntimeToolRegistry::from_tools(tools).unwrap();
    let context = RuntimeToolContext {
        operation_id: "operation".into(),
        run: zhir_core::run::RunContext::new("parent", 0),
        cancellation: Default::default(),
        progress: None,
    };
    let catalog = registry
        .open_catalog(CatalogContext {
            run: context.run.clone(),
            cancellation: Default::default(),
        })
        .await
        .unwrap();
    let call = RuntimeToolCall {
        id: "call".into(),
        name: "agent_run".into(),
        input: RuntimeToolInput::Structured(serde_json::json!({"prompt":"work"})),
    };
    let ToolExecution::Active(mut handle) =
        catalog.bind(&call).unwrap().start(context).await.unwrap()
    else {
        panic!("child must return an operation")
    };
    assert!(matches!(
        handle.events.receive().await.unwrap().unwrap().update,
        OperationUpdate::Finished { .. }
    ));
    assert_eq!(backend.0.load(Ordering::SeqCst), 1);
    let bad = RuntimeToolCall {
        input: RuntimeToolInput::Structured(serde_json::json!({"prompt":"work","key":"legacy"})),
        ..call
    };
    assert!(catalog.bind(&bad).is_err());
}
