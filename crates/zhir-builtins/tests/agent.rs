#![cfg(feature = "agent")]
use std::sync::Arc;
use zhir_builtins::agent::{self, AgentBackend, AgentSnapshot, AgentStatus};
use zhir_core::{
    BoxFuture, Cancellation, Result,
    run::RunContext,
    tool::{
        CatalogContext, RuntimeToolCall, RuntimeToolCatalogProvider, RuntimeToolContext,
        RuntimeToolInput,
    },
};

struct ExternalBackend;
impl AgentBackend for ExternalBackend {
    fn start_or_get(
        &self,
        key: String,
        _: String,
        requester: RunContext,
    ) -> BoxFuture<'_, Result<AgentSnapshot>> {
        self.get(key, requester)
    }
    fn get(&self, id: String, _: RunContext) -> BoxFuture<'_, Result<AgentSnapshot>> {
        Box::pin(async move {
            Ok(AgentSnapshot {
                id,
                status: AgentStatus::Completed,
                content: vec![],
                error: None,
            })
        })
    }
    fn wait(&self, id: String, requester: RunContext) -> BoxFuture<'_, Result<AgentSnapshot>> {
        self.get(id, requester)
    }
    fn cancel(&self, id: String, requester: RunContext) -> BoxFuture<'_, Result<AgentSnapshot>> {
        self.get(id, requester)
    }
}

#[tokio::test]
async fn agent_tools_accept_an_external_backend_without_the_kernel_feature() {
    let registry = zhir_tools::RuntimeToolRegistry::from_tools(
        agent::tools(Arc::new(ExternalBackend)).unwrap(),
    )
    .unwrap();
    let run = RunContext::new("external-host", 0);
    let catalog = registry
        .open_catalog(CatalogContext {
            run: run.clone(),
            cancellation: Cancellation::default(),
        })
        .await
        .unwrap();
    let call = RuntimeToolCall {
        id: "call".into(),
        name: "agent_start".into(),
        input: RuntimeToolInput::Structured(serde_json::json!({"key":"child", "prompt":"work"})),
    };
    let reply = catalog
        .bind(&call)
        .unwrap()
        .invoke(RuntimeToolContext {
            run,
            cancellation: Cancellation::default(),
            progress: None,
        })
        .await
        .unwrap();
    assert_eq!(reply.outcome.structured().unwrap()["id"], "child");
    assert!(reply.suspension.is_none());
}
