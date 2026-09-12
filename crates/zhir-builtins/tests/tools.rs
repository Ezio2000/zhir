#![cfg(all(
    feature = "filesystem",
    feature = "shell",
    feature = "interaction",
    feature = "agent"
))]
use serde_json::{Value, json};
use std::sync::Arc;
use zhir_core::{
    BoxFuture, Cancellation, Result,
    message::Message,
    model::{Capabilities, Model, ModelContext, ModelRequest, ModelResponse},
    run::RunContext,
    tool::{
        RuntimeTool, RuntimeToolCall, RuntimeToolCatalogProvider, RuntimeToolContext,
        RuntimeToolInput,
    },
};
use zhir_tools::RuntimeToolRegistry;
async fn invoke(
    tool: Arc<dyn RuntimeTool>,
    input: Value,
) -> Result<zhir_core::tool::RuntimeToolResult> {
    let name = tool.spec().name.clone();
    let registry = RuntimeToolRegistry::from_tools([tool])?;
    registry
        .open_catalog(Default::default())
        .await?
        .bind(&RuntimeToolCall {
            id: "call".into(),
            name,
            input: RuntimeToolInput::Structured(input),
        })?
        .invoke(RuntimeToolContext {
            run: RunContext::default(),
            cancellation: Cancellation::default(),
            progress: None,
        })
        .await
}
#[tokio::test]
async fn files_use_common_validation_digest_and_atomic_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let result = invoke(
        zhir_builtins::filesystem::write_file(root).unwrap(),
        json!({"path":"hello.txt","content":"one\ntwo\none\n","expected_sha256":null}),
    )
    .await
    .unwrap();
    let digest = result.outcome.structured().unwrap()["sha256"].clone();
    let result = invoke(
        zhir_builtins::filesystem::read_file(root).unwrap(),
        json!({"path":"hello.txt","offset":2,"limit":1}),
    )
    .await
    .unwrap();
    assert_eq!(result.outcome.structured().unwrap()["content"], "two");
    assert!(invoke(zhir_builtins::filesystem::edit_file(root).unwrap(),json!({"path":"hello.txt","old_string":"one","new_string":"three","expected_sha256":digest})).await.is_err());
    invoke(zhir_builtins::filesystem::edit_file(root).unwrap(),json!({"path":"hello.txt","old_string":"one","new_string":"three","replace_all":true,"expected_sha256":digest})).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("hello.txt")).unwrap(),
        "three\ntwo\nthree\n"
    );
    assert!(
        invoke(
            zhir_builtins::filesystem::write_file(root).unwrap(),
            json!({"path":"hello.txt","content":"stale","expected_sha256":digest})
        )
        .await
        .is_err()
    );
    let found = invoke(
        zhir_builtins::filesystem::grep(root).unwrap(),
        json!({"pattern":"three","output_mode":"count"}),
    )
    .await
    .unwrap();
    assert_eq!(
        found.outcome.structured().unwrap()["results"][0]["count"],
        2
    );
    let found = invoke(
        zhir_builtins::filesystem::glob(root).unwrap(),
        json!({"pattern":"*.txt"}),
    )
    .await
    .unwrap();
    assert_eq!(
        found.outcome.structured().unwrap()["matches"],
        json!(["hello.txt"])
    );
    assert!(
        invoke(
            zhir_builtins::filesystem::read_file(root).unwrap(),
            json!({"path":"hello.txt","offset":0})
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn shell_drains_both_streams_and_times_out() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = zhir_builtins::shell::ShellOptions::new(dir.path());
    options.max_output_bytes = 16;
    let result = invoke(
        zhir_builtins::shell::bash(options.clone()).unwrap(),
        json!({"command":"printf hello; printf err >&2"}),
    )
    .await
    .unwrap();
    assert_eq!(result.outcome.structured().unwrap()["stdout"], "hello");
    assert_eq!(result.outcome.structured().unwrap()["stderr"], "err");
    options.timeout = std::time::Duration::from_millis(40);
    assert!(
        invoke(
            zhir_builtins::shell::bash(options).unwrap(),
            json!({"command":"sleep 10"})
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn questions_produce_a_host_suspension() {
    let result = invoke(
        zhir_builtins::interaction::ask_question().unwrap(),
        json!({"questions":[{"id":"color","title":"Choose a color","options":["red","blue"]}]}),
    )
    .await
    .unwrap();
    assert_eq!(result.suspension.as_ref().unwrap().source, "ask_question");
    result.validate().unwrap();
    assert!(
        invoke(
            zhir_builtins::interaction::ask_question().unwrap(),
            json!({"questions":[{"id":"x","title":"a"},{"id":"x","title":"b"}]})
        )
        .await
        .is_err()
    );
}
struct Child {
    caps: Capabilities,
    block: bool,
}
impl Model for Child {
    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }
    fn invoke(&self, _: ModelRequest, _: ModelContext) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            if self.block {
                std::future::pending::<()>().await;
            }
            Ok(ModelResponse::text("child done"))
        })
    }
}
#[tokio::test]
async fn child_agents_are_idempotent_and_owned_tasks_cancel() {
    use zhir_builtins::agent::{AgentBackend, InMemoryAgentBackend};
    let runtime = zhir_kernel::Runtime::builder(Arc::new(Child {
        caps: Capabilities::default(),
        block: false,
    }))
    .build()
    .unwrap();
    let backend = InMemoryAgentBackend::new(runtime, "child instructions");
    let owner = RunContext::default();
    let first = backend
        .start_or_get("key".into(), "work".into(), owner.clone())
        .await
        .unwrap();
    let second = backend
        .start_or_get("key".into(), "work".into(), owner.clone())
        .await
        .unwrap();
    assert_eq!(first.id, second.id);
    assert!(
        backend
            .start_or_get("key".into(), "different".into(), owner.clone())
            .await
            .is_err()
    );
    assert!(
        backend
            .get(first.id.clone(), RunContext::default())
            .await
            .is_err()
    );
    assert_eq!(
        backend.wait(first.id, owner.clone()).await.unwrap().status,
        "completed"
    );
    let runtime = zhir_kernel::Runtime::builder(Arc::new(Child {
        caps: Capabilities::default(),
        block: true,
    }))
    .build()
    .unwrap();
    let backend = InMemoryAgentBackend::new(runtime, "");
    let child = backend
        .start_or_get("key".into(), "work".into(), owner.clone())
        .await
        .unwrap();
    assert_eq!(
        backend.cancel(child.id, owner).await.unwrap().status,
        "cancelled"
    );
}
#[tokio::test]
async fn custom_tools_do_not_require_builtins_at_runtime() {
    let model = Arc::new(Child {
        caps: Capabilities::default(),
        block: false,
    });
    let mut invocation = zhir_kernel::Runtime::builder(model)
        .build()
        .unwrap()
        .start(zhir_core::run::RunRequest::new(vec![Message::user(
            "hello",
        )]))
        .unwrap();
    assert!(
        invocation
            .result()
            .await
            .unwrap()
            .into_checkpoint()
            .state
            .terminal()
    );
}
