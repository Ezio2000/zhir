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
        .open_catalog(zhir_core::tool::CatalogContext {
            run: zhir_core::run::RunContext::new("port-test", 0),
            cancellation: Default::default(),
        })
        .await?
        .bind(&RuntimeToolCall {
            id: "call".into(),
            name,
            input: RuntimeToolInput::Structured(input),
        })?
        .invoke(RuntimeToolContext {
            run: RunContext::new("builtin-test", 0),
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

#[tokio::test]
async fn derived_schemas_preserve_required_fields_bounds_and_modes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    for input in [
        json!({"pattern":"x","limit":0}),
        json!({"pattern":"x","limit":10001}),
        json!({"pattern":"x","context":101}),
        json!({"pattern":"x","output_mode":"unknown"}),
    ] {
        assert!(
            invoke(zhir_builtins::filesystem::grep(root).unwrap(), input)
                .await
                .is_err()
        );
    }
    assert!(
        invoke(
            zhir_builtins::filesystem::write_file(root).unwrap(),
            json!({"path":"x","content":"x"})
        )
        .await
        .is_err()
    );
    assert!(!root.join("x").exists());
    assert!(
        invoke(
            zhir_builtins::filesystem::read_file(root).unwrap(),
            json!({"path":"x","limit":2001})
        )
        .await
        .is_err()
    );
    assert!(invoke(zhir_builtins::interaction::ask_question().unwrap(), json!({"questions":[
        {"id":"1","title":"a"},{"id":"2","title":"a"},{"id":"3","title":"a"},{"id":"4","title":"a"}
    ]})).await.is_err());
    assert!(
        invoke(
            zhir_builtins::shell::bash(zhir_builtins::shell::ShellOptions::new(root)).unwrap(),
            json!({"command":""})
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn grep_preserves_context_and_probes_exactly_one_extra_match_for_truncation() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("a.txt"),
        "before\nhit one\nmiddle\nhit two\nafter\n",
    )
    .unwrap();
    let tool = || zhir_builtins::filesystem::grep(dir.path()).unwrap();
    let result = invoke(
        tool(),
        json!({"pattern":"hit","output_mode":"content","context":1,"limit":1}),
    )
    .await
    .unwrap();
    let result = result.outcome.structured().unwrap();
    assert_eq!(result["truncated"], true);
    assert_eq!(
        result["results"],
        json!([{"path":"a.txt","line":2,"text":"hit one","before":["before"],"after":["middle"]}])
    );
    let result = invoke(
        tool(),
        json!({"pattern":"hit","output_mode":"content","limit":2}),
    )
    .await
    .unwrap();
    assert_eq!(result.outcome.structured().unwrap()["truncated"], false);
    let result = invoke(tool(), json!({"pattern":"hit"})).await.unwrap();
    assert_eq!(
        result.outcome.structured().unwrap()["results"],
        json!(["a.txt"])
    );
    let result = invoke(tool(), json!({"pattern":"hit","output_mode":"count"}))
        .await
        .unwrap();
    assert_eq!(
        result.outcome.structured().unwrap()["results"][0]["count"],
        2
    );
}

#[tokio::test]
async fn grep_does_not_evaluate_unneeded_tail_lines() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("a.txt"),
        format!("hit\nhit\n{}!\n", "a".repeat(40)),
    )
    .unwrap();
    let tool = || zhir_builtins::filesystem::grep(dir.path()).unwrap();
    let pattern = "hit|(?:a+)+(?=b)";
    assert!(
        invoke(tool(), json!({"pattern":pattern,"output_mode":"count"}))
            .await
            .is_err()
    );
    let paths = invoke(tool(), json!({"pattern":pattern})).await.unwrap();
    assert_eq!(
        paths.outcome.structured().unwrap()["results"],
        json!(["a.txt"])
    );
    let content = invoke(
        tool(),
        json!({"pattern":pattern,"output_mode":"content","limit":1}),
    )
    .await
    .unwrap();
    assert_eq!(content.outcome.structured().unwrap()["truncated"], true);
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
        caps: test_capabilities(),
        block: false,
    }))
    .build()
    .unwrap();
    let backend = InMemoryAgentBackend::new(runtime, "child instructions");
    let owner = RunContext::new("builtin-test", 0);
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
            .get(first.id.clone(), RunContext::new("different-owner", 0))
            .await
            .is_err()
    );
    assert_eq!(
        backend.wait(first.id, owner.clone()).await.unwrap().status,
        zhir_builtins::agent::AgentStatus::Completed
    );
    let runtime = zhir_kernel::Runtime::builder(Arc::new(Child {
        caps: test_capabilities(),
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
        zhir_builtins::agent::AgentStatus::Cancelled
    );
}
#[tokio::test]
async fn custom_tools_do_not_require_builtins_at_runtime() {
    let model = Arc::new(Child {
        caps: test_capabilities(),
        block: false,
    });
    let mut invocation = zhir_kernel::Runtime::builder(model)
        .build()
        .unwrap()
        .start(zhir_kernel::RunRequest::new(vec![Message::user("hello")]))
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

#[cfg(feature = "agent")]
fn test_capabilities() -> Capabilities {
    Capabilities {
        input_modalities: vec!["text".into()],
        output_modalities: vec!["text".into()],
        structured_runtime_tools: true,
        freeform_runtime_tools: false,
        provider_tools: false,
        parallel_runtime_tools: true,
        parallel_control: true,
        streaming: true,
        usage: true,
        structured_output: false,
        json_mode: false,
        seed: false,
        tool_choices: vec![
            "auto".into(),
            "none".into(),
            "required".into(),
            "runtime_tool".into(),
        ],
    }
}
