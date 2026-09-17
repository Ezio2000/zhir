#![cfg(all(
    feature = "filesystem",
    feature = "shell",
    feature = "interaction",
    feature = "agent-runtime"
))]
use serde_json::{Value, json};
use std::sync::Arc;
use zhir_core::{
    Cancellation, Result,
    message::Message,
    run::RunContext,
    tool::{
        RuntimeTool, RuntimeToolCall, RuntimeToolCatalogProvider, RuntimeToolContext,
        RuntimeToolInput,
    },
};
use zhir_testing::FinalExecution;
use zhir_tools::RuntimeToolRegistry;
async fn start_tool(
    tool: Arc<dyn RuntimeTool>,
    input: Value,
) -> Result<zhir_core::operation::ToolExecution> {
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
        .start(RuntimeToolContext {
            run: RunContext::new("builtin-test", 0),
            operation_id: "operation".into(),
            cancellation: Cancellation::default(),
            progress: None,
        })
        .await
}
#[tokio::test]
async fn files_use_common_validation_digest_and_atomic_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let result = start_tool(
        zhir_builtins::filesystem::write_file(root).unwrap(),
        json!({"path":"hello.txt","content":"one\ntwo\none\n","expected_sha256":null}),
    )
    .await
    .unwrap();
    let digest = result.final_outcome().structured().unwrap()["sha256"].clone();
    let result = start_tool(
        zhir_builtins::filesystem::read_file(root).unwrap(),
        json!({"path":"hello.txt","offset":2,"limit":1}),
    )
    .await
    .unwrap();
    assert_eq!(
        result.final_outcome().structured().unwrap()["content"],
        "two"
    );
    assert!(start_tool(zhir_builtins::filesystem::edit_file(root).unwrap(),json!({"path":"hello.txt","old_string":"one","new_string":"three","expected_sha256":digest})).await.is_err());
    start_tool(zhir_builtins::filesystem::edit_file(root).unwrap(),json!({"path":"hello.txt","old_string":"one","new_string":"three","replace_all":true,"expected_sha256":digest})).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("hello.txt")).unwrap(),
        "three\ntwo\nthree\n"
    );
    assert!(
        start_tool(
            zhir_builtins::filesystem::write_file(root).unwrap(),
            json!({"path":"hello.txt","content":"stale","expected_sha256":digest})
        )
        .await
        .is_err()
    );
    let found = start_tool(
        zhir_builtins::filesystem::grep(root).unwrap(),
        json!({"pattern":"three","output_mode":"count"}),
    )
    .await
    .unwrap();
    assert_eq!(
        found.final_outcome().structured().unwrap()["results"][0]["count"],
        2
    );
    let found = start_tool(
        zhir_builtins::filesystem::glob(root).unwrap(),
        json!({"pattern":"*.txt"}),
    )
    .await
    .unwrap();
    assert_eq!(
        found.final_outcome().structured().unwrap()["matches"],
        json!(["hello.txt"])
    );
    assert!(
        start_tool(
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
    let result = start_tool(
        zhir_builtins::shell::bash(options.clone()).unwrap(),
        json!({"command":"printf hello; printf err >&2"}),
    )
    .await
    .unwrap();
    assert_eq!(
        result.final_outcome().structured().unwrap()["stdout"],
        "hello"
    );
    assert_eq!(
        result.final_outcome().structured().unwrap()["stderr"],
        "err"
    );
    options.timeout = std::time::Duration::from_millis(40);
    assert!(
        start_tool(
            zhir_builtins::shell::bash(options).unwrap(),
            json!({"command":"sleep 10"})
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn questions_produce_a_host_suspension() {
    let result = start_tool(
        zhir_builtins::interaction::ask_question().unwrap(),
        json!({"questions":[{"id":"color","title":"Choose a color","options":["red","blue"]}]}),
    )
    .await
    .unwrap();
    let zhir_core::operation::ToolExecution::Active(mut handle) = result else {
        panic!("question must remain active")
    };
    assert!(matches!(
        handle.events.receive().await.unwrap().unwrap().update,
        zhir_core::operation::OperationUpdate::Waiting { .. }
    ));
    handle.control.reply(json!({"color":"red"})).await.unwrap();
    assert!(matches!(
        handle.events.receive().await.unwrap().unwrap().update,
        zhir_core::operation::OperationUpdate::Finished { .. }
    ));
    assert!(
        start_tool(
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
            start_tool(zhir_builtins::filesystem::grep(root).unwrap(), input)
                .await
                .is_err()
        );
    }
    assert!(
        start_tool(
            zhir_builtins::filesystem::write_file(root).unwrap(),
            json!({"path":"x","content":"x"})
        )
        .await
        .is_err()
    );
    assert!(!root.join("x").exists());
    assert!(
        start_tool(
            zhir_builtins::filesystem::read_file(root).unwrap(),
            json!({"path":"x","limit":2001})
        )
        .await
        .is_err()
    );
    assert!(start_tool(zhir_builtins::interaction::ask_question().unwrap(), json!({"questions":[
        {"id":"1","title":"a"},{"id":"2","title":"a"},{"id":"3","title":"a"},{"id":"4","title":"a"}
    ]})).await.is_err());
    assert!(
        start_tool(
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
    let result = start_tool(
        tool(),
        json!({"pattern":"hit","output_mode":"content","context":1,"limit":1}),
    )
    .await
    .unwrap();
    let result = result.final_outcome().structured().unwrap();
    assert_eq!(result["truncated"], true);
    assert_eq!(
        result["results"],
        json!([{"path":"a.txt","line":2,"text":"hit one","before":["before"],"after":["middle"]}])
    );
    let result = start_tool(
        tool(),
        json!({"pattern":"hit","output_mode":"content","limit":2}),
    )
    .await
    .unwrap();
    assert_eq!(
        result.final_outcome().structured().unwrap()["truncated"],
        false
    );
    let result = start_tool(tool(), json!({"pattern":"hit"})).await.unwrap();
    assert_eq!(
        result.final_outcome().structured().unwrap()["results"],
        json!(["a.txt"])
    );
    let result = start_tool(tool(), json!({"pattern":"hit","output_mode":"count"}))
        .await
        .unwrap();
    assert_eq!(
        result.final_outcome().structured().unwrap()["results"][0]["count"],
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
        start_tool(tool(), json!({"pattern":pattern,"output_mode":"count"}))
            .await
            .is_err()
    );
    let paths = start_tool(tool(), json!({"pattern":pattern}))
        .await
        .unwrap();
    assert_eq!(
        paths.final_outcome().structured().unwrap()["results"],
        json!(["a.txt"])
    );
    let content = start_tool(
        tool(),
        json!({"pattern":pattern,"output_mode":"content","limit":1}),
    )
    .await
    .unwrap();
    assert_eq!(
        content.final_outcome().structured().unwrap()["truncated"],
        true
    );
}
#[tokio::test]
async fn custom_tools_do_not_require_builtins_at_runtime() {
    let model = Arc::new(zhir_testing::ScriptedModel::responses([
        zhir_core::model::GenerationOutput::text("done"),
    ]));
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
            .checkpoint()
            .state
            .terminal()
    );
}
