//! Version 2 contract cases. Scenario drivers remain outside production packages.
mod fixtures;
mod validation;
use serde::Deserialize;
use serde_json::Value;
use std::{path::Path, sync::Arc};
use zhir_core::{
    Result,
    error::Error,
    message::{Content, Message},
    run::State,
};
pub fn load(path: &Path) -> Result<Value> {
    serde_json::from_slice(&std::fs::read(path).map_err(|e| Error::Invalid(e.to_string()))?)
        .map_err(|e| Error::Invalid(e.to_string()))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeCase {
    messages: Vec<Message>,
    steps: Vec<fixtures::Step>,
    #[serde(default)]
    tools: Vec<fixtures::Tool>,
    #[serde(default)]
    max_model_turns: Option<u64>,
    #[serde(default)]
    max_tool_calls: Option<u64>,
    #[serde(default)]
    elapsed_ms: Option<u64>,
    #[serde(default)]
    max_tokens: Option<u64>,
    expected: Expected,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    state: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    tool_outcomes: Option<Vec<String>>,
    #[serde(default)]
    unused_steps: usize,
}
pub async fn run_case(case: &Value) -> Result<()> {
    if case["version"] != 3 {
        return Err(Error::Invalid("case requires version 3".into()));
    }
    match case["kind"].as_str() {
        Some("value") => validation::run(case),
        Some("runtime") => {
            let case: RuntimeCase = serde_json::from_value(case["case"].clone())
                .map_err(|e| Error::Invalid(e.to_string()))?;
            let model = Arc::new(fixtures::CaseModel::new(case.steps));
            let store = Arc::new(zhir_testing::RecordingStore::new(Arc::new(
                zhir_storage::MemoryRunStore::new(),
            )));
            let registry = zhir_tools::RuntimeToolRegistry::from_tools(
                case.tools
                    .into_iter()
                    .map(|tool| Arc::new(tool) as Arc<dyn zhir_core::tool::RuntimeTool>),
            )?;
            let runtime = zhir_kernel::Runtime::builder(model.clone())
                .store(store.clone())
                .runtime_tools(Arc::new(registry))
                .defaults(|mut options| {
                    if let Some(limit) = case.max_model_turns {
                        options.limits.max_model_turns = limit;
                    }
                    if let Some(limit) = case.max_tool_calls {
                        options.limits.max_runtime_tool_calls = limit;
                    }
                    options.limits.elapsed_ms = case.elapsed_ms;
                    options.limits.max_total_tokens = case.max_tokens;
                    options
                })
                .build()?;
            let mut invocation = match runtime.start(zhir_kernel::RunRequest::new(case.messages)) {
                Ok(invocation) => invocation,
                Err(_) if case.expected.state == "rejected" => return Ok(()),
                Err(error) => return Err(error),
            };
            let checkpoint = invocation
                .result()
                .await
                .map_err(|e| e.error)?
                .into_checkpoint();
            require(
                checkpoint.state.kind().as_str() == case.expected.state,
                format!("unexpected state {:?}", checkpoint.state),
            )?;
            if let Some(text) = case.expected.text {
                let State::Completed { content } = &checkpoint.state else {
                    return Err(Error::Protocol("expected completed content".into()));
                };
                require(
                    content
                        .iter()
                        .filter_map(Content::as_text)
                        .collect::<String>()
                        == text,
                    "output text mismatch",
                )?;
            }
            if let Some(expected) = case.expected.tool_outcomes {
                let actual: Vec<_> = checkpoint
                    .history
                    .messages()
                    .iter()
                    .filter_map(|message| {
                        if let Message::RuntimeTool { outcome, .. } = message {
                            Some(outcome.kind().to_string())
                        } else {
                            None
                        }
                    })
                    .collect();
                require(actual == expected, format!("tool outcomes: {actual:?}"))?;
            }
            require(
                model.remaining() == case.expected.unused_steps,
                "unexpected unconsumed model steps",
            )?;
            store.verify_traces()?;
            let decoded = zhir_core::wire::decode_checkpoint(&zhir_core::wire::encode_checkpoint(
                &checkpoint,
            )?)?;
            require(
                decoded.history.digest() == checkpoint.history.digest(),
                "wire history changed",
            )
        }
        _ => Err(Error::Invalid("unknown native case kind".into())),
    }
}
fn require(ok: bool, message: impl Into<String>) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Protocol(message.into()))
    }
}
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn native_contract_cases() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("cases");
        for path in std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
        {
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                let case = super::load(&path).unwrap();
                tokio::time::timeout(std::time::Duration::from_secs(5), super::run_case(&case))
                    .await
                    .unwrap_or_else(|_| panic!("{} timed out", path.display()))
                    .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
            }
        }
    }
}
