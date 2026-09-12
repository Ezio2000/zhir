use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use zhir_core::{
    BoxFuture, Result,
    error::{Error, Failure},
    message::{Content, Message},
    model::{Capabilities, Model, ModelContext, ModelRequest, ModelResponse},
    run::{Checkpoint, History, HistoryReducer, HistoryRewrite, RunContext, Suspension},
    storage::{Commit, RunStore},
    tool::{
        ApprovalDecision, ApprovalPolicy, ApprovalRequest, BatchPolicy, RuntimeTool,
        RuntimeToolBatch, RuntimeToolCall, RuntimeToolContext, RuntimeToolInput,
        RuntimeToolOutcome, RuntimeToolResult, RuntimeToolSpec,
    },
};
use zhir_tools::RuntimeToolRegistry;
pub fn decode<T: serde::de::DeserializeOwned>(v: &Value) -> Result<T> {
    serde_json::from_value(v.clone()).map_err(|e| Error::Invalid(e.to_string()))
}
pub async fn delay(v: &Value, key: &str) {
    if let Some(seconds) = v.get(key).and_then(Value::as_f64)
        && seconds > 0.0
    {
        tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
    }
}
pub struct CaseModel {
    pub steps: Mutex<VecDeque<Value>>,
    pub requests: Mutex<Vec<Vec<String>>>,
    capabilities: Capabilities,
}
impl CaseModel {
    pub fn new(steps: Vec<Value>) -> Self {
        Self {
            steps: Mutex::new(steps.into()),
            requests: Mutex::new(Vec::new()),
            capabilities: Capabilities {
                freeform_runtime_tools: true,
                provider_tools: true,
                input_modalities: vec![
                    "text".into(),
                    "image".into(),
                    "audio".into(),
                    "video".into(),
                    "file".into(),
                ],
                ..case_capabilities()
            },
        }
    }
}
impl Model for CaseModel {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            self.requests
                .lock()
                .unwrap()
                .push(request.messages.iter().map(|m| m.role().into()).collect());
            let step = self
                .steps
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| Error::Protocol("model fixture exhausted".into()))?;
            if let Some(deltas) = step.get("deltas").and_then(Value::as_array) {
                for delta in deltas {
                    context
                        .deltas
                        .as_ref()
                        .ok_or_else(|| Error::Protocol("missing delta sink".into()))?
                        .emit(decode(delta)?)
                        .await?;
                }
            }
            if step["outcome"]["kind"] == "block" {
                std::future::pending::<()>().await;
            }
            delay(&step, "delay_seconds").await;
            match step["outcome"]["kind"].as_str() {
                Some("response") => decode(&step["outcome"]["response"]),
                Some("error") => {
                    let error: Failure = decode(&step["outcome"]["error"])?;
                    if error.code == "model_protocol_error" {
                        Err(Error::Protocol(error.message))
                    } else {
                        Err(Error::Model(error))
                    }
                }
                _ => Err(Error::Protocol("invalid model fixture".into())),
            }
        })
    }
}
struct StandardTool {
    spec: RuntimeToolSpec,
    behavior: String,
    defaults: Value,
}
impl RuntimeTool for StandardTool {
    fn spec(&self) -> &RuntimeToolSpec {
        &self.spec
    }
    fn invoke(
        &self,
        call: RuntimeToolCall,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<RuntimeToolResult>> {
        Box::pin(async move {
            if let RuntimeToolInput::Freeform(input) = call.input {
                return Ok(RuntimeToolResult::json(json!(input)));
            }
            let RuntimeToolInput::Structured(input) = call.input else {
                unreachable!()
            };
            let mut args = self.defaults.as_object().cloned().unwrap_or_default();
            args.extend(input.as_object().cloned().unwrap_or_default());
            let mut args = Value::Object(args);
            for (_, value) in args.as_object_mut().unwrap() {
                if value == "$call_id" {
                    *value = json!(call.id);
                }
            }
            delay(&args, "delay").await;
            context.cancellation.check()?;
            match self.behavior.as_str() {
                "exception" => Err(Error::RuntimeTool(Failure::new(
                    "implementation",
                    "deterministic tool failure",
                ))),
                "success" => Ok(RuntimeToolResult::json(
                    args.get("text").cloned().unwrap_or(json!("")),
                )),
                "strict" => Ok(RuntimeToolResult::json(json!(args["count"].to_string()))),
                "invalid_output" => Ok(RuntimeToolResult::json(json!({"value":"not-an-integer"}))),
                "accepted" => Ok(RuntimeToolResult {
                    outcome: RuntimeToolOutcome::Accepted {
                        task_id: args["correlation_id"].as_str().unwrap().into(),
                        content: vec![Content::text(args["text"].as_str().unwrap())],
                        structured: Value::Null,
                    },
                    suspension: None,
                }),
                "waiting" => Ok(RuntimeToolResult {
                    outcome: RuntimeToolOutcome::Waiting {
                        wait_id: args["wait_id"].as_str().unwrap().into(),
                        content: vec![Content::text(args["text"].as_str().unwrap())],
                        structured: Value::Null,
                    },
                    suspension: Some(Suspension {
                        reason: args["reason"].as_str().unwrap().into(),
                        source: args["source"].as_str().unwrap().into(),
                        wait_id: Some(args["wait_id"].as_str().unwrap().into()),
                        metadata: BTreeMap::new(),
                    }),
                }),
                "progress" => {
                    for step in args["steps"].as_array().unwrap() {
                        if args["burst"] != true {
                            delay(&args, "step_delay").await;
                        }
                        context.emit_progress(json!({"step":step})).await?;
                        if args["burst"] != true {
                            tokio::task::yield_now().await;
                        }
                        context.cancellation.check()?;
                    }
                    Ok(RuntimeToolResult::json(args["text"].clone()))
                }
                _ => Err(Error::Invalid("unknown fixture tool behavior".into())),
            }
        })
    }
}
pub fn tools() -> Result<Arc<RuntimeToolRegistry>> {
    let document: Value = serde_json::from_str(include_str!("../tools.json")).unwrap();
    let registry = RuntimeToolRegistry::new();
    for entry in document["tools"].as_array().unwrap() {
        registry.register(Arc::new(StandardTool {
            spec: decode(&entry["spec"])?,
            behavior: entry["behavior"].as_str().unwrap().into(),
            defaults: entry["defaults"].clone(),
        }))?;
    }
    Ok(Arc::new(registry))
}
pub struct Approval {
    pub decisions: Value,
    pub delay: f64,
}
impl ApprovalPolicy for Approval {
    fn decide(
        &self,
        requests: Vec<ApprovalRequest>,
        _: RunContext,
    ) -> BoxFuture<'_, Result<Vec<ApprovalDecision>>> {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs_f64(self.delay)).await;
            let mut result = Vec::new();
            for request in requests {
                let decision = &self.decisions[&request.call.id];
                if decision
                    .get("call_id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| id != request.call.id)
                {
                    return Err(Error::Protocol("approval identity mismatch".into()));
                }
                result.push(match decision["kind"].as_str() {
                    Some("allow") => ApprovalDecision::Allow,
                    Some("deny") => ApprovalDecision::Deny(
                        decision["message"].as_str().unwrap_or("denied").into(),
                    ),
                    Some("suspend") => ApprovalDecision::Suspend(decode(&decision["suspension"])?),
                    _ => return Err(Error::Protocol("invalid approval fixture".into())),
                });
            }
            Ok(result)
        })
    }
}
pub struct FaultBatch(pub String);
impl BatchPolicy for FaultBatch {
    fn select(&self, calls: &[RuntimeToolCall], _: &[RuntimeToolSpec]) -> Result<RuntimeToolBatch> {
        let mut selected = calls.to_vec();
        let mut parallel = true;
        match self.0.as_str() {
            "empty" => selected.clear(),
            "skip_prefix" => selected = vec![calls[1].clone()],
            "serial_multiple" => parallel = false,
            "oversized" => {
                let mut extra = calls[0].clone();
                extra.id.push_str("-extra");
                selected.push(extra);
            }
            _ => {}
        }
        Ok(RuntimeToolBatch {
            calls: selected,
            parallel,
        })
    }
}
pub struct Reducer {
    pub config: Value,
    pub used: AtomicBool,
}
impl HistoryReducer for Reducer {
    fn reduce(&self, checkpoint: Arc<Checkpoint>) -> BoxFuture<'_, Result<Option<HistoryRewrite>>> {
        Box::pin(async move {
            if self.config["trigger_history_count"]
                .as_u64()
                .is_some_and(|n| n != checkpoint.history.len() as u64)
                || self.used.swap(true, Ordering::AcqRel)
            {
                return Ok(None);
            }
            delay(&self.config, "delay_seconds").await;
            Ok(Some(HistoryRewrite {
                messages: decode(&self.config["messages"])?,
                reason: self.config["reason"].as_str().unwrap_or("fixture").into(),
            }))
        })
    }
}
#[derive(Default)]
struct StoreState {
    head: Option<Arc<Checkpoint>>,
    ids: HashMap<String, String>,
    pub commits: Vec<Commit>,
}
#[derive(Default)]
pub struct RecordingStore {
    state: Mutex<StoreState>,
    config: Mutex<Value>,
}
impl RecordingStore {
    pub fn configure(&self, config: Value) {
        *self.config.lock().unwrap() = config;
    }
    pub fn seed(&self, checkpoint: Arc<Checkpoint>) {
        let mut state = self.state.lock().unwrap();
        state.ids.insert(
            checkpoint.id.clone(),
            zhir_core::wire::CheckpointCore::from(checkpoint.as_ref())
                .digest()
                .unwrap(),
        );
        state.head = Some(checkpoint);
    }
    pub fn checkpoints(&self) -> Vec<Arc<Checkpoint>> {
        self.state
            .lock()
            .unwrap()
            .commits
            .iter()
            .map(|c| c.checkpoint.clone())
            .collect()
    }
    pub fn commits(&self) -> Vec<Commit> {
        self.state.lock().unwrap().commits.clone()
    }
}
impl RunStore for RecordingStore {
    fn commit(&self, commit: Commit) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let config = self.config.lock().unwrap().clone();
            let digest = commit.digest()?;
            if let Some(old) = self
                .state
                .lock()
                .unwrap()
                .ids
                .get(&commit.checkpoint.id)
                .cloned()
            {
                return if old == digest {
                    Ok(())
                } else {
                    Err(Error::Storage("checkpoint identity reused".into()))
                };
            }
            if config["store_delay"]["fact_kind"].as_str() == Some(commit.checkpoint.fact.kind()) {
                delay(&config["store_delay"], "delay_seconds").await;
            }
            if config["store_failure"]["revision"].as_u64() == Some(commit.checkpoint.revision) {
                return Err(Error::Storage(
                    config["store_failure"]["message"]
                        .as_str()
                        .unwrap_or("commit failed")
                        .into(),
                ));
            }
            let mut state = self.state.lock().unwrap();
            commit.validate_against(state.head.as_deref())?;
            state.ids.insert(commit.checkpoint.id.clone(), digest);
            state.head = Some(commit.checkpoint.clone());
            state.commits.push(commit);
            Ok(())
        })
    }
    fn load_head(&self, _: &str) -> BoxFuture<'_, Result<Option<Arc<Checkpoint>>>> {
        Box::pin(async { Ok(self.state.lock().unwrap().head.clone()) })
    }
}
pub fn seed(value: &Value) -> Result<Arc<Checkpoint>> {
    let revision = value["revision"].as_u64().unwrap();
    let id = value["id"].as_str().unwrap().to_owned();
    Ok(Arc::new(Checkpoint {
        options: zhir_kernel::defaults::run_options(),
        id: id.clone(),
        parent_id: if revision > 0 {
            Some(format!("{id}-parent"))
        } else {
            None
        },
        revision,
        context: decode(&value["context"])?,
        history: History::new(decode::<Vec<Message>>(&value["history"])?)?,
        state: decode(&value["state"])?,
        metrics: decode(&value["metrics"])?,
        fact: zhir_core::run::Fact::Control {
            action: "fixture".into(),
        },
    }))
}

fn case_capabilities() -> Capabilities {
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
