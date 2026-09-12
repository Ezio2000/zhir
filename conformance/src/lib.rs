//! Executes native JSON behavior cases against the public SDK contracts.
mod fixtures;
mod validation;
use fixtures::{Approval, CaseModel, FaultBatch, RecordingStore, Reducer, decode};
use futures::StreamExt;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use zhir_core::{
    Result,
    error::Error,
    message::{Content, Message},
    run::{Checkpoint, Event, EventData, State},
    storage::RunStore,
};
use zhir_kernel::{ResumeRequest, Runtime, control::ControlHandle};
pub fn load(path: &Path) -> Result<Value> {
    let bytes = std::fs::read(path).map_err(|e| Error::Invalid(e.to_string()))?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    deserializer.disable_recursion_limit();
    serde::Deserialize::deserialize(&mut deserializer).map_err(|e| Error::Invalid(e.to_string()))
}
fn require(ok: bool, message: impl Into<String>) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Protocol(message.into()))
    }
}
fn event_kind(event: &Event) -> String {
    serde_json::to_value(&event.data).unwrap()["kind"]
        .as_str()
        .unwrap()
        .into()
}
fn resume_request(checkpoint: Arc<Checkpoint>, request: &Value) -> Result<ResumeRequest> {
    let selector = if let Some(s) = request.get("selector").filter(|v| !v.is_null()) {
        Some(zhir_core::run::SuspensionSelector {
            reason: s.get("reason").and_then(Value::as_str).map(str::to_owned),
            source: s.get("source").and_then(Value::as_str).map(str::to_owned),
            wait_id: s.get("wait_id").and_then(Value::as_str).map(str::to_owned),
            metadata: s
                .get("metadata")
                .map(decode)
                .transpose()?
                .unwrap_or_default(),
        })
    } else {
        None
    };
    Ok(ResumeRequest {
        target: zhir_core::run::ResumeTarget::Checkpoint(checkpoint),
        selector,
        messages: request
            .get("messages")
            .map(decode)
            .transpose()?
            .unwrap_or_default(),
        metadata: request
            .get("metadata")
            .map(decode)
            .transpose()?
            .unwrap_or_default(),
    })
}
fn command(control: &ControlHandle, value: &Value) -> Result<()> {
    match value["kind"].as_str() {
        Some("pause") => control.pause(decode(&value["suspension"])?),
        Some("insert") => control.insert(
            decode(&value["message"])?,
            value["source"].as_str().unwrap_or("host"),
        ),
        Some("cancel_tool") => control.cancel_tool(value["tool_call_id"].as_str().unwrap()),
        _ => Err(Error::Invalid("unknown action".into())),
    }
}
pub async fn run_case(case: &Value) -> Result<()> {
    require(case["version"] == 1, "case version must be 1")?;
    if case["kind"] == "validation" {
        return validation::run(case).await;
    }
    let mut last = case.get("seed").map(fixtures::seed).transpose()?;
    for (index, invocation) in case["invocations"]
        .as_array()
        .ok_or_else(|| Error::Invalid("missing invocations".into()))?
        .iter()
        .enumerate()
    {
        let model = Arc::new(CaseModel::new(decode(&invocation["model_steps"])?));
        let store = Arc::new(RecordingStore::default());
        store.configure(invocation.clone());
        if invocation["request"]["kind"] != "start"
            && let Some(c) = &last
        {
            store.seed(c.clone());
        }
        let tools = fixtures::tools()?;
        let mut builder = Runtime::builder(model.clone())
            .runtime_tools(tools)
            .store(store.clone())
            .defaults(|run| run.stream(true));
        if let Some(limits) = invocation.get("limits") {
            let limits = decode(limits)?;
            builder = builder.defaults(|run| run.limits(limits));
        }
        if let Some(tools) = invocation.get("provider_tools") {
            let tools = decode(tools)?;
            builder = builder.defaults(|run| run.provider_tools(tools));
        }
        if let Some(decisions) = invocation.get("approval_decisions") {
            builder = builder.approval(Arc::new(Approval {
                decisions: decisions.clone(),
                delay: invocation["approval_delay_seconds"].as_f64().unwrap_or(0.0),
            }));
        }
        if let Some(kind) = invocation["batch_policy"].as_str() {
            builder = builder.batch_policy(Arc::new(FaultBatch(kind.into())));
        }
        if let Some(config) = invocation.get("history_rewrite") {
            builder = builder.history_reducer(Arc::new(Reducer {
                config: config.clone(),
                used: false.into(),
            }));
        }
        let runtime = builder.build()?;
        let request = &invocation["request"];
        let expected = &invocation["expected"];
        let created = match request["kind"].as_str() {
            Some("start") => runtime.start(zhir_core::run::RunRequest::new(
                decode::<Vec<Message>>(&request["messages"])?,
            )),
            Some("continue") => runtime.continue_from(
                last.clone()
                    .ok_or_else(|| Error::Invalid("no checkpoint to continue".into()))?,
            ),
            Some("resume") => {
                runtime
                    .resume(resume_request(
                        last.clone()
                            .ok_or_else(|| Error::Invalid("no checkpoint to resume".into()))?,
                        request,
                    )?)
                    .await
            }
            _ => return Err(Error::Invalid("unknown invocation request".into())),
        };
        let mut events = Vec::new();
        let mut request_error = false;
        let mut store_error = None;
        let checkpoint = match created {
            Err(_) if expected["request_error"] == true => {
                request_error = true;
                last.clone().expect("rejected resume checkpoint")
            }
            Err(error) => return Err(error),
            Ok(mut run) => {
                let control = run.control();
                let mut stream = run.events()?;
                let mut counts: BTreeMap<String, u64> = BTreeMap::new();
                let actions = invocation
                    .get("actions")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let mut applied = vec![false; actions.len()];
                while let Some(event) = stream.next().await {
                    let kind = event_kind(&event);
                    let count = counts.entry(kind.clone()).or_default();
                    *count += 1;
                    for (i, action) in actions.iter().enumerate() {
                        if !applied[i]
                            && action["when"]["event_kind"] == kind
                            && action["when"]["occurrence"].as_u64().unwrap_or(1) == *count
                        {
                            command(&control, &action["command"])?;
                            applied[i] = true;
                        }
                    }
                    events.push(event);
                }
                require(
                    applied.iter().all(|v| *v),
                    format!("invocation {index}: an action never fired"),
                )?;
                match run.result().await {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        if expected.get("store_error").is_none() {
                            return Err(error.error);
                        }
                        store_error = Some(error.error.to_string());
                        error.last_checkpoint.ok_or_else(|| {
                            Error::Protocol(
                                "missing authoritative checkpoint on store failure".into(),
                            )
                        })?
                    }
                }
            }
        };
        require(
            model.steps.lock().unwrap().is_empty(),
            format!("invocation {index}: unused model steps"),
        )?;
        if expected["store_idempotent"] == true {
            for mut commit in store.commits() {
                commit.deadline = None;
                store.commit(commit).await?;
            }
        }
        let observed = observe(&checkpoint, &events, &model, request_error, store_error);
        for (key, value) in expected.as_object().unwrap() {
            match key.as_str() {
                "store_idempotent" => {}
                "store_error" => require(
                    observed[key]
                        .as_str()
                        .is_some_and(|s| s.contains(value.as_str().unwrap())),
                    format!("{key}: {:?}", observed[key]),
                )?,
                "event_counts" => {
                    for (kind, count) in value.as_object().unwrap() {
                        require(
                            observed[key].get(kind).cloned().unwrap_or(json!(0)) == *count,
                            format!(
                                "{key}.{kind}: expected {count}, got {}",
                                observed[key][kind]
                            ),
                        )?;
                    }
                }
                "forbidden_event_kinds" => {
                    for kind in value.as_array().unwrap() {
                        require(
                            events
                                .iter()
                                .all(|e| event_kind(e) != kind.as_str().unwrap()),
                            format!("forbidden event {kind}"),
                        )?;
                    }
                }
                _ => require(
                    observed.get(key) == Some(value),
                    format!(
                        "invocation {index} {key}: expected {value}, got {}",
                        observed[key]
                    ),
                )?,
            }
        }
        zhir_kernel::diagnostics::verify_trace(&store.checkpoints()).or_else(|e| {
            if store.checkpoints().is_empty() {
                Ok(())
            } else {
                Err(e)
            }
        })?;
        let bytes = zhir_core::wire::encode_checkpoint(&checkpoint)?;
        let recovered = zhir_core::wire::decode_checkpoint(&bytes)?;
        require(
            recovered.history.digest() == checkpoint.history.digest(),
            "checkpoint wire roundtrip changed history",
        )?;
        last = Some(checkpoint);
    }
    Ok(())
}
fn observe(
    c: &Checkpoint,
    events: &[Event],
    model: &CaseModel,
    request_error: bool,
    store_error: Option<String>,
) -> Value {
    let mut counts = BTreeMap::<String, u64>::new();
    let mut facts = Vec::new();
    let mut activity = Vec::new();
    let mut progress = Vec::new();
    let mut approvals = Vec::new();
    let mut active = 0;
    let mut max_active = 0;
    for event in events {
        let kind = event_kind(event);
        *counts.entry(kind.clone()).or_default() += 1;
        match &event.data {
            EventData::CheckpointCommitted { fact, .. } => facts.push(fact.kind()),
            EventData::RuntimeToolStarted { call_id } => {
                active += 1;
                max_active = max_active.max(active);
                activity.push(json!({"kind":kind,"tool_call_id":call_id}));
            }
            EventData::RuntimeToolFinished { call_id, .. } => {
                active -= 1;
                activity.push(json!({"kind":kind,"tool_call_id":call_id}));
            }
            EventData::RuntimeToolProgress { value, .. } => progress.push(value.clone()),
            EventData::ApprovalRequested { call_id } => approvals.push(call_id.clone()),
            _ => {}
        }
    }
    let messages = c.history.messages();
    let content = if let State::Completed { content } = &c.state {
        content.clone()
    } else {
        vec![]
    };
    let mut value = json!({"status":c.state.kind(),"revision":c.revision,"planning_steps":c.metrics.planning_steps,"runtime_tool_calls":c.metrics.runtime_tool_calls,"usage":c.metrics.usage,"final_text":content.iter().filter_map(Content::as_text).collect::<Vec<_>>().join(""),"final_parts":content,"final_part_types":content.iter().map(|p|serde_json::to_value(p).unwrap()["kind"].as_str().unwrap().to_owned()).collect::<Vec<_>>(),"message_roles":messages.iter().map(Message::role).collect::<Vec<_>>(),"model_request_roles":*model.requests.lock().unwrap(),"tool_outcome_kinds":messages.iter().filter_map(|m|if let Message::RuntimeTool {outcome,..}=m {Some(outcome.kind())} else {None}).collect::<Vec<_>>(),"fact_kinds":facts,"event_counts":counts,"tool_activity":activity,"max_active_tools":max_active,"progress":progress,"approval_call_ids":approvals,"request_error":request_error,"store_error":store_error});
    let active = match &c.state {
        State::Suspended {
            suspension,
            resume_to,
        } => {
            value["suspension"] = json!(suspension);
            Some(resume_to.clone())
        }
        _ => c.state.active(),
    };
    value["pending_call_ids"] = json!(match active {
        Some(zhir_core::run::ActiveState::RuntimeToolsPending { calls, .. }) =>
            calls.iter().map(|c| c.id.clone()).collect::<Vec<_>>(),
        _ => vec![],
    });
    if let State::Limited { reason } = &c.state {
        value["limit_reason"] = json!(reason);
    }
    if let State::Failed { error } = &c.state {
        value["error_code"] = json!(error.code);
    }
    value
}
