//! Explicit, paid live checks. Requires DEEPSEEK_API_KEY and --ignored.
use futures::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use zhir::{
    BoxFuture, Result, Runtime,
    core::Cancellation,
    error::Error,
    message::{Content, Message, visible_content},
    model::{
        Capabilities, DeltaSink, Model, ModelContext, ModelDelta, ModelOptions, ModelRequest,
        ModelResponse, ResponseFormat, ToolChoice,
    },
    models::{HttpModel, ModelConfig, Protocol, anthropic, openai},
    run::{EventData, Limits, State},
    runtime_tools::{FunctionTool, RuntimeToolRegistry},
    tool::{Execution, InputSpec, RuntimeTool, RuntimeToolCall, RuntimeToolInput, RuntimeToolSpec},
};
fn require(valid: bool, message: &str) -> Result<()> {
    if valid {
        Ok(())
    } else {
        Err(Error::Protocol(message.into()))
    }
}
fn name(protocol: Protocol) -> &'static str {
    match protocol {
        Protocol::Chat => "chat",
        Protocol::Responses => "responses",
        Protocol::Messages => "anthropic",
    }
}
fn base(protocol: Protocol) -> &'static str {
    match protocol {
        Protocol::Messages => "https://api.deepseek.com/anthropic/v1",
        _ => "https://api.deepseek.com",
    }
}
fn model(protocol: Protocol, key: &str, model_id: &str) -> Result<HttpModel> {
    let mut config = ModelConfig::new(base(protocol), key, model_id);
    config.timeout = Duration::from_secs(60);
    match protocol {
        Protocol::Chat => openai::chat::model(config),
        Protocol::Responses => openai::responses::model(config),
        Protocol::Messages => anthropic::messages::model(config),
    }
}
fn options(protocol: Protocol, thinking: bool) -> ModelOptions {
    let limit = if thinking { 1024 } else { 128 };
    let mut options = ModelOptions {
        max_output_tokens: (protocol != Protocol::Chat).then_some(limit),
        ..Default::default()
    };
    if protocol == Protocol::Chat {
        options.extra.insert("max_tokens".into(), json!(limit));
    }
    match protocol {
        Protocol::Responses => {
            options.extra.insert(
                "reasoning".into(),
                json!({"effort":if thinking {"low"} else {"none"}}),
            );
        }
        _ => {
            options.extra.insert(
                "thinking".into(),
                json!({"type":if thinking {"enabled"} else {"disabled"}}),
            );
            if thinking {
                match protocol {
                    Protocol::Chat => {
                        options
                            .extra
                            .insert("reasoning_effort".into(), json!("low"));
                    }
                    Protocol::Messages => {
                        options
                            .extra
                            .insert("output_config".into(), json!({"effort":"low"}));
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
    options
}
#[derive(Default)]
struct Observations(Mutex<Vec<ModelDelta>>);
impl DeltaSink for Observations {
    fn emit(&self, delta: ModelDelta) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(delta);
            Ok(())
        })
    }
}
fn delta_name(delta: &ModelDelta) -> &'static str {
    match delta {
        ModelDelta::Text { .. } => "text",
        ModelDelta::Reasoning { .. } => "reasoning",
        ModelDelta::RuntimeTool { .. } => "runtime_tool",
        ModelDelta::Usage { .. } => "usage",
        ModelDelta::ProtocolEvent { .. } => "protocol_event",
        ModelDelta::ProviderToolProgress { .. } => "provider_tool_progress",
    }
}
fn summary(response: &ModelResponse) -> Value {
    json!({"model_id":response.model_id,"response_id":response.response_id,"finish_reason":response.finish_reason,"usage":response.usage})
}
async fn text_case(
    protocol: Protocol,
    key: &str,
    model_id: &str,
    stream: bool,
    json_output: bool,
) -> Result<Value> {
    let model = model(protocol, key, model_id)?;
    let deltas = Arc::new(Observations::default());
    let response = model
        .invoke(
            ModelRequest {
                messages: vec![Message::user(if json_output {
                    "Return only the JSON object {\"answer\":42}."
                } else {
                    "Reply with exactly: zhir实测OK"
                })],
                runtime_tools: vec![],
                provider_tools: vec![],
                options: options(protocol, false),
                tool_choice: ToolChoice::Auto,
                response_format: json_output.then_some(ResponseFormat::Json),
                stream,
            },
            ModelContext {
                run: zhir::kernel::defaults::context(),
                cancellation: Cancellation::default(),
                deltas: Some(deltas.clone()),
            },
        )
        .await?;
    let text = visible_content(&response.output)
        .iter()
        .filter_map(Content::as_text)
        .collect::<String>();
    require(
        response.usage.input_tokens.is_some() && response.usage.output_tokens.is_some(),
        "missing token usage",
    )?;
    if json_output {
        let value: Value =
            serde_json::from_str(&text).map_err(|e| Error::Protocol(e.to_string()))?;
        require(value == json!({"answer":42}), "unexpected JSON output")?;
    } else {
        require(text.trim() == "zhir实测OK", "unexpected text output")?;
    }
    let observed = deltas.0.lock().unwrap();
    let mut counts = BTreeMap::<&str, usize>::new();
    let mut streamed = String::new();
    for delta in observed.iter() {
        *counts.entry(delta_name(delta)).or_default() += 1;
        if let ModelDelta::Text { text, .. } = delta {
            streamed.push_str(text);
        }
    }
    if stream {
        require(
            streamed == text && !streamed.is_empty(),
            "stream text differs from final response",
        )?;
    }
    Ok(json!({"text":text,"response":summary(&response),"deltas":counts}))
}
struct RecordedModel {
    inner: HttpModel,
    responses: Mutex<Vec<Value>>,
}
impl Model for RecordedModel {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            let response = self.inner.invoke(request, context).await?;
            self.responses.lock().unwrap().push(summary(&response));
            Ok(response)
        })
    }
}
async fn tool_case(
    protocol: Protocol,
    key: &str,
    model_id: &str,
    thinking: bool,
    freeform: bool,
) -> Result<Value> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let spec = RuntimeToolSpec {
        name: if freeform { "apply_patch" } else { "add" }.into(),
        description: if freeform {
            "Accept a patch for this in-memory protocol test. No files are written."
        } else {
            "Add two integers. Call this to calculate the answer."
        }
        .into(),
        input: if freeform {
            InputSpec::Freeform { format: None }
        } else {
            InputSpec::Structured {
                schema: json!({"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"],"additionalProperties":false}),
            }
        },
        output_schema: None,
        execution: Execution {
            parallel: false,
            read_only: true,
            idempotent: true,
        },
    };
    let tool = Arc::new(FunctionTool::new(spec, move |call: RuntimeToolCall, _| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            match call.input {
                RuntimeToolInput::Structured(args) => {
                    let a = args["a"]
                        .as_i64()
                        .ok_or_else(|| Error::Invalid("missing integer a".into()))?;
                    let b = args["b"]
                        .as_i64()
                        .ok_or_else(|| Error::Invalid("missing integer b".into()))?;
                    Ok(zhir::runtime_tools::reply::json(json!({"answer":a+b})))
                }
                RuntimeToolInput::Freeform(patch) => {
                    require(
                        patch.contains("*** Begin Patch") && patch.contains("+hello"),
                        "invalid patch input",
                    )?;
                    Ok(zhir::runtime_tools::reply::json(json!("PATCH_ACCEPTED")))
                }
            }
        }
    })) as Arc<dyn RuntimeTool>;
    let model = Arc::new(RecordedModel {
        inner: model(protocol, key, model_id)?,
        responses: Mutex::new(vec![]),
    });
    let runtime = Runtime::builder(model.clone())
        .runtime_tools(Arc::new(RuntimeToolRegistry::from_tools([tool])?))
        .defaults(|run| run.options(options(protocol, thinking)))
        .defaults(|run| run.stream(true))
        .defaults(|run| {
            run.limits(Limits {
                max_planning_steps: 3,
                max_runtime_tool_calls: 2,
                elapsed_ms: Some(90_000),
                ..zhir::kernel::defaults::limits()
            })
        })
        .build()?;
    let prompt = if freeform {
        "Call apply_patch exactly once with a patch adding protocol-demo.txt containing the line hello. This is an in-memory test. After the tool returns PATCH_ACCEPTED, reply exactly PATCH_ACCEPTED and do not call any more tools."
    } else {
        "You must call the add tool exactly once with a=19 and b=23. Do not calculate without the tool. After receiving its result, reply with exactly the number 42 and do not call any more tools."
    };
    let mut invocation = runtime.start(zhir::RunRequest::new(vec![Message::user(prompt)]))?;
    let mut events = invocation.events()?;
    let mut counts = BTreeMap::<String, usize>::new();
    while let Some(event) = events.next().await {
        let kind = match &event.data {
            EventData::ModelDelta { delta } => format!("delta_{}", delta_name(delta)),
            _ => serde_json::to_value(&event.data).unwrap()["kind"]
                .as_str()
                .unwrap()
                .to_owned(),
        };
        *counts.entry(kind).or_default() += 1;
    }
    let checkpoint = invocation
        .result()
        .await
        .map_err(|error| error.error)?
        .into_checkpoint();
    let State::Completed { content } = &checkpoint.state else {
        return Err(Error::Protocol(format!(
            "run ended as {:?}",
            checkpoint.state
        )));
    };
    let text = content
        .iter()
        .filter_map(Content::as_text)
        .collect::<String>();
    require(
        text.trim() == if freeform { "PATCH_ACCEPTED" } else { "42" },
        "wrong final tool answer",
    )?;
    require(
        calls.load(Ordering::SeqCst) == 1
            && checkpoint.metrics.runtime_tool_calls == 1
            && checkpoint.metrics.planning_steps == 2,
        "tool loop did not execute exactly once",
    )?;
    require(
        counts.get("delta_runtime_tool").copied().unwrap_or(0) > 0,
        "missing streamed tool deltas",
    )?;
    if thinking {
        require(
            counts.get("delta_reasoning").copied().unwrap_or(0) > 0,
            "missing reasoning deltas",
        )?;
    }
    Ok(
        json!({"text":text,"metrics":checkpoint.metrics,"revision":checkpoint.revision,"events":counts,"responses":*model.responses.lock().unwrap()}),
    )
}
#[tokio::test]
#[ignore = "paid live protocol matrix; requires DEEPSEEK_API_KEY"]
async fn live_protocol_matrix() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let key = std::env::var("DEEPSEEK_API_KEY")?;
    let model_id = std::env::var("DEEPSEEK_MODEL").unwrap_or_else(|_| "deepseek-flash".into());
    let filter = std::env::var("DEEPSEEK_LIVE_FILTER").unwrap_or_default();
    let output =
        std::env::var("DEEPSEEK_LIVE_REPORT").unwrap_or_else(|_| "deepseek-live.json".into());
    let started = zhir::kernel::defaults::context().started_at_ms;
    let mut rows = Vec::new();
    for protocol in [Protocol::Chat, Protocol::Responses, Protocol::Messages] {
        let mut scenarios = vec!["text", "stream", "tool_stream", "thinking_tool_stream"];
        if protocol != Protocol::Messages {
            scenarios.push("json");
        }
        if protocol == Protocol::Responses {
            scenarios.push("freeform_tool_stream");
        }
        for scenario in scenarios {
            let label = format!("{}/{scenario}", name(protocol));
            if !filter.is_empty() && !label.contains(&filter) {
                continue;
            }
            eprintln!("RUN {label}");
            let instant = Instant::now();
            let result = match scenario {
                "text" => text_case(protocol, &key, &model_id, false, false).await,
                "stream" => text_case(protocol, &key, &model_id, true, false).await,
                "json" => text_case(protocol, &key, &model_id, false, true).await,
                _ => {
                    tool_case(
                        protocol,
                        &key,
                        &model_id,
                        scenario == "thinking_tool_stream",
                        scenario == "freeform_tool_stream",
                    )
                    .await
                }
            };
            let (status, detail) = match result {
                Ok(value) => ("passed", value),
                Err(error) => ("failed", json!({"error":error.to_string()})),
            };
            eprintln!("{status}: {label} ({} ms)", instant.elapsed().as_millis());
            if status == "failed" {
                eprintln!("{detail}");
            }
            rows.push(json!({"protocol":name(protocol),"base_url":base(protocol),"scenario":scenario,"status":status,"elapsed_ms":instant.elapsed().as_millis(),"detail":detail}));
            std::fs::write(
                &output,
                serde_json::to_vec_pretty(
                    &json!({"started_at_ms":started,"model":model_id,"cases":rows}),
                )?,
            )?;
        }
    }
    if rows.is_empty() {
        return Err("no live scenarios matched DEEPSEEK_LIVE_FILTER".into());
    }
    let failures = rows.iter().filter(|row| row["status"] == "failed").count();
    eprintln!(
        "{} passed; {failures} failed; report: {output}",
        rows.len() - failures
    );
    if failures > 0 {
        return Err("live protocol checks failed".into());
    }
    Ok(())
}
