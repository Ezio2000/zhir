use super::harness::*;
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zhir::{
    Result, Runtime,
    error::{Error, Failure},
    message::{Content, Message, Output},
    model::{DeltaSink, Model, ModelDelta, ModelRequest, ModelResponse},
    models::{
        Protocol, ProtocolExtension,
        decorators::{FallbackModel, ObservedModel, RetryingModel},
        transport::SseEvent,
    },
    run::{EventData, Limits, State},
};

#[derive(Clone)]
struct Reply {
    status: u16,
    body: String,
    sse: bool,
    chunk: usize,
    pause_ms: u64,
}
struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(replies: Vec<Reply>) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(vec![]));
    let captured = requests.clone();
    let task = tokio::spawn(async move {
        let mut turn = 0;
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![];
            let mut buffer = [0u8; 8192];
            let end = loop {
                let n = socket.read(&mut buffer).await.unwrap();
                if n == 0 {
                    return;
                }
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(i) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    break i + 4;
                }
            };
            let length = String::from_utf8_lossy(&bytes[..end])
                .lines()
                .find_map(|l| {
                    l.to_lowercase()
                        .strip_prefix("content-length:")
                        .map(|s| s.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            while bytes.len() < end + length {
                let n = socket.read(&mut buffer).await.unwrap();
                if n == 0 {
                    return;
                }
                bytes.extend_from_slice(&buffer[..n]);
            }
            let request: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
            captured.lock().unwrap().push(request.clone());
            let reply = &replies[turn.min(replies.len() - 1)];
            turn += 1;
            let body = if request["jsonrpc"] == "2.0" {
                let mut envelope: Value = serde_json::from_str(&reply.body).unwrap();
                envelope["id"] = request["id"].clone();
                envelope.to_string()
            } else {
                reply.body.clone()
            };
            let header = format!(
                "HTTP/1.1 {} fixture\r\nContent-Length: {}\r\nContent-Type: {}\r\nConnection: close\r\n\r\n",
                reply.status,
                body.len(),
                if reply.sse {
                    "text/event-stream"
                } else {
                    "application/json"
                }
            );
            if socket.write_all(header.as_bytes()).await.is_err() {
                continue;
            }
            for bytes in body.as_bytes().chunks(reply.chunk) {
                if socket.write_all(bytes).await.is_err() {
                    break;
                }
                if reply.pause_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(reply.pause_ms)).await;
                }
            }
        }
    });
    Server {
        url,
        requests,
        task,
    }
}
fn complete(protocol: Protocol, text: &str) -> Value {
    match protocol {
        Protocol::Chat => {
            json!({"id":"wire-chat","model":"fixture","choices":[{"message":{"role":"assistant","content":text},"finish_reason":"stop"}],"usage":{"prompt_tokens":4,"completion_tokens":1}})
        }
        Protocol::Responses => {
            json!({"id":"wire-responses","model":"fixture","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":text}]}],"usage":{"input_tokens":4,"output_tokens":1}})
        }
        Protocol::Messages => {
            json!({"id":"wire-messages","model":"fixture","content":[{"type":"text","text":text}],"stop_reason":"end_turn","usage":{"input_tokens":4,"output_tokens":1}})
        }
    }
}
fn frame(value: Value) -> String {
    format!("data: {value}\n\n")
}
fn body(protocol: Protocol, count: usize, terminal: bool, custom: bool) -> String {
    let mut body = String::new();
    if protocol == Protocol::Messages {
        body += &frame(
            json!({"type":"message_start","message":{"id":"wire-messages","model":"fixture","content":[],"usage":{"input_tokens":4}}}),
        );
        body += &frame(
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        );
    }
    for i in 0..count {
        body += &frame(if custom {
            json!({"type":"consumer.segment","text":"界","ordinal":i})
        } else {
            delta(protocol, "界")
        });
    }
    if terminal {
        match protocol {
            Protocol::Chat => {
                body += &frame(
                    json!({"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":4,"completion_tokens":count}}),
                );
                body += "data: [DONE]\n\n";
            }
            Protocol::Responses => {
                body += &frame(
                    json!({"type":"response.completed","response":complete(protocol,&"界".repeat(count))}),
                )
            }
            Protocol::Messages => {
                body += &frame(json!({"type":"content_block_stop","index":0}));
                body += &frame(
                    json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":count}}),
                );
                body += &frame(json!({"type":"message_stop"}));
            }
        }
    }
    body
}
fn delta(protocol: Protocol, text: &str) -> Value {
    match protocol {
        Protocol::Chat => {
            json!({"id":"wire-chat","model":"fixture","choices":[{"delta":{"content":text}}]})
        }
        Protocol::Responses => {
            json!({"type":"response.output_text.delta","output_index":0,"delta":text})
        }
        Protocol::Messages => {
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}})
        }
    }
}
#[derive(Default)]
struct FeatureSession {
    segments: usize,
    ordinal_sum: u64,
}
impl ProtocolExtension for FeatureSession {
    fn encode_request(
        &mut self,
        protocol: Protocol,
        _: &ModelRequest,
        body: &mut Value,
    ) -> Result<()> {
        body[if protocol == Protocol::Responses {
            "input"
        } else {
            "messages"
        }][0]["consumer_options"] = json!({"mode":"new_feature"});
        Ok(())
    }
    fn decode_event(
        &mut self,
        protocol: Protocol,
        event: &mut SseEvent,
    ) -> Result<Vec<ModelDelta>> {
        if let Ok(value) = serde_json::from_str::<Value>(&event.data)
            && value["type"] == "consumer.segment"
        {
            self.segments += 1;
            self.ordinal_sum += value["ordinal"].as_u64().unwrap();
            event.data = delta(protocol, value["text"].as_str().unwrap()).to_string();
        }
        Ok(vec![])
    }
    fn decode_response(
        &mut self,
        _: Protocol,
        _: &Value,
        decoded: Result<ModelResponse>,
    ) -> Result<ModelResponse> {
        let mut response = decoded?;
        response.output.push(Output::Content {
            content: Content::Opaque {
                provider: "consumer".into(),
                data: json!({"segments":self.segments,"ordinal_sum":self.ordinal_sum}),
            },
        });
        response.provider_data["consumer_segments"] = json!(self.segments);
        Ok(response)
    }
}
struct EventFailure;
impl ProtocolExtension for EventFailure {
    fn decode_event(&mut self, _: Protocol, event: &mut SseEvent) -> Result<Vec<ModelDelta>> {
        if event.data.contains("consumer.retryable_error") {
            return Err(Error::Model(Failure {
                code: "consumer_busy".into(),
                message: "injected retryable stream failure".into(),
                retryable: true,
            }));
        }
        Ok(vec![])
    }
}
fn invalid_tool(protocol: Protocol) -> String {
    match protocol {
        Protocol::Chat => {
            frame(
                json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"bad","function":{"name":"read","arguments":"{"}}]},"finish_reason":"tool_calls"}]}),
            ) + "data: [DONE]\n\n"
        }
        Protocol::Responses => frame(
            json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"function_call","call_id":"bad","name":"read","arguments":"{"}]}}),
        ),
        Protocol::Messages => {
            frame(json!({"type":"message_start","message":{"content":[],"usage":{}}}))
                + &frame(
                    json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"bad","name":"read","input":{}}}),
                )
                + &frame(
                    json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{"}}),
                )
                + &frame(json!({"type":"content_block_stop","index":0}))
                + &frame(json!({"type":"message_stop"}))
        }
    }
}
#[derive(Default)]
struct ObserverCounts {
    text: usize,
    provider: usize,
}
struct AuditSink {
    counts: Mutex<ObserverCounts>,
    fail_after: Option<usize>,
}
impl DeltaSink for AuditSink {
    fn emit(&self, delta: ModelDelta) -> zhir::BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut counts = self.counts.lock().unwrap();
            match delta {
                ModelDelta::Text { .. } => {
                    counts.text += 1;
                    if self.fail_after == Some(counts.text) {
                        return Err(Error::Model(Failure {
                            code: "observer_write".into(),
                            message: "fixture audit writer failed".into(),
                            retryable: true,
                        }));
                    }
                }
                ModelDelta::ProtocolEvent { .. } => counts.provider += 1,
                _ => {}
            }
            Ok(())
        })
    }
}
async fn wire_case(
    protocol: Protocol,
    family: &str,
    repeat: usize,
    count: usize,
    chunk: usize,
) -> Value {
    let started = Instant::now();
    let slow = matches!(family, "slow_consumer" | "observed_slow_consumer");
    let audited = matches!(family, "observed_slow_consumer" | "observer_failure");
    let observer = Arc::new(AuditSink {
        counts: Mutex::new(ObserverCounts::default()),
        fail_after: (family == "observer_failure").then_some(3),
    });
    let good = Reply {
        status: 200,
        body: complete(protocol, "ok").to_string(),
        sse: false,
        chunk,
        pause_ms: 0,
    };
    let response = match family {
        "retry_429" => vec![
            Reply {
                status: 429,
                body: "busy".into(),
                ..good.clone()
            },
            good.clone(),
        ],
        "fallback_503" => vec![
            Reply {
                status: 503,
                body: "busy".into(),
                ..good.clone()
            },
            good.clone(),
        ],
        "permanent_400" => vec![Reply {
            status: 400,
            body: "invalid".into(),
            ..good.clone()
        }],
        "malformed_tool" => vec![Reply {
            body: invalid_tool(protocol),
            sse: true,
            ..good.clone()
        }],
        "visible_error_no_retry" => vec![Reply {
            body: frame(json!({"type":"consumer.notice","payload":"visible"}))
                + &frame(json!({"type":"consumer.retryable_error"})),
            sse: true,
            ..good.clone()
        }],
        _ => vec![Reply {
            body: body(
                protocol,
                count,
                family != "truncated_stream",
                family == "custom_events",
            ),
            sse: true,
            chunk: if family == "cancel_stream" { 64 } else { chunk },
            pause_ms: if family == "cancel_stream" { 4 } else { 0 },
            ..good.clone()
        }],
    };
    let server = server(response).await;
    let mut model: Arc<dyn Model> =
        Arc::new(http(protocol, &server.url, "fixture", "primary").unwrap());
    if family == "custom_events" {
        model = Arc::new(
            http(protocol, &server.url, "fixture", "primary")
                .unwrap()
                .with_extension(|_| Ok(FeatureSession::default())),
        );
    }
    if family == "visible_error_no_retry" {
        model = Arc::new(
            http(protocol, &server.url, "fixture", "primary")
                .unwrap()
                .with_extension(|_| Ok(EventFailure)),
        );
    }
    if audited {
        let sink = observer.clone();
        model = Arc::new(ObservedModel::new(model, move |_, _| Ok(sink.clone())));
    }
    if matches!(
        family,
        "retry_429" | "permanent_400" | "visible_error_no_retry" | "observer_failure"
    ) {
        model = Arc::new(RetryingModel::new(model, 3, Duration::from_millis(1)).unwrap());
    }
    if family == "fallback_503" {
        model = Arc::new(
            FallbackModel::new(vec![
                model,
                Arc::new(http(protocol, &server.url, "fixture", "backup").unwrap()),
            ])
            .unwrap(),
        );
    }
    let mut checks = std::collections::BTreeMap::new();
    let mut detail = json!({});
    if matches!(family, "retry_429" | "fallback_503" | "permanent_400") {
        let result = model
            .invoke(
                empty_request(false),
                zhir::model::ModelContext {
                    run: Default::default(),
                    cancellation: Default::default(),
                    deltas: None,
                },
            )
            .await;
        checks.insert(
            "expected_result",
            match &result {
                Ok(response) => {
                    family != "permanent_400" && response.output == vec![Output::text("ok")]
                }
                Err(Error::Model(failure)) => {
                    family == "permanent_400" && failure.code == "http_400"
                }
                _ => false,
            },
        );
        detail["error"] = json!(result.err().map(|e| e.to_string()));
    } else {
        let runtime = Runtime::builder(model)
            .defaults(|run| run.stream(true))
            .defaults(|run| {
                run.limits(Limits {
                    max_planning_steps: 2,
                    max_runtime_tool_calls: 0,
                    max_progress_events: if slow { 16 } else { 16384 },
                    elapsed_ms: Some(10_000),
                    ..Default::default()
                })
            })
            .build()
            .unwrap();
        let mut invocation = runtime
            .start(zhir_core::run::RunRequest::new(vec![Message::user(
                "exercise fixture",
            )]))
            .unwrap();
        let control = invocation.control();
        let mut events = invocation.events().unwrap();
        let mut summary = Events::new();
        let mut cancelled = false;
        let mut original_custom = 0;
        while let Some(event) = events.next().await {
            if family == "cancel_stream"
                && !cancelled
                && matches!(
                    event.data,
                    EventData::ModelDelta {
                        delta: ModelDelta::Text { .. }
                    }
                )
            {
                control.cancel();
                cancelled = true;
            }
            if matches!(&event.data,EventData::ModelDelta {delta:ModelDelta::ProtocolEvent {data,..}} if data["data"]["type"]=="consumer.segment")
            {
                original_custom += 1;
            }
            summary.record(&event);
            if slow {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
        let result = invocation.result().await;
        let expected_success = slow || matches!(family, "custom_events" | "dense_stream");
        let (checkpoint, error) = match result {
            Ok(c) => (Some(c), None),
            Err(e) => (e.last_checkpoint, Some(e.error)),
        };
        checks.insert("ordered_events", summary.ordered);
        if expected_success {
            checks.insert("full_final_content",checkpoint.as_ref().is_some_and(|c|matches!(&c.state,State::Completed{content} if text(content)=="界".repeat(count))) && error.is_none());
            if let Some(checkpoint) = &checkpoint {
                checks.insert("wire_roundtrip", roundtrip(checkpoint).is_ok());
                if family == "custom_events" {
                    checks.insert("raw_events_preserved", original_custom == count);
                    checks.insert("custom_metadata",checkpoint.history.messages().iter().any(|m|matches!(m,Message::Assistant{provider_data,..} if provider_data["consumer_segments"]==count)));
                    checks.insert("custom_content",matches!(&checkpoint.state,State::Completed{content} if content.iter().any(|c|matches!(c,Content::Opaque{data,..} if data["segments"]==count && data["ordinal_sum"]==(count*(count-1)/2)))));
                }
            }
            if slow {
                checks.insert(
                    "bounded_progress_loss_observed",
                    summary.counts.get("delta_text").copied().unwrap_or(0) < count,
                );
            }
            if audited {
                let counts = observer.counts.lock().unwrap();
                checks.insert("observer_all_text_deltas", counts.text == count);
                let terminal_frames = match protocol {
                    Protocol::Chat => 2,
                    Protocol::Responses => 1,
                    Protocol::Messages => 5,
                };
                checks.insert(
                    "observer_all_raw_frames",
                    counts.provider == count + terminal_frames,
                );
            }
            if family == "dense_stream" {
                checks.insert(
                    "all_text_deltas",
                    summary.counts.get("delta_text") == Some(&count),
                );
            }
        } else {
            let failure = error.as_ref().map(Error::failure).or_else(|| {
                checkpoint.as_ref().and_then(|c| {
                    if let State::Failed { error } = &c.state {
                        Some(error.clone())
                    } else {
                        None
                    }
                })
            });
            checks.insert(
                "expected_failure_code",
                failure.as_ref().is_some_and(|f| {
                    f.code
                        == match family {
                            "cancel_stream" => "cancelled",
                            "visible_error_no_retry" => "consumer_busy",
                            "observer_failure" => "observer_write",
                            _ => "protocol",
                        }
                }),
            );
            if family == "truncated_stream" {
                checks.insert(
                    "partial_stream_was_observed",
                    summary.counts.get("delta_text") == Some(&count),
                );
                checks.insert(
                    "missing_terminal_marker",
                    failure
                        .as_ref()
                        .is_some_and(|f| f.message.contains("before complete response")),
                );
            }
            if family == "malformed_tool" {
                checks.insert(
                    "tool_json_parse_error",
                    failure.as_ref().is_some_and(|f| f.message.contains("EOF")),
                );
            }
            checks.insert(
                "failure_settled",
                error.is_some()
                    || checkpoint
                        .as_ref()
                        .is_some_and(|c| matches!(c.state, State::Failed { .. })),
            );
            checks.insert(
                "no_partial_assistant_commit",
                checkpoint.as_ref().is_some_and(|c| {
                    !c.history
                        .messages()
                        .iter()
                        .any(|m| matches!(m, Message::Assistant { .. }))
                }),
            );
            checks.insert(
                "no_tool_execution",
                summary
                    .counts
                    .get("runtime_tool_started")
                    .copied()
                    .unwrap_or(0)
                    == 0,
            );
            if family == "cancel_stream" {
                checks.insert("cancelled", matches!(error, Some(Error::Cancelled)));
            }
            if family == "visible_error_no_retry" {
                checks.insert(
                    "visible_provider_event",
                    summary
                        .counts
                        .get("delta_protocol_event")
                        .copied()
                        .unwrap_or(0)
                        == 1,
                );
            }
            if family == "observer_failure" {
                checks.insert(
                    "partial_observation_no_duplicate",
                    observer.counts.lock().unwrap().text == 3,
                );
            }
        }
        detail = json!({"events":summary.counts,"error":error.map(|e|e.to_string()),"settled_failure":checkpoint.as_ref().and_then(|c|if let State::Failed{error}=&c.state{Some(error)}else{None}),"state":checkpoint.as_ref().map(|c|c.state.kind()),"raw_custom_events":original_custom});
        if audited {
            let counts = observer.counts.lock().unwrap();
            detail["observer"] = json!({"text":counts.text,"provider":counts.provider});
        }
    }
    let requests = server.requests.lock().unwrap().clone();
    let expected_requests = if matches!(family, "retry_429" | "fallback_503") {
        2
    } else {
        1
    };
    checks.insert("exact_http_attempts", requests.len() == expected_requests);
    if family == "fallback_503" {
        checks.insert(
            "fallback_routing",
            requests
                .iter()
                .map(|r| r["model"].as_str().unwrap())
                .collect::<Vec<_>>()
                == vec!["primary", "backup"],
        );
    }
    if family == "custom_events" {
        checks.insert(
            "nested_request_extension",
            requests[0][if protocol == Protocol::Responses {
                "input"
            } else {
                "messages"
            }][0]["consumer_options"]["mode"]
                == "new_feature",
        );
    }
    json!({"case_id":format!("{family}-{}-{repeat}-{count}-{chunk}",name(protocol)),"family":family,"protocol":name(protocol),"repeat":repeat,"text_frames":count,"write_chunk_bytes":chunk,"passed":checks.values().all(|v|*v),"checks":checks,"http_requests":requests.len(),"detail":detail,"elapsed_ms":started.elapsed().as_millis()})
}
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn local_transport_scale() {
    let mut cases = vec![];
    for protocol in [Protocol::Chat, Protocol::Responses, Protocol::Messages] {
        for family in [
            "retry_429",
            "fallback_503",
            "permanent_400",
            "truncated_stream",
            "visible_error_no_retry",
            "malformed_tool",
            "cancel_stream",
            "slow_consumer",
            "observed_slow_consumer",
            "observer_failure",
            "custom_events",
        ] {
            for repeat in 0..8 {
                cases.push((
                    protocol,
                    family,
                    repeat,
                    if matches!(family, "slow_consumer" | "observed_slow_consumer") {
                        512
                    } else {
                        32
                    },
                    if repeat % 2 == 0 { 7 } else { 4096 },
                ));
            }
        }
        for count in [1, 16, 256, 2048] {
            for chunk in [1, 4096] {
                cases.push((protocol, "dense_stream", 0, count, chunk));
            }
        }
    }
    let path = std::env::var("ZHIR_SCALE_LOCAL_REPORT")
        .unwrap_or_else(|_| "/tmp/zhir-scenario-local.json".into());
    let mut jobs = stream::iter(
        cases
            .into_iter()
            .map(|(p, f, r, n, c)| wire_case(p, f, r, n, c)),
    )
    .buffer_unordered(16);
    let mut rows = vec![];
    while let Some(row) = jobs.next().await {
        if row["passed"] != true {
            eprintln!("FAIL {row}");
        }
        rows.push(row);
    }
    save(&path, &rows);
    let failed = rows.iter().filter(|r| r["passed"] != true).count();
    eprintln!(
        "{} local HTTP workflows, {failed} failed; report {path}",
        rows.len()
    );
    assert_eq!(failed, 0);
}

/// Standard protocol grouping works without a consumer normalization hook.
#[tokio::test]
async fn messages_groups_tool_results_without_extensions() {
    use zhir::tool::{RuntimeToolCall, RuntimeToolInput, RuntimeToolOutcome};
    let mut rows = vec![];
    for count in [2, 3, 16, 64] {
        let mut request = empty_request(false);
        let calls: Vec<_> = (0..count)
            .map(|i| RuntimeToolCall {
                id: format!("call-{i}"),
                name: "read".into(),
                input: RuntimeToolInput::Structured(json!({})),
            })
            .collect();
        request.messages.push(Message::Assistant {
            output: calls
                .iter()
                .cloned()
                .map(|call| Output::RuntimeToolCall { call })
                .collect(),
            provider_data: Value::Null,
        });
        for call in calls {
            request.messages.push(Message::RuntimeTool {
                call_id: call.id,
                name: call.name,
                outcome: RuntimeToolOutcome::Success {
                    content: vec![Content::text("ok")],
                    structured: json!("ok"),
                },
            });
        }
        let reply = Reply {
            status: 200,
            body: complete(Protocol::Messages, "done").to_string(),
            sse: false,
            chunk: 4096,
            pause_ms: 0,
        };
        let server = server(vec![reply]).await;
        let plain = http(Protocol::Messages, &server.url, "fixture", "fixture").unwrap();
        let context = || zhir::model::ModelContext {
            run: Default::default(),
            cancellation: Default::default(),
            deltas: None,
        };
        plain.invoke(request, context()).await.unwrap();
        let sent = server.requests.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["messages"].as_array().unwrap().len(), 3);
        let grouped = sent[0]["messages"][2]["content"].as_array().unwrap();
        assert_eq!(grouped.len(), count);
        for (i, part) in grouped.iter().enumerate() {
            assert_eq!(part["tool_use_id"], format!("call-{i}"));
        }
        rows.push(json!({"tool_results":count,"default_grouping_supported":true,"messages":3}));
    }
    save("/tmp/zhir-scenario-grouping.json", &rows);
}

/// A separate application protocol, implemented entirely in this test module.
struct RpcModel {
    url: String,
    client: reqwest::Client,
    capabilities: zhir::model::Capabilities,
}
impl Model for RpcModel {
    fn capabilities(&self) -> &zhir::model::Capabilities {
        &self.capabilities
    }
    fn invoke(
        &self,
        request: ModelRequest,
        context: zhir::model::ModelContext,
    ) -> zhir::BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            context.cancellation.check()?;
            let body = json!({"jsonrpc":"2.0","id":context.run.run_id,"method":"agent.next","params":{"conversation":request.messages,"catalog":request.runtime_tools}});
            let envelope: Value = self
                .client
                .post(&self.url)
                .json(&body)
                .send()
                .await
                .map_err(|e| Error::Protocol(e.to_string()))?
                .json()
                .await
                .map_err(|e| Error::Protocol(e.to_string()))?;
            context.cancellation.check()?;
            require(
                envelope["jsonrpc"] == "2.0"
                    && envelope["id"] == context.run.run_id
                    && envelope.get("error").is_none(),
                "RPC response envelope mismatch",
            )?;
            let value = &envelope["result"];
            if let Some(sink) = context.deltas {
                sink.emit(ModelDelta::ProtocolEvent {
                    output_index: 0,
                    data: json!({"rpc_annotation":value["annotation"]}),
                })
                .await?;
            }
            let mut response = ModelResponse::text(value["text"].as_str().unwrap_or_default());
            if let Some(action) = value.get("action") {
                response.output = vec![Output::RuntimeToolCall {
                    call: zhir::tool::RuntimeToolCall {
                        id: action["id"].as_str().unwrap().into(),
                        name: action["name"].as_str().unwrap().into(),
                        input: zhir::tool::RuntimeToolInput::Structured(action["args"].clone()),
                    },
                }];
            }
            response.provider_data = envelope;
            response.validate()?;
            Ok(response)
        })
    }
}
async fn rpc_case(index: usize, client: reqwest::Client) -> Value {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use zhir::runtime_tools::{FunctionTool, RuntimeToolRegistry};
    use zhir::tool::{Execution, InputSpec, RuntimeTool, RuntimeToolResult, RuntimeToolSpec};
    let tag = format!("RPC_{index:03}_OK");
    let first = json!({"jsonrpc":"2.0","result":{"action":{"id":"read-1","name":"lookup","args":{"key":tag}},"annotation":{"phase":"lookup"}}});
    let final_value = json!({"jsonrpc":"2.0","result":{"text":tag,"annotation":{"phase":"done"}}});
    let replies = [first, final_value]
        .into_iter()
        .map(|value| Reply {
            status: 200,
            body: value.to_string(),
            sse: false,
            chunk: 7,
            pause_ms: 0,
        })
        .collect();
    let server = server(replies).await;
    let counter = Arc::new(AtomicUsize::new(0));
    let observed = counter.clone();
    let value = tag.clone();
    let tool = Arc::new(FunctionTool::new(
        RuntimeToolSpec {
            name: "lookup".into(),
            description: "Read the fixture record".into(),
            input: InputSpec::Structured {
                schema: json!({"type":"object","properties":{"key":{"type":"string"}},"required":["key"],"additionalProperties":false}),
            },
            output_schema: None,
            execution: Execution::default(),
        },
        move |call: zhir::tool::RuntimeToolCall, _| {
            let counter = observed.clone();
            let value = value.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                require(
                    call.input == zhir::tool::RuntimeToolInput::Structured(json!({"key":value})),
                    "RPC tool argument mismatch",
                )?;
                Ok(RuntimeToolResult::json(json!({"value":value})))
            }
        },
    )) as Arc<dyn RuntimeTool>;
    let store = StoreFixture::new(index.is_multiple_of(4)).await.unwrap();
    let model = Arc::new(RpcModel {
        url: server.url.clone(),
        client,
        capabilities: zhir::model::Capabilities {
            usage: false,
            ..Default::default()
        },
    });
    let runtime = Runtime::builder(model)
        .runtime_tools(Arc::new(RuntimeToolRegistry::from_tools([tool]).unwrap()))
        .store(store.api.clone())
        .defaults(|run| run.stream(true))
        .build()
        .unwrap();
    let (result, events) = drain(
        runtime
            .start(zhir_core::run::RunRequest::new(vec![Message::user(
                "look up the fixture record",
            )]))
            .unwrap(),
    )
    .await;
    let checkpoint = result.unwrap();
    let wire = server.requests.lock().unwrap().clone();
    let checks = json!({"completed":matches!(&checkpoint.state,State::Completed{content} if text(content)==tag),"exact_tool_execution":counter.load(Ordering::SeqCst)==1 && checkpoint.metrics.runtime_tool_calls==1,"two_rpc_requests":wire.len()==2 && wire.iter().all(|r|r["jsonrpc"]=="2.0" && r["method"]=="agent.next"),"tool_result_returned":wire[1]["params"]["conversation"].as_array().unwrap().iter().any(|m|m["role"]=="runtime_tool" && m["outcome"]["structured"]["value"]==tag),"events":events.ordered && events.counts.get("delta_protocol_event")==Some(&2),"metadata_preserved":checkpoint.history.messages().iter().any(|m|matches!(m,Message::Assistant{provider_data,..} if provider_data["result"]["annotation"]["phase"]=="done")),"wire_roundtrip":roundtrip(&checkpoint).is_ok(),"valid_trace":store.verify().is_ok()});
    store.close().await;
    json!({"case_id":format!("custom_rpc-{index}"),"passed":checks.as_object().unwrap().values().all(|v|v==true),"checks":checks,"http_requests":wire.len(),"store":if index.is_multiple_of(4) {"sqlite"} else {"memory"}})
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_owned_rpc_model_tool_workflows() {
    let client = reqwest::Client::new();
    let mut jobs = stream::iter((0..32).map(|i| rpc_case(i, client.clone()))).buffer_unordered(8);
    let mut rows = vec![];
    while let Some(row) = jobs.next().await {
        rows.push(row);
    }
    save("/tmp/zhir-scenario-rpc.json", &rows);
    assert!(
        rows.iter().all(|r| r["passed"] == true),
        "RPC integration failures: {rows:#?}"
    );
}
