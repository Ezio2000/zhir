//! Consumer-side acceptance using only public SDK APIs; vendor policies stay here.
#![cfg(all(
    feature = "openai-chat",
    feature = "openai-responses",
    feature = "anthropic"
))]
use base64::Engine as _;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zhir::{
    BoxFuture, Result,
    core::Cancellation,
    error::Error,
    message::{Content, MediaSource, Message, Output, visible_content},
    model::{
        Capabilities, DeltaSink, Model, ModelContext, ModelDelta, ModelOptions, ModelRequest,
        ModelResponse, ResponseFormat, ToolChoice,
    },
    models::{
        HttpModel, ModelConfig, Protocol, ProtocolExtension, anthropic, openai, transport::SseEvent,
    },
    tool::{
        Execution, InputSpec, RuntimeToolCall, RuntimeToolInput, RuntimeToolOutcome,
        RuntimeToolSpec,
    },
};
fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![Message::user("hello")],
        runtime_tools: vec![],
        provider_tools: vec![],
        options: ModelOptions::default(),
        tool_choice: ToolChoice::Auto,
        response_format: None,
        stream: false,
    }
}
fn context() -> ModelContext {
    ModelContext {
        run: zhir::kernel::defaults::context(),
        cancellation: Cancellation::default(),
        deltas: None,
    }
}
fn spec() -> RuntimeToolSpec {
    RuntimeToolSpec {
        name: "observe".into(),
        description: "Return an image".into(),
        input: InputSpec::Structured {
            schema: json!({"type":"object","properties":{},"additionalProperties":false}),
        },
        output_schema: None,
        execution: Execution::default(),
    }
}
fn image() -> Content {
    Content::Image {
        source: MediaSource::Inline {
            mime_type: "image/png".into(),
            base64: base64::engine::general_purpose::STANDARD
                .encode(include_bytes!("fixtures/vision.png")),
        },
    }
}
fn with_tool_image(mut r: ModelRequest) -> ModelRequest {
    r.runtime_tools = vec![spec()];
    r.messages = vec![
        Message::user("Read the alphanumeric code in the tool image. Reply with only that code."),
        Message::Assistant {
            output: vec![Output::RuntimeToolCall {
                call: RuntimeToolCall {
                    id: "observe-1".into(),
                    name: "observe".into(),
                    input: RuntimeToolInput::Structured(json!({})),
                },
            }],
            provider_data: Value::Null,
        },
        Message::RuntimeTool {
            call_id: "observe-1".into(),
            name: "observe".into(),
            outcome: RuntimeToolOutcome::Success {
                content: vec![image()],
                structured: Value::Null,
            },
        },
    ];
    r
}
async fn server(body: Value, sse: bool) -> (String, tokio::task::JoinHandle<Value>) {
    let (url, worker) = batch_server(body, sse, 1).await;
    (
        url,
        tokio::spawn(async move { worker.await.unwrap().remove(0) }),
    )
}
async fn batch_server(
    body: Value,
    sse: bool,
    count: usize,
) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let worker = tokio::spawn(async move {
        let mut requests = Vec::new();
        for _ in 0..count {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buffer = [0u8; 4096];
            let end = loop {
                let n = stream.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(i) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    break i + 4;
                }
            };
            let length = String::from_utf8_lossy(&bytes[..end])
                .lines()
                .find_map(|line| {
                    line.to_lowercase()
                        .strip_prefix("content-length:")
                        .map(|s| s.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            while bytes.len() - end < length {
                let n = stream.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
            }
            let sent = serde_json::from_slice(&bytes[end..end + length]).unwrap();
            let body = if sse {
                body.as_str().unwrap().to_owned()
            } else {
                body.to_string()
            };
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: {}\r\nConnection: close\r\n\r\n{}",body.len(),if sse {"text/event-stream"} else {"application/json"},body).as_bytes()).await.unwrap();
            requests.push(sent);
        }
        requests
    });
    (format!("http://{address}"), worker)
}
#[derive(Default)]
struct Deltas(Mutex<Vec<ModelDelta>>);
impl DeltaSink for Deltas {
    fn emit(&self, delta: ModelDelta) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(delta);
            Ok(())
        })
    }
}
#[tokio::test]
async fn public_extension_boundaries() {
    let mut findings = Vec::new();
    let (url, wire) = server(json!({"output":[],"status":"completed"}), false).await;
    let model = openai::responses::model(ModelConfig::new(url, "fixture", "fixture")).unwrap();
    let mut r = request();
    r.options
        .extra
        .insert("future_option".into(), json!({"strength":"new"}));
    r.messages.push(Message::User {
        content: vec![Content::Opaque {
            provider: "consumer".into(),
            data: json!({"type":"input_image","file_id":"file-test","detail":"original"}),
        }],
    });
    model.invoke(r, context()).await.unwrap();
    let sent = wire.await.unwrap();
    assert_eq!(sent["future_option"]["strength"], "new");
    assert_eq!(sent["input"][1]["content"][0]["file_id"], "file-test");
    findings.push(
        json!({"feature":"new top-level parameters and opaque input blocks","supported":true}),
    );

    let (url, wire) = server(json!({"output":[],"status":"completed"}), false).await;
    let model = openai::responses::model(ModelConfig::new(url, "fixture", "fixture")).unwrap();
    model
        .invoke(with_tool_image(request()), context())
        .await
        .unwrap();
    let sent = wire.await.unwrap();
    let image_preserved = sent["input"][2]["output"].is_array();
    assert!(image_preserved);
    assert_eq!(sent["input"][2]["output"][0]["type"], "input_image");
    assert!(
        sent["input"][2]["output"][0]["image_url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,")
    );
    findings.push(json!({"feature":"Responses image tool output","supported":image_preserved,"wire_content_type":sent["input"][2]["output"][0]["type"]}));

    let (url, wire)=server(json!({"choices":[{"message":{"role":"assistant","content":"ok"},"logprobs":{"content":[{"token":"ok","logprob":-0.1}]}}]}),false).await;
    let model = openai::chat::model(ModelConfig::new(url, "fixture", "fixture"))
        .unwrap()
        .with_extension(|_| Ok(StrictDeclarations));
    let mut r = request();
    r.options.extra.insert("logprobs".into(), json!(true));
    r.runtime_tools = vec![spec()];
    let response = model.invoke(r, context()).await.unwrap();
    let sent = wire.await.unwrap();
    assert_eq!(sent["logprobs"], true);
    let logprobs_preserved = response.provider_data.to_string().contains("logprobs");
    assert!(logprobs_preserved);
    assert_eq!(
        response.provider_data["response"]["choices"][0]["logprobs"]["content"][0]["logprob"],
        -0.1
    );
    assert_eq!(sent["tools"][0]["function"]["strict"], true);
    findings
        .push(json!({"feature":"Chat logprobs response metadata","supported":logprobs_preserved}));
    findings.push(json!({"feature":"strict flag on registered tool declaration","supported":true,"via":"user ProtocolExtension::encode_request"}));

    let model =
        openai::chat::model(ModelConfig::new("http://127.0.0.1:1", "fixture", "fixture")).unwrap();
    let mut r = request();
    r.options.extra.insert("tools".into(), json!([]));
    assert!(matches!(
        model.invoke(r, context()).await,
        Err(Error::Invalid(_))
    ));
    findings.push(
        json!({"feature":"consumer transformation after protocol encoding","supported":true,"extra_override_rejected":true}),
    );

    let stream = "data: {\"type\":\"response.future_feature.delta\",\"delta\":\"new\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[],\"usage\":{}}}\n\n";
    let (url, wire) = server(json!(stream), true).await;
    let model = openai::responses::model(ModelConfig::new(url, "fixture", "fixture")).unwrap();
    let observed = Arc::new(Deltas::default());
    let mut ctx = context();
    ctx.deltas = Some(observed.clone());
    let mut r = request();
    r.stream = true;
    model.invoke(r, ctx).await.unwrap();
    wire.await.unwrap();
    let preserved=observed.0.lock().unwrap().iter().any(|d|matches!(d,ModelDelta::ProtocolEvent {data,..} if data["data"]["type"]=="response.future_feature.delta"));
    assert!(preserved);
    findings.push(json!({"feature":"unknown SSE event forwarding","supported":preserved}));
    println!("{}", serde_json::to_string_pretty(&findings).unwrap());
    if let Ok(path) = std::env::var("ZHIR_AUDIT_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&findings).unwrap()).unwrap();
    }
}
#[tokio::test]
async fn multimodal_tool_results_preserve_order_for_both_responses_tool_kinds() {
    for freeform in [false, true] {
        let mut r = with_tool_image(request());
        if freeform {
            r.runtime_tools[0].input = InputSpec::Freeform { format: None };
            if let Message::Assistant { output, .. } = &mut r.messages[1]
                && let Output::RuntimeToolCall { call } = &mut output[0]
            {
                call.input = RuntimeToolInput::Freeform("observe".into());
            }
        }
        if let Message::RuntimeTool { outcome, .. } = &mut r.messages[2] {
            *outcome = RuntimeToolOutcome::Success {
                content: vec![
                    Content::text("caption"),
                    image(),
                    Content::Opaque {
                        provider: "consumer".into(),
                        data: json!({"type":"input_file","file_id":"file-fixture"}),
                    },
                ],
                structured: Value::Null,
            };
        }
        let (url, wire) = server(json!({"output":[],"status":"completed"}), false).await;
        let model = openai::responses::model(ModelConfig::new(url, "fixture", "fixture")).unwrap();
        model.invoke(r, context()).await.unwrap();
        let sent = wire.await.unwrap();
        assert_eq!(
            sent["input"][2]["type"],
            if freeform {
                "custom_tool_call_output"
            } else {
                "function_call_output"
            }
        );
        let parts = sent["input"][2]["output"].as_array().unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], json!({"type":"input_text","text":"caption"}));
        assert_eq!(parts[1]["type"], "input_image");
        assert_eq!(
            parts[2],
            json!({"type":"input_file","file_id":"file-fixture"})
        );
    }
}
struct StrictDeclarations;
impl ProtocolExtension for StrictDeclarations {
    fn encode_request(
        &mut self,
        protocol: Protocol,
        _: &ModelRequest,
        body: &mut Value,
    ) -> Result<()> {
        assert_eq!(protocol, Protocol::Chat);
        for tool in body["tools"].as_array_mut().unwrap() {
            tool["function"]["strict"] = json!(true);
        }
        Ok(())
    }
}
struct AssistantPrefix;
impl ProtocolExtension for AssistantPrefix {
    fn encode_request(&mut self, _: Protocol, _: &ModelRequest, body: &mut Value) -> Result<()> {
        let last = body["messages"].as_array_mut().unwrap().last_mut().unwrap();
        assert_eq!(last["role"], "assistant");
        let text: String = last["content"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|part| part["text"].as_str())
            .collect();
        last["content"] = json!(text);
        last["prefix"] = json!(true);
        Ok(())
    }
}
#[derive(Default)]
struct Session {
    tag: String,
    fragments: usize,
}
impl ProtocolExtension for Session {
    fn encode_request(
        &mut self,
        _: Protocol,
        request: &ModelRequest,
        body: &mut Value,
    ) -> Result<()> {
        self.tag = request.options.extra["tag"].as_str().unwrap().into();
        body["messages"][0]["consumer_tag"] = json!(self.tag);
        Ok(())
    }
    fn decode_event(&mut self, _: Protocol, event: &mut SseEvent) -> Result<Vec<ModelDelta>> {
        if event.data != "[DONE]" {
            let mut value: Value = serde_json::from_str(&event.data).unwrap();
            if let Some(text) = value["choices"][0]["delta"].get("new_text").cloned() {
                self.fragments += 1;
                value["choices"][0]["delta"]["content"] = text;
                event.data = value.to_string();
            }
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
        response.provider_data["consumer"] = json!({"tag":self.tag,"fragments":self.fragments});
        Ok(response)
    }
}
struct Rendezvous {
    barrier: Arc<tokio::sync::Barrier>,
    seen: std::sync::atomic::AtomicBool,
}
impl DeltaSink for Rendezvous {
    fn emit(&self, _: ModelDelta) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if !self.seen.swap(true, std::sync::atomic::Ordering::SeqCst) {
                self.barrier.wait().await;
            }
            Ok(())
        })
    }
}
#[tokio::test]
async fn extension_sessions_are_isolated_during_overlapping_calls() {
    let stream = concat!(
        "data: {\"choices\":[{\"delta\":{\"new_text\":\"A\"},\"logprobs\":{\"content\":[{\"token\":\"A\"}]}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"new_text\":\"B\"},\"logprobs\":{\"content\":[{\"token\":\"B\"}]},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );
    let (url, wire) = batch_server(json!(stream), true, 2).await;
    let model = openai::chat::model(ModelConfig::new(url, "fixture", "fixture"))
        .unwrap()
        .with_extension(|_| Ok(Session::default()));
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let invoke = |tag: &'static str| {
        let mut r = request();
        r.stream = true;
        r.options.extra.insert("tag".into(), json!(tag));
        let mut ctx = context();
        ctx.deltas = Some(Arc::new(Rendezvous {
            barrier: barrier.clone(),
            seen: false.into(),
        }));
        model.invoke(r, ctx)
    };
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(invoke("first"), invoke("second"))
    })
    .await
    .unwrap();
    for (response, tag) in [(a.unwrap(), "first"), (b.unwrap(), "second")] {
        assert_eq!(output(&response), "AB");
        assert_eq!(
            response.provider_data["consumer"],
            json!({"tag":tag,"fragments":2})
        );
        assert_eq!(
            response.provider_data["response"]["choices"][0]["logprobs"]["content"],
            json!([{"token":"A"},{"token":"B"}])
        );
    }
    let sent = wire.await.unwrap();
    assert_eq!(sent.len(), 2);
    for body in sent {
        assert_eq!(body["messages"][0]["consumer_tag"], body["tag"]);
    }
}
struct ResponseMapping {
    invalid: bool,
}
impl ProtocolExtension for ResponseMapping {
    fn decode_response(
        &mut self,
        _: Protocol,
        raw: &Value,
        decoded: Result<ModelResponse>,
    ) -> Result<ModelResponse> {
        assert!(decoded.is_err()); // New response shape, standard codec cannot decode it.
        let mut response = ModelResponse::text(raw["answer"].as_str().unwrap());
        if self.invalid {
            response.output.push(Output::RuntimeToolCall {
                call: RuntimeToolCall {
                    id: "".into(),
                    name: "observe".into(),
                    input: RuntimeToolInput::Structured(json!({})),
                },
            });
        }
        Ok(response)
    }
}
#[tokio::test]
async fn response_mapping_handles_new_shapes_and_still_validates_results() {
    for invalid in [false, true] {
        let (url, wire) = server(json!({"answer":"mapped"}), false).await;
        let model = openai::chat::model(ModelConfig::new(url, "fixture", "fixture"))
            .unwrap()
            .with_extension(move |_| Ok(ResponseMapping { invalid }));
        let response = model.invoke(request(), context()).await;
        if invalid {
            assert!(response.is_err());
        } else {
            assert_eq!(output(&response.unwrap()), "mapped");
        }
        wire.await.unwrap();
    }
}
struct RejectRequest;
impl ProtocolExtension for RejectRequest {
    fn encode_request(&mut self, _: Protocol, _: &ModelRequest, _: &mut Value) -> Result<()> {
        Err(Error::Invalid("consumer rejected request".into()))
    }
}
struct RejectEvent;
impl ProtocolExtension for RejectEvent {
    fn decode_event(&mut self, _: Protocol, _: &mut SseEvent) -> Result<Vec<ModelDelta>> {
        Err(Error::Protocol("consumer rejected event".into()))
    }
}
#[tokio::test]
async fn extension_errors_abort_before_http_or_response_completion() {
    let model = openai::chat::model(ModelConfig::new("http://127.0.0.1:1", "fixture", "fixture"))
        .unwrap()
        .with_extension(|_| Ok(RejectRequest));
    assert!(
        matches!(model.invoke(request(), context()).await, Err(Error::Invalid(s)) if s=="consumer rejected request")
    );
    let (url, wire) = server(
        json!("data: {\"choices\":[{\"delta\":{\"content\":\"hidden\"}}]}\n\ndata: [DONE]\n\n"),
        true,
    )
    .await;
    let model = openai::chat::model(ModelConfig::new(url, "fixture", "fixture"))
        .unwrap()
        .with_extension(|_| Ok(RejectEvent));
    let observed = Arc::new(Deltas::default());
    let mut ctx = context();
    ctx.deltas = Some(observed.clone());
    let mut r = request();
    r.stream = true;
    assert!(
        matches!(model.invoke(r, ctx).await, Err(Error::Protocol(s)) if s=="consumer rejected event")
    );
    assert!(observed.0.lock().unwrap().is_empty());
    wire.await.unwrap();
}
#[tokio::test]
async fn response_metadata_is_retained_but_only_assistant_output_is_replayed() {
    for protocol in [Protocol::Chat, Protocol::Responses, Protocol::Messages] {
        let raw = match protocol {
            Protocol::Chat => {
                json!({"choices":[{"message":{"role":"assistant","content":"ok","future_message":"retained"},"logprobs":{"content":[]}}],"future_metadata":123})
            }
            Protocol::Responses => {
                json!({"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"status":"completed","future_metadata":123})
            }
            Protocol::Messages => {
                json!({"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","future_metadata":123})
            }
        };
        let (url, wire) = batch_server(raw.clone(), false, 2).await;
        let config = ModelConfig::new(url, "fixture", "fixture");
        let model = match protocol {
            Protocol::Chat => openai::chat::model(config),
            Protocol::Responses => openai::responses::model(config),
            Protocol::Messages => anthropic::messages::model(config),
        }
        .unwrap();
        let response = model.invoke(request(), context()).await.unwrap();
        assert_eq!(response.provider_data["response"], raw);
        let mut r = request();
        r.messages.push(Message::Assistant {
            output: response.output,
            provider_data: response.provider_data,
        });
        model.invoke(r, context()).await.unwrap();
        let sent = wire.await.unwrap();
        assert!(!sent[1].to_string().contains("future_metadata"));
        assert!(!sent[1].to_string().contains("logprobs"));
        if protocol == Protocol::Chat {
            assert!(sent[1].to_string().contains("future_message"));
        }
    }
}
fn live_model(protocol: Protocol, key: &str, beta: bool) -> HttpModel {
    let base = match protocol {
        Protocol::Messages => "https://api.deepseek.com/anthropic/v1",
        _ if beta => "https://api.deepseek.com/beta",
        _ => "https://api.deepseek.com",
    };
    let mut config = ModelConfig::new(base, key, "deepseek-flash");
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "anthropic-beta",
        reqwest::header::HeaderValue::from_static("files-api-2025-04-14"),
    );
    config.client = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();
    match protocol {
        Protocol::Chat => openai::chat::model(config),
        Protocol::Responses => openai::responses::model(config),
        Protocol::Messages => anthropic::messages::model(config),
    }
    .unwrap()
}
fn live_request(protocol: Protocol, effort: &str) -> ModelRequest {
    let mut r = request();
    r.options.max_output_tokens = Some(1024);
    match protocol {
        Protocol::Chat => {
            r.options.max_output_tokens = None;
            r.options.extra.insert("max_tokens".into(), json!(1024));
            r.options.extra.insert(
                "thinking".into(),
                json!({"type":if effort=="none" {"disabled"} else {"enabled"}}),
            );
            if effort != "none" {
                r.options
                    .extra
                    .insert("reasoning_effort".into(), json!(effort));
            }
        }
        Protocol::Responses => {
            r.options
                .extra
                .insert("reasoning".into(), json!({"effort":effort}));
        }
        Protocol::Messages => {
            r.options.extra.insert(
                "thinking".into(),
                json!({"type":if effort=="none" {"disabled"} else {"enabled"}}),
            );
            if effort != "none" {
                r.options
                    .extra
                    .insert("output_config".into(), json!({"effort":effort}));
            }
        }
    }
    r
}
fn output(response: &ModelResponse) -> String {
    visible_content(&response.output)
        .iter()
        .filter_map(Content::as_text)
        .collect()
}
async fn invoke_check(
    protocol: Protocol,
    key: &str,
    r: ModelRequest,
    expected: &str,
    beta: bool,
) -> Value {
    invoke_model_check(live_model(protocol, key, beta), r, expected).await
}
async fn invoke_model_check(model: HttpModel, r: ModelRequest, expected: &str) -> Value {
    let started = Instant::now();
    let deltas = Arc::new(Deltas::default());
    let mut ctx = context();
    ctx.deltas = Some(deltas.clone());
    match model.invoke(r, ctx).await {
        Ok(response) => {
            let text = output(&response);
            json!({"passed":text.trim()==expected,"text":text,"model":response.model_id,"response_id":response.response_id,"usage":response.usage,"reasoning_deltas":deltas.0.lock().unwrap().iter().filter(|d|matches!(d,ModelDelta::Reasoning {..})).count(),"elapsed_ms":started.elapsed().as_millis()})
        }
        Err(error) => {
            json!({"passed":false,"error":error.to_string(),"elapsed_ms":started.elapsed().as_millis()})
        }
    }
}
#[tokio::test]
#[ignore = "paid consumer-side capability audit; requires DEEPSEEK_API_KEY"]
async fn live_consumer_capability_audit() {
    let key = std::env::var("DEEPSEEK_API_KEY").unwrap();
    let report = std::env::var("ZHIR_LIVE_AUDIT_REPORT")
        .unwrap_or_else(|_| "/tmp/zhir-capability-live.json".into());
    let mut rows = Vec::new();
    let client = reqwest::Client::new();
    let bytes = include_bytes!("fixtures/vision.png");
    let upload: Value = client
        .post("https://api.deepseek.com/files")
        .bearer_auth(&key)
        .multipart(
            reqwest::multipart::Form::new()
                .text("purpose", "user_data")
                .text("expires_after[anchor]", "created_at")
                .text("expires_after[seconds]", "3600")
                .part(
                    "file",
                    reqwest::multipart::Part::bytes(bytes.to_vec())
                        .file_name("zhir-audit.png")
                        .mime_str("image/png")
                        .unwrap(),
                ),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let file_id = upload["id"].as_str().expect("uploaded image id").to_owned();
    rows.push(json!({"scenario":"user_files_upload","passed":true,"file_id":file_id}));
    for protocol in [Protocol::Chat, Protocol::Responses, Protocol::Messages] {
        for scenario in [
            "vision_inline",
            "vision_file_id",
            "thinking_high",
            "thinking_max",
        ] {
            let mut r = live_request(
                protocol,
                if scenario == "thinking_high" {
                    "high"
                } else if scenario == "thinking_max" {
                    "max"
                } else {
                    "none"
                },
            );
            r.stream = true;
            let expected = if scenario.starts_with("thinking") {
                "323"
            } else {
                "K7X42"
            };
            if scenario.starts_with("thinking") {
                r.messages = vec![Message::user(
                    "What is 17 times 19? Reply with only the integer.",
                )];
            } else {
                let content = if scenario == "vision_inline" {
                    image()
                } else {
                    Content::Opaque {
                        provider: "consumer".into(),
                        data: match protocol {
                            Protocol::Chat => json!({"type":"file","file_id":file_id}),
                            Protocol::Responses => json!({"type":"input_image","file_id":file_id}),
                            Protocol::Messages => {
                                json!({"type":"image","source":{"type":"file","file_id":file_id}})
                            }
                        },
                    }
                };
                r.messages = vec![Message::User {
                    content: vec![
                        Content::text(
                            "Read the alphanumeric code printed in this image. Reply with only the code.",
                        ),
                        content,
                    ],
                }];
            }
            eprintln!("RUN {protocol:?}/{scenario}");
            let mut result = invoke_check(protocol, &key, r, expected, false).await;
            if scenario.starts_with("thinking")
                && result["reasoning_deltas"].as_u64().unwrap_or(0) == 0
            {
                result["passed"] = json!(false);
            }
            result["protocol"] = json!(format!("{protocol:?}"));
            result["scenario"] = json!(scenario);
            eprintln!("{}", result);
            rows.push(result);
            std::fs::write(&report, serde_json::to_vec_pretty(&rows).unwrap()).unwrap();
        }
    }
    for protocol in [Protocol::Responses, Protocol::Messages] {
        let r = with_tool_image(live_request(protocol, "none"));
        let mut result = invoke_check(protocol, &key, r, "K7X42", false).await;
        result["protocol"] = json!(format!("{protocol:?}"));
        result["scenario"] = json!("image_from_tool");
        eprintln!("{result}");
        rows.push(result);
    }
    let mut r = live_request(Protocol::Responses, "none");
    r.response_format = Some(ResponseFormat::Schema {
        name: "code".into(),
        schema: json!({"type":"object","properties":{"code":{"type":"string","enum":["SCHEMA_OK"]}},"required":["code"],"additionalProperties":false}),
    });
    r.messages = vec![Message::user("Return JSON with code SCHEMA_OK.")];
    let response = live_model(Protocol::Responses, &key, false)
        .invoke(r, context())
        .await;
    rows.push(match response {Ok(response)=>json!({"scenario":"responses_json_schema","passed":serde_json::from_str::<Value>(&output(&response)).ok()==Some(json!({"code":"SCHEMA_OK"})),"usage":response.usage}),Err(e)=>json!({"scenario":"responses_json_schema","passed":false,"error":e.to_string()})});
    let mut r = live_request(Protocol::Chat, "none");
    r.messages = vec![
        Message::user("Complete the next line with the integer 42 only."),
        Message::Assistant {
            output: vec![Output::text("Answer: ")],
            provider_data: Value::Null,
        },
    ];
    let model = live_model(Protocol::Chat, &key, true).with_extension(|_| Ok(AssistantPrefix));
    let mut result = invoke_model_check(model, r, "42").await;
    result["scenario"] = json!("prefix_via_user_extension");
    eprintln!("{result}");
    rows.push(result);
    let mut r = live_request(Protocol::Chat, "none");
    r.runtime_tools = vec![RuntimeToolSpec {
        input: InputSpec::Structured {
            schema: json!({"type":"object","properties":{"code":{"type":"string","enum":["STRICT_OK"]}},"required":["code"],"additionalProperties":false}),
        },
        ..spec()
    }];
    r.tool_choice = ToolChoice::RuntimeTool {
        name: "observe".into(),
    };
    r.messages = vec![Message::user("Call observe with code STRICT_OK.")];
    let response = live_model(Protocol::Chat, &key, true)
        .with_extension(|_| Ok(StrictDeclarations))
        .invoke(r, context())
        .await;
    rows.push(match response {
        Ok(response) => json!({"scenario":"strict_tool_via_user_extension","passed":response.output.iter().any(|o| matches!(o,Output::RuntimeToolCall{call} if call.name=="observe" && call.input==RuntimeToolInput::Structured(json!({"code":"STRICT_OK"})))),"response_id":response.response_id,"usage":response.usage}),
        Err(e) => json!({"scenario":"strict_tool_via_user_extension","passed":false,"error":e.to_string()}),
    });
    let fim:Value=client.post("https://api.deepseek.com/beta/completions").bearer_auth(&key).json(&json!({"model":"deepseek-flash","prompt":"def answer():\n    return ","suffix":"\n# answer returns the integer forty two\n","max_tokens":32,"thinking":{"type":"disabled"}})).send().await.unwrap().json().await.unwrap();
    rows.push(json!({"scenario":"user_fim_client","passed":fim.pointer("/choices/0/text").and_then(Value::as_str).is_some_and(|text|text.contains("42")),"response":fim}));
    for (scenario, url) in [
        (
            "user_files_metadata",
            format!("https://api.deepseek.com/files/{file_id}"),
        ),
        (
            "user_files_list",
            "https://api.deepseek.com/files?limit=100".into(),
        ),
    ] {
        let value: Value = client
            .get(url)
            .bearer_auth(&key)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let found = value["id"] == file_id
            || value["data"]
                .as_array()
                .is_some_and(|items| items.iter().any(|item| item["id"] == file_id));
        rows.push(json!({"scenario":scenario,"passed":found}));
    }
    let deleted: Value = client
        .delete(format!("https://api.deepseek.com/files/{file_id}"))
        .bearer_auth(&key)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    rows.push(json!({"scenario":"user_files_delete","passed":deleted["deleted"]==true}));
    std::fs::write(&report, serde_json::to_vec_pretty(&rows).unwrap()).unwrap();
    eprintln!(
        "Capability acceptance completed; {} cases: {report}",
        rows.len()
    );
    let failed: Vec<_> = rows.iter().filter(|row| row["passed"] != true).collect();
    assert!(failed.is_empty(), "live acceptance failures: {failed:#?}");
}

// A user-defined model can carry new semantics through the unchanged kernel.
struct ConsumerModel(Capabilities);
impl Model for ConsumerModel {
    fn capabilities(&self) -> &Capabilities {
        &self.0
    }
    fn invoke(
        &self,
        _: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            if let Some(sink) = context.deltas {
                sink.emit(ModelDelta::ProtocolEvent {
                    output_index: 0,
                    data: json!({"type":"consumer.new_event","payload":123}),
                })
                .await?;
            }
            let mut response = ModelResponse::text("custom model completed");
            response.output.push(Output::Content {
                content: Content::Opaque {
                    provider: "consumer".into(),
                    data: json!({"type":"consumer.new_output","payload":123}),
                },
            });
            response.provider_data = json!({"new_response_metadata":456});
            Ok(response)
        })
    }
}
#[tokio::test]
async fn user_model_new_events_and_content_survive_kernel_and_wire() {
    use futures::StreamExt;
    let runtime =
        zhir::Runtime::builder(Arc::new(ConsumerModel(zhir_testing::model_capabilities())))
            .defaults(|run| run.stream(true))
            .build()
            .unwrap();
    let mut invocation = runtime
        .start(zhir::RunRequest::new(vec![Message::user("go")]))
        .unwrap();
    let mut events = invocation.events().unwrap();
    let mut found = false;
    while let Some(event) = events.next().await {
        if matches!(event.data,zhir::run::EventData::ModelDelta {delta:ModelDelta::ProtocolEvent {ref data,..}} if data["type"]=="consumer.new_event")
        {
            found = true;
        }
    }
    let checkpoint = invocation.result().await.unwrap().into_checkpoint();
    assert!(found);
    let decoded =
        zhir::wire::decode_checkpoint(&zhir::wire::encode_checkpoint(&checkpoint).unwrap())
            .unwrap();
    assert!(decoded.history.messages().iter().any(|message|matches!(message,Message::Assistant {provider_data,..} if provider_data["new_response_metadata"]==456)));
}
