#![cfg(all(
    feature = "openai-chat",
    feature = "openai-responses",
    feature = "anthropic"
))]
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zhir_core::{
    BoxFuture, Cancellation, Result,
    message::{Content, Message, Output},
    model::{DeltaSink, Model, ModelContext, ModelDelta, ModelOptions, ModelRequest, ToolChoice},
    run::RunContext,
};
use zhir_models::{ModelConfig, anthropic, openai};
async fn server(body: String, status: &str, sse: bool) -> (String, tokio::task::JoinHandle<Value>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let status = status.to_owned();
    let worker = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 4096];
        let end;
        loop {
            let n = stream.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buffer[..n]);
            if let Some(i) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                end = i + 4;
                break;
            }
        }
        let headers = String::from_utf8_lossy(&bytes[..end]);
        let size = headers
            .lines()
            .find_map(|l| {
                l.to_lowercase()
                    .strip_prefix("content-length:")
                    .map(|s| s.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        while bytes.len() - end < size {
            let n = stream.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buffer[..n]);
        }
        let request = serde_json::from_slice(&bytes[end..end + size]).unwrap();
        stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: {}\r\nConnection: close\r\n\r\n",body.len(),if sse {"text/event-stream"} else {"application/json"}).as_bytes()).await.unwrap();
        for chunk in body.as_bytes().chunks(7) {
            stream.write_all(chunk).await.unwrap();
        }
        request
    });
    (format!("http://{address}/v1"), worker)
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
fn request(stream: bool) -> ModelRequest {
    ModelRequest {
        messages: vec![Message::system("Be concise"), Message::user("hello")],
        runtime_tools: vec![],
        provider_tools: vec![],
        options: ModelOptions::default(),
        tool_choice: ToolChoice::Auto,
        response_format: None,
        stream,
    }
}
fn context(deltas: Arc<Deltas>) -> ModelContext {
    ModelContext {
        run: RunContext::new("model-test", 0),
        cancellation: Cancellation::default(),
        deltas: Some(deltas),
    }
}
fn sse(values: &[Value]) -> String {
    values.iter().map(|v| format!("data: {v}\n\n")).collect()
}
#[tokio::test]
async fn chat_stream_assembles_calls_unicode_and_usage() {
    let body = sse(&[
        json!({"id":"r1","model":"fixture","choices":[{"delta":{"content":"你好"}}]}),
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"echo","arguments":"{\"text\":"}}]}}]}),
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"ok\"}"}}]},"finish_reason":"tool_calls"}]}),
        json!({"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8,"prompt_tokens_details":{"cached_tokens":3}}}),
    ]) + "data: [DONE]\n\n";
    let (url, worker) = server(body, "200 OK", true).await;
    let model = openai::chat::model(ModelConfig::new(url, "test-key", "fixture")).unwrap();
    let deltas = Arc::new(Deltas::default());
    let response = model
        .invoke(request(true), context(deltas.clone()))
        .await
        .unwrap();
    assert_eq!(response.usage.total_tokens, Some(8));
    assert_eq!(response.usage.input_tokens, Some(5));
    assert_eq!(response.usage.cache_read_tokens, Some(3));
    assert_eq!(response.output[0], Output::text("你好"));
    assert!(matches!(&response.output[1],Output::RuntimeToolCall {call} if call.id=="c1"));
    assert_eq!(
        deltas
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|d| !matches!(d, ModelDelta::ProtocolEvent { .. }))
            .count(),
        4
    );
    let sent = worker.await.unwrap();
    assert_eq!(sent["stream_options"]["include_usage"], true);
}
#[tokio::test]
async fn responses_preserves_provider_output_and_continuation_history() {
    let raw = json!({"id":"resp","model":"fixture","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"image"}]},{"type":"image_generation_call","id":"img","status":"completed","result":"aGVsbG8="}],"usage":{"input_tokens":2,"output_tokens":4}});
    let (url, worker) = server(raw.to_string(), "200 OK", false).await;
    let model = openai::responses::model(ModelConfig::new(url, "test", "fixture"))
        .unwrap()
        .with_extension(|_| Ok(image_fixture()));
    let mut input = request(false);
    input.provider_tools = vec![image_spec()];
    let response = model
        .invoke(input, context(Arc::new(Deltas::default())))
        .await
        .unwrap();
    assert!(
        matches!(&response.output[1],Output::ProviderToolCall {call} if matches!(&call.output[0],Content::Image {..}))
    );
    worker.await.unwrap();
    let (url, worker) = server(
        json!({"output":[],"status":"completed"}).to_string(),
        "200 OK",
        false,
    )
    .await;
    let model = openai::responses::model(ModelConfig::new(url, "test", "fixture"))
        .unwrap()
        .with_extension(|_| Ok(image_fixture()));
    let mut next = request(false);
    next.messages.push(Message::Assistant {
        output: response.output,
        provider_data: response.provider_data,
    });
    model
        .invoke(next, context(Arc::new(Deltas::default())))
        .await
        .unwrap();
    let sent = worker.await.unwrap();
    assert_eq!(sent["input"][3]["type"], "image_generation_call");
}
#[tokio::test]
async fn anthropic_stream_assembles_tool_blocks_and_usage() {
    let body = sse(&[
        json!({"type":"message_start","message":{"id":"m1","model":"fixture","role":"assistant","content":[],"usage":{"input_tokens":5,"cache_read_input_tokens":9,"cache_creation_input_tokens":4}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"c1","name":"echo","input":{}}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"x\":1}"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":2}}),
        json!({"type":"message_stop"}),
    ]);
    let (url, worker) = server(body, "200 OK", true).await;
    let model = anthropic::messages::model(ModelConfig::new(url, "test", "fixture")).unwrap();
    let response = model
        .invoke(request(true), context(Arc::new(Deltas::default())))
        .await
        .unwrap();
    assert_eq!(response.usage.input_tokens, Some(18));
    assert_eq!(response.usage.total_tokens, Some(20));
    assert_eq!(response.usage.cache_read_tokens, Some(9));
    assert_eq!(response.usage.cache_write_tokens, Some(4));
    assert!(matches!(&response.output[0],Output::RuntimeToolCall {call} if call.name=="echo"));
    let sent = worker.await.unwrap();
    assert_eq!(sent["system"][0]["text"], "Be concise");
    assert_eq!(sent["messages"][0]["role"], "user");
}
#[tokio::test]
async fn truncated_stream_and_http_errors_are_explicit() {
    let (url, worker) = server(
        "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n".into(),
        "200 OK",
        true,
    )
    .await;
    let model = openai::chat::model(ModelConfig::new(url, "test", "fixture")).unwrap();
    assert!(
        model
            .invoke(request(true), context(Arc::new(Deltas::default())))
            .await
            .is_err()
    );
    worker.await.unwrap();
    let (url, worker) = server(
        "{\"error\":\"busy\"}".into(),
        "429 Too Many Requests",
        false,
    )
    .await;
    let model = openai::chat::model(ModelConfig::new(url, "test", "fixture")).unwrap();
    assert!(
        matches!(model.invoke(request(false),context(Arc::new(Deltas::default()))).await,Err(zhir_core::error::Error::Model(e)) if e.retryable && e.code=="http_429")
    );
    worker.await.unwrap();
}
#[test]
fn sse_handles_split_unicode_crlf_and_multiline_data() {
    let input = "data: 你好\r\ndata: world\r\n\r\n";
    let mut decoder = zhir_models::transport::SseDecoder::default();
    let mut events = Vec::new();
    for byte in input.as_bytes() {
        events.extend(decoder.push(&[*byte]).unwrap());
    }
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].data, "你好\nworld");
}

#[tokio::test]
async fn responses_freeform_call_result_keeps_its_protocol_type() {
    use zhir_core::tool::{RuntimeToolCall, RuntimeToolInput, RuntimeToolResult};
    let (url, worker) = server(
        json!({"output":[],"status":"completed"}).to_string(),
        "200 OK",
        false,
    )
    .await;
    let model = openai::responses::model(ModelConfig::new(url, "test", "fixture")).unwrap();
    let mut next = request(false);
    next.messages.push(Message::Assistant {
        output: vec![Output::RuntimeToolCall {
            call: RuntimeToolCall {
                id: "custom-1".into(),
                name: "code".into(),
                input: RuntimeToolInput::Freeform("print(1)".into()),
            },
        }],
        provider_data: Value::Null,
    });
    next.messages.push(Message::RuntimeTool {
        call_id: "custom-1".into(),
        name: "code".into(),
        outcome: RuntimeToolResult::json(json!("1")).outcome,
    });
    model
        .invoke(next, context(Arc::new(Deltas::default())))
        .await
        .unwrap();
    let sent = worker.await.unwrap();
    assert_eq!(sent["input"][2]["type"], "custom_tool_call");
    assert_eq!(sent["input"][3]["type"], "custom_tool_call_output");
    assert_eq!(sent["input"][3]["output"], "1");
}

#[tokio::test]
async fn anthropic_nonstream_usage_includes_cached_input() {
    let raw = json!({"id":"cache","model":"fixture","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"cache_read_input_tokens":7,"cache_creation_input_tokens":11,"output_tokens":2}});
    let (url, worker) = server(raw.to_string(), "200 OK", false).await;
    let model = anthropic::messages::model(ModelConfig::new(url, "test", "fixture")).unwrap();
    let response = model
        .invoke(request(false), context(Arc::new(Deltas::default())))
        .await
        .unwrap();
    assert_eq!(response.usage.input_tokens, Some(21));
    assert_eq!(response.usage.total_tokens, Some(23));
    assert_eq!(response.usage.cache_read_tokens, Some(7));
    assert_eq!(response.usage.cache_write_tokens, Some(11));
    worker.await.unwrap();
}

#[tokio::test]
async fn every_protocol_forwards_unknown_sse_frames_with_their_fields() {
    let unknown = "event: future.feature\r\nid: frame-7\r\nretry: 1000\r\ndata: {\"type\":\"future.feature\",\"payload\":{\"value\":7}}\r\n\r\n";
    for protocol in [
        zhir_models::Protocol::Chat,
        zhir_models::Protocol::Responses,
        zhir_models::Protocol::Messages,
    ] {
        let terminal = match protocol {
            zhir_models::Protocol::Chat => {
                "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
            }
            zhir_models::Protocol::Responses => {
                "data: {\"type\":\"response.completed\",\"response\":{\"output\":[],\"status\":\"completed\"}}\n\n"
            }
            zhir_models::Protocol::Messages => {
                "data: {\"type\":\"message_start\",\"message\":{\"content\":[]}}\n\ndata: {\"type\":\"message_stop\"}\n\n"
            }
        };
        let (url, worker) = server(format!("{unknown}{terminal}"), "200 OK", true).await;
        let config = ModelConfig::new(url, "fixture", "fixture");
        let model = match protocol {
            zhir_models::Protocol::Chat => openai::chat::model(config),
            zhir_models::Protocol::Responses => openai::responses::model(config),
            zhir_models::Protocol::Messages => anthropic::messages::model(config),
        }
        .unwrap();
        let deltas = Arc::new(Deltas::default());
        model
            .invoke(request(true), context(deltas.clone()))
            .await
            .unwrap();
        assert!(deltas.0.lock().unwrap().iter().any(|d|matches!(d, ModelDelta::ProtocolEvent {data,..} if *data==json!({"event":"future.feature","id":"frame-7","retry":1000,"data":{"type":"future.feature","payload":{"value":7}}}))));
        worker.await.unwrap();
    }
}

#[test]
fn sse_cr_empty_data_and_unfinished_frames_follow_event_boundaries() {
    use zhir_models::transport::SseDecoder;
    let mut parser = SseDecoder::default();
    let mut frames = Vec::new();
    for byte in b": comment\revent: empty\rdata\r\rdata: next\r\ndata: line\r\n\r\ndata: unfinished"
    {
        frames.extend(parser.push(&[*byte]).unwrap());
    }
    frames.extend(parser.finish().unwrap());
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].data, "");
    assert_eq!(frames[0].event.as_deref(), Some("empty"));
    assert_eq!(frames[1].data, "next\nline");
    assert_eq!(frames[1].event, None);
}

fn protocol_model(protocol: zhir_models::Protocol, url: String) -> zhir_models::HttpModel {
    let config = ModelConfig::new(url, "test", "fixture");
    match protocol {
        zhir_models::Protocol::Chat => openai::chat::model(config),
        zhir_models::Protocol::Responses => openai::responses::model(config),
        zhir_models::Protocol::Messages => anthropic::messages::model(config),
    }
    .unwrap()
}
async fn capture_request(protocol: zhir_models::Protocol, request: ModelRequest) -> Value {
    use zhir_models::Protocol;
    let raw = match protocol {
        Protocol::Chat => {
            json!({"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]})
        }
        Protocol::Responses => json!({"output":[],"status":"completed"}),
        Protocol::Messages => json!({"content":[],"stop_reason":"end_turn"}),
    };
    let body = if request.stream {
        assert_eq!(protocol, Protocol::Chat);
        sse(&[json!({"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]})])
            + "data: [DONE]\n\n"
    } else {
        raw.to_string()
    };
    let (url, worker) = server(body, "200 OK", request.stream).await;
    protocol_model(protocol, url)
        .invoke(request, context(Arc::new(Deltas::default())))
        .await
        .unwrap();
    worker.await.unwrap()
}
fn schema_format() -> zhir_core::model::ResponseFormat {
    zhir_core::model::ResponseFormat::Schema {
        name: "answer".into(),
        schema: json!({"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}),
    }
}
#[tokio::test]
async fn extra_adds_nested_options_without_losing_generated_format_or_stream_options() {
    use zhir_models::Protocol;
    for (protocol, extra, expected) in [
        (
            Protocol::Messages,
            json!({"output_config":{"effort":"low","format":{"consumer_hint":7}}}),
            json!({"effort":"low","format":{"type":"json_schema","consumer_hint":7}}),
        ),
        (
            Protocol::Responses,
            json!({"text":{"verbosity":"low","format":{"consumer_hint":7}}}),
            json!({"verbosity":"low","format":{"type":"json_schema","consumer_hint":7}}),
        ),
        (
            Protocol::Chat,
            json!({"stream_options":{"consumer_hint":{"sample":2}}}),
            json!({"include_usage":true,"consumer_hint":{"sample":2}}),
        ),
    ] {
        let mut next = request(protocol == Protocol::Chat);
        next.response_format = Some(schema_format());
        next.options.extra = serde_json::from_value(extra.clone()).unwrap();
        let sent = capture_request(protocol, next).await;
        let key = extra.as_object().unwrap().keys().next().unwrap();
        for (field, value) in expected.as_object().unwrap() {
            if field == "format" {
                for (nested, value) in value.as_object().unwrap() {
                    assert_eq!(sent[key][field][nested], *value);
                }
                assert_eq!(sent[key][field]["schema"]["required"], json!(["answer"]));
                assert_eq!(
                    sent[key][field]["schema"]["properties"]["answer"]["type"],
                    "string"
                );
            } else {
                assert_eq!(sent[key][field], *value);
            }
        }
        if protocol == Protocol::Chat {
            assert_eq!(
                sent["response_format"]["json_schema"]["schema"]["required"],
                json!(["answer"])
            );
        }
    }
}
#[tokio::test]
async fn extra_conflicts_report_exact_paths_and_reserved_fields_remain_owned() {
    use zhir_models::Protocol;
    let mut cases = vec![
        (
            Protocol::Messages,
            json!({"output_config":{"format":{"type":"text"}}}),
            "output_config.format.type",
        ),
        (
            Protocol::Messages,
            json!({"output_config":{"format":{"schema":{"required":["other"]}}}}),
            "output_config.format.schema.required",
        ),
        (
            Protocol::Messages,
            json!({"output_config":{"format":null}}),
            "output_config.format",
        ),
        (
            Protocol::Messages,
            json!({"output_config":[]}),
            "output_config",
        ),
        (
            Protocol::Responses,
            json!({"text":{"format":{"strict":false}}}),
            "text.format.strict",
        ),
        (
            Protocol::Chat,
            json!({"stream_options":{"include_usage":true}}),
            "stream_options.include_usage",
        ),
        (
            Protocol::Chat,
            json!({"response_format":{"json_schema":{"schema":{"properties":{"answer":{"type":"number"}}}}}}),
            "response_format.json_schema.schema.properties.answer.type",
        ),
    ];
    for protocol in [Protocol::Chat, Protocol::Responses, Protocol::Messages] {
        for key in [
            "input",
            "messages",
            "tools",
            "tool_choice",
            "system",
            "model",
            "stream",
        ] {
            cases.push((protocol, json!({key:{}}), key));
        }
    }
    for (protocol, extra, path) in cases {
        let mut next = request(protocol == Protocol::Chat);
        next.response_format = Some(schema_format());
        next.options.extra = serde_json::from_value(extra).unwrap();
        let error = protocol_model(protocol, "http://127.0.0.1:1".into())
            .invoke(next, context(Arc::new(Deltas::default())))
            .await
            .unwrap_err();
        assert!(
            matches!(error, zhir_core::error::Error::Invalid(ref message) if message==&format!("extra option overrides controlled field {path}")),
            "{protocol:?} {path}: {error}"
        );
    }
}
#[tokio::test]
async fn tool_result_grouping_preserves_order_content_errors_and_turn_boundaries() {
    use zhir_core::{
        error::Failure,
        message::MediaSource,
        tool::{RuntimeToolCall, RuntimeToolInput, RuntimeToolOutcome, RuntimeToolResult},
    };
    use zhir_models::Protocol;
    let assistant = |ids: &[&str]| Message::Assistant {
        output: ids
            .iter()
            .map(|id| Output::RuntimeToolCall {
                call: RuntimeToolCall {
                    id: (*id).into(),
                    name: "read".into(),
                    input: RuntimeToolInput::Structured(json!({})),
                },
            })
            .collect(),
        provider_data: Value::Null,
    };
    let result = |id: &str, outcome| Message::RuntimeTool {
        call_id: id.into(),
        name: "read".into(),
        outcome,
    };
    for protocol in [Protocol::Chat, Protocol::Responses, Protocol::Messages] {
        for boundary in [
            Message::user("next"),
            Message::external("next"),
            Message::Assistant {
                output: vec![Output::text("next")],
                provider_data: Value::Null,
            },
        ] {
            let mut next = request(false);
            next.messages.extend([
                assistant(&["a", "b"]),
                result(
                    "a",
                    RuntimeToolOutcome::Success {
                        content: vec![
                            Content::text("first"),
                            Content::Image {
                                source: MediaSource::Url {
                                    url: "https://example.com/image.png".into(),
                                },
                            },
                            Content::Opaque {
                                provider: "fixture".into(),
                                data: json!({"type":"consumer_block","value":7}),
                            },
                        ],
                        structured: Value::Null,
                    },
                ),
                result(
                    "b",
                    RuntimeToolOutcome::Failure {
                        error: Failure {
                            code: "missing".into(),
                            message: "missing entry".into(),
                            retryable: false,
                        },
                    },
                ),
                boundary,
                assistant(&["c"]),
                result("c", RuntimeToolResult::json(json!("last")).outcome),
            ]);
            let sent = capture_request(protocol, next).await;
            let messages = sent[if protocol == Protocol::Responses {
                "input"
            } else {
                "messages"
            }]
            .as_array()
            .unwrap();
            if protocol == Protocol::Messages {
                assert_eq!(messages.len(), 6);
                let results = messages[2]["content"].as_array().unwrap();
                assert_eq!(results.len(), 2);
                assert_eq!(results[0]["tool_use_id"], "a");
                assert_eq!(results[0]["is_error"], false);
                assert_eq!(results[0]["content"][0]["text"], "first");
                assert_eq!(results[0]["content"][1]["type"], "image");
                assert_eq!(
                    results[0]["content"][2],
                    json!({"type":"consumer_block","value":7})
                );
                assert_eq!(results[1]["tool_use_id"], "b");
                assert_eq!(results[1]["is_error"], true);
                assert_eq!(results[1]["content"][0]["text"], "missing entry");
                assert_eq!(messages[3]["content"][0]["text"], "next");
                assert_eq!(messages[5]["content"].as_array().unwrap().len(), 1);
                assert_eq!(messages[5]["content"][0]["tool_use_id"], "c");
                assert_eq!(messages[5]["content"][0]["content"][0]["text"], "last");
            } else {
                let results: Vec<_> = messages
                    .iter()
                    .filter(|m| m["role"] == "tool" || m["type"] == "function_call_output")
                    .collect();
                assert_eq!(results.len(), 3);
                let id_key = if protocol == Protocol::Chat {
                    "tool_call_id"
                } else {
                    "call_id"
                };
                assert_eq!(
                    results
                        .iter()
                        .map(|m| m[id_key].as_str().unwrap())
                        .collect::<Vec<_>>(),
                    ["a", "b", "c"]
                );
                let content_key = if protocol == Protocol::Chat {
                    "content"
                } else {
                    "output"
                };
                assert_eq!(results[0][content_key].as_array().unwrap().len(), 3);
                assert_eq!(results[1][content_key], "missing entry");
                assert_eq!(results[2][content_key], "last");
            }
        }
    }
}

fn image_spec() -> zhir_core::model::ProviderToolSpec {
    zhir_core::model::ProviderToolSpec {
        provider: "consumer".into(),
        name: "image".into(),
        options: json!({}),
    }
}
fn image_fixture() -> zhir_models::provider_tools::ProviderTools {
    let mut registry = zhir_models::provider_tools::ProviderTools::new();
    registry.register(ImageFixture).unwrap();
    registry
}
struct ImageFixture;
impl zhir_models::provider_tools::ProviderToolAdapter for ImageFixture {
    fn identity(&self) -> (&str, &str) {
        ("consumer", "image")
    }
    fn encode(
        &mut self,
        _: zhir_models::Protocol,
        _: &zhir_core::model::ProviderToolSpec,
    ) -> Result<Value> {
        Ok(json!({"type":"image_generation"}))
    }
    fn decode(
        &mut self,
        _: zhir_models::Protocol,
        item: &Value,
        _: &Value,
    ) -> Result<Option<Vec<Output>>> {
        if item["type"] != "image_generation_call" {
            return Ok(None);
        }
        Ok(Some(vec![Output::ProviderToolCall {
            call: zhir_core::message::ProviderToolCall {
                id: item["id"].as_str().unwrap().into(),
                provider: "consumer".into(),
                name: "image".into(),
                status: zhir_core::message::ProviderToolStatus::Completed,
                output: vec![Content::Image {
                    source: zhir_core::message::MediaSource::Inline {
                        mime_type: "image/png".into(),
                        base64: item["result"].as_str().unwrap().into(),
                    },
                }],
                data: item.clone(),
            },
        }]))
    }
    fn replay(
        &mut self,
        _: zhir_models::Protocol,
        call: &zhir_core::message::ProviderToolCall,
    ) -> Result<Vec<Value>> {
        Ok(vec![call.data.clone()])
    }
}
