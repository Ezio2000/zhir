#![cfg(all(feature = "openai-chat", feature = "anthropic"))]
use serde_json::{Value, json};
use std::sync::Arc;
use zhir_core::{
    message::Message,
    model::{ModelContext, ModelRequest, TurnOutput},
};
use zhir_models::{ModelConfig, Protocol, credentials::StaticCredential};
use zhir_testing::{
    ModelTestExt,
    http::{HttpFixture, HttpReply},
};
fn frame(value: Value) -> String {
    format!("data: {value}\n\n")
}
async fn exchange(protocol: Protocol, frames: String) -> TurnOutput {
    let server = HttpFixture::start([
        HttpReply::bytes("text/event-stream", frames.into_bytes()).fragment_bytes(13)
    ])
    .await
    .unwrap();
    let config = ModelConfig::new(
        server.url(),
        Arc::new(StaticCredential::new("Bearer", "fixture")),
        "fixture",
    );
    let model = match protocol {
        Protocol::Chat => zhir_models::openai::chat::model(config),
        Protocol::Messages => zhir_models::anthropic::messages::model(config),
        _ => unreachable!(),
    }
    .unwrap();
    let response = model
        .turn(
            ModelRequest {
                messages: vec![Message::user("run")],
                runtime_tools: vec![],
                provider_tools: vec![],
                profile: Default::default(),
                tool_choice: Default::default(),
                response_format: None,
                stream: true,
            },
            ModelContext {
                run: zhir_kernel::defaults::context(),
                cancellation: Default::default(),
                deltas: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(server.finish().await.unwrap().len(), 1);
    response
}
#[tokio::test]
async fn fragmented_unicode_and_interleaved_tool_fields_keep_native_metadata() {
    let mut frames = frame(
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c","type":"function","function":{"name":"echo","arguments":"{\"x\":\""}}]}}]}),
    );
    for _ in 0..2048 {
        frames += &frame(
            json!({"trace":"keep","choices":[{"annotation":"choice","delta":{"content":"你🙂","reasoning_content":"想","tool_calls":[{"index":0,"function":{"arguments":"x"}}]},"logprobs":{"content":[{"token":"你"}],"refusal":null}}]}),
        );
    }
    frames += &frame(
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"}"}}]},"finish_reason":"tool_calls"}]}),
    );
    frames += "data: [DONE]\n\n";
    let response = exchange(Protocol::Chat, frames).await;
    let value = &response.provider_data["response"];
    assert_eq!(value["trace"], "keep");
    assert_eq!(value["choices"][0]["annotation"], "choice");
    assert_eq!(
        value["choices"][0]["message"]["content"],
        "你🙂".repeat(2048)
    );
    assert_eq!(
        value["choices"][0]["message"]["reasoning_content"],
        "想".repeat(2048)
    );
    assert_eq!(
        value["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
        "echo"
    );
    assert_eq!(
        value["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
        json!({"x":"x".repeat(2048)}).to_string()
    );
    assert_eq!(
        value["choices"][0]["logprobs"]["content"]
            .as_array()
            .unwrap()
            .len(),
        2048
    );
}
#[tokio::test]
async fn messages_accumulates_text_thinking_signature_and_partial_json_independently() {
    let mut frames = frame(
        json!({"type":"message_start","message":{"id":"msg","type":"message","role":"assistant","model":"fixture","content":[],"usage":{"input_tokens":1}}}),
    );
    for (index, block) in [
        (0, json!({"type":"text","text":""})),
        (1, json!({"type":"thinking","thinking":"","signature":""})),
        (
            2,
            json!({"type":"tool_use","id":"c","name":"echo","input":{}}),
        ),
    ] {
        frames += &frame(json!({"type":"content_block_start","index":index,"content_block":block}));
    }
    for _ in 0..1024 {
        for (index, delta) in [
            (0, json!({"type":"text_delta","text":"你"})),
            (1, json!({"type":"thinking_delta","thinking":"想"})),
            (1, json!({"type":"signature_delta","signature":"s"})),
        ] {
            frames += &frame(json!({"type":"content_block_delta","index":index,"delta":delta}));
        }
    }
    for input in ["{\"x\":", "\"你好\"}"] {
        frames += &frame(
            json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":input}}),
        );
    }
    for index in 0..3 {
        frames += &frame(json!({"type":"content_block_stop","index":index}));
    }
    frames += &frame(
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use","custom":null},"usage":{"output_tokens":5}}),
    );
    frames += &frame(json!({"type":"message_stop"}));
    let response = exchange(Protocol::Messages, frames).await;
    let value = &response.provider_data["response"];
    assert_eq!(value["content"][0]["text"], "你".repeat(1024));
    assert_eq!(value["content"][1]["thinking"], "想".repeat(1024));
    assert_eq!(value["content"][1]["signature"], "s".repeat(1024));
    assert_eq!(value["content"][2]["input"], json!({"x":"你好"}));
    assert_eq!(value["usage"]["output_tokens"], 5);
    assert!(value.get("custom").is_some_and(Value::is_null));
}
#[tokio::test]
#[ignore = "local release scaling measurement over public streaming sessions"]
async fn stream_append_scale() {
    for count in [4000, 8000, 16000] {
        let frames = frame(json!({"choices":[{"delta":{"content":"x".repeat(64)}}]})).repeat(count)
            + "data: [DONE]\n\n";
        let start = std::time::Instant::now();
        let response = exchange(Protocol::Chat, frames).await;
        assert_eq!(
            zhir_core::message::visible_content(&response.output)
                .iter()
                .filter_map(zhir_core::message::Content::as_text)
                .collect::<String>()
                .len(),
            count * 64
        );
        println!(
            "{}",
            json!({"case":"stream_session","fragments":count,"elapsed_ms":start.elapsed().as_secs_f64()*1000.0})
        );
    }
}
