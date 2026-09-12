use super::*;

#[test]
fn fragmented_unicode_and_interleaved_tool_fields_keep_native_metadata() {
    let mut state = Accumulator::new(Protocol::Chat);
    for i in 0..2048 {
        state.push(&json!({"trace":"keep","choices":[{"annotation":"choice","delta":{"content":"你🙂","reasoning_content":"想",
            "tool_calls":[{"index":0,"id":"c","function":{"name":if i==0 {"echo"} else {""},"arguments":"x"}}]},
            "logprobs":{"content":[{"token":"你"}],"refusal":null}}]}).to_string()).unwrap();
    }
    state.push("[DONE]").unwrap();
    let value = state.finish().unwrap();
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
        "x".repeat(2048)
    );
    assert_eq!(
        value["choices"][0]["logprobs"]["content"]
            .as_array()
            .unwrap()
            .len(),
        2048
    );
}

#[test]
fn messages_accumulates_text_thinking_signature_and_partial_json_independently() {
    let mut state = Accumulator::new(Protocol::Messages);
    for (index, block) in [
        (0, json!({"type":"text","text":""})),
        (1, json!({"type":"thinking","thinking":"","signature":""})),
        (
            2,
            json!({"type":"tool_use","id":"c","name":"echo","input":{}}),
        ),
    ] {
        state
            .push(
                &json!({"type":"content_block_start","index":index,"content_block":block})
                    .to_string(),
            )
            .unwrap();
    }
    for _ in 0..1024 {
        for (index, delta) in [
            (0, json!({"type":"text_delta","text":"你"})),
            (1, json!({"type":"thinking_delta","thinking":"想"})),
            (1, json!({"type":"signature_delta","signature":"s"})),
        ] {
            state
                .push(
                    &json!({"type":"content_block_delta","index":index,"delta":delta}).to_string(),
                )
                .unwrap();
        }
    }
    for input in ["{\"x\":", "\"你好\"}"] {
        state.push(&json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":input}}).to_string()).unwrap();
    }
    state
        .push(r#"{"type":"content_block_stop","index":2}"#)
        .unwrap();
    state.push(r#"{"type":"message_delta","delta":{"stop_reason":"tool_use","custom":null},"usage":{"output_tokens":5}}"#).unwrap();
    state.push(r#"{"type":"message_stop"}"#).unwrap();
    let value = state.finish().unwrap();
    assert_eq!(value["content"][0]["text"], "你".repeat(1024));
    assert_eq!(value["content"][1]["thinking"], "想".repeat(1024));
    assert_eq!(value["content"][1]["signature"], "s".repeat(1024));
    assert_eq!(value["content"][2]["input"], json!({"x":"你好"}));
    assert_eq!(value["usage"]["output_tokens"], 5);
    assert!(value.get("custom").is_some_and(Value::is_null));
}

#[test]
#[ignore = "local release scaling measurement"]
fn stream_append_scale() {
    for count in [4000, 8000, 16000] {
        let text = "x".repeat(64);
        let mut samples = Vec::new();
        for _ in 0..3 {
            let mut value = json!({"text":""});
            let start = std::time::Instant::now();
            for _ in 0..count {
                append(
                    std::hint::black_box(&mut value),
                    "text",
                    std::hint::black_box(&text),
                );
            }
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(value["text"].as_str().unwrap().len(), count * 64);
        }
        samples.sort_by(f64::total_cmp);
        println!(
            "{}",
            json!({"case":"stream_append","fragments":count,"median_ms":samples[1]})
        );
    }
}
