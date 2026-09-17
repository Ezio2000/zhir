#![cfg(feature = "openai-responses")]
use serde_json::{Value, json};
use std::sync::Arc;
use zhir_core::{
    Result,
    message::{Message, Output, ProviderToolCall, ProviderToolStatus},
    model::{ModelContext, ModelRequest},
};
use zhir_models::{ModelConfig, Protocol, ProtocolExtension, credentials::StaticCredential};
use zhir_testing::{
    ModelTestExt,
    http::{HttpFixture, HttpReply},
};
struct Replay;
impl ProtocolExtension for Replay {
    fn encode_provider_history(
        &mut self,
        _: Protocol,
        call: &ProviderToolCall,
    ) -> Result<Option<Vec<Value>>> {
        Ok(Some(vec![json!({"id":call.id})]))
    }
}
#[tokio::test]
async fn replay_indexes_calls_without_changing_native_order_or_ownership_checks() {
    let mut calls: Vec<_> = (0..512)
        .map(|i| Output::ProviderToolCall {
            call: ProviderToolCall {
                outcome: None,
                id: format!("c{i}"),
                provider: "test".into(),
                name: "remote".into(),
                status: ProviderToolStatus::Completed,
                output: vec![],
                data: Value::Null,
            },
        })
        .collect();
    calls.reverse();
    let ids: Vec<_> = (0..512).map(|i| format!("c{i}")).collect();
    let items = vec![
        json!({"type":"reasoning","text":"prefix"}),
        json!({"$zhir_provider_calls":ids}),
    ];
    let server = HttpFixture::start([HttpReply::json(&json!({"output":[],"status":"completed"}))])
        .await
        .unwrap();
    let model = zhir_models::openai::responses::model(ModelConfig::new(
        server.url(),
        Arc::new(StaticCredential::new("Bearer", "fixture")),
        "fixture",
    ))
    .unwrap()
    .with_extension(|_| Ok(Replay));
    let request = |items| ModelRequest {
        messages: vec![
            Message::user("run"),
            Message::Assistant {
                output: calls.clone(),
                provider_data: json!({"protocol":"responses","response":{"output":items}}),
            },
        ],
        runtime_tools: vec![],
        provider_tools: vec![],
        profile: Default::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream: false,
    };
    let context = || ModelContext {
        run: zhir_kernel::defaults::context(),
        cancellation: Default::default(),
        deltas: None,
    };
    model
        .generate(request(items.clone()), context())
        .await
        .unwrap();
    let sent = server.finish().await.unwrap();
    let body = sent[0].json().unwrap();
    assert_eq!(body["input"][1], items[0]);
    for i in 0..512 {
        assert_eq!(body["input"][i + 2]["id"], format!("c{i}"));
    }
    for items in [
        vec![json!({"$zhir_provider_calls":["absent"]})],
        vec![json!({"$zhir_provider_calls":["c0","c0"]})],
        vec![json!({"$zhir_provider_calls":["c0"]})],
        vec![json!({"$zhir_provider_calls":ids,"other":true})],
    ] {
        assert!(model.generate(request(items), context()).await.is_err());
    }
}
