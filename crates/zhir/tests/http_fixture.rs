#![cfg(all(
    feature = "openai-responses",
    feature = "openai-chat",
    feature = "anthropic"
))]
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use zhir::{
    Result,
    error::{Error, Failure},
    model::{ModelContext, ModelRequest},
    models::{
        ModelConfig, Protocol, ProtocolExtension, anthropic, decorators::RetryingModel, openai,
    },
};
use zhir_testing::http::{HttpFixture, HttpReply};
fn request(stream: bool) -> ModelRequest {
    ModelRequest {
        messages: vec![zhir::message::Message::user("你好")],
        runtime_tools: vec![],
        provider_tools: vec![],
        profile: Default::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream,
    }
}
fn context() -> ModelContext {
    ModelContext {
        run: zhir::kernel::defaults::context(),
        cancellation: Default::default(),
        deltas: None,
    }
}
fn frame() -> Value {
    json!({"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"你好"}]}],"status":"completed"})
}
struct Session {
    marker: Value,
    encoded: bool,
}
impl ProtocolExtension for Session {
    fn encode_request(&mut self, _: Protocol, _: &ModelRequest, body: &mut Value) -> Result<()> {
        assert!(!self.encoded);
        self.encoded = true;
        body["caller_marker"] = self.marker.clone();
        Ok(())
    }
}
#[tokio::test]
async fn contextual_factories_and_http_errors_are_explicit_without_command_replay() {
    for protocol in [Protocol::Chat, Protocol::Responses, Protocol::Messages] {
        let raw = match protocol {
            Protocol::Chat => {
                json!({"choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]})
            }
            Protocol::Responses => frame(),
            Protocol::Messages => {
                json!({"content":[{"type":"text","text":"done"}],"stop_reason":"end_turn"})
            }
        };
        let fixture = HttpFixture::start([
            HttpReply::json(&json!({"error":"busy"})).status(503),
            HttpReply::json(&raw),
        ])
        .await
        .unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let called = count.clone();
        let config = ModelConfig::new(
            format!("{}/v1", fixture.url()),
            std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                "Bearer", "fixture",
            )),
            "fixture",
        );
        let model = match protocol {
            Protocol::Chat => openai::chat::model(config),
            Protocol::Responses => openai::responses::model(config),
            Protocol::Messages => anthropic::messages::model(config),
        }
        .unwrap()
        .with_extension(move |ctx| {
            assert_eq!(ctx.protocol, protocol);
            assert_eq!(
                ctx.request.profile.extensions["consumer"]["marker"],
                ctx.run.metadata["marker"]
            );
            if called.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(Error::Model(Failure {
                    code: "session_busy".into(),
                    message: "retry factory".into(),
                    retryable: true,
                }));
            }
            Ok(Session {
                marker: ctx.run.metadata["marker"].clone(),
                encoded: false,
            })
        });
        let mut ctx = context();
        ctx.run.metadata.insert("marker".into(), json!("caller"));
        let mut input = request(false);
        input
            .profile
            .extensions
            .entry("consumer".into())
            .or_default()
            .insert("marker".into(), json!("caller"));
        let model = RetryingModel::new(
            Arc::new(model),
            zhir_policies::RetryPolicy::new(3)
                .unwrap()
                .backoff(zhir_policies::Backoff::fixed(Duration::ZERO)),
        )
        .unwrap();
        assert!(model.turn(input.clone(), ctx.clone()).await.is_err());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(model.turn(input.clone(), ctx.clone()).await.is_err());
        assert_eq!(count.load(Ordering::SeqCst), 2);
        model.turn(input, ctx).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 3);
        let sent = fixture.finish().await.unwrap();
        assert_eq!(sent.len(), 2);
        assert!(
            sent.iter()
                .all(|r| r.json().unwrap()["caller_marker"] == "caller")
        );
    }
}
#[tokio::test]
async fn factory_failures_and_pre_cancelled_calls_send_no_requests() {
    let fixture = HttpFixture::with_timeout([HttpReply::json(&frame())], Duration::from_millis(30))
        .await
        .unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let called = count.clone();
    let model = openai::responses::model(ModelConfig::new(
        fixture.url(),
        std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
            "Bearer", "fixture",
        )),
        "fixture",
    ))
    .unwrap()
    .with_extension(move |_| -> Result<Session> {
        called.fetch_add(1, Ordering::SeqCst);
        Err(Error::Invalid("user registration failed".into()))
    });
    let ctx = context();
    ctx.cancellation.cancel();
    assert!(matches!(
        model.turn(request(false), ctx).await,
        Err(Error::Cancelled)
    ));
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(
        matches!(model.turn(request(false),context()).await,Err(Error::Invalid(e)) if e=="user registration failed")
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(
        matches!(fixture.finish().await,Err(Error::Invalid(e)) if e.contains("exchange 0 timed out"))
    );
}
#[tokio::test]
async fn fixture_exercises_unicode_sse_fragmentation_raw_capture_and_disconnects() {
    for size in [1, 2, 3, 7, 13, 64, 1024] {
        let body = format!(
            "id: opaque\r\nevent: response.completed\r\ndata: {}\r\n\r\n",
            json!({"type":"response.completed","response":frame()})
        );
        let fixture = HttpFixture::start([HttpReply::sse(body).fragment_bytes(size)])
            .await
            .unwrap();
        let model = openai::responses::model(ModelConfig::new(
            format!("{}/v1", fixture.url()),
            std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                "Bearer", "fixture",
            )),
            "fixture",
        ))
        .unwrap();
        let result = model.turn(request(true), context()).await.unwrap();
        assert_eq!(result.output, vec![zhir::message::Output::text("你好")]);
        let sent = fixture.finish().await.unwrap();
        assert_eq!(sent[0].method, "POST");
        assert_eq!(sent[0].target, "/v1/responses");
        assert_eq!(
            sent[0].json().unwrap()["input"][0]["content"][0]["text"],
            "你好"
        );
    }
    for sse in [false, true] {
        let reply = if sse {
            HttpReply::sse(format!(
                "data: {}\n\n",
                json!({"type":"response.completed","response":frame()})
            ))
        } else {
            HttpReply::json(&frame())
        };
        let fixture = HttpFixture::start([reply.disconnect_after(9)])
            .await
            .unwrap();
        let model = openai::responses::model(ModelConfig::new(
            fixture.url(),
            std::sync::Arc::new(zhir_models::credentials::StaticCredential::new(
                "Bearer", "fixture",
            )),
            "fixture",
        ))
        .unwrap();
        assert!(model.turn(request(sse), context()).await.is_err());
        assert_eq!(fixture.finish().await.unwrap().len(), 1);
    }
}
#[tokio::test]
async fn fixture_delays_timeouts_drop_and_invalid_scripts_are_observable() {
    let fixture = HttpFixture::with_timeout(
        [HttpReply::json(&json!({}))
            .delay(Duration::from_millis(10))
            .fragment_bytes(1)
            .fragment_delay(Duration::from_millis(3))
            .header("x-fixture", "custom")],
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let now = std::time::Instant::now();
    let response = reqwest::Client::new()
        .get(fixture.url())
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["x-fixture"], "custom");
    assert_eq!(response.text().await.unwrap(), "{}");
    assert!(now.elapsed() >= Duration::from_millis(10));
    assert_eq!(fixture.finish().await.unwrap()[0].method, "GET");
    let fixture =
        HttpFixture::with_timeout([HttpReply::json(&json!({}))], Duration::from_millis(15))
            .await
            .unwrap();
    assert!(fixture.finish().await.is_err());
    let fixture = HttpFixture::start([HttpReply::json(&json!({}))])
        .await
        .unwrap();
    let address = fixture.url().trim_start_matches("http://").to_owned();
    drop(fixture);
    tokio::task::yield_now().await;
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    assert!(
        HttpFixture::start([HttpReply::json(&json!({})).fragment_bytes(0)])
            .await
            .is_err()
    );
    assert!(
        HttpFixture::start([HttpReply::json(&json!({})).header("content-length", "4")])
            .await
            .is_err()
    );
    // Dropping a pending finish future still owns and aborts the server task.
    let fixture = HttpFixture::start([HttpReply::json(&json!({}))])
        .await
        .unwrap();
    let address = fixture.url().trim_start_matches("http://").to_owned();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), fixture.finish())
            .await
            .is_err()
    );
    tokio::task::yield_now().await;
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}

use zhir_testing::ModelTestExt;
