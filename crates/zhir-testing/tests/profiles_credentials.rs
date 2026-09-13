#![cfg(feature = "openai-responses")]
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use zhir_core::{
    credential::{Credential, CredentialContext, CredentialProvider},
    message::{Content, Message},
    model::{Model, ModelContext, ModelRequest},
    profile::{Fidelity, RequestProfile, Requirement, ResourceUsage, Serving},
    resource::{ResourceInput, ResourceRef, ResourceSource},
};
use zhir_models::{
    ModelConfig,
    credentials::{RefreshingCredential, StaticCredential},
    openai,
    profiles::ProfileMapping,
};
use zhir_testing::{
    ModelTestExt,
    http::{HttpFixture, HttpReply},
};
fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![Message::user("test")],
        runtime_tools: vec![],
        provider_tools: vec![],
        profile: Default::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream: false,
    }
}
fn context() -> ModelContext {
    ModelContext {
        run: zhir_kernel::defaults::context(),
        cancellation: Default::default(),
        deltas: None,
    }
}
fn response() -> serde_json::Value {
    json!({"id":"response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]})
}
#[tokio::test]
async fn refresh_is_single_flight_generation_aware_and_scoped_by_audience() {
    let refreshes = Arc::new(AtomicUsize::new(0));
    let calls = refreshes.clone();
    let provider = Arc::new(RefreshingCredential::new(move |context| {
        let generation = calls.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            tokio::task::yield_now().await;
            Ok(Credential {
                generation: format!("g{generation}"),
                scheme: "Bearer".into(),
                value: format!("token-{generation}"),
                expires_at_ms: Some(context.now_ms + 10),
                metadata: Default::default(),
            })
        })
    }));
    let lookup = CredentialContext {
        audience: "endpoint-a".into(),
        now_ms: 100,
    };
    let results =
        futures::future::join_all((0..64).map(|_| provider.resolve(lookup.clone()))).await;
    assert!(
        results
            .iter()
            .all(|result| result.as_ref().unwrap().generation == "g1")
    );
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    provider.invalidate("g1").await.unwrap();
    assert_eq!(
        provider.resolve(lookup.clone()).await.unwrap().generation,
        "g2"
    );
    provider.invalidate("g1").await.unwrap();
    assert_eq!(
        provider.resolve(lookup.clone()).await.unwrap().generation,
        "g2"
    );
    assert_eq!(
        provider
            .resolve(CredentialContext {
                audience: "endpoint-b".into(),
                now_ms: 100
            })
            .await
            .unwrap()
            .generation,
        "g3"
    );
    assert_eq!(
        provider
            .resolve(CredentialContext {
                now_ms: 111,
                ..lookup
            })
            .await
            .unwrap()
            .generation,
        "g4"
    );
}
#[tokio::test]
async fn oauth_style_refresh_and_account_headers_are_injected_without_kernel_auth_logic() {
    let server = HttpFixture::start([
        HttpReply::json(&json!({"error":"expired"})).status(401),
        HttpReply::json(&response()),
    ])
    .await
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let provider = Arc::new(RefreshingCredential::new(move |_| {
        let generation = seen.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            Ok(Credential {
                generation: generation.to_string(),
                scheme: "Bearer".into(),
                value: format!("oauth-{generation}"),
                expires_at_ms: None,
                metadata: [("header:chatgpt-account-id".into(), "fixture-account".into())].into(),
            })
        })
    }));
    let model =
        openai::responses::model(ModelConfig::new(server.url(), provider, "fixture")).unwrap();
    model.turn(request(), context()).await.unwrap();
    let requests = server.finish().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].headers["authorization"], "Bearer oauth-1");
    assert_eq!(requests[1].headers["authorization"], "Bearer oauth-2");
    assert_eq!(requests[1].headers["chatgpt-account-id"], "fixture-account");
    assert_eq!(requests[0].body, requests[1].body);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn required_fast_and_original_are_encoded_and_actual_unconfirmed_values_stay_unknown() {
    let server = HttpFixture::start([HttpReply::json(&response())])
        .await
        .unwrap();
    let model = openai::responses::model(ModelConfig::new(
        server.url(),
        Arc::new(StaticCredential::new("Bearer", "fixture")),
        "fixture",
    ))
    .unwrap();
    let mut caps = model.capabilities().clone();
    caps.constraints.insert(
        "resource.image.fidelity".into(),
        vec![json!("original"), json!("high")],
    );
    let model = model
        .with_capabilities(caps)
        .with_profile_mapping(
            ProfileMapping::new(
                "serving",
                json!("low_latency"),
                "service_tier",
                json!("priority"),
            )
            .unwrap(),
        )
        .unwrap();
    let profile = RequestProfile {
        serving: Some(Requirement::Required(Serving::LowLatency)),
        ..Default::default()
    };
    let resource = Content::Resource {
        input: ResourceInput {
            resource: ResourceRef {
                id: "image".into(),
                media_type: "image/png".into(),
                name: None,
                source: ResourceSource::Url {
                    url: "https://fixture.invalid/image.png".into(),
                },
                metadata: Default::default(),
            },
            usage: ResourceUsage {
                fidelity: Some(Requirement::Required(Fidelity::Original)),
                ..Default::default()
            },
        },
    };
    let runtime = zhir_kernel::Runtime::builder(Arc::new(model))
        .build()
        .unwrap();
    let mut invocation = runtime
        .start(
            zhir_kernel::RunRequest::new(vec![Message::User {
                content: vec![Content::text("inspect"), resource],
            }])
            .profile(profile),
        )
        .unwrap();
    let checkpoint = invocation.result().await.unwrap().into_checkpoint();
    assert!(
        matches!(checkpoint.state, zhir_core::run::State::Completed { .. }),
        "{:?}",
        checkpoint.state
    );
    assert_eq!(
        checkpoint.active.session.negotiated.selected["serving"],
        json!("low_latency")
    );
    assert_eq!(
        checkpoint.active.session.negotiated.selected["resource.image.fidelity"],
        json!("original")
    );
    assert_eq!(
        checkpoint.active.session.effective.values["serving"],
        zhir_core::profile::Confirmation::Unknown
    );
    let requests = server.finish().await.unwrap();
    let sent = requests[0].json().unwrap();
    assert_eq!(sent["service_tier"], "priority");
    assert_eq!(sent["input"][0]["content"][1]["detail"], "original");
}
#[tokio::test]
async fn unsupported_requirements_fail_and_preferences_only_use_explicit_alternatives() {
    let model = openai::responses::model(ModelConfig::new(
        "http://127.0.0.1:1",
        Arc::new(StaticCredential::new("Bearer", "fixture")),
        "fixture",
    ))
    .unwrap();
    let mut request = request();
    request.profile.serving = Some(Requirement::Required(Serving::LowLatency));
    assert!(model.negotiate(&request).is_err());
    request.profile.serving = Some(Requirement::Preferred(Serving::LowLatency));
    assert!(model.negotiate(&request).is_err());
    request
        .profile
        .alternatives
        .insert("serving".into(), vec![]);
    let selected = model.negotiate(&request).unwrap();
    assert!(selected.selected.is_empty());
    assert!(selected.unmet_preferences.contains_key("serving"));
    let mut caps = model.capabilities().clone();
    caps.constraints
        .insert("serving".into(), vec![json!("low_latency")]);
    let model = model.with_capabilities(caps);
    request.profile.serving = Some(Requirement::Required(Serving::LowLatency));
    assert!(
        model.negotiate(&request).is_err(),
        "declared capability without a wire mapping must be rejected"
    );
}
