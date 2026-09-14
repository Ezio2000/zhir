//! Native SDP/DataChannel/RTP integration. The subscription test is opt-in.
#[path = "gpt_live/fixture.rs"]
mod fixture;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use zhir_core::{
    BoxFuture, Result,
    credential::{Credential, CredentialContext, CredentialProvider},
    message::Message,
    model::*,
    resource::{MediaChunk, MediaReceiver},
};
use zhir_models::openai::live::{AUDIO_TYPE, LiveConfig, LiveModel};

struct Credentials(Credential);
impl CredentialProvider for Credentials {
    fn resolve(&self, _: CredentialContext) -> BoxFuture<'_, Result<Credential>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
    fn invalidate(&self, _: &str) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
fn credentials(token: String, account: String) -> Arc<dyn CredentialProvider> {
    Arc::new(Credentials(Credential {
        generation: "live-test".into(),
        scheme: "Bearer".into(),
        value: token,
        expires_at_ms: None,
        metadata: [("header:ChatGPT-Account-Id".into(), account)].into(),
    }))
}
fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![Message::user("Test voice")],
        runtime_tools: vec![],
        provider_tools: vec![],
        profile: Default::default(),
        tool_choice: ToolChoice::Auto,
        response_format: None,
        stream: true,
    }
}
async fn open(model: &LiveModel) -> ModelSession {
    model
        .open_session(SessionOpen {
            session_id: "test-live".into(),
            output_epoch: 0,
            after_sequence: None,
            recovery: None,
            limits: zhir_kernel::defaults::limits(),
            request: request(),
            context: ModelContext {
                run: zhir_core::run::RunContext::new("live-test", 0),
                cancellation: Default::default(),
                deltas: None,
            },
        })
        .await
        .unwrap()
}
async fn send(session: &ModelSession, id: &str, body: SessionCommandBody) {
    session
        .input
        .send(SessionCommand {
            id: id.into(),
            body,
        })
        .await
        .unwrap();
}
#[tokio::test]
async fn native_webrtc_preserves_transcripts_delegation_and_audio() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (endpoint, server) = fixture::serve().await;
        let mut config=LiveConfig::new(credentials("test-secret".into(),"test-account".into()));
        config.endpoint=endpoint;
        let model=LiveModel::new(config).unwrap();
        assert!(!model.capabilities().supports(Capability::InterruptOutput));
        let mut session=open(&model).await;
        send(&session,"start",SessionCommandBody::StartTurn {turn_id:"execution".into(),request:Box::new(request())}).await;
        assert!(matches!(session.output.receive().await.unwrap().unwrap().body,SessionEventBody::Acknowledged {command_id,..} if command_id=="start"));
        session.media_input.as_ref().unwrap().send(MediaChunk {stream_id:"mic".into(),turn_id:"execution".into(),epoch:0,sequence:0,timestamp_us:0,media_type:AUDIO_TYPE.into(),bytes:vec![0xf8,0xff,0xfe],end:false}).await.unwrap();
        let mut messages=vec![];
        while messages.len()<2 {
            if let SessionEventBody::ConversationItem {item_id,message}=session.output.receive().await.unwrap().unwrap().body { messages.push((item_id,message)); }
        }
        assert_eq!(messages[0],("user-1".into(),Message::user("hello")));
        assert_eq!(messages[1].0,"assistant-1");
        loop {
            if let SessionEventBody::Output {output:zhir_core::message::Output::Delegation {request},..}=session.output.receive().await.unwrap().unwrap().body { assert_eq!(request.prompt,"inspect files");break; }
        }
        let origin=zhir_core::operation::CallRef {session_id:"test-live".into(),turn_id:"execution".into(),caller_id:"live".into(),call_id:"delegate-1".into()};
        send(&session,"context",SessionCommandBody::DelegationContext {operation_id:"op".into(),origin:origin.clone(),content:vec![zhir_core::message::Content::text("中".repeat(400))]}).await;
        send(&session,"end",SessionCommandBody::EndInput).await;
        send(&session,"result",SessionCommandBody::DelegationResult {operation_id:"op".into(),origin,outcome:zhir_core::operation::OperationOutcome::Success {content:vec![zhir_core::message::Content::text("done")],structured:serde_json::Value::Null}}).await;
        let mut audio=session.media_output.take().unwrap();
        let receive=tokio::spawn(async move {let mut chunks=vec![];while let Some(chunk)=audio.receive().await.unwrap(){chunks.push(chunk);}chunks});

        let mut finished=0;
        loop {
            match session.output.receive().await.unwrap().unwrap().body {
                SessionEventBody::TurnFinished {..}=>finished+=1,
                SessionEventBody::Closed=>break,
                _=>(),
            }
        }
        assert_eq!(finished,1);
        let chunks=receive.await.unwrap();
        assert!(chunks.len()>=2);
        assert_eq!(chunks[0].bytes,vec![0xf8,0xff,0xfe]);
        assert!(chunks.last().unwrap().end);
        send(&session,"close",SessionCommandBody::Close).await;
        assert!(matches!(session.output.receive().await.unwrap().unwrap().body,SessionEventBody::Acknowledged {command_id,..} if command_id=="close"));
        let evidence=server.await.unwrap();
        assert!(evidence.audio>0,"no native upstream RTP");
        let context:Vec<_>=evidence.commands.iter().filter(|e|e["type"]=="delegation.context.append").collect();
        assert_eq!(context.len(),4);
        for event in &context {assert_eq!(event["delegation_item_id"],"delegate-1");assert!(event["content"][0]["text"].as_str().unwrap().len()<=500);}
        assert_eq!(context.last().unwrap()["channel"],"speakable");
    }).await.expect("WebRTC fixture stalled");
}

#[tokio::test]
async fn recovery_and_unsupported_profiles_fail_before_signaling() {
    let mut config = LiveConfig::new(credentials("unused".into(), "unused".into()));
    config.endpoint = "http://127.0.0.1:1".into();
    let model = LiveModel::new(config).unwrap();
    let result = model
        .open_session(SessionOpen {
            session_id: "s".into(),
            after_sequence: Some(1),
            output_epoch: 0,
            limits: zhir_kernel::defaults::limits(),
            request: request(),
            recovery: None,
            context: ModelContext {
                run: zhir_core::run::RunContext::new("live-test", 0),
                cancellation: Default::default(),
                deltas: None,
            },
        })
        .await;
    assert!(matches!(result, Err(zhir_core::error::Error::Protocol(_))));
    let mut request = request();
    request.response_format = Some(ResponseFormat::Json);
    assert!(model.negotiate(&request).is_err());
}

#[tokio::test]
#[ignore = "requires ZHIR_LIVE_AUTH_JSON and a Codex subscription; creates one voice session"]
async fn codex_subscription_through_native_runtime() {
    let auth: serde_json::Value = serde_json::from_slice(
        &std::fs::read(std::env::var("ZHIR_LIVE_AUTH_JSON").unwrap()).unwrap(),
    )
    .unwrap();
    let mut config = LiveConfig::new(credentials(
        auth["tokens"]["access_token"].as_str().unwrap().into(),
        auth["tokens"]["account_id"].as_str().unwrap().into(),
    ));
    if let Ok(proxy) = std::env::var("ZHIR_LIVE_PROXY") {
        config.http_client = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(proxy).unwrap())
            .build()
            .unwrap();
    }
    config.instructions = "请用中文简短回答，每次只说测试成功。".into();
    let model = Arc::new(zhir_testing::RecordingModel::new(Arc::new(
        LiveModel::new(config).unwrap(),
    )));
    let runtime = zhir_kernel::Runtime::builder(model.clone())
        .resources(Arc::new(zhir_storage::MemoryResourceStore::new()))
        .build()
        .unwrap();
    let mut invocation = runtime
        .start(
            zhir_kernel::RunRequest::new([Message::user("语音测试")])
                .mode(zhir_core::run::RunMode::Interactive),
        )
        .unwrap();
    let control = invocation.control();
    let microphone = invocation.media_input();
    let mut media = invocation.media_output().unwrap();
    let audio = tokio::spawn(async move {
        let mut chunks = vec![];
        while let Some(chunk) = media.receive().await.unwrap() {
            chunks.push(chunk);
        }
        chunks
    });
    let input = tokio::spawn(async move {
        let turn = loop {
            let id = model
                .records()
                .iter()
                .flat_map(|r| r.commands.iter())
                .find_map(|c| match &c.body {
                    SessionCommandBody::StartTurn { turn_id, .. } => Some(turn_id.clone()),
                    _ => None,
                });
            if let Some(id) = id {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let voice = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(20));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            for sequence in 0..600 {
                interval.tick().await;
                microphone
                    .send(MediaChunk {
                        stream_id: "mic".into(),
                        turn_id: turn.clone(),
                        epoch: 0,
                        sequence,
                        timestamp_us: sequence * 20000,
                        media_type: AUDIO_TYPE.into(),
                        bytes: vec![0xf8, 0xff, 0xfe],
                        end: sequence == 599,
                    })
                    .await
                    .unwrap();
            }
        });
        tokio::time::sleep(Duration::from_secs(3)).await;
        control
            .input(Message::user("现在请只说测试成功。"), "test")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        voice.await.unwrap();
        control.end_input().await.unwrap();
    });
    let result = tokio::time::timeout(Duration::from_secs(45), invocation.result())
        .await
        .unwrap()
        .unwrap()
        .into_checkpoint();
    input.await.unwrap();
    let chunks = audio.await.unwrap();
    assert!(
        matches!(result.state, zhir_core::run::State::Completed { .. }),
        "{:?}",
        result.state
    );
    assert!(chunks.iter().any(|c| !c.bytes.is_empty()));
    assert!(
        result
            .history
            .messages()
            .iter()
            .any(|m| matches!(m,Message::Assistant {output,..} if !output.is_empty()))
    );
    assert!(result.active.media.is_empty());
    assert!(result.active.session.media_archive.is_some());
    eprintln!(
        "{}",
        json!({"state":"completed","audio_packets":chunks.len(),"history_entries":result.history.len(),"committed_revisions":result.revision})
    );
}

#[tokio::test]
async fn credential_resolution_is_cancelled_and_startup_is_bounded() {
    struct Blocked;
    impl CredentialProvider for Blocked {
        fn resolve(&self, _: CredentialContext) -> BoxFuture<'_, Result<Credential>> {
            Box::pin(std::future::pending())
        }
        fn invalidate(&self, _: &str) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }
    for cancel in [false, true] {
        let mut config = LiveConfig::new(Arc::new(Blocked));
        config.connect_timeout = Duration::from_millis(100);
        let model = LiveModel::new(config).unwrap();
        let cancellation = zhir_core::Cancellation::default();
        let mut session = model
            .open_session(SessionOpen {
                session_id: "timeout".into(),
                after_sequence: None,
                output_epoch: 0,
                limits: zhir_kernel::defaults::limits(),
                request: request(),
                recovery: None,
                context: ModelContext {
                    run: zhir_core::run::RunContext::new("timeout", 0),
                    cancellation: cancellation.clone(),
                    deltas: None,
                },
            })
            .await
            .unwrap();
        send(
            &session,
            "start",
            SessionCommandBody::StartTurn {
                turn_id: "t".into(),
                request: Box::new(request()),
            },
        )
        .await;
        if cancel {
            tokio::time::sleep(Duration::from_millis(20)).await;
            cancellation.cancel();
        }
        let error = tokio::time::timeout(Duration::from_secs(2), session.output.receive())
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            if cancel {
                matches!(error, zhir_core::error::Error::Cancelled)
            } else {
                matches!(error, zhir_core::error::Error::Uncertain(_))
            },
            "{error:?}"
        );
    }
}

#[tokio::test]
async fn rejected_signaling_has_one_refresh_attempt_and_redacts_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/calls", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = vec![];
            loop {
                let mut buf = [0; 8192];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                data.extend_from_slice(&buf[..n]);
                if let Some(end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&data[..end]).to_lowercase();
                    let length: usize = headers
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .unwrap()
                        .trim()
                        .parse()
                        .unwrap();
                    if data.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            socket.write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 11\r\nConnection: close\r\n\r\nsecret-body").await.unwrap();
        }
    });
    let mut config = LiveConfig::new(credentials("test-secret".into(), "test-account".into()));
    config.endpoint = endpoint;
    let model = LiveModel::new(config).unwrap();
    let mut session = open(&model).await;
    send(
        &session,
        "start",
        SessionCommandBody::StartTurn {
            turn_id: "t".into(),
            request: Box::new(request()),
        },
    )
    .await;
    let error = tokio::time::timeout(Duration::from_secs(3), session.output.receive())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(&error,zhir_core::error::Error::Model(f) if f.code=="http_401"));
    assert!(!error.to_string().contains("secret"));
    server.await.unwrap();
}
