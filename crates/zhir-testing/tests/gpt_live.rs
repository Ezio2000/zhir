//! Native SDP/DataChannel/RTP integration. The subscription test is opt-in.
#[path = "gpt_live/fixture.rs"]
mod fixture;
#[path = "gpt_live/network.rs"]
mod network;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use zhir_core::{
    BoxFuture, Result,
    credential::{Credential, CredentialContext, CredentialProvider},
    message::Message,
    model::*,
    resource::{MediaChunk, MediaReceiver},
};
use zhir_models::openai::live::{self, AUDIO_TYPE, LiveConfig};

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
async fn open(model: &zhir_models::WebRtcModel) -> ModelSession {
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
        let (endpoint, server) = fixture::serve(fixture::Fault::None).await;
        let mut config=LiveConfig::new(credentials("test-secret".into(),"test-account".into()));
        let relay = network::Relay::start(reqwest::Client::builder().no_proxy().build().unwrap(), endpoint).await;
        config.endpoint=relay.endpoint.clone();
        config.connect_timeout=Duration::from_secs(5);
        let model=live::model(config).unwrap();
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
        for enabled in [false, true] {
            send(&session,"audio-gate",SessionCommandBody::SetInputAudio {enabled}).await;
            assert!(matches!(session.output.receive().await.unwrap().unwrap().body,SessionEventBody::Acknowledged {command_id,..} if command_id=="audio-gate"));
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
        assert!(matches!(session.media_input.as_ref().unwrap().send(MediaChunk {stream_id:"mic".into(),turn_id:"execution".into(),epoch:0,sequence:1,timestamp_us:20000,media_type:AUDIO_TYPE.into(),bytes:vec![],end:true}).await, Err(zhir_core::error::Error::Cancelled)), "EndInput must close direct session media admission");
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
    let model = live::model(config).unwrap();
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
async fn remote_rejection_preserves_details_without_acknowledging_command() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let (endpoint, server) = fixture::serve(fixture::Fault::RejectedAudio).await;
        let mut config = LiveConfig::new(credentials("test-secret".into(), "test-account".into()));
        config.endpoint = endpoint;
        let model = live::model(config).unwrap();
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
        loop {
            if matches!(
                session.output.receive().await.unwrap().unwrap().body,
                SessionEventBody::Output { .. }
            ) {
                break;
            }
        }
        send(
            &session,
            "pause",
            SessionCommandBody::SetInputAudio { enabled: false },
        )
        .await;
        let mut observed_error = false;
        loop {
            match session.output.receive().await {
                Ok(Some(event)) => match event.body {
                    SessionEventBody::Delta {
                        delta: ModelDelta::ProtocolEvent { data, .. },
                        ..
                    } => {
                        assert_eq!(data["error"]["event_id"], "pause");
                        assert_eq!(data["error"]["param"], "type");
                        observed_error = true;
                    }
                    other => panic!("unexpected success after rejected control: {other:?}"),
                },
                Err(zhir_core::error::Error::Model(failure)) => {
                    assert_eq!(failure.code, "control_rejected");
                    assert_eq!(failure.message, "Input control was rejected");
                    break;
                }
                other => panic!("missing provider rejection: {other:?}"),
            }
        }
        assert!(observed_error);
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn abnormal_remote_close_cannot_confirm_successful_drain() {
    for fault in [
        fixture::Fault::ConnectionLostOnClose,
        fixture::Fault::ExpiredOnClose,
    ] {
        tokio::time::timeout(Duration::from_secs(8), async {
            let (endpoint, server) = fixture::serve(fault).await;
            let mut config =
                LiveConfig::new(credentials("test-secret".into(), "test-account".into()));
            config.endpoint = endpoint;
            let model = live::model(config).unwrap();
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
            loop {
                if matches!(
                    session.output.receive().await.unwrap().unwrap().body,
                    SessionEventBody::Output { .. }
                ) {
                    break;
                }
            }
            send(&session, "close", SessionCommandBody::Close).await;
            assert!(matches!(
                session.output.receive().await,
                Err(zhir_core::error::Error::Uncertain(_))
            ));
            server.await.unwrap();
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn expiry_while_end_input_waits_for_delegation_is_uncertain() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let (endpoint, server) = fixture::serve(fixture::Fault::ExpiredWithDelegation).await;
        let mut config = LiveConfig::new(credentials("test-secret".into(), "test-account".into()));
        config.endpoint = endpoint;
        let model = live::model(config).unwrap();
        let mut session = open(&model).await;
        send(&session, "start", SessionCommandBody::StartTurn { turn_id: "t".into(), request: Box::new(request()) }).await;
        loop {
            if matches!(session.output.receive().await.unwrap().unwrap().body, SessionEventBody::Output { .. }) { break; }
        }
        send(&session, "end", SessionCommandBody::EndInput).await;
        send(&session, "progress", SessionCommandBody::DelegationContext {
            operation_id: "op".into(),
            origin: zhir_core::operation::CallRef { session_id: "test-live".into(), turn_id:"t".into(), caller_id:"live".into(), call_id:"delegate-1".into() },
            content: vec![zhir_core::message::Content::text("still working")],
        }).await;
        assert!(matches!(session.output.receive().await.unwrap().unwrap().body, SessionEventBody::Acknowledged { command_id,.. } if command_id=="progress"));
        assert!(matches!(session.output.receive().await, Err(zhir_core::error::Error::Uncertain(_))));
        assert!(session.output.receive().await.unwrap().is_none());
        let evidence = server.await.unwrap();
        assert!(!evidence.commands.iter().any(|event|event["type"]=="session.close"));
    }).await.unwrap();
}

#[tokio::test]
#[ignore = "requires ZHIR_LIVE_AUTH_JSON and a Codex subscription; creates one voice session"]
async fn codex_subscription_through_native_runtime() {
    tokio::time::timeout(Duration::from_secs(60), subscription_session(false))
        .await
        .expect("subscription E2E timed out");
}

#[tokio::test]
#[ignore = "requires Codex OAuth; drops the real WebRTC UDP path for eight seconds"]
async fn codex_subscription_survives_udp_blackout() {
    tokio::time::timeout(Duration::from_secs(60), subscription_session(true))
        .await
        .expect("subscription blackout E2E timed out");
}

async fn subscription_session(blackout: bool) {
    // Optional 20ms Opus packets produced from a real spoken fixture by the host.
    let speech: Vec<Vec<u8>> = std::env::var("ZHIR_LIVE_INPUT_PACKETS")
        .ok()
        .map(|path| serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap())
        .unwrap_or_default();
    let spoken_packets = speech.len();
    assert!(
        spoken_packets < 125,
        "speech fixture must be shorter than 2.5 seconds"
    );
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
    let relay = if blackout {
        let relay =
            network::Relay::start(config.http_client.clone(), config.endpoint.clone()).await;
        config.endpoint = relay.endpoint.clone();
        config.http_client = reqwest::Client::builder().no_proxy().build().unwrap();
        Some(relay)
    } else {
        None
    };
    let traffic = relay.as_ref().map(|relay| relay.traffic.clone());
    let model = Arc::new(zhir_testing::RecordingModel::new(Arc::new(
        live::model(config).unwrap(),
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
    let observed = model.clone();
    let input = tokio::spawn(async move {
        let turn = loop {
            let records = model.records();
            let start = records
                .iter()
                .flat_map(|r| &r.commands)
                .find_map(|c| match &c.body {
                    SessionCommandBody::StartTurn { turn_id, .. } => Some((&c.id, turn_id)),
                    _ => None,
                });
            if let Some((id,turn)) = start && records.iter().flat_map(|r| &r.events).any(|event| matches!(&event.body,SessionEventBody::Acknowledged {command_id,..} if command_id==id)) { break turn.clone(); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let disruption = tokio::spawn(async move {
            if let Some(traffic) = traffic {
                use std::sync::atomic::Ordering;
                tokio::time::sleep(Duration::from_secs(2)).await;
                traffic.dropping.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(8)).await;
                traffic.restored.store(true, Ordering::SeqCst);
                traffic.dropping.store(false, Ordering::SeqCst);
            }
        });
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
                        bytes: sequence
                            .checked_sub(25)
                            .and_then(|index| speech.get(index as usize))
                            .cloned()
                            .unwrap_or_else(|| vec![0xf8, 0xff, 0xfe]),
                        end: sequence == 599,
                    })
                    .await
                    .unwrap();
            }
        });
        tokio::time::sleep(Duration::from_secs(3)).await;
        for enabled in [false, true] {
            let receipt = control.set_input_audio_enabled(enabled).await.unwrap();
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    if model.records().iter().flat_map(|r| &r.events).any(|event| matches!(&event.body,SessionEventBody::Acknowledged {command_id,..} if command_id==&receipt.command_id)) { break; }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.expect("audio input mode was not acknowledged by subscription server");
        }
        control
            .input(Message::user("现在请只说测试成功。"), "test")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        voice.await.unwrap();
        disruption.await.unwrap();
        control.end_input().await.unwrap();
    });
    let result = tokio::time::timeout(Duration::from_secs(45), invocation.result())
        .await
        .unwrap()
        .unwrap()
        .into_checkpoint();
    if !matches!(result.state, zhir_core::run::State::Completed { .. }) {
        for record in observed.records() {
            eprintln!(
                "session events={}, failure={:?}",
                record.events.len(),
                record.failure
            );
            eprintln!(
                "commands={:?}",
                record
                    .commands
                    .iter()
                    .map(|c| (
                        &c.id,
                        match c.body {
                            SessionCommandBody::StartTurn { .. } => "start",
                            SessionCommandBody::EndInput => "end",
                            SessionCommandBody::SetInputAudio { .. } => "input-audio",
                            SessionCommandBody::Close => "close",
                            _ => "context",
                        }
                    ))
                    .collect::<Vec<_>>()
            );
            for event in record.events.iter().rev().take(12).rev() {
                eprintln!("last session event: {:?}", event);
            }
        }
        if let Some(relay) = &relay {
            eprintln!(
                "relay dropped={}, restored={}, client={}, server={}",
                relay
                    .traffic
                    .dropped
                    .load(std::sync::atomic::Ordering::SeqCst),
                relay
                    .traffic
                    .forwarded_after
                    .load(std::sync::atomic::Ordering::SeqCst),
                relay
                    .traffic
                    .client_datagrams
                    .load(std::sync::atomic::Ordering::SeqCst),
                relay
                    .traffic
                    .server_datagrams
                    .load(std::sync::atomic::Ordering::SeqCst)
            );
        }
    }
    assert!(
        matches!(result.state, zhir_core::run::State::Completed { .. }),
        "{:?}",
        result.state
    );
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
    assert!(result.active.session.input_audio_enabled);
    let transcripts: Vec<String> = result
        .history
        .messages()
        .iter()
        .filter_map(|message| match message {
            Message::User { content } => Some(
                content
                    .iter()
                    .filter_map(zhir_core::message::Content::as_text)
                    .collect::<String>(),
            ),
            _ => None,
        })
        .filter(|text| !text.is_empty() && text != "语音测试" && text != "现在请只说测试成功。")
        .collect();
    if spoken_packets > 0 {
        assert!(
            !transcripts.is_empty(),
            "no provider-observed spoken input transcript"
        );
    }
    assert!(result.active.commands.is_empty());
    assert!(result.active.session.media_archive.is_some());
    if let Some(relay) = &relay {
        use std::sync::atomic::Ordering;
        assert_eq!(relay.traffic.creations.load(Ordering::SeqCst), 1);
        assert!(relay.traffic.dropped.load(Ordering::SeqCst) > 0);
        assert!(relay.traffic.forwarded_after.load(Ordering::SeqCst) > 0);
        eprintln!(
            "{}",
            json!({"udp_blackout_ms":8000,"dropped_datagrams":relay.traffic.dropped.load(Ordering::SeqCst),"forwarded_after_recovery":relay.traffic.forwarded_after.load(Ordering::SeqCst),"remote_creations":1})
        );
    }
    eprintln!(
        "{}",
        json!({"state":"completed","audio_packets":chunks.len(),"spoken_input_packets":spoken_packets,"spoken_transcripts":transcripts,"history_entries":result.history.len(),"committed_revisions":result.revision})
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
        let model = live::model(config).unwrap();
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
async fn rejected_signaling_has_one_refresh_attempt_and_preserves_remote_error() {
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
            let body = r#"{"error":{"code":"invalid_token","message":"Token rejected by subscription endpoint"}}"#;
            socket.write_all(format!("HTTP/1.1 401 Unauthorized\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
    });
    let mut config = LiveConfig::new(credentials("test-secret".into(), "test-account".into()));
    config.endpoint = endpoint;
    let model = live::model(config).unwrap();
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
    assert!(
        matches!(&error,zhir_core::error::Error::Model(f) if f.code=="invalid_token" && f.message=="Token rejected by subscription endpoint")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn native_controls_progress_while_audio_output_is_full() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (endpoint, server) = fixture::serve(fixture::Fault::None).await;
        let mut config=LiveConfig::new(credentials("test-secret".into(),"test-account".into()));
        config.endpoint=endpoint;
        config.command_timeout=Duration::from_millis(500);
        let model=live::model(config).unwrap();
        let mut limits=zhir_kernel::defaults::limits();
        limits.max_media_chunk_bytes=3;
        limits.max_buffered_media_bytes=12;
        let mut session=model.open_session(SessionOpen {
            session_id:"pressure".into(),after_sequence:None,output_epoch:0,limits,request:request(),recovery:None,
            context:ModelContext {run:zhir_core::run::RunContext::new("pressure",0),cancellation:Default::default(),deltas:None},
        }).await.unwrap();
        send(&session,"start",SessionCommandBody::StartTurn {turn_id:"execution".into(),request:Box::new(request())}).await;
        loop {
            if matches!(session.output.receive().await.unwrap().unwrap().body,SessionEventBody::Output {..}) {break;}
        }
        // The fixture emits four packets before reading commands; leave media unconsumed.
        for enabled in [false,true] {
            send(&session,"gate",SessionCommandBody::SetInputAudio {enabled}).await;
            let event=tokio::time::timeout(Duration::from_secs(1),session.output.receive()).await.unwrap().unwrap().unwrap();
            assert!(matches!(event.body,SessionEventBody::Acknowledged {command_id,..} if command_id=="gate"));
        }
        send(&session,"close",SessionCommandBody::Close).await;
        let mut media=session.media_output.take().unwrap();
        let reader=tokio::spawn(async move {let mut chunks=vec![];while let Some(chunk)=media.receive().await.unwrap(){chunks.push(chunk);}chunks});
        loop {if matches!(session.output.receive().await.unwrap().unwrap().body,SessionEventBody::Closed){break;}}
        let chunks=reader.await.unwrap();
        assert_eq!(chunks.iter().filter(|c|!c.end).count(),4,"closure lost buffered RTP");
        assert!(chunks.last().unwrap().end);
        while session.output.receive().await.unwrap().is_some() {}
        server.await.unwrap();
    }).await.expect("control or closure stalled behind media");
}

#[path = "gpt_live/scheduling.rs"]
mod scheduling;

#[tokio::test]
async fn audio_control_needs_a_matching_remote_confirmation() {
    use fixture::Fault;
    for fault in [
        Fault::MissingAudioConfirmation,
        Fault::WrongAudioConfirmation,
        Fault::CloseBeforeAudioConfirmation,
    ] {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (endpoint, server) = fixture::serve(fault).await;
            let mut config = LiveConfig::new(credentials("test-secret".into(), "test-account".into()));
            config.endpoint = endpoint;
            config.command_timeout = Duration::from_millis(100);
            let model = live::model(config).unwrap();
            let mut session = open(&model).await;
            send(&session, "start", SessionCommandBody::StartTurn { turn_id: "execution".into(), request: Box::new(request()) }).await;
            loop {
                if matches!(session.output.receive().await.unwrap().unwrap().body, SessionEventBody::Output { .. }) { break; }
            }
            send(&session, "pause", SessionCommandBody::SetInputAudio { enabled: false }).await;
            let error = loop {
                match session.output.receive().await {
                    Err(error) => break error,
                    Ok(Some(event)) => assert!(!matches!(event.body, SessionEventBody::Acknowledged { command_id, .. } if command_id == "pause")),
                    Ok(None) => panic!("unconfirmed command was silently settled"),
                }
            };
            if fault == Fault::WrongAudioConfirmation {
                assert!(matches!(error, zhir_core::error::Error::Protocol(_)), "{error:?}");
            } else {
                assert!(matches!(error, zhir_core::error::Error::Uncertain(_)), "{error:?}");
            }
            drop(session);
            server.await.unwrap();
        }).await.expect("pending control did not terminate");
    }
}
