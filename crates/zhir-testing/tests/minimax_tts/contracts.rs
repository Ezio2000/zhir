use zhir_minimax::tts::{self, TtsConfig};
// Public SDK contracts, transport authentication and task lifetime checks.
use futures::{SinkExt, StreamExt};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot};
use tokio_tungstenite::{
    accept_async, accept_hdr_async,
    tungstenite::{self, Message as Frame},
};
use zhir_core::{credential::Credential, error::Error, message::Message, model::*};
use zhir_models::credentials::{RefreshingCredential, StaticCredential};

pub(super) fn config(url: &str) -> TtsConfig {
    let mut config = TtsConfig::new(
        "fixture-model",
        "fixture-voice",
        Arc::new(StaticCredential::new("Bearer", "fixture-key")),
    );
    config.connection.url = url.into();
    config
}
pub(super) fn open() -> SessionOpen {
    SessionOpen {
        binding: None,
        context_revision: 0,
        input_position: 0,
        profile_revision: 0,
        mode: zhir_core::run::RunMode::Interactive,
        session_id: "fixture-session".into(),
        after_sequence: None,
        output_epoch: 0,
        limits: zhir_kernel::defaults::limits(),
        recovery: None,
        request: ModelRequest {
            messages: vec![Message::user("hello")],
            runtime_tools: vec![],
            provider_tools: vec![],
            profile: Default::default(),
            tool_choice: Default::default(),
            response_format: None,
            stream: true,
        },
        context: ModelContext {
            run: zhir_kernel::defaults::context(),
            cancellation: Default::default(),
            deltas: None,
        },
    }
}
pub(super) async fn send_json(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    value: serde_json::Value,
) {
    socket
        .send(Frame::Text(value.to_string().into()))
        .await
        .unwrap();
}

#[tokio::test]
async fn tts_rejects_unsupported_requests_and_recovery_before_connecting() {
    let model = tts::model(config("ws://127.0.0.1:1")).unwrap();
    assert!(model.capabilities().supports(Capability::Duplex));
    assert!(model.capabilities().supports(Capability::Streaming));
    assert!(!model.capabilities().supports(Capability::Resume));
    assert_eq!(model.capabilities().input_modalities, ["text"]);
    assert!(model.negotiate(&open().request).is_ok());
    for request in [
        {
            let mut r = open().request;
            r.messages = vec![Message::user("")];
            r
        },
        {
            let mut r = open().request;
            r.messages = vec![Message::user("a".repeat(10001))];
            r
        },
        {
            let mut r = open().request;
            r.profile.generation.temperature = Some(0.5);
            r
        },
        {
            let mut r = open().request;
            r.response_format = Some(ResponseFormat::Json);
            r
        },
        {
            let mut r = open().request;
            r.tool_choice = ToolChoice::Required;
            r
        },
    ] {
        assert!(matches!(model.negotiate(&request), Err(Error::Invalid(_))));
    }
    for recovery in [false, true] {
        let mut request = open();
        if recovery {
            request.recovery = Some(zhir_core::operation::RecoveryRef {
                adapter: "minimax".into(),
                data: serde_json::Value::Null,
            });
        } else {
            request.after_sequence = Some(0);
        }
        assert!(matches!(
            model.open_session(request).await,
            Err(Error::Protocol(_))
        ));
    }
    let mut invalid = config("ws://127.0.0.1:1");
    invalid.voice.speed = f64::NAN;
    assert!(tts::model(invalid).is_err());
    let mut invalid = config("ws://127.0.0.1:1");
    invalid.connection.heartbeat_interval = Duration::ZERO;
    assert!(tts::model(invalid).is_err());
}

#[tokio::test]
#[allow(
    clippy::result_large_err,
    reason = "tungstenite handshake callbacks require an unboxed HTTP response"
)]
async fn websocket_refreshes_one_rejected_handshake_and_preserves_account_headers() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint=format!("ws://{}/tts",listener.local_addr().unwrap());
        let (ping, got_ping)=oneshot::channel();
        let server=tokio::spawn(async move {
            for attempt in 1..=2 {
                let (socket,_)=listener.accept().await.unwrap();
                let result=accept_hdr_async(socket, move |request: &tungstenite::handshake::server::Request, response: tungstenite::handshake::server::Response| {
                    assert_eq!(request.headers()["Authorization"], format!("Bearer token-{attempt}"));
                    assert_eq!(request.headers()["x-account-id"], "fixture-account");
                    if attempt==1 {
                        Err(tungstenite::http::Response::builder().status(401).body(Some("do not log response secrets".into())).unwrap())
                    } else { Ok(response) }
                }).await;
                if attempt==1 { assert!(result.is_err()); continue; }
                let mut socket=result.unwrap();
                send_json(&mut socket, serde_json::json!({"event":"connected_success"})).await;
                assert!(matches!(socket.next().await, Some(Ok(Frame::Ping(_)))));
                socket.flush().await.unwrap();
                ping.send(()).unwrap();
                assert!(matches!(socket.next().await, Some(Ok(Frame::Close(_)))));
                break;
            }
        });
        let calls=Arc::new(AtomicUsize::new(0));
        let count=calls.clone();
        let audience=endpoint.clone();
        let credentials=Arc::new(RefreshingCredential::new(move |context| {
            assert_eq!(context.audience,audience);
            let generation=count.fetch_add(1,Ordering::SeqCst)+1;
            Box::pin(async move { Ok(Credential {
                generation:generation.to_string(), scheme:"Bearer".into(), value:format!("token-{generation}"), expires_at_ms:None,
                metadata:[("header:x-account-id".into(),"fixture-account".into()),("ignored-metadata".into(),"value".into())].into(),
            }) })
        }));
        let mut settings=config(&endpoint);
        settings.connection.credentials=credentials;
        settings.connection.heartbeat_interval=Duration::from_millis(20);
        let model=tts::model(settings).unwrap();
        let mut session=model.open_session(open()).await.unwrap();
        assert!(session.media_input.is_none());
        assert!(session.media_output.is_some());
        got_ping.await.unwrap();
        session.input.send(SessionCommand { id:"close".into(), body:SessionCommandBody::Close }).await.unwrap();
        let first=next_event(&mut session.output).await.unwrap().unwrap();
        assert_eq!(first.sequence,2);
        assert!(matches!(first.body,SessionEventBody::Acknowledged { command_id, .. } if command_id=="close"));
        assert!(matches!(next_event(&mut session.output).await.unwrap().unwrap().body,SessionEventBody::Closed { .. }));
        assert!(next_event(&mut session.output).await.unwrap().is_none());
        server.await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst),2);
    }).await.unwrap();
}

#[tokio::test]
#[allow(
    clippy::result_large_err,
    reason = "tungstenite handshake callbacks require an unboxed HTTP response"
)]
async fn websocket_unauthorized_handshake_has_a_fixed_budget_and_redacted_errors() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                let result = accept_hdr_async(
                    socket,
                    |_: &tungstenite::handshake::server::Request,
                     _: tungstenite::handshake::server::Response| {
                        Err(tungstenite::http::Response::builder()
                            .status(401)
                            .body(Some("fixture-key".into()))
                            .unwrap())
                    },
                )
                .await;
                assert!(result.is_err());
            }
        });
        let model = tts::model(config(&endpoint)).unwrap();
        let error = model.open_session(open()).await.err().unwrap();
        assert!(matches!(&error,Error::Model(failure) if failure.code=="http_401"));
        assert!(!error.to_string().contains("fixture-key"));
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn websocket_cancellation_and_dropped_consumers_release_a_pending_task() {
    for cancel in [true, false] {
        tokio::time::timeout(Duration::from_secs(3), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
            let (started, ready) = oneshot::channel();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(socket).await.unwrap();
                send_json(
                    &mut socket,
                    serde_json::json!({"event":"connected_success"}),
                )
                .await;
                let Some(Ok(Frame::Text(frame))) = socket.next().await else {
                    panic!("missing task_start")
                };
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&frame).unwrap()["event"],
                    "task_start"
                );
                started.send(()).unwrap();
                assert!(!matches!(socket.next().await, Some(Ok(Frame::Text(_)))));
            });
            let model = tts::model(config(&endpoint)).unwrap();
            let open = open();
            let cancellation = open.context.cancellation.clone();
            let mut session = model.open_session(open).await.unwrap();
            session
                .input
                .send(SessionCommand {
                    id: "start".into(),
                    body: SessionCommandBody::Generate {
                        generation_id: "turn".into(),
                        context_revision: 0,
                        input_position: 0,
                        profile_revision: 0,
                    },
                })
                .await
                .unwrap();
            ready.await.unwrap();
            if cancel {
                cancellation.cancel();
                assert!(matches!(
                    next_event(&mut session.output).await,
                    Err(Error::Cancelled)
                ));
                assert!(
                    session
                        .media_output
                        .as_mut()
                        .unwrap()
                        .receive()
                        .await
                        .unwrap()
                        .is_none()
                );
            } else {
                // Keep the sender alive: dropping the event consumer must still stop the actor.
                drop(session.output);
            }
            server.await.unwrap();
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn websocket_connect_timeout_includes_a_stalled_credential_provider() {
    let mut settings = config("ws://127.0.0.1:1");
    settings.connection.connect_timeout = Duration::from_millis(20);
    settings.connection.credentials = Arc::new(RefreshingCredential::new(|_| {
        Box::pin(std::future::pending())
    }));
    let model = tts::model(settings).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), model.open_session(open()))
        .await
        .unwrap();
    assert!(matches!(result, Err(Error::Deadline)));
}

#[tokio::test]
async fn websocket_terminal_error_survives_a_full_event_queue() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(socket).await.unwrap();
            send_json(
                &mut socket,
                serde_json::json!({"event":"connected_success"}),
            )
            .await;
            assert!(matches!(socket.next().await, Some(Ok(Frame::Text(_)))));
            send_json(&mut socket, serde_json::json!({"event":"task_started"})).await;
            assert!(matches!(socket.next().await, Some(Ok(Frame::Text(_)))));
            // Generate's acknowledgement fills the single event slot.
            send_json(&mut socket, serde_json::json!({"event":"unknown_event"})).await;
            assert!(!matches!(socket.next().await, Some(Ok(Frame::Text(_)))));
        });
        let mut settings = config(&endpoint);
        settings.connection.write_timeout = Duration::from_millis(10);
        let model = tts::model(settings).unwrap();
        let mut opening = open();
        opening.limits.max_session_events = 1;
        let mut session = model.open_session(opening).await.unwrap();
        session
            .input
            .send(SessionCommand {
                id: "start".into(),
                body: SessionCommandBody::Generate {
                    generation_id: "turn".into(),
                    context_revision: 0,
                    input_position: 0,
                    profile_revision: 0,
                },
            })
            .await
            .unwrap();
        // Delay consumption beyond the previous error-delivery timeout.
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(matches!(
            next_event(&mut session.output).await.unwrap().unwrap().body,
            SessionEventBody::Acknowledged { .. }
        ));
        assert!(matches!(
            next_event(&mut session.output).await,
            Err(Error::Protocol(_))
        ));
        assert!(next_event(&mut session.output).await.unwrap().is_none());
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn websocket_rejects_malformed_events_without_guessing_field_types() {
    for event in [
        serde_json::json!({"event":"connected_success","base_resp":{"status_code":"0"}}),
        serde_json::json!({"event":17,"data":{"audio":""}}),
        serde_json::json!({"event":"connected_success","data":{"audio":12}}),
        serde_json::json!({"event":"","data":{"audio":""}}),
        serde_json::json!([]),
    ] {
        tokio::time::timeout(Duration::from_secs(2), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(socket).await.unwrap();
                send_json(&mut socket, event).await;
                let _ = socket.next().await;
            });
            let model = tts::model(config(&endpoint)).unwrap();
            let mut session = model.open_session(open()).await.unwrap();
            assert!(matches!(
                next_event(&mut session.output).await,
                Err(Error::Protocol(_))
            ));
            server.await.unwrap();
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
#[allow(
    clippy::result_large_err,
    reason = "tungstenite handshake callbacks require an unboxed HTTP response"
)]
async fn api_and_token_plan_keys_use_the_same_bearer_handshake() {
    for key in ["sk-api-fixture", "sk-cp-fixture"] {
        tokio::time::timeout(Duration::from_secs(2), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("ws://{}/ws/v1/t2a_v2_bidi", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = accept_hdr_async(
                    socket,
                    move |request: &tungstenite::handshake::server::Request,
                          response: tungstenite::handshake::server::Response| {
                        assert_eq!(request.uri().path(), "/ws/v1/t2a_v2_bidi");
                        assert_eq!(request.headers()["authorization"], format!("Bearer {key}"));
                        Ok(response)
                    },
                )
                .await
                .unwrap();
                send_json(
                    &mut socket,
                    serde_json::json!({"event":"connected_success"}),
                )
                .await;
                assert!(matches!(socket.next().await, Some(Ok(Frame::Close(_)))));
            });
            let mut settings = config(&endpoint);
            settings.connection.credentials = Arc::new(StaticCredential::new("Bearer", key));
            let model = tts::model(settings).unwrap();
            let mut session = model.open_session(open()).await.unwrap();
            session
                .input
                .send(SessionCommand {
                    id: "close".into(),
                    body: SessionCommandBody::Close,
                })
                .await
                .unwrap();
            while next_event(&mut session.output).await.unwrap().is_some() {}
            server.await.unwrap();
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn websocket_requires_real_sentence_boundaries_before_completion() {
    for duplicate_end in [false, true] {
        tokio::time::timeout(Duration::from_secs(2), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(socket).await.unwrap();
                send_json(
                    &mut socket,
                    serde_json::json!({"event":"connected_success"}),
                )
                .await;
                assert!(matches!(socket.next().await, Some(Ok(Frame::Text(_)))));
                send_json(&mut socket, serde_json::json!({"event":"task_started"})).await;
                assert!(matches!(socket.next().await, Some(Ok(Frame::Text(_)))));
                send_json(&mut socket, serde_json::json!({"event":"sentence_start"})).await;
                // The current protocol documents unnamed audio frames as well.
                send_json(&mut socket, serde_json::json!({"data":{"audio":"0102"}})).await;
                assert!(matches!(socket.next().await, Some(Ok(Frame::Text(_)))));
                if duplicate_end {
                    send_json(&mut socket, serde_json::json!({"event":"sentence_end"})).await;
                    send_json(&mut socket, serde_json::json!({"event":"sentence_end"})).await;
                } else {
                    send_json(&mut socket, serde_json::json!({"event":"task_finished"})).await;
                }
                let _ = socket.next().await;
            });
            let model = tts::model(config(&endpoint)).unwrap();
            let opening = open();
            let mut session = model.open_session(opening).await.unwrap();
            session
                .input
                .send(SessionCommand {
                    id: "start".into(),
                    body: SessionCommandBody::Generate {
                        generation_id: "turn".into(),
                        context_revision: 0,
                        input_position: 0,
                        profile_revision: 0,
                    },
                })
                .await
                .unwrap();
            assert!(matches!(
                next_event(&mut session.output).await.unwrap().unwrap().body,
                SessionEventBody::Acknowledged { .. }
            ));
            session
                .input
                .send(SessionCommand {
                    id: "finish".into(),
                    body: SessionCommandBody::SealUserInput,
                })
                .await
                .unwrap();
            assert!(matches!(
                next_event(&mut session.output).await,
                Err(Error::Protocol(_))
            ));
            assert_eq!(
                session
                    .media_output
                    .as_mut()
                    .unwrap()
                    .receive()
                    .await
                    .unwrap()
                    .unwrap()
                    .bytes,
                [1, 2]
            );
            server.await.unwrap();
        })
        .await
        .unwrap();
    }
}

pub(super) async fn next_event(
    output: &mut Box<dyn SessionReceiver>,
) -> zhir_core::Result<Option<SessionEvent>> {
    loop {
        let event = output.receive().await?;
        if !matches!(
            &event,
            Some(SessionEvent {
                body: SessionEventBody::Delta { .. }
                    | SessionEventBody::Ready { .. }
                    | SessionEventBody::ResponseStarted { .. },
                ..
            })
        ) {
            return Ok(event);
        }
    }
}
