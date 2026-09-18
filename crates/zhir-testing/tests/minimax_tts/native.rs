use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message as Frame};
use zhir_core::resource::MediaReceiver;
use zhir_core::{error::Error, message::Message, model::*};
use zhir_minimax::tts::{self, TtsConfig};
use zhir_models::credentials::StaticCredential;

fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![Message::user("hello")],
        runtime_tools: vec![],
        provider_tools: vec![],
        profile: Default::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream: true,
    }
}
async fn wire(socket: &mut WebSocketStream<TcpStream>, value: Value) {
    socket
        .send(Frame::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn next(socket: &mut WebSocketStream<TcpStream>) -> Option<Value> {
    while let Some(Ok(frame)) = socket.next().await {
        match frame {
            Frame::Text(text) => return Some(serde_json::from_str(&text).unwrap()),
            Frame::Ping(_) => {
                // The fixed deadline can close the peer while its auto-pong is pending.
                if let Err(error) = socket.flush().await {
                    let closed = match &error {
                        tokio_tungstenite::tungstenite::Error::ConnectionClosed
                        | tokio_tungstenite::tungstenite::Error::AlreadyClosed => true,
                        tokio_tungstenite::tungstenite::Error::Io(error) => matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::ConnectionAborted
                                | std::io::ErrorKind::BrokenPipe
                        ),
                        _ => false,
                    };
                    assert!(closed, "fixture pong flush failed: {error}");
                    return None;
                }
            }
            Frame::Close(_) => return None,
            _ => (),
        }
    }
    None
}
async fn setup(mode: u8) -> (ModelSession, zhir_core::Cancellation, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        wire(&mut socket, json!({"event":"connected_success"})).await;
        assert_eq!(next(&mut socket).await.unwrap()["event"], "task_start");
        if mode == 0 {
            // The transport stays responsive, but the task never acknowledges startup.
            while next(&mut socket).await.is_some() {}
            return;
        }
        wire(&mut socket, json!({"event":"task_started"})).await;
        assert_eq!(next(&mut socket).await.unwrap()["event"], "task_continue");
        if mode == 1 {
            assert_eq!(next(&mut socket).await.unwrap()["text"], " ");
            while next(&mut socket).await.is_some() {}
            return;
        }
        assert_eq!(next(&mut socket).await.unwrap()["event"], "task_finish");
        if mode == 3 {
            wire(&mut socket, json!({"event":"sentence_start"})).await;
            wire(
                &mut socket,
                json!({"event":"sentence_end","data":{"audio":"01020304"}}),
            )
            .await;
        }

        wire(
            &mut socket,
            json!({"event":"task_finished", "session_id":"remote",
            "extra_info":{"usage_characters":5,"audio_length":200}}),
        )
        .await;
        let _ = socket.close(None).await;
    });
    let mut config = TtsConfig::new(
        "fixture-model",
        "fixture-voice",
        Arc::new(StaticCredential::new("Bearer", "fixture")),
    );
    config.connection.url = endpoint;
    config.connection.command_timeout = Duration::from_millis(200);
    config.connection.connect_timeout = Duration::from_millis(50);
    config.connection.write_timeout = Duration::from_millis(20);
    config.connection.heartbeat_interval = Duration::from_millis(20);
    let model = tts::model(config).unwrap();
    let cancel = zhir_core::Cancellation::default();
    let mut limits = zhir_kernel::defaults::limits();
    if mode == 3 {
        limits.max_media_chunk_bytes = 4;
        limits.max_buffered_media_bytes = 4;
    }
    let session = model
        .open_session(SessionOpen {
            binding: None,
            context_revision: 0,
            input_position: 0,
            profile_revision: 0,
            mode: zhir_core::run::RunMode::Interactive,
            session_id: "probe".into(),
            after_sequence: None,
            output_epoch: 0,
            limits,
            request: request(),
            recovery: None,
            context: ModelContext {
                run: zhir_core::run::RunContext::new("probe", 0),
                cancellation: cancel.clone(),
                deltas: None,
            },
        })
        .await
        .unwrap();
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
    (session, cancel, server)
}
async fn ack(session: &mut ModelSession) {
    assert!(
        matches!(event(&mut session.output).await.unwrap().unwrap().body,
        SessionEventBody::Acknowledged { command_id, .. } if command_id == "start")
    );
}

#[tokio::test]
async fn responsive_transport_does_not_extend_protocol_deadline() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (mut session, cancel, server) = setup(0).await;
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), event(&mut session.output))
                .await
                .unwrap(),
            Err(Error::Uncertain(_))
        ));
        cancel.cancel();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn whitespace_fragment_is_forwarded_without_closing() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (mut session, cancel, server) = setup(1).await;
        ack(&mut session).await;
        session.input.send(SessionCommand {id:"space".into(), body:SessionCommandBody::Append {
            context_revision:1,input_position:1,source:AppendSource::Submitted,
            entry:zhir_core::run::HistoryEntry {id:"space".into(),origin:None,message:Message::user(" ")}
        }}).await.unwrap();
        assert!(matches!(event(&mut session.output).await.unwrap().unwrap().body, SessionEventBody::Acknowledged {command_id,..} if command_id=="space"));
        cancel.cancel();
        assert!(matches!(event(&mut session.output).await, Err(Error::Cancelled)));
        server.await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn task_mode_seals_synthesis_and_drains_tail_without_host_control() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
        let (sealed_tx, sealed_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(socket).await.unwrap();
            wire(&mut socket, json!({"event":"connected_success"})).await;
            assert_eq!(next(&mut socket).await.unwrap()["event"], "task_start");
            wire(&mut socket, json!({"event":"task_started"})).await;
            assert_eq!(next(&mut socket).await.unwrap()["event"], "task_continue");
            assert_eq!(next(&mut socket).await.unwrap()["event"], "task_finish");
            sealed_tx.send(()).unwrap();
            release_rx.await.unwrap();
            for value in [
                json!({"event":"sentence_start"}),
                json!({"event":"task_continued","data":{"audio":"010203"}}),
                json!({"event":"sentence_end"}),
                json!({"event":"task_finished"}),
            ] {
                wire(&mut socket, value).await;
            }
            let _ = socket.close(None).await;
        });
        let model = tts::model(super::contracts::config(&endpoint)).unwrap();
        let runtime = zhir_kernel::Runtime::builder(Arc::new(model))
            .resources(Arc::new(zhir_storage::MemoryResourceStore::new()))
            .build()
            .unwrap();
        let mut invocation = runtime
            .start(zhir_kernel::RunRequest::new([Message::user("hello")]))
            .unwrap();
        let mut media = invocation.media_output().unwrap();
        invocation.start();
        tokio::select! {
            sealed = sealed_rx => sealed.unwrap(),
            completion = invocation.result() => panic!("completed before input seal: {completion:?}"),
        }
        assert!(
            invocation
                .control()
                .input(Message::user("too late"), "test")
                .await
                .is_err()
        );
        release_tx.send(()).unwrap();
        let (completion, chunks) = tokio::join!(invocation.result(), async {
            let mut chunks = vec![];
            while let Some(chunk) = media.receive().await.unwrap() {
                chunks.push(chunk);
            }
            chunks
        });
        let checkpoint = completion.unwrap().into_checkpoint();
        assert!(
            matches!(checkpoint.state, zhir_core::run::State::Completed { .. }),
            "{:?}",
            checkpoint.state
        );
        assert_eq!(checkpoint.metrics.generation_requests, 1);
        assert!(checkpoint.active.session.closure.is_some());
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.session_id == checkpoint.active.session.id)
        );
        assert_eq!(
            chunks
                .iter()
                .flat_map(|chunk| chunk.bytes.clone())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(chunks.last().unwrap().end);
        server.await.unwrap();
    })
    .await
    .expect("Task synthesis did not settle");
}

#[tokio::test]
async fn native_events_include_observations_and_provider_metadata() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (mut session, _, server) = setup(2).await;
        ack(&mut session).await;
        session
            .input
            .send(SessionCommand {
                id: "end".into(),
                body: SessionCommandBody::SealUserInput,
            })
            .await
            .unwrap();
        let mut deltas = 0;
        loop {
            match session.output.receive().await.unwrap().unwrap().body {
                SessionEventBody::Delta { .. } => deltas += 1,
                SessionEventBody::ResponseFinished { provider_data, .. } => {
                    assert_eq!(provider_data["extra_info"]["usage_characters"], 5);
                    break;
                }
                _ => (),
            }
        }
        assert!(deltas > 0);
        drop(session);
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn interruption_progresses_while_media_consumer_is_blocked() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
        let (filled_tx, filled_rx) = tokio::sync::oneshot::channel();
        let (seen_tx, mut seen_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            wire(&mut socket, json!({"event":"connected_success"})).await;
            assert_eq!(next(&mut socket).await.unwrap()["event"], "task_start");
            wire(&mut socket, json!({"event":"task_started"})).await;
            assert_eq!(next(&mut socket).await.unwrap()["event"], "task_continue");
            wire(&mut socket, json!({"event":"sentence_start"})).await;
            for _ in 0..3 { wire(&mut socket, json!({"event":"task_continued","data":{"audio":"0102"}})).await; }
            filled_tx.send(()).unwrap();
            assert_eq!(next(&mut socket).await.unwrap()["event"], "task_cancel");
            seen_tx.send(()).unwrap();
            wire(&mut socket, json!({"event":"task_canceled"})).await;
            while next(&mut socket).await.is_some() {}
        });
        let mut config = TtsConfig::new("fixture-model", "fixture-voice", Arc::new(StaticCredential::new("Bearer", "fixture")));
        config.connection.url = endpoint;
        let model = tts::model(config).unwrap();
        let cancellation = zhir_core::Cancellation::default();
        let mut limits = zhir_kernel::defaults::limits();
        limits.max_media_chunk_bytes = 4;
        limits.max_buffered_media_bytes = 4;
        let mut session = model.open_session(SessionOpen {
        binding: None,
 context_revision: 0, input_position: 0, profile_revision: 0, mode: zhir_core::run::RunMode::Interactive,
            session_id:"probe".into(), after_sequence:None, output_epoch:0,
            limits, request:request(), recovery:None,
            context:ModelContext {run:zhir_core::run::RunContext::new("probe",0), cancellation:cancellation.clone(), deltas:None},
        }).await.unwrap();
        session.input.send(SessionCommand {id:"start".into(),body:SessionCommandBody::Generate {generation_id:"turn".into(),context_revision:0,input_position:0,profile_revision:0}}).await.unwrap();
        ack(&mut session).await;
        filled_rx.await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        session.input.send(SessionCommand {id:"interrupt".into(),body:SessionCommandBody::InterruptOutput {generation_id:"turn".into(),output_epoch:7}}).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), &mut seen_rx).await.unwrap().unwrap();
        assert!(matches!(event(&mut session.output).await.unwrap().unwrap().body, SessionEventBody::Acknowledged {command_id,..} if command_id=="interrupt"));
        cancellation.cancel();
        let _ = event(&mut session.output).await;
        assert!(session.media_output.as_mut().unwrap().receive().await.unwrap().is_none(), "retired media escaped");
        server.await.unwrap();
    }).await.unwrap();
}

async fn event(output: &mut Box<dyn SessionReceiver>) -> zhir_core::Result<Option<SessionEvent>> {
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

#[tokio::test]
async fn flush_waits_for_remote_receipt_and_allows_more_input() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
        let (seen, received) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            wire(&mut socket, json!({"event":"connected_success"})).await;
            let start = next(&mut socket).await.unwrap();
            assert_eq!(start["language_boost"], "Chinese");
            assert_eq!(start["pronunciation_dict"]["tone"][0], "测试/(ce4)(shi4)");
            wire(&mut socket, json!({"event":"task_started"})).await;
            assert_eq!(next(&mut socket).await.unwrap()["event"], "task_continue");
            assert_eq!(next(&mut socket).await.unwrap()["event"], "task_flush");
            seen.send(()).unwrap();
            released.await.unwrap();
            wire(&mut socket, json!({"event":"task_flushed"})).await;
            assert_eq!(next(&mut socket).await.unwrap()["text"], "after flush");
            assert_eq!(next(&mut socket).await.unwrap()["event"], "task_finish");
            wire(&mut socket, json!({"event":"task_finished"})).await;
        });
        let mut config = TtsConfig::new("fixture", "voice", Arc::new(StaticCredential::new("Bearer", "fixture")));
        config.connection.url = endpoint;
        config.language_boost = Some("Chinese".into());
        config.pronunciation_dictionary = vec!["测试/(ce4)(shi4)".into()];
        let model = tts::model(config).unwrap();
        let mut session = model.open_session(SessionOpen {
        binding: None,
 context_revision: 0, input_position: 0, profile_revision: 0, mode: zhir_core::run::RunMode::Interactive,
            session_id:"flush".into(), after_sequence:None, output_epoch:0,
            limits:zhir_kernel::defaults::limits(), request:request(), recovery:None,
            context:ModelContext {run:zhir_core::run::RunContext::new("flush",0), cancellation:Default::default(), deltas:None},
        }).await.unwrap();
        session.input.send(SessionCommand {id:"start".into(),body:SessionCommandBody::Generate {generation_id:"turn".into(),context_revision:0,input_position:0,profile_revision:0}}).await.unwrap();
        ack(&mut session).await;
        session.input.send(SessionCommand {id:"flush".into(),body:SessionCommandBody::FlushInput}).await.unwrap();
        received.await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(30),event(&mut session.output)).await.is_err(), "flush acknowledged before the remote barrier");
        release.send(()).unwrap();
        assert!(matches!(event(&mut session.output).await.unwrap().unwrap().body,SessionEventBody::Acknowledged {command_id,..} if command_id=="flush"));
        session.input.send(SessionCommand {id:"more".into(),body:SessionCommandBody::Append {context_revision:1,input_position:1,source:AppendSource::Submitted,entry:zhir_core::run::HistoryEntry {id:"more".into(),origin:None,message:Message::user("after flush")}}}).await.unwrap();
        session.input.send(SessionCommand {id:"end".into(),body:SessionCommandBody::SealUserInput}).await.unwrap();
        loop {
            if matches!(event(&mut session.output).await.unwrap().unwrap().body,SessionEventBody::ResponseFinished {..}) {break;}
        }
        server.await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn final_audio_payload_can_fill_the_media_byte_budget() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (mut session, _, server) = setup(3).await;
        let mut media = session.media_output.take().unwrap();
        let reader = tokio::spawn(async move {
            let mut chunks = vec![];
            while let Some(chunk) = media.receive().await.unwrap() {
                chunks.push(chunk);
            }
            chunks
        });
        ack(&mut session).await;
        session
            .input
            .send(SessionCommand {
                id: "finish".into(),
                body: SessionCommandBody::SealUserInput,
            })
            .await
            .unwrap();
        loop {
            if matches!(
                event(&mut session.output).await.unwrap().unwrap().body,
                SessionEventBody::ResponseFinished { .. }
            ) {
                break;
            }
        }
        let chunks = reader.await.unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].bytes, [1, 2, 3, 4]);
        assert!(chunks[1].end && chunks[1].bytes.is_empty());
        server.await.unwrap();
    })
    .await
    .unwrap();
}
