use super::*;

async fn configured(
    fault: fixture::Fault,
    limits: zhir_core::run::Limits,
) -> (ModelSession, tokio::task::JoinHandle<fixture::Evidence>) {
    let (endpoint, server) = fixture::serve(fault).await;
    let mut config = LiveConfig::new(credentials("test-secret".into(), "test-account".into()));
    config.endpoint = endpoint;
    config.command_timeout = Duration::from_millis(500);
    let model = live::model(config).unwrap();
    let session = model
        .open_session(SessionOpen {
            session_id: "scheduling".into(),
            after_sequence: None,
            output_epoch: 0,
            limits,
            request: request(),
            recovery: None,
            context: ModelContext {
                run: zhir_core::run::RunContext::new("scheduling", 0),
                cancellation: Default::default(),
                deltas: None,
            },
        })
        .await
        .unwrap();
    send(
        &session,
        "start",
        SessionCommandBody::StartTurn {
            turn_id: "execution".into(),
            request: Box::new(request()),
        },
    )
    .await;
    (session, server)
}
async fn close(mut session: ModelSession) -> Vec<MediaChunk> {
    send(&session, "close", SessionCommandBody::Close).await;
    let mut media = session.media_output.take().unwrap();
    let reader = tokio::spawn(async move {
        let mut chunks = vec![];
        while let Some(chunk) = media.receive().await.unwrap() {
            chunks.push(chunk);
        }
        chunks
    });
    let mut closed = 0;
    let mut acknowledged = 0;
    while let Some(event) = session.output.receive().await.unwrap() {
        match event.body {
            SessionEventBody::Closed => closed += 1,
            SessionEventBody::Acknowledged { command_id, .. } if command_id == "close" => {
                acknowledged += 1
            }
            _ => (),
        }
    }
    assert_eq!(
        (closed, acknowledged),
        (1, 1),
        "Close must settle without a second Close command"
    );
    reader.await.unwrap()
}

#[tokio::test]
async fn confirmed_controls_survive_blocked_event_delivery() {
    tokio::time::timeout(Duration::from_secs(6), async {
        let mut limits = zhir_kernel::defaults::limits();
        limits.max_session_events = 8;
        let (mut session, server) = configured(fixture::Fault::EventPressure, limits).await;
        assert!(matches!(session.output.receive().await.unwrap().unwrap().body, SessionEventBody::Acknowledged { command_id, .. } if command_id == "start"));
        tokio::time::sleep(Duration::from_millis(300)).await;
        for (id, enabled) in [("pause", false), ("resume", true)] {
            send(&session, id, SessionCommandBody::SetInputAudio { enabled }).await;
        }
        // Both remote confirmations must be processed while public delivery stalls.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let mut acknowledged = vec![];
        let mut observations = vec![];
        let mut previous_sequence = 0;
        while acknowledged.len() < 2 {
            let event = session.output.receive().await.unwrap().unwrap();
            assert!(event.sequence > previous_sequence);
            previous_sequence = event.sequence;
            match event.body {
                SessionEventBody::Acknowledged { command_id, .. } => acknowledged.push(command_id),
                SessionEventBody::Delta { delta: ModelDelta::ProtocolEvent { data, .. }, .. }
                    if data["type"] == "fixture.observation" => observations.push(data["i"].as_u64().unwrap()),
                _ => (),
            }
        }
        assert_eq!(acknowledged, ["pause", "resume"]);
        assert_eq!(observations, (0..10).collect::<Vec<_>>());
        close(session).await;
        let evidence = server.await.unwrap();
        assert_eq!(evidence.commands.iter().filter(|e| matches!(e["type"].as_str(), Some("input_audio.pause" | "input_audio.resume"))).count(), 2);
    }).await.expect("confirmed control blocked behind observation delivery");
}

#[tokio::test]
async fn small_audio_packets_use_bytes_independently_of_event_capacity() {
    tokio::time::timeout(Duration::from_secs(6), async {
        let mut limits = zhir_kernel::defaults::limits();
        limits.max_session_events = 8;
        limits.max_media_chunk_bytes = 1024 * 1024;
        limits.max_buffered_media_bytes = 1024 * 1024;
        let (mut session, server) = configured(fixture::Fault::SmallPackets, limits).await;
        loop { if matches!(session.output.receive().await.unwrap().unwrap().body, SessionEventBody::Output { .. }) { break; } }
        send(&session, "pause", SessionCommandBody::SetInputAudio { enabled: false }).await;
        // The fixture sends all twelve packets before processing this command.
        assert!(matches!(session.output.receive().await.unwrap().unwrap().body, SessionEventBody::Acknowledged { command_id, .. } if command_id == "pause"));
        let chunks = close(session).await;
        assert_eq!(chunks.iter().filter(|c| !c.end).count(), 12);
        assert!(chunks.last().unwrap().end);
        assert_eq!(chunks.iter().map(|c| c.bytes.len()).sum::<usize>(), 36);
        server.await.unwrap();
    }).await.expect("small-packet buffering exhausted an unrelated event limit");
}

#[tokio::test]
async fn actual_media_budget_exhaustion_is_explicit() {
    tokio::time::timeout(Duration::from_secs(6), async {
        let mut limits = zhir_kernel::defaults::limits();
        limits.max_media_chunk_bytes = 3;
        limits.max_buffered_media_bytes = 3;
        let (mut session, server) = configured(fixture::Fault::None, limits).await;
        let error = loop {
            match session.output.receive().await {
                Ok(Some(_)) => (), Err(error) => break error, Ok(None) => panic!("overflow became clean EOF"),
            }
        };
        assert!(matches!(error, zhir_core::error::Error::Uncertain(message) if message.contains("media receive budget exceeded")));
        assert!(session.output.receive().await.unwrap().is_none());
        server.abort();
    }).await.unwrap();
}

#[tokio::test]
async fn malformed_and_out_of_order_events_fail_after_preserving_accepted_history() {
    for event in [
        r#"{"type":"turn.done","turn":{"id":"bad","role":17,"transcript":"text"}}"#,
        r#"{"type":"turn.done","turn":{"id":"","role":"user","transcript":"text"}}"#,
        r#"{"type":"session.started","session":{"id":"duplicate"}}"#,
        r#"{"type":"delegation.created","item":{"id":"","type":"delegation","target":"client","content":[]}}"#,
    ] {
        tokio::time::timeout(Duration::from_secs(4), async {
            let (mut session, server) = configured(
                fixture::Fault::InvalidEvent(event),
                zhir_kernel::defaults::limits(),
            )
            .await;
            let mut items = 0;
            let error = loop {
                match session.output.receive().await {
                    Ok(Some(event)) => {
                        if matches!(event.body, SessionEventBody::ConversationItem { .. }) {
                            items += 1;
                        }
                    }
                    Err(error) => break error,
                    Ok(None) => panic!("malformed protocol became clean EOF"),
                }
            };
            assert!(
                matches!(error, zhir_core::error::Error::Protocol(_)),
                "{error:?}"
            );
            assert_eq!(items, 2);
            assert!(session.output.receive().await.unwrap().is_none());
            server.await.unwrap();
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn provider_rejection_settles_while_public_events_are_blocked() {
    tokio::time::timeout(Duration::from_secs(6), async {
        let mut limits = zhir_kernel::defaults::limits();
        limits.max_session_events = 8;
        let (mut session, server) = configured(fixture::Fault::RejectedUnderPressure, limits).await;
        assert!(matches!(session.output.receive().await.unwrap().unwrap().body, SessionEventBody::Acknowledged { command_id, .. } if command_id == "start"));
        tokio::time::sleep(Duration::from_millis(300)).await;
        send(&session, "pause", SessionCommandBody::SetInputAudio { enabled: false }).await;
        server.await.unwrap();
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(session.input.send(SessionCommand { id: "after-failure".into(), body: SessionCommandBody::Input { message: Message::user("late") } }).await.is_err(), "worker must settle without waiting for event consumption");
        let mut observations = 0;
        let mut rejection = false;
        let error = loop {
            match session.output.receive().await {
                Ok(Some(event)) => match event.body {
                    SessionEventBody::Delta { delta: ModelDelta::ProtocolEvent { data, .. }, .. } => {
                        if data["type"] == "fixture.observation" { observations += 1; }
                        if data["type"] == "error" {
                            assert_eq!(data["error"]["event_id"], "pause");
                            rejection = true;
                        }
                    }
                    SessionEventBody::Acknowledged { .. } => panic!("rejected command was acknowledged"),
                    _ => (),
                },
                Err(error) => break error,
                Ok(None) => panic!("provider rejection became clean EOF"),
            }
        };
        assert_eq!(observations, 10);
        assert!(rejection);
        assert!(matches!(error, zhir_core::error::Error::Model(failure) if failure.code == "control_rejected"));
        assert!(session.output.receive().await.unwrap().is_none());
    }).await.unwrap();
}
