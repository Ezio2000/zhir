use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use zhir_core::{
    error::Error,
    message::{Content, Message, Output, ProviderToolCall, ProviderToolStatus},
    model::{Capability, SessionCommand, SessionCommandBody, SessionEventBody, TurnDisposition},
    operation::{CallRef, OperationEvent, OperationUpdate, RecoveryRef},
    run::{Checkpoint, State},
    tool::RuntimeToolOutcome,
};
use zhir_kernel::{ResumeRequest, RunRequest, Runtime};
use zhir_testing::{RecordingStore, SessionModel};
async fn settle(mut invocation: zhir_kernel::Invocation) -> Arc<Checkpoint> {
    tokio::time::timeout(Duration::from_secs(3), invocation.result())
        .await
        .expect("runtime stalled")
        .unwrap()
        .into_checkpoint()
}
fn provider(status: ProviderToolStatus, text: Option<&str>) -> Output {
    Output::ProviderToolCall {
        call: ProviderToolCall {
            outcome: None,
            provider: "media".into(),
            name: "video".into(),
            id: "job".into(),
            status,
            output: text.into_iter().map(Content::text).collect(),
            data: json!({}),
        },
    }
}
#[tokio::test]
async fn provider_continuation_reuses_operation_and_includes_its_final_output() {
    let model = Arc::new(SessionModel::new(
        zhir_testing::model_capabilities(),
        |_, mut peer| async move {
            for index in 0..2 {
                let command = peer.commands.recv().await.unwrap();
                let SessionCommandBody::StartTurn { turn_id, .. } = &command.body else {
                    panic!("expected turn");
                };
                let turn_id = turn_id.clone();
                peer.acknowledge(&command, None).await?;
                peer.event(SessionEventBody::Output {
                    turn_id: turn_id.clone(),
                    item_id: "same-provider-item".into(),
                    caller_id: "model".into(),
                    output: if index == 0 {
                        provider(ProviderToolStatus::Running, None)
                    } else {
                        provider(ProviderToolStatus::Completed, Some("video ready"))
                    },
                })
                .await?;
                peer.finished(
                    turn_id,
                    if index == 0 {
                        TurnDisposition::Continue
                    } else {
                        TurnDisposition::Finished
                    },
                )
                .await?;
            }
            Ok(())
        },
    ));
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir_storage::MemoryRunStore::new(),
    )));
    let runtime = Runtime::builder(model)
        .store(store.clone())
        .build()
        .unwrap();
    let final_state = settle(
        runtime
            .start(RunRequest::new(vec![Message::user("video")]))
            .unwrap(),
    )
    .await;
    assert!(
        matches!(&final_state.state, State::Completed { content } if content == &vec![Content::text("video ready")]),
        "{:?}",
        final_state.state
    );
    let ids: std::collections::BTreeSet<_> = store
        .commits()
        .iter()
        .flat_map(|c| c.checkpoint.active.operations.keys().cloned())
        .collect();
    assert_eq!(ids.len(), 1);
    let projected = zhir_core::model::conversation(final_state.history.entries());
    let calls: Vec<_> = projected
        .iter()
        .filter_map(|m| {
            if let Message::Assistant { output, .. } = m {
                Some(output)
            } else {
                None
            }
        })
        .flatten()
        .filter(|o| matches!(o, Output::ProviderToolCall { .. }))
        .collect();
    assert_eq!(calls.len(), 1);
    store.verify_traces().unwrap();
}
#[tokio::test]
async fn uncertain_outbox_is_attached_without_resending_and_only_cas_winner_opens() {
    let opens = Arc::new(AtomicUsize::new(0));
    let sent = Arc::new(Mutex::new(None::<SessionCommand>));
    let model = Arc::new(SessionModel::new(zhir_testing::model_capabilities(), {
        let opens = opens.clone();
        let sent = sent.clone();
        move |open, mut peer| {
            let opens = opens.clone();
            let sent = sent.clone();
            async move {
                opens.fetch_add(1, Ordering::SeqCst);
                if open.recovery.is_none() {
                    let command = peer.commands.recv().await.unwrap();
                    *sent.lock().unwrap() = Some(command);
                    peer.event(SessionEventBody::Recovery {
                        reference: RecoveryRef {
                            adapter: "fixture".into(),
                            data: json!("remote-session"),
                        },
                    })
                    .await?;
                    return Err(Error::Protocol(
                        "connection lost before command acknowledgement".into(),
                    ));
                }
                assert_eq!(open.after_sequence, Some(0));
                let command = sent.lock().unwrap().clone().unwrap();
                // Give a broken implementation time to redispatch before delivering its recovered ack.
                assert!(
                    tokio::time::timeout(Duration::from_millis(50), peer.commands.recv())
                        .await
                        .is_err(),
                    "sent command was replayed"
                );
                let SessionCommandBody::StartTurn { turn_id, .. } = &command.body else {
                    unreachable!()
                };
                peer.acknowledge(&command, open.recovery).await?;
                peer.event(SessionEventBody::Output {
                    turn_id: turn_id.clone(),
                    item_id: "answer".into(),
                    caller_id: "model".into(),
                    output: Output::text("recovered"),
                })
                .await?;
                peer.finished(turn_id.clone(), TurnDisposition::Finished)
                    .await?;
                Ok(())
            }
        }
    }));
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir_storage::MemoryRunStore::new(),
    )));
    let runtime = Runtime::builder(model)
        .store(store.clone())
        .build()
        .unwrap();
    let checkpoint = settle(
        runtime
            .start(RunRequest::new(vec![Message::user("start")]))
            .unwrap(),
    )
    .await;
    assert!(matches!(checkpoint.state, State::Suspended { .. }));
    assert!(checkpoint.active.commands[0].sent);
    let mut first = runtime
        .resume(ResumeRequest::from_checkpoint(checkpoint.clone()))
        .await
        .unwrap();
    let mut second = runtime
        .resume(ResumeRequest::from_checkpoint(checkpoint))
        .await
        .unwrap();
    let (a, b) = tokio::join!(first.result(), second.result());
    let (ok, error) = match (a, b) {
        (Ok(ok), Err(error)) | (Err(error), Ok(ok)) => (ok, error),
        _ => panic!("exactly one attach must win"),
    };
    assert!(matches!(error.error, Error::Conflict { .. }));
    assert!(matches!(ok.checkpoint().state, State::Completed { .. }));
    assert_eq!(opens.load(Ordering::SeqCst), 2);
    store.verify_traces().unwrap();
}
#[tokio::test]
async fn duplicate_provider_completion_is_idempotent_and_conflicts_fail() {
    for conflict in [false, true] {
        let mut caps = zhir_testing::model_capabilities();
        caps.features.insert(Capability::AsyncResults);
        let model = Arc::new(SessionModel::new(caps, move |open, mut peer| async move {
            let command = peer.commands.recv().await.unwrap();
            let SessionCommandBody::StartTurn { turn_id, .. } = &command.body else {
                unreachable!()
            };
            let turn_id = turn_id.clone();
            peer.acknowledge(&command, None).await?;
            peer.event(SessionEventBody::Output {
                turn_id: turn_id.clone(),
                item_id: "job".into(),
                caller_id: "model".into(),
                output: provider(ProviderToolStatus::Running, None),
            })
            .await?;
            for i in 0..2 {
                peer.event(SessionEventBody::Operation {
                    origin: CallRef {
                        session_id: open.session_id.clone(),
                        turn_id: turn_id.clone(),
                        caller_id: "model".into(),
                        call_id: "job".into(),
                    },
                    event: OperationEvent {
                        sequence: 4,
                        update: OperationUpdate::Finished {
                            outcome: RuntimeToolOutcome::Success {
                                content: vec![Content::text(if conflict && i == 1 {
                                    "different"
                                } else {
                                    "done"
                                })],
                                structured: json!(null),
                            },
                        },
                    },
                })
                .await?;
            }
            peer.finished(turn_id, TurnDisposition::Finished).await?;
            Ok(())
        }));
        let runtime = Runtime::builder(model).build().unwrap();
        let checkpoint = settle(
            runtime
                .start(RunRequest::new(vec![Message::user("job")]))
                .unwrap(),
        )
        .await;
        assert_eq!(
            matches!(checkpoint.state, State::Completed { .. }),
            !conflict,
            "{:?}",
            checkpoint.state
        );
        assert_eq!(
            checkpoint
                .history
                .entries()
                .iter()
                .filter(|e| e.id.starts_with("operation:"))
                .count(),
            1
        );
        if conflict {
            assert!(
                matches!(&checkpoint.state, State::Failed { error } if error.message.contains("conflicting"))
            );
        }
    }
}

#[tokio::test]
async fn duplex_end_input_drains_buffered_media_and_interrupt_commits_with_its_intent() {
    use zhir_core::{
        model::*,
        profile::*,
        resource::*,
        run::{CommandIntent, RunMode},
    };
    let (ready, mut turn_rx) = tokio::sync::mpsc::channel(1);
    let mut caps = zhir_testing::model_capabilities();
    caps.features.extend([
        Capability::Duplex,
        Capability::Steering,
        Capability::ProfileUpdates,
    ]);
    caps.constraints
        .insert("serving".into(), vec![json!("low_latency")]);
    let model = Arc::new(SessionModel::new(caps, move |_, mut peer| {
        let ready = ready.clone();
        async move {
            let command = peer.commands.recv().await.unwrap();
            let SessionCommandBody::StartTurn { turn_id, .. } = &command.body else {
                unreachable!()
            };
            let turn_id = turn_id.clone();
            peer.acknowledge(&command, None).await?;
            ready.send(turn_id.clone()).await.unwrap();
            let update = peer.commands.recv().await.unwrap();
            assert!(matches!(
                update.body,
                SessionCommandBody::UpdateProfile { revision: 1, .. }
            ));
            peer.acknowledge(&update, None).await?;
            let interrupt = peer.commands.recv().await.unwrap();
            assert!(matches!(
                interrupt.body,
                SessionCommandBody::Interrupt { .. }
            ));
            peer.acknowledge(&interrupt, None).await?;
            for sequence in 0..8 {
                let chunk = peer.media_input.recv().await.unwrap();
                assert_eq!(chunk.epoch, 1);
                assert_eq!(chunk.sequence, sequence);
                assert_eq!(chunk.bytes, vec![sequence as u8; 4]);
            }
            let end = peer.commands.recv().await.unwrap();
            assert!(matches!(end.body, SessionCommandBody::EndInput));
            peer.acknowledge(&end, None).await?;
            peer.event(SessionEventBody::Output {
                turn_id: turn_id.clone(),
                item_id: "done".into(),
                caller_id: "model".into(),
                output: Output::text("audio received"),
            })
            .await?;
            peer.finished(turn_id, TurnDisposition::Finished).await?;
            if let Some(close) = peer.commands.recv().await {
                assert!(matches!(close.body, SessionCommandBody::Close));
                peer.acknowledge(&close, None).await?;
                peer.event(SessionEventBody::Closed).await?;
            }
            Ok(())
        }
    }));
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir_storage::MemoryRunStore::new(),
    )));
    let runtime = Runtime::builder(model)
        .store(store.clone())
        .resources(Arc::new(zhir_storage::MemoryResourceStore::new()))
        .build()
        .unwrap();
    let mut invocation = runtime
        .start(
            RunRequest::new(vec![Message::user("voice")])
                .mode(RunMode::Interactive)
                .limits(zhir_core::run::Limits {
                    max_media_chunk_bytes: 4,
                    max_buffered_media_bytes: 8,
                    ..zhir_kernel::defaults::limits()
                }),
        )
        .unwrap();
    invocation.start();
    let turn_id = turn_rx.recv().await.unwrap();
    let control = invocation.control();
    control
        .update_profile(RequestProfile {
            serving: Some(Requirement::Required(Serving::LowLatency)),
            ..Default::default()
        })
        .await
        .unwrap();
    control.interrupt().await.unwrap();
    let input = invocation.media_input();
    input
        .send(MediaChunk {
            stream_id: "voice".into(),
            turn_id: turn_id.clone(),
            epoch: 0,
            sequence: 0,
            timestamp_us: 0,
            media_type: "audio/pcm".into(),
            bytes: vec![9],
            end: false,
        })
        .await
        .unwrap();
    for sequence in 0..8 {
        input
            .send(MediaChunk {
                stream_id: "voice".into(),
                turn_id: turn_id.clone(),
                epoch: 1,
                sequence,
                timestamp_us: sequence * 1000,
                media_type: "audio/pcm".into(),
                bytes: vec![sequence as u8; 4],
                end: sequence == 7,
            })
            .await
            .unwrap();
    }
    control.end_input().await.unwrap();
    let completion = settle(invocation).await;
    assert!(
        matches!(completion.state, State::Completed { .. }),
        "{:?}",
        completion.state
    );
    assert_eq!(completion.active.session.profile_revision, 1);
    assert_eq!(
        completion.active.session.negotiated.selected["serving"],
        json!("low_latency")
    );
    assert_eq!(
        completion.active.session.effective.values["serving"],
        Confirmation::Unknown
    );
    let commits = store.commits();
    let interrupted = commits
        .iter()
        .find(|c| c.checkpoint.active.session.epoch == 1)
        .unwrap();
    assert!(
        interrupted
            .checkpoint
            .active
            .commands
            .iter()
            .any(|c| matches!(c.intent, CommandIntent::Interrupt { .. }))
    );
    let ended = commits
        .iter()
        .find(|c| c.checkpoint.active.session.input_closed)
        .unwrap();
    assert!(
        ended
            .checkpoint
            .active
            .commands
            .iter()
            .any(|c| matches!(c.intent, CommandIntent::EndInput))
    );
    assert_eq!(completion.active.media.len(), 1);
    assert_eq!(completion.active.media.values().next().unwrap().sequence, 7);
    store.verify_traces().unwrap();
}
