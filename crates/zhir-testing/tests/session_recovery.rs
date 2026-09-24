use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use zhir_core::operation::OperationOutcome;
use zhir_core::{
    error::Error,
    message::{Content, Message, Output, ProviderToolCall, ProviderToolStatus},
    model::{Capability, ResponseStatus, SessionCommand, SessionCommandBody, SessionEventBody},
    operation::{CallRef, OperationEvent, OperationUpdate, RecoveryRef},
    run::{Checkpoint, State},
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
                let command = peer.command().await?.unwrap();
                let SessionCommandBody::Generate { generation_id, .. } = &command.body else {
                    panic!("expected turn");
                };
                let generation_id = generation_id.clone();
                peer.acknowledge(&command, None).await?;
                peer.event(SessionEventBody::Output {
                    generation_id: Some(generation_id.clone()),
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
                    generation_id,
                    if index == 0 {
                        ResponseStatus::Continuation
                    } else {
                        ResponseStatus::Completed
                    },
                )
                .await?;
            }
            peer.close().await
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
        .flat_map(|c| c.checkpoint().active.operations.keys().cloned())
        .collect();
    assert_eq!(ids.len(), 1);
    let projected = zhir_core::model::conversation(final_state.history.iter());
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
                    let command = peer.command().await?.unwrap();
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
                assert_eq!(open.after_sequence, Some(1));
                let command = sent.lock().unwrap().clone().unwrap();
                // Give a broken implementation time to redispatch before delivering its recovered ack.
                assert!(
                    tokio::time::timeout(Duration::from_millis(50), peer.commands.recv())
                        .await
                        .is_err(),
                    "sent command was replayed"
                );
                let SessionCommandBody::Generate { generation_id, .. } = &command.body else {
                    unreachable!()
                };
                peer.acknowledge(&command, open.recovery).await?;
                peer.event(SessionEventBody::Output {
                    generation_id: Some(generation_id.clone()),
                    item_id: "answer".into(),
                    caller_id: "model".into(),
                    output: Output::text("recovered"),
                })
                .await?;
                peer.finished(generation_id.clone(), ResponseStatus::Completed)
                    .await?;
                peer.close().await
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
            let command = peer.command().await?.unwrap();
            let SessionCommandBody::Generate { generation_id, .. } = &command.body else {
                unreachable!()
            };
            let generation_id = generation_id.clone();
            peer.acknowledge(&command, None).await?;
            peer.event(SessionEventBody::Output {
                generation_id: Some(generation_id.clone()),
                item_id: "job".into(),
                caller_id: "model".into(),
                output: provider(ProviderToolStatus::Running, None),
            })
            .await?;
            for i in 0..2 {
                peer.event(SessionEventBody::Operation {
                    origin: CallRef {
                        session_id: open.session_id.clone(),
                        item_id: "job".into(),
                        generation_id: Some(generation_id.clone()),
                        caller_id: "model".into(),
                        call_id: "job".into(),
                    },
                    event: OperationEvent {
                        sequence: 4,
                        update: OperationUpdate::Finished {
                            outcome: OperationOutcome::Success {
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
            peer.finished(generation_id, ResponseStatus::Completed)
                .await?;
            peer.close().await
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
async fn duplex_seal_user_input_drains_buffered_media_and_interrupt_commits_with_its_intent() {
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
        Capability::InterruptOutput,
        Capability::ProfileUpdates,
    ]);
    caps.constraints
        .insert("serving".into(), vec![json!("low_latency")]);
    let model = Arc::new(SessionModel::new(caps, move |open, mut peer| {
        let ready = ready.clone();
        async move {
            let command = peer.command().await?.unwrap();
            let SessionCommandBody::Generate { generation_id, .. } = &command.body else {
                unreachable!()
            };
            let generation_id = generation_id.clone();
            peer.acknowledge(&command, None).await?;
            ready.send(open.session_id.clone()).await.unwrap();
            let update = peer.command().await?.unwrap();
            assert!(matches!(
                update.body,
                SessionCommandBody::UpdateProfile { revision: 1, .. }
            ));
            peer.acknowledge(&update, None).await?;
            let interrupt = peer.command().await?.unwrap();
            assert!(matches!(
                interrupt.body,
                SessionCommandBody::InterruptOutput { .. }
            ));
            peer.acknowledge(&interrupt, None).await?;
            for sequence in 0..8 {
                let chunk = peer.media_input.recv().await.unwrap();
                assert_eq!(chunk.epoch, 0);
                assert_eq!(chunk.sequence, sequence);
                assert_eq!(chunk.bytes, vec![sequence as u8; 4]);
            }
            let end = peer.command().await?.unwrap();
            assert!(matches!(end.body, SessionCommandBody::SealUserInput));
            peer.acknowledge(&end, None).await?;
            peer.event(SessionEventBody::Output {
                generation_id: Some(generation_id.clone()),
                item_id: "done".into(),
                caller_id: "model".into(),
                output: Output::text("audio received"),
            })
            .await?;
            peer.finished(generation_id, ResponseStatus::Completed)
                .await?;
            if let Some(close) = peer.command().await? {
                assert!(matches!(close.body, SessionCommandBody::Close));
                peer.acknowledge(&close, None).await?;
                peer.event(SessionEventBody::Closed {
                    reason: "host_request".into(),
                    provider_data: serde_json::Value::Null,
                })
                .await?;
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
    let generation_id = turn_rx.recv().await.unwrap();
    let control = invocation.control();
    control
        .update_profile(RequestProfile {
            serving: Some(Requirement::Required(Serving::LowLatency)),
            ..Default::default()
        })
        .await
        .unwrap();
    control.interrupt_output().await.unwrap();
    let input = invocation.media_input();
    for sequence in 0..8 {
        input
            .send(MediaChunk {
                stream_id: "voice".into(),
                session_id: generation_id.clone(),
                epoch: 0,
                sequence,
                timestamp_us: sequence * 1000,
                media_type: "audio/pcm".into(),
                bytes: vec![sequence as u8; 4],
                end: sequence == 7,
            })
            .await
            .unwrap();
    }
    control.seal_user_input().await.unwrap();
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
        .find(|c| c.checkpoint().active.session.output_epoch == 1)
        .unwrap();
    assert!(
        interrupted
            .checkpoint()
            .active
            .commands
            .iter()
            .any(|c| matches!(c.intent, CommandIntent::InterruptOutput { .. }))
    );
    let ended = commits
        .iter()
        .find(|c| c.checkpoint().active.session.input_closed)
        .unwrap();
    assert!(
        ended
            .checkpoint()
            .active
            .commands
            .iter()
            .any(|c| matches!(c.intent, CommandIntent::SealUserInput))
    );
    assert!(completion.active.media.is_empty());
    assert!(completion.active.session.media_archive.is_some());
    store.verify_traces().unwrap();
}

struct Crash {
    calls: Arc<AtomicUsize>,
    store: Arc<RecordingStore>,
    runtime: Runtime,
    head: Arc<Checkpoint>,
}
fn counting_model(calls: Arc<AtomicUsize>) -> Arc<zhir_models::FunctionModel> {
    Arc::new(zhir_models::FunctionModel::new(
        zhir_testing::model_capabilities(),
        move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(zhir_core::model::GenerationOutput::text("done")) }
        },
    ))
}
/// Runs a local projection until the crash predicate matches and returns the durable
/// head left behind, with a fresh runtime over the same store.
async fn crash_at(crash: impl Fn(&Checkpoint) -> bool + Send + Sync + 'static) -> Crash {
    let calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir_storage::MemoryRunStore::new(),
    )));
    let crashing = Arc::new(zhir_testing::CrashingStore::new(store.clone(), crash));
    let context = zhir_kernel::defaults::context();
    let run_id = context.run_id.clone();
    let mut invocation = Runtime::builder(counting_model(calls.clone()))
        .store(crashing)
        .build()
        .unwrap()
        .start(RunRequest::new(vec![Message::user("start")]).context(context))
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(3), invocation.result())
        .await
        .expect("runtime stalled")
        .unwrap_err();
    assert!(matches!(error.error, Error::Storage(_)));
    let head = zhir_core::storage::RunStore::load_head(store.as_ref(), &run_id)
        .await
        .unwrap()
        .unwrap();
    let runtime = Runtime::builder(counting_model(calls.clone()))
        .store(store.clone())
        .build()
        .unwrap();
    Crash {
        calls,
        store,
        runtime,
        head,
    }
}
fn generate_command(checkpoint: &Checkpoint) -> Option<&zhir_core::run::PendingCommand> {
    checkpoint
        .active
        .commands
        .iter()
        .find(|c| matches!(c.intent, zhir_core::run::CommandIntent::Generate { .. }))
}
async fn suspended_after_sent_generation() -> (Crash, Arc<Checkpoint>) {
    let crash = crash_at(|checkpoint| generate_command(checkpoint).is_some_and(|c| c.sent)).await;
    // Appends of a local projection never persist a send boundary.
    assert!(
        crash
            .head
            .active
            .commands
            .iter()
            .all(|c| c.sent == matches!(c.intent, zhir_core::run::CommandIntent::Generate { .. }))
    );
    let suspended = settle(crash.runtime.continue_from(crash.head.clone()).unwrap()).await;
    assert!(matches!(suspended.state, State::Suspended { .. }));
    (crash, suspended)
}
fn abandon(generation_id: &str) -> zhir_core::operation::RecoveryResolution {
    zhir_core::operation::RecoveryResolution::AbandonGeneration {
        generation_id: generation_id.into(),
        reason: "provider request was not billed".into(),
    }
}
#[tokio::test]
async fn local_projection_runs_continue_after_a_crash_before_the_generation_is_sent() {
    for opening in [true, false] {
        let crash = crash_at(move |checkpoint| {
            if opening {
                checkpoint.active.session.establishment
                    == zhir_core::run::SessionEstablishment::Opening
            } else {
                // A local Generate is committed with its send boundary, so the last
                // commit before the send is the ready session that needs a generation.
                checkpoint.active.session.ready
                    && checkpoint.active.session.needs_generation
                    && generate_command(checkpoint).is_none()
            }
        })
        .await;
        assert_eq!(crash.calls.load(Ordering::SeqCst), 0);
        let checkpoint = settle(crash.runtime.continue_from(crash.head).unwrap()).await;
        assert!(
            matches!(checkpoint.state, State::Completed { .. }),
            "{:?}",
            checkpoint.state
        );
        assert_eq!(crash.calls.load(Ordering::SeqCst), 1);
        crash.store.verify_traces().unwrap();
    }
}
#[tokio::test]
async fn an_abandoned_local_generation_is_generated_again() {
    let (crash, suspended) = suspended_after_sent_generation().await;
    let generation_id = suspended.active.session.generation_id.clone().unwrap();
    let before = crash.calls.load(Ordering::SeqCst);
    let checkpoint = settle(
        crash
            .runtime
            .resume(ResumeRequest::from_checkpoint(suspended).resolve(abandon(&generation_id)))
            .await
            .unwrap(),
    )
    .await;
    assert!(
        matches!(checkpoint.state, State::Completed { .. }),
        "{:?}",
        checkpoint.state
    );
    assert_eq!(crash.calls.load(Ordering::SeqCst), before + 1);
    assert_ne!(
        checkpoint.active.session.generation_id,
        Some(generation_id.clone())
    );
    assert!(crash.store.commits().iter().any(|c| matches!(
        &c.checkpoint().fact,
        zhir_core::run::Fact::GenerationAbandoned { generation_id: id, .. } if *id == generation_id
    )));
    crash.store.verify_traces().unwrap();
}
#[tokio::test]
async fn generation_abandonment_requires_the_matching_local_generation() {
    let (crash, suspended) = suspended_after_sent_generation().await;
    let before = crash.calls.load(Ordering::SeqCst);
    let failed = settle(
        crash
            .runtime
            .resume(ResumeRequest::from_checkpoint(suspended).resolve(abandon("other")))
            .await
            .unwrap(),
    )
    .await;
    assert!(
        matches!(&failed.state, State::Failed { error } if error.message.contains("no matching unfinished generation")),
        "{:?}",
        failed.state
    );
    assert_eq!(crash.calls.load(Ordering::SeqCst), before);

    // A remote session can only be reconciled through its recovery reference.
    let model = Arc::new(SessionModel::new(
        zhir_testing::model_capabilities(),
        |_, mut peer| async move {
            peer.command().await?;
            Err(Error::Uncertain("connection lost after send".into()))
        },
    ));
    let runtime = Runtime::builder(model).build().unwrap();
    let suspended = settle(
        runtime
            .start(RunRequest::new(vec![Message::user("start")]))
            .unwrap(),
    )
    .await;
    assert!(matches!(suspended.state, State::Suspended { .. }));
    let generation_id = suspended.active.session.generation_id.clone().unwrap();
    let failed = settle(
        runtime
            .resume(ResumeRequest::from_checkpoint(suspended).resolve(abandon(&generation_id)))
            .await
            .unwrap(),
    )
    .await;
    assert!(
        matches!(&failed.state, State::Failed { error } if error.message.contains("only a local projection")),
        "{:?}",
        failed.state
    );
}
#[cfg(feature = "openai-chat")]
#[tokio::test]
async fn refused_http_connections_fail_the_run_instead_of_suspending() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    drop(listener);
    let model = zhir_models::openai::chat::model(zhir_models::ModelConfig::new(
        url,
        Arc::new(zhir_models::credentials::StaticCredential::new(
            "Bearer", "test",
        )),
        "fixture",
    ))
    .unwrap()
    .with_retry(
        zhir_policies::RetryPolicy::new(2)
            .unwrap()
            .backoff(zhir_policies::Backoff::fixed(Duration::ZERO)),
    );
    let checkpoint = settle(
        Runtime::builder(Arc::new(model))
            .build()
            .unwrap()
            .start(RunRequest::new(vec![Message::user("start")]))
            .unwrap(),
    )
    .await;
    assert!(
        matches!(&checkpoint.state, State::Failed { error } if error.code == "http_connect"),
        "{:?}",
        checkpoint.state
    );
}
/// A local model whose first response starts a provider job and asks to continue;
/// later responses finish it.
fn continuing_model(calls: Arc<AtomicUsize>) -> Arc<zhir_models::FunctionModel> {
    Arc::new(zhir_models::FunctionModel::new(
        zhir_testing::model_capabilities(),
        move |_, _| {
            let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
            async move {
                Ok(if first {
                    zhir_core::model::GenerationOutput {
                        output: vec![provider(ProviderToolStatus::Running, None)],
                        status: ResponseStatus::Continuation,
                        ..zhir_core::model::GenerationOutput::text("")
                    }
                } else {
                    zhir_core::model::GenerationOutput {
                        output: vec![
                            provider(ProviderToolStatus::Completed, Some("video")),
                            Output::text("done"),
                        ],
                        ..zhir_core::model::GenerationOutput::text("")
                    }
                })
            }
        },
    ))
}
#[tokio::test]
async fn abandoning_a_continuation_generation_regenerates_with_its_provider_job_running() {
    let calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir_storage::MemoryRunStore::new(),
    )));
    // Crash once the continuation's Generate has been sent while the job still runs.
    let crashing = Arc::new(zhir_testing::CrashingStore::new(
        store.clone(),
        |checkpoint| {
            !checkpoint.active.operations.is_empty()
                && generate_command(checkpoint).is_some_and(|c| c.sent)
        },
    ));
    let context = zhir_kernel::defaults::context();
    let run_id = context.run_id.clone();
    let mut invocation = Runtime::builder(continuing_model(calls.clone()))
        .store(crashing)
        .build()
        .unwrap()
        .start(RunRequest::new(vec![Message::user("start")]).context(context))
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(3), invocation.result())
        .await
        .expect("runtime stalled")
        .unwrap_err();
    assert!(matches!(error.error, Error::Storage(_)));
    let head = zhir_core::storage::RunStore::load_head(store.as_ref(), &run_id)
        .await
        .unwrap()
        .unwrap();
    let runtime = Runtime::builder(continuing_model(calls.clone()))
        .store(store.clone())
        .build()
        .unwrap();
    let suspended = settle(runtime.continue_from(head).unwrap()).await;
    assert!(matches!(suspended.state, State::Suspended { .. }));
    assert_eq!(suspended.active.operations.len(), 1);
    let generation_id = suspended.active.session.generation_id.clone().unwrap();
    let checkpoint = settle(
        runtime
            .resume(ResumeRequest::from_checkpoint(suspended).resolve(abandon(&generation_id)))
            .await
            .unwrap(),
    )
    .await;
    assert!(
        matches!(&checkpoint.state, State::Completed { .. }),
        "{:?}",
        checkpoint.state
    );
    assert!(checkpoint.active.operations.is_empty());
    // The start, the continuation lost in the crash, and its replacement.
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    store.verify_traces().unwrap();
}
#[tokio::test]
async fn abandoning_a_generation_requires_settling_its_own_provider_jobs() {
    let mut caps = zhir_testing::model_capabilities();
    caps.features.insert(Capability::LocalProjection);
    let opened = Arc::new(AtomicUsize::new(0));
    let sessions = opened.clone();
    // The first session starts a provider job and loses its response; later ones finish.
    let model = Arc::new(SessionModel::new(caps, move |_, mut peer| {
        let first = sessions.fetch_add(1, Ordering::SeqCst) == 0;
        async move {
            let start = peer.command().await?.unwrap();
            let SessionCommandBody::Generate { generation_id, .. } = &start.body else {
                unreachable!()
            };
            let generation_id = generation_id.clone();
            peer.acknowledge(&start, None).await?;
            if first {
                peer.event(SessionEventBody::Output {
                    generation_id: Some(generation_id),
                    item_id: "job".into(),
                    caller_id: "model".into(),
                    output: provider(ProviderToolStatus::Running, None),
                })
                .await?;
                return Err(Error::Uncertain("response lost".into()));
            }
            peer.event(SessionEventBody::Output {
                generation_id: Some(generation_id.clone()),
                item_id: "done".into(),
                caller_id: "model".into(),
                output: Output::text("done"),
            })
            .await?;
            peer.finished(generation_id, ResponseStatus::Completed)
                .await?;
            peer.close().await
        }
    }));
    let runtime = Runtime::builder(model).build().unwrap();
    let paused = settle(
        runtime
            .start(RunRequest::new(vec![Message::user("start")]))
            .unwrap(),
    )
    .await;
    assert!(
        matches!(paused.state, State::Suspended { .. }),
        "{:?}",
        paused.state
    );
    assert_eq!(paused.active.operations.len(), 1);
    let generation_id = paused.active.session.generation_id.clone().unwrap();
    let job = paused.active.operations.keys().next().unwrap().clone();
    let failed = settle(
        runtime
            .resume(ResumeRequest::from_checkpoint(paused.clone()).resolve(abandon(&generation_id)))
            .await
            .unwrap(),
    )
    .await;
    assert!(
        matches!(&failed.state, State::Failed { error } if error.message.contains("provider operations first")),
        "{:?}",
        failed.state
    );
    // Settling the job first lets the generation be abandoned and generated again.
    let completed = settle(
        runtime
            .resume(
                ResumeRequest::from_checkpoint(paused)
                    .resolve(zhir_core::operation::RecoveryResolution::Abandon {
                        operation_id: job,
                        reason: "lost with its response".into(),
                    })
                    .resolve(abandon(&generation_id)),
            )
            .await
            .unwrap(),
    )
    .await;
    assert!(
        matches!(&completed.state, State::Completed { .. }),
        "{:?}",
        completed.state
    );
}
/// A profile update acknowledged in the same batch as earlier output negotiates over
/// that output too, not only over the last committed history.
#[tokio::test]
async fn batched_profile_updates_negotiate_over_staged_output() {
    use zhir_core::{
        profile::{Fidelity, RequestProfile, Requirement, ResourceUsage, Serving},
        resource::{ResourceInput, ResourceRef, ResourceSource},
    };
    let mut caps = zhir_testing::model_capabilities();
    caps.features
        .extend([Capability::Steering, Capability::ProfileUpdates]);
    caps.input_modalities.push("image".into());
    caps.output_modalities.push("image".into());
    caps.constraints
        .insert("serving".into(), vec![json!("low_latency")]);
    caps.constraints
        .insert("resource.image.fidelity".into(), vec![json!("original")]);
    let (ready_tx, mut ready) = tokio::sync::mpsc::channel(1);
    let model = Arc::new(SessionModel::new(caps, move |_, mut peer| {
        let ready_tx = ready_tx.clone();
        async move {
            let start = peer.command().await?.unwrap();
            let SessionCommandBody::Generate { generation_id, .. } = &start.body else {
                unreachable!()
            };
            let generation_id = generation_id.clone();
            peer.acknowledge(&start, None).await?;
            ready_tx.send(()).await.unwrap();
            let update = peer.command().await?.unwrap();
            assert!(matches!(
                update.body,
                SessionCommandBody::UpdateProfile { .. }
            ));
            // Output and acknowledgement are queued together, so they share one batch.
            peer.event(SessionEventBody::Output {
                generation_id: Some(generation_id.clone()),
                item_id: "image".into(),
                caller_id: "model".into(),
                output: Output::Content {
                    content: Content::Resource {
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
                    },
                },
            })
            .await?;
            peer.acknowledge(&update, None).await?;
            peer.finished(generation_id, ResponseStatus::Completed)
                .await?;
            peer.close().await
        }
    }));
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir_storage::MemoryRunStore::new(),
    )));
    let runtime = Runtime::builder(model)
        .store(store.clone())
        .build()
        .unwrap();
    let mut invocation = runtime
        .start(RunRequest::new(vec![Message::user("draw")]))
        .unwrap();
    invocation.start();
    ready.recv().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        invocation.control().update_profile(RequestProfile {
            serving: Some(Requirement::Required(Serving::LowLatency)),
            ..Default::default()
        }),
    )
    .await
    .expect("profile update stalled")
    .unwrap();
    let checkpoint = settle(invocation).await;
    assert!(
        matches!(checkpoint.state, State::Completed { .. }),
        "{:?}",
        checkpoint.state
    );
    let commits = store.commits();
    let updated = commits
        .iter()
        .find(|c| c.checkpoint().active.session.profile_revision == 1)
        .unwrap()
        .checkpoint();
    assert!(
        updated
            .history
            .messages()
            .iter()
            .any(|m| matches!(m, Message::Assistant { .. })),
        "the acknowledgement did not share a batch with the output"
    );
    assert_eq!(
        updated.active.session.negotiated.selected["resource.image.fidelity"],
        json!("original")
    );
}
