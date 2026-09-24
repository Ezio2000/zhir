use std::{collections::VecDeque, sync::Arc, time::Duration};
use zhir_core::{
    BoxFuture, Result,
    message::{Content, Message, Output},
    model::*,
    operation::*,
    resource::*,
    run::{History, HistoryEntry, State},
};
use zhir_kernel::{RunRequest, Runtime};
use zhir_testing::{RecordingStore, SessionModel};
struct Backend;
struct Events(VecDeque<OperationEvent>);
impl OperationEvents for Events {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<OperationEvent>>> {
        Box::pin(async { Ok(self.0.pop_front()) })
    }
}
struct Control;
impl OperationControl for Control {
    fn cancel(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn reply(&self, _: serde_json::Value) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Err(zhir_core::error::Error::Invalid("no reply".into())) })
    }
}
impl DelegationHandler for Backend {
    fn start(
        &self,
        request: DelegationRequest,
        context: DelegationContext,
    ) -> BoxFuture<'_, Result<OperationHandle>> {
        Box::pin(async move {
            assert_eq!(request.prompt, "inspect");
            assert!(!context.operation_id.is_empty());
            Ok(OperationHandle {
                recovery: None,
                control: Arc::new(Control),
                events: Box::new(Events(VecDeque::from([
                    OperationEvent {
                        sequence: 0,
                        update: OperationUpdate::Context {
                            content: vec![Content::text("working")],
                        },
                    },
                    OperationEvent {
                        sequence: 1,
                        update: OperationUpdate::Finished {
                            outcome: OperationOutcome::Success {
                                content: vec![Content::text("done")],
                                structured: serde_json::Value::Null,
                            },
                        },
                    },
                ]))),
            })
        })
    }
}
#[tokio::test]
async fn independent_conversation_and_durable_delegation_share_one_execution() {
    let mut caps = zhir_testing::model_capabilities();
    caps.features.extend([
        Capability::ConversationItems,
        Capability::Delegation,
        Capability::AsyncResults,
    ]);
    let model = SessionModel::new(caps, |_, mut peer| async move {
        let start = peer.command().await?.unwrap();
        let SessionCommandBody::Generate { generation_id, .. } = &start.body else {
            panic!()
        };
        let turn = generation_id.clone();
        peer.acknowledge(&start, None).await?;
        for (id, message) in [
            ("u1", Message::user("first")),
            ("a1", assistant("answer one")),
            ("u2", Message::user("second")),
            ("a2", assistant("answer two")),
            ("a2", assistant("answer two")),
        ] {
            peer.event(SessionEventBody::ConversationItem {
                item_id: id.into(),
                message,
            })
            .await?;
        }
        peer.event(SessionEventBody::Output {
            generation_id: Some(turn.clone()),
            item_id: "delegation".into(),
            caller_id: "live".into(),
            output: Output::Delegation {
                request: DelegationRequest {
                    id: "d1".into(),
                    prompt: "inspect".into(),
                },
            },
        })
        .await?;
        let context = peer.command().await?.unwrap();
        assert!(
            matches!(&context.body,SessionCommandBody::DelegationContext {origin,content,..} if origin.call_id=="d1" && content==&vec![Content::text("working")])
        );
        peer.acknowledge(&context, None).await?;
        let result = peer.command().await?.unwrap();
        assert!(
            matches!(&result.body,SessionCommandBody::Append {entry: HistoryEntry { message: Message::DelegationResult {id,outcome}, .. },..} if id=="d1" && outcome.content()==[Content::text("done")])
        );
        peer.acknowledge(&result, None).await?;
        peer.finished(turn, ResponseStatus::Completed).await?;
        peer.close().await
    });
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir_storage::MemoryRunStore::new(),
    )));
    let runtime = Runtime::builder(Arc::new(model))
        .delegation(Arc::new(Backend))
        .store(store.clone())
        .build()
        .unwrap();
    let checkpoint = tokio::time::timeout(
        Duration::from_secs(3),
        runtime
            .start(RunRequest::new([Message::user("initial")]))
            .unwrap()
            .result(),
    )
    .await
    .unwrap()
    .unwrap()
    .into_checkpoint();
    assert!(
        matches!(checkpoint.state, State::Completed { .. }),
        "{:?}",
        checkpoint.state
    );
    assert_eq!(checkpoint.metrics.generation_requests, 1);
    assert_eq!(checkpoint.metrics.runtime_tool_calls, 0);
    let messages: Vec<_> = checkpoint
        .history
        .entries()
        .into_iter()
        .filter(|e| e.id.contains("conversation"))
        .map(|e| e.message)
        .collect();
    assert_eq!(
        messages,
        vec![
            Message::user("first"),
            assistant("answer one"),
            Message::user("second"),
            assistant("answer two")
        ]
    );
    assert!(checkpoint.active.operations.is_empty());
    assert!(
        store
            .commits()
            .iter()
            .any(|c| c.checkpoint().active.commands.iter().any(|c| matches!(
                c.intent,
                zhir_core::run::CommandIntent::DelegationContext { .. }
            )))
    );
    store.verify_traces().unwrap();
}
async fn bytes(store: &dyn ResourceStore, reference: ResourceRef) -> Vec<u8> {
    let mut reader = store.open(reference).await.unwrap();
    let mut bytes = vec![];
    loop {
        let part = reader.read(4096).await.unwrap();
        if part.is_empty() {
            return bytes;
        }
        bytes.extend(part);
    }
}
#[tokio::test]
async fn completed_streams_archive_without_exhausting_active_capacity() {
    let mut caps = zhir_testing::model_capabilities();
    caps.features.insert(Capability::Duplex);
    let model = SessionModel::new(caps, |open, mut peer| async move {
        let start = peer.command().await?.unwrap();
        let SessionCommandBody::Generate { generation_id, .. } = &start.body else {
            panic!()
        };
        let turn = generation_id.clone();
        peer.acknowledge(&start, None).await?;
        for stream in 0..80 {
            for sequence in 0..2 {
                peer.media_output
                    .send(MediaChunk {
                        stream_id: format!("speech-{stream}"),
                        session_id: open.session_id.clone(),
                        epoch: 0,
                        sequence,
                        timestamp_us: sequence * 20000,
                        media_type: "audio/pcm".into(),
                        bytes: vec![stream as u8],
                        end: sequence == 1,
                    })
                    .await?;
            }
        }
        peer.finished(turn, ResponseStatus::Completed).await?;
        peer.close().await
    });
    let resources = Arc::new(zhir_storage::MemoryResourceStore::new());
    let runtime = Runtime::builder(Arc::new(model))
        .resources(resources.clone())
        .defaults(|mut o| {
            o.limits.max_media_streams = 1;
            o
        })
        .build()
        .unwrap();
    let mut invocation = runtime
        .start(RunRequest::new([Message::user("go")]))
        .unwrap();
    let mut media = invocation.media_output().unwrap();
    let receive = tokio::spawn(async move {
        let mut count = 0;
        while media.receive().await.unwrap().is_some() {
            count += 1;
        }
        count
    });
    let checkpoint = tokio::time::timeout(Duration::from_secs(10), invocation.result())
        .await
        .unwrap()
        .unwrap()
        .into_checkpoint();
    assert!(
        matches!(checkpoint.state, State::Completed { .. }),
        "{:?}",
        checkpoint.state
    );
    assert_eq!(receive.await.unwrap(), 160);
    assert!(checkpoint.active.media.is_empty());
    let mut reference = checkpoint.active.session.media_archive.clone();
    let mut count = 0;
    while let Some(r) = reference {
        let node: ArchivedMedia =
            serde_json::from_slice(&bytes(resources.as_ref(), r).await).unwrap();
        assert!(node.complete);
        // Each stream has two chunks, sealed in one or two segments ending the stream.
        let mut segment = Some(node.sealed);
        let mut chunks = vec![];
        while let Some(sealed) = segment {
            let sealed: SealedMedia =
                serde_json::from_slice(&bytes(resources.as_ref(), sealed).await).unwrap();
            chunks.splice(0..0, sealed.chunks);
            segment = sealed.previous;
        }
        assert_eq!(chunks.len(), 2);
        assert!(chunks[1].end && !chunks[0].end);
        count += 1;
        reference = node.previous;
    }
    assert_eq!(count, 80);
}
#[test]
fn native_delegation_history_rejects_orphans_and_duplicate_completion() {
    let origin = CallRef {
        session_id: "s".into(),
        item_id: "d".into(),
        generation_id: Some("t".into()),
        caller_id: "c".into(),
        call_id: "d".into(),
    };
    let result = HistoryEntry {
        id: "result".into(),
        origin: Some(origin.clone()),
        message: Message::DelegationResult {
            id: "d".into(),
            outcome: OperationOutcome::Cancelled {
                reason: "stop".into(),
            },
        },
    };
    assert!(
        History::from_entries(vec![result.clone()])
            .unwrap()
            .validate()
            .is_err()
    );
    let call = HistoryEntry {
        id: "call".into(),
        origin: Some(origin),
        message: Message::Assistant {
            output: vec![Output::Delegation {
                request: DelegationRequest {
                    id: "d".into(),
                    prompt: "inspect".into(),
                },
            }],
            provider_data: serde_json::Value::Null,
        },
    };
    let history = History::from_entries(vec![call, result.clone()]).unwrap();
    history.validate().unwrap();
    let mut repeated = result;
    repeated.id = "result2".into();
    assert!(history.append(vec![repeated]).unwrap().validate().is_err());
}

fn assistant(text: &str) -> Message {
    Message::Assistant {
        output: vec![Output::text(text)],
        provider_data: serde_json::Value::Null,
    }
}

/// A run suspended for recovery with one delegation in an unknown state.
fn recovering_delegation() -> (zhir_core::run::Checkpoint, CapabilitySet) {
    let origin = CallRef {
        session_id: "fixture-session".into(),
        item_id: "d".into(),
        generation_id: Some("t".into()),
        caller_id: "live".into(),
        call_id: "d".into(),
    };
    let history = History::from_entries(vec![HistoryEntry {
        id: "call".into(),
        origin: Some(origin.clone()),
        message: Message::Assistant {
            output: vec![Output::Delegation {
                request: DelegationRequest {
                    id: "d".into(),
                    prompt: "inspect".into(),
                },
            }],
            provider_data: serde_json::Value::Null,
        },
    }])
    .unwrap();
    let mut checkpoint = zhir_testing::checkpoint_with_history(history);
    checkpoint.active.operations.insert(
        "operation".into(),
        OperationRecord {
            id: "operation".into(),
            origin,
            owner: OperationOwner::Delegation,
            state: OperationState::Unknown,
            call_entry: 0,
            result_entry: None,
            recovery: None,
            last_sequence: None,
            last_update: None,
            wait: None,
        },
    );
    checkpoint.active.session.generation_id = Some("t".into());
    checkpoint.active.session.generation_started = true;
    checkpoint.active.session.recovery = Some(RecoveryRef {
        adapter: "session-fixture".into(),
        data: serde_json::Value::Null,
    });
    checkpoint.active.session.last_sequence = Some(0);
    checkpoint.state = State::Suspended {
        suspension: zhir_core::run::Suspension {
            reason: "RecoveryRequired".into(),
            source: "test".into(),
            wait_id: None,
            metadata: Default::default(),
        },
    };
    checkpoint.validate().unwrap();
    let mut caps = zhir_testing::model_capabilities();
    caps.features.extend([
        Capability::Delegation,
        Capability::AsyncResults,
        Capability::Resume,
    ]);
    (checkpoint, caps)
}

#[tokio::test]
async fn delegation_recovery_attaches_without_repeating_start() {
    struct Recover;
    impl DelegationHandler for Recover {
        fn start(
            &self,
            _: DelegationRequest,
            _: DelegationContext,
        ) -> BoxFuture<'_, Result<OperationHandle>> {
            Box::pin(async { panic!("recovery must never start backend work") })
        }
        fn recover(
            &self,
            record: OperationRecord,
            context: DelegationContext,
        ) -> BoxFuture<'_, Result<OperationHandle>> {
            Box::pin(async move {
                assert_eq!(record.id, "operation");
                assert_eq!(context.operation_id, record.id);
                assert_eq!(record.recovery.unwrap().adapter, "backend");
                Ok(OperationHandle {
                    recovery: None,
                    control: Arc::new(Control),
                    events: Box::new(Events(VecDeque::from([OperationEvent {
                        sequence: 0,
                        update: OperationUpdate::Finished {
                            outcome: OperationOutcome::Success {
                                content: vec![Content::text("recovered")],
                                structured: serde_json::Value::Null,
                            },
                        },
                    }]))),
                })
            })
        }
    }
    let (checkpoint, caps) = recovering_delegation();
    let model = SessionModel::new(caps, |open, mut peer| async move {
        assert!(open.recovery.is_some());
        let result = peer.command().await?.unwrap();
        assert!(matches!(
            result.body,
            SessionCommandBody::Append {
                entry: HistoryEntry {
                    message: Message::DelegationResult { .. },
                    ..
                },
                ..
            }
        ));
        peer.acknowledge(&result, None).await?;
        peer.finished("t".into(), ResponseStatus::Completed).await?;
        peer.close().await
    });
    let runtime = Runtime::builder(Arc::new(model))
        .delegation(Arc::new(Recover))
        .build()
        .unwrap();
    let mut invocation = runtime
        .resume(
            zhir_kernel::ResumeRequest::from_checkpoint(Arc::new(checkpoint)).resolve(
                RecoveryResolution::Attach {
                    operation_id: "operation".into(),
                    reference: RecoveryRef {
                        adapter: "backend".into(),
                        data: serde_json::Value::Null,
                    },
                },
            ),
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), invocation.result())
        .await
        .unwrap()
        .unwrap()
        .into_checkpoint();
    assert!(
        matches!(result.state, State::Completed { .. }),
        "{:?}",
        result.state
    );
    assert!(result.active.operations.is_empty());
}

/// A recovery that fails cannot establish whether the delegated work still runs, so
/// the operation stays Unknown instead of becoming a definite failure.
#[tokio::test]
async fn a_failed_delegation_recovery_leaves_the_operation_unknown() {
    struct Rejects;
    impl DelegationHandler for Rejects {
        fn start(
            &self,
            _: DelegationRequest,
            _: DelegationContext,
        ) -> BoxFuture<'_, Result<OperationHandle>> {
            Box::pin(async { panic!("recovery must never start backend work") })
        }
        fn recover(
            &self,
            _: OperationRecord,
            _: DelegationContext,
        ) -> BoxFuture<'_, Result<OperationHandle>> {
            Box::pin(async {
                Err(zhir_core::error::Error::Invalid(
                    "backend has no such run".into(),
                ))
            })
        }
    }
    let (checkpoint, caps) = recovering_delegation();
    let model = SessionModel::new(caps, |_, mut peer| async move {
        peer.finished("t".into(), ResponseStatus::Completed).await?;
        while peer.command().await?.is_some() {}
        Ok(())
    });
    let runtime = Runtime::builder(Arc::new(model))
        .delegation(Arc::new(Rejects))
        .build()
        .unwrap();
    let mut invocation = runtime
        .resume(
            zhir_kernel::ResumeRequest::from_checkpoint(Arc::new(checkpoint)).resolve(
                RecoveryResolution::Attach {
                    operation_id: "operation".into(),
                    reference: RecoveryRef {
                        adapter: "backend".into(),
                        data: serde_json::Value::Null,
                    },
                },
            ),
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), invocation.result())
        .await
        .unwrap()
        .unwrap()
        .into_checkpoint();
    assert!(
        matches!(&result.state, State::Suspended { suspension } if suspension.reason == "RecoveryRequired"),
        "{:?}",
        result.state
    );
    assert_eq!(
        result.active.operations["operation"].state,
        OperationState::Unknown
    );
    assert!(
        !result
            .history
            .iter()
            .any(|entry| matches!(entry.message, Message::DelegationResult { .. }))
    );
}

/// Counts opened archive nodes and can slow every create to let media queue up.
#[derive(Default)]
struct MediaResources {
    inner: zhir_storage::MemoryResourceStore,
    archive_reads: std::sync::atomic::AtomicUsize,
    create_delay: Duration,
}
impl ResourceStore for MediaResources {
    fn create(
        &self,
        key: String,
        media_type: String,
    ) -> BoxFuture<'_, Result<Box<dyn ResourceWriter>>> {
        Box::pin(async move {
            tokio::time::sleep(self.create_delay).await;
            self.inner.create(key, media_type).await
        })
    }
    fn open(&self, reference: ResourceRef) -> BoxFuture<'_, Result<Box<dyn ResourceReader>>> {
        if reference.media_type == "application/vnd.zhir.archived-media+json" {
            self.archive_reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.inner.open(reference)
    }
    fn delete(&self, reference: ResourceRef) -> BoxFuture<'_, Result<()>> {
        self.inner.delete(reference)
    }
}
/// A duplex session sending `streams` output streams of `chunks` chunks each.
fn media_model(streams: usize, chunks: u64) -> SessionModel {
    let mut caps = zhir_testing::model_capabilities();
    caps.features.insert(Capability::Duplex);
    SessionModel::new(caps, move |open, mut peer| async move {
        let start = peer.command().await?.unwrap();
        let SessionCommandBody::Generate { generation_id, .. } = &start.body else {
            panic!()
        };
        let turn = generation_id.clone();
        peer.acknowledge(&start, None).await?;
        for stream in 0..streams {
            for sequence in 0..chunks {
                peer.media_output
                    .send(MediaChunk {
                        stream_id: format!("speech-{stream}"),
                        session_id: open.session_id.clone(),
                        epoch: 0,
                        sequence,
                        timestamp_us: sequence * 20000,
                        media_type: "audio/pcm".into(),
                        bytes: vec![sequence as u8; 2],
                        end: sequence + 1 == chunks,
                    })
                    .await?;
            }
        }
        peer.finished(turn, ResponseStatus::Completed).await?;
        peer.close().await
    })
}
async fn run_media(
    model: SessionModel,
    resources: Arc<MediaResources>,
    store: Arc<RecordingStore>,
) -> (Arc<zhir_core::run::Checkpoint>, Vec<MediaChunk>) {
    let runtime = Runtime::builder(Arc::new(model))
        .resources(resources)
        .store(store)
        .build()
        .unwrap();
    let mut invocation = runtime
        .start(RunRequest::new([Message::user("go")]))
        .unwrap();
    let mut media = invocation.media_output().unwrap();
    let receive = tokio::spawn(async move {
        let mut chunks = vec![];
        while let Some(chunk) = media.receive().await.unwrap() {
            chunks.push(chunk);
        }
        chunks
    });
    let checkpoint = tokio::time::timeout(Duration::from_secs(60), invocation.result())
        .await
        .unwrap()
        .unwrap()
        .into_checkpoint();
    assert!(
        matches!(checkpoint.state, State::Completed { .. }),
        "{:?}",
        checkpoint.state
    );
    (checkpoint, receive.await.unwrap())
}
#[tokio::test]
async fn new_streams_do_not_rescan_the_media_archive() {
    let resources = Arc::new(MediaResources::default());
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir_storage::MemoryRunStore::new(),
    )));
    let (checkpoint, chunks) = run_media(media_model(2000, 1), resources.clone(), store).await;
    assert_eq!(chunks.len(), 2000);
    let mut archived = 0;
    let mut reference = checkpoint.active.session.media_archive.clone();
    while let Some(r) = reference {
        let node: ArchivedMedia =
            serde_json::from_slice(&bytes(&resources.inner, r).await).unwrap();
        archived += 1;
        reference = node.previous;
    }
    assert_eq!(archived, 2000);
    // Rescanning the chain for each new stream would read about two million nodes.
    let reads = resources
        .archive_reads
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(reads <= 2000 + archived, "{reads} archive reads");
}
#[tokio::test]
async fn slow_media_storage_seals_queued_chunks_as_one_segment() {
    let resources = Arc::new(MediaResources {
        create_delay: Duration::from_millis(5),
        ..Default::default()
    });
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir_storage::MemoryRunStore::new(),
    )));
    let (checkpoint, chunks) =
        run_media(media_model(1, 64), resources.clone(), store.clone()).await;
    assert_eq!(
        chunks.iter().map(|c| c.sequence).collect::<Vec<_>>(),
        (0..64).collect::<Vec<_>>()
    );
    let media_commits = store
        .commits()
        .iter()
        .filter(|c| matches!(c.checkpoint().fact, zhir_core::run::Fact::Media { .. }))
        .count();
    assert!(media_commits < 64, "{media_commits} media commits");
    // The segments hold every chunk once, in order, with their bytes.
    let archive: ArchivedMedia = serde_json::from_slice(
        &bytes(
            &resources.inner,
            checkpoint.active.session.media_archive.clone().unwrap(),
        )
        .await,
    )
    .unwrap();
    let mut segment = Some(archive.sealed);
    let mut sealed = vec![];
    while let Some(reference) = segment {
        let node: SealedMedia =
            serde_json::from_slice(&bytes(&resources.inner, reference).await).unwrap();
        let data = bytes(&resources.inner, node.resource.clone()).await;
        for chunk in node.chunks.iter().rev() {
            let range = chunk.offset as usize..(chunk.offset + chunk.length) as usize;
            assert_eq!(data[range], vec![chunk.sequence as u8; 2]);
            sealed.push(chunk.sequence);
        }
        segment = node.previous;
    }
    sealed.reverse();
    assert_eq!(sealed, (0..64).collect::<Vec<_>>());
    store.verify_traces().unwrap();
}
/// How many host input packets are accepted while the model stops reading input.
async fn accepted_input_packets(max_packets: usize) -> usize {
    let mut caps = zhir_testing::model_capabilities();
    caps.features.insert(Capability::Duplex);
    let (session_tx, mut session_rx) = tokio::sync::mpsc::channel(1);
    let model = SessionModel::new(caps, move |open, mut peer| {
        let session_tx = session_tx.clone();
        async move {
            let start = peer.command().await?.unwrap();
            peer.acknowledge(&start, None).await?;
            session_tx.send(open.session_id.clone()).await.unwrap();
            // Hold the input port without reading it.
            let _input = peer.media_input;
            std::future::pending::<()>().await;
            Ok(())
        }
    });
    let runtime = Runtime::builder(Arc::new(model))
        .resources(Arc::new(zhir_storage::MemoryResourceStore::new()))
        .build()
        .unwrap();
    let mut invocation = runtime
        .start(
            RunRequest::new([Message::user("voice")])
                .mode(zhir_core::run::RunMode::Interactive)
                .limits(zhir_core::run::Limits {
                    // One chunk fills the model's input queue.
                    max_media_chunk_bytes: 1024,
                    max_buffered_media_bytes: 1024,
                    max_buffered_media_packets: max_packets,
                    ..zhir_kernel::defaults::limits()
                }),
        )
        .unwrap();
    invocation.start();
    let session_id = session_rx.recv().await.unwrap();
    let input = invocation.media_input();
    let mut accepted = 0;
    for sequence in 0..64 {
        let send = input.send(MediaChunk {
            stream_id: "voice".into(),
            session_id: session_id.clone(),
            epoch: 0,
            sequence,
            timestamp_us: sequence * 20000,
            media_type: "audio/pcm".into(),
            bytes: vec![1],
            end: false,
        });
        match tokio::time::timeout(Duration::from_millis(200), send).await {
            Ok(result) => {
                result.unwrap();
                accepted += 1;
            }
            Err(_) => break,
        }
    }
    invocation.control().cancel();
    let _ = invocation.result().await;
    accepted
}
#[tokio::test]
async fn host_media_input_blocks_at_the_packet_limit() {
    // The model queue holds one chunk, the in-flight segment at most the limit, and
    // the host queue the limit.
    let small = accepted_input_packets(2).await;
    assert!((4..=5).contains(&small), "{small} packets accepted");
    let large = accepted_input_packets(16).await;
    assert!(
        large > small && large <= 1 + 2 * 16,
        "{large} packets accepted"
    );
}
