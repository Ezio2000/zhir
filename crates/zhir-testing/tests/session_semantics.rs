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
        let tail: SealedMedia =
            serde_json::from_slice(&bytes(resources.as_ref(), node.sealed).await).unwrap();
        assert!(tail.end);
        assert!(tail.previous.is_some());
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
