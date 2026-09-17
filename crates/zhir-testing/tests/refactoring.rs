use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use zhir_core::operation::OperationOutcome;
use zhir_core::{
    BoxFuture, Result,
    message::{Content, Message, Output, ProviderToolCall, ProviderToolStatus},
    model::{Capability, ResponseStatus, SessionCommandBody, SessionEventBody, conversation},
    operation::{CallRef, OperationControl, OperationEvent, OperationUpdate, ToolExecution},
    run::{HistoryEntry, State},
    tool::{Execution, InputSpec, RuntimeToolCall, RuntimeToolInput, RuntimeToolSpec},
};
use zhir_kernel::{RunRequest, Runtime};
use zhir_models::provider_tools::ProviderOutput;
use zhir_testing::{RecordingStore, SessionModel};

#[tokio::test]
async fn settled_provider_replay_survives_next_turn_and_checkpoint_roundtrip() {
    let outcomes = [
        OperationOutcome::Success {
            content: vec![Content::text("done")],
            structured: json!({"receipt":42}),
        },
        OperationOutcome::Failure {
            error: zhir_core::error::Failure::new("remote", "failed"),
        },
        OperationOutcome::Cancelled {
            reason: "user cancelled".into(),
        },
    ];
    for outcome in outcomes {
        let mut caps = zhir_testing::model_capabilities();
        caps.features.insert(Capability::AsyncResults);
        let expected = outcome.clone();
        let model = Arc::new(SessionModel::new(caps, move |open, mut peer| {
            let outcome = expected.clone();
            async move {
                let command = peer.command().await?.unwrap();
                let SessionCommandBody::Generate { generation_id, .. } = &command.body else {
                    unreachable!()
                };
                let generation_id = generation_id.clone();
                peer.acknowledge(&command, None).await?;
                let mut output =
                    ProviderOutput::new("media", "video", "job", ProviderToolStatus::Running)
                        .native(json!({"type":"video_job","id":"job"}))
                        .finish()?;
                let Output::ProviderToolCall { call } = &mut output else {
                    unreachable!()
                };
                call.data["custom"] = json!({"ticket":"keep-at-top-level"});
                let original = call.data.clone();
                peer.event(SessionEventBody::Output {
                    generation_id: Some(generation_id.clone()),
                    item_id: "job-item".into(),
                    caller_id: "model".into(),
                    output,
                })
                .await?;
                // Also exercise duplicate completion with a new session sequence.
                for _ in 0..2 {
                    peer.event(SessionEventBody::Operation {
                        origin: CallRef {
                            session_id: open.session_id.clone(),
                            item_id: "job-item".into(),
                            generation_id: Some(generation_id.clone()),
                            caller_id: "model".into(),
                            call_id: "job".into(),
                        },
                        event: OperationEvent {
                            sequence: 0,
                            update: OperationUpdate::Finished {
                                outcome: outcome.clone(),
                            },
                        },
                    })
                    .await?;
                }
                peer.finished(generation_id, ResponseStatus::Continuation)
                    .await?;
                let next = peer.command().await?.unwrap();
                let SessionCommandBody::Generate { generation_id, .. } = &next.body else {
                    unreachable!()
                };
                let projection = peer.projection();
                let calls: Vec<_> = projection
                    .iter()
                    .filter_map(|message| {
                        if let Message::Assistant { output, .. } = message {
                            Some(output)
                        } else {
                            None
                        }
                    })
                    .flatten()
                    .filter_map(|item| {
                        if let Output::ProviderToolCall { call } = item {
                            Some(call)
                        } else {
                            None
                        }
                    })
                    .collect();
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].data, original);
                assert!(calls[0].matches_outcome(&outcome));
                assert_eq!(
                    ProviderOutput::replay(calls[0])?,
                    vec![json!({"type":"video_job","id":"job"})]
                );
                peer.acknowledge(&next, None).await?;
                peer.finished(generation_id.clone(), ResponseStatus::Completed)
                    .await?;
                peer.close().await
            }
        }));
        let runtime = Runtime::builder(model).build().unwrap();
        let mut invocation = runtime
            .start(RunRequest::new([Message::user("video")]))
            .unwrap();
        let completion = tokio::time::timeout(Duration::from_secs(3), invocation.result())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            completion.checkpoint().state,
            State::Completed { .. }
        ));
        let encoded = zhir_core::wire::encode_checkpoint(completion.checkpoint()).unwrap();
        let restored = zhir_core::wire::decode_checkpoint(&encoded).unwrap();
        let projected = conversation(restored.history.entries());
        let call = projected
            .iter()
            .find_map(|message| match message {
                Message::Assistant { output, .. } => output.iter().find_map(|item| match item {
                    Output::ProviderToolCall { call } => Some(call),
                    _ => None,
                }),
                _ => None,
            })
            .unwrap();
        assert!(call.matches_outcome(&outcome));
        assert!(ProviderOutput::replay(call).is_ok());
    }
}

#[test]
fn projection_indexes_provider_updates_without_reordering_other_content_or_turns() {
    fn entry(index: usize, turn: &str, output: Output) -> HistoryEntry {
        HistoryEntry {
            id: format!("{turn}:{index}"),
            origin: Some(CallRef {
                session_id: "session".into(),
                item_id: index.to_string(),
                generation_id: Some(turn.into()),
                caller_id: "model".into(),
                call_id: index.to_string(),
            }),
            message: Message::Assistant {
                output: vec![output],
                provider_data: Value::Null,
            },
        }
    }
    fn provider(id: usize, status: ProviderToolStatus, name: &str) -> Output {
        Output::ProviderToolCall {
            call: ProviderToolCall {
                id: id.to_string(),
                provider: name.into(),
                name: "job".into(),
                status,
                outcome: None,
                output: vec![],
                data: json!(id),
            },
        }
    }
    let mut entries = vec![entry(0, "a", Output::text("before"))];
    for id in 0..4096 {
        entries.push(entry(
            id + 1,
            "a",
            provider(id, ProviderToolStatus::Running, "p"),
        ));
    }
    entries.push(entry(
        5000,
        "b",
        provider(0, ProviderToolStatus::Running, "p"),
    ));
    entries.push(entry(
        5001,
        "a",
        provider(0, ProviderToolStatus::Completed, "other-provider"),
    ));
    for id in (0..4096).rev() {
        entries.push(entry(
            6000 + id,
            "a",
            provider(id, ProviderToolStatus::Completed, "p"),
        ));
    }
    entries.push(entry(11000, "a", Output::text("after")));
    let messages = conversation(entries);
    assert_eq!(messages.len(), 2);
    let Message::Assistant { output, .. } = &messages[0] else {
        unreachable!()
    };
    assert_eq!(output.len(), 4099);
    assert_eq!(output.first(), Some(&Output::text("before")));
    assert_eq!(output.last(), Some(&Output::text("after")));
    for (id, output) in output[1..4097].iter().enumerate() {
        assert_eq!(output, &provider(id, ProviderToolStatus::Completed, "p"));
    }
    let Message::Assistant { output, .. } = &messages[1] else {
        unreachable!()
    };
    assert_eq!(output, &vec![provider(0, ProviderToolStatus::Running, "p")]);
}

struct HangingCancel {
    _inner: Arc<dyn OperationControl>,
    calls: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
}
struct ActiveCancel(Arc<AtomicUsize>);
impl Drop for ActiveCancel {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl OperationControl for HangingCancel {
    fn cancel(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.active.fetch_add(1, Ordering::SeqCst);
            let _active = ActiveCancel(self.active.clone());
            std::future::pending().await
        })
    }
    fn reply(&self, value: Value) -> BoxFuture<'_, Result<()>> {
        self._inner.reply(value)
    }
}

#[tokio::test(start_paused = true)]
async fn cleanup_uses_one_budget_and_bounded_concurrent_cancellation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let tool = zhir_tools::function::FunctionTool::new(
        RuntimeToolSpec {
            name: "wait".into(),
            description: "wait".into(),
            input: InputSpec::Structured {
                schema: json!({"type":"object"}),
            },
            output_schema: None,
            execution: Execution {
                parallel: true,
                ..Default::default()
            },
        },
        {
            let calls = calls.clone();
            let active = active.clone();
            move |_, _| {
                let calls = calls.clone();
                let active = active.clone();
                async move {
                    let mut handle = zhir_testing::waiting_operation(json!({}), 0);
                    handle.control = Arc::new(HangingCancel {
                        _inner: handle.control,
                        calls,
                        active,
                    });
                    Ok(ToolExecution::Active(handle))
                }
            }
        },
    );
    let model = Arc::new(SessionModel::new(
        zhir_testing::model_capabilities(),
        |_, mut peer| async move {
            let command = peer.command().await?.unwrap();
            let SessionCommandBody::Generate { generation_id, .. } = &command.body else {
                unreachable!()
            };
            peer.acknowledge(&command, None).await?;
            for index in 0..12 {
                peer.event(SessionEventBody::Output {
                    generation_id: Some(generation_id.clone()),
                    item_id: index.to_string(),
                    caller_id: "model".into(),
                    output: Output::RuntimeToolCall {
                        call: RuntimeToolCall {
                            id: index.to_string(),
                            name: "wait".into(),
                            input: RuntimeToolInput::Structured(json!({})),
                        },
                    },
                })
                .await?;
            }
            std::future::pending().await
        },
    ));
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir_storage::MemoryRunStore::new(),
    )));
    let tools = zhir_tools::RuntimeToolRegistry::from_tools([Arc::new(tool) as _]).unwrap();
    let runtime = Runtime::builder(model)
        .store(store.clone())
        .runtime_tools(Arc::new(tools))
        .build()
        .unwrap();
    let limits = zhir_core::run::Limits {
        max_operation_concurrency: 4,
        ..zhir_kernel::defaults::limits()
    };
    let mut invocation = runtime
        .start(RunRequest::new([Message::user("wait")]).limits(limits))
        .unwrap();
    invocation.start();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let ready = store.commits().last().is_some_and(|commit| {
                let operations = &commit.checkpoint.active.operations;
                operations.len() == 12 && operations.values().all(|op| op.last_sequence == Some(0))
            });
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "{error}: {:?}",
            store.commits().last().map(|c| &c.checkpoint)
        )
    });
    let before = tokio::time::Instant::now();
    invocation.control().cancel();
    let completion = invocation.result().await.unwrap();
    assert!(matches!(completion.checkpoint().state, State::Cancelled));
    assert!(before.elapsed() <= Duration::from_millis(100));
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}
