#![cfg(all(
    feature = "models",
    feature = "tools",
    feature = "memory",
    feature = "interaction"
))]
use serde_json::json;
use std::{sync::Arc, time::Duration};
use zhir::{
    ResumeRequest, RunRequest, Runtime,
    core::operation::RecoveryResolution,
    message::{Message, Output},
    model::TurnOutput,
    run::State,
};
use zhir_testing::{RecordingStore, ScriptedModel};
fn response(output: Vec<Output>) -> TurnOutput {
    TurnOutput {
        output,
        ..TurnOutput::text("")
    }
}
fn call(id: &str, name: &str, args: serde_json::Value) -> Output {
    Output::RuntimeToolCall {
        call: zhir::tool::RuntimeToolCall {
            id: id.into(),
            name: name.into(),
            input: zhir::tool::RuntimeToolInput::Structured(args),
        },
    }
}
async fn result(runtime: &Runtime) -> Arc<zhir::run::Checkpoint> {
    tokio::time::timeout(
        Duration::from_secs(3),
        runtime
            .start(RunRequest::new(vec![Message::user("test")]))
            .unwrap()
            .result(),
    )
    .await
    .expect("runtime stalled")
    .unwrap()
    .into_checkpoint()
}
#[tokio::test]
async fn text_session_commits_and_settles() {
    let model = Arc::new(ScriptedModel::responses([TurnOutput::text("done")]));
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir::stores::MemoryRunStore::new(),
    )));
    let runtime = Runtime::builder(model.clone())
        .store(store.clone())
        .build()
        .unwrap();
    let checkpoint = result(&runtime).await;
    assert!(
        matches!(&checkpoint.state, State::Completed { content } if content == &vec![zhir::message::Content::text("done")])
    );
    assert!(checkpoint.active.commands.is_empty());
    model.verify().unwrap();
    store.verify_traces().unwrap();
}
#[tokio::test]
async fn waiting_operation_recovers_without_repeating_start() {
    let model = Arc::new(ScriptedModel::responses([
        response(vec![call(
            "question",
            "ask_question",
            json!({"questions":[{"id":"choice","title":"Choose","options":["a","b"]}]}),
        )]),
        TurnOutput::text("answered"),
    ]));
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir::stores::MemoryRunStore::new(),
    )));
    let registry = Arc::new(
        zhir::runtime_tools::RuntimeToolRegistry::from_tools(vec![
            zhir::builtins::interaction::ask_question().unwrap(),
        ])
        .unwrap(),
    );
    let runtime = Runtime::builder(model.clone())
        .runtime_tools(registry)
        .store(store.clone())
        .build()
        .unwrap();
    let checkpoint = result(&runtime).await;
    assert!(
        matches!(checkpoint.state, State::Suspended { .. }),
        "{:?}",
        checkpoint.state
    );
    let operation_id = checkpoint.active.operations.keys().next().unwrap().clone();
    let outcome = zhir::operation::OperationOutcome::Success {
        content: vec![],
        structured: json!({"choice":"a"}),
    };
    let mut invocation = runtime
        .resume(
            ResumeRequest::from_checkpoint(checkpoint).resolve(RecoveryResolution::Complete {
                operation_id,
                outcome,
            }),
        )
        .await
        .unwrap();
    let settled = tokio::time::timeout(Duration::from_secs(3), invocation.result())
        .await
        .expect("resume stalled")
        .unwrap()
        .into_checkpoint();
    assert!(
        matches!(settled.state, State::Completed { .. }),
        "{:?}",
        settled.state
    );
    assert!(settled.active.operations.is_empty());
    assert_eq!(settled.metrics.runtime_tool_calls, 1);
    model.verify().unwrap();
    store.verify_traces().unwrap();
}

#[tokio::test]
async fn native_session_runs_tools_before_turn_end_and_tracks_provider_jobs() {
    use zhir::core::operation::{CallRef, OperationEvent, OperationUpdate};
    use zhir::model::{Capability, SessionCommandBody, SessionEventBody, TurnDisposition};
    let mut caps = zhir_testing::model_capabilities();
    caps.features
        .extend([Capability::AsyncResults, Capability::ProviderTools]);
    let model = Arc::new(zhir_testing::SessionModel::new(
        caps,
        |open, mut peer| async move {
            let command = peer.commands.recv().await.unwrap();
            let SessionCommandBody::StartTurn { turn_id, .. } = &command.body else {
                panic!("first command")
            };
            let turn_id = turn_id.clone();
            peer.acknowledge(&command, None).await?;
            peer.event(SessionEventBody::Output {
                turn_id: turn_id.clone(),
                item_id: "local".into(),
                caller_id: "planner".into(),
                output: call("shared-id", "echo", json!({})),
            })
            .await?;
            let command = peer.commands.recv().await.unwrap();
            let SessionCommandBody::ToolResult { origin, .. } = &command.body else {
                panic!("result expected before turn end")
            };
            assert_eq!(origin.caller_id, "planner");
            peer.acknowledge(&command, None).await?;
            peer.event(SessionEventBody::Output {
                turn_id: turn_id.clone(),
                item_id: "video-job".into(),
                caller_id: "provider".into(),
                output: Output::ProviderToolCall {
                    call: zhir::message::ProviderToolCall {
                        outcome: None,
                        id: "video-job".into(),
                        name: "video".into(),
                        provider: "native".into(),
                        status: zhir::message::ProviderToolStatus::Running,
                        output: vec![],
                        data: json!({}),
                    },
                },
            })
            .await?;
            peer.event(SessionEventBody::Operation {
                origin: CallRef {
                    session_id: open.session_id,
                    turn_id: turn_id.clone(),
                    caller_id: "provider".into(),
                    call_id: "video-job".into(),
                },
                event: OperationEvent {
                    sequence: 0,
                    update: OperationUpdate::Finished {
                        outcome: zhir::operation::OperationOutcome::Success {
                            content: vec![zhir::message::Content::text("video finished")],
                            structured: json!({"job":"video-job"}),
                        },
                    },
                },
            })
            .await?;
            peer.event(SessionEventBody::Output {
                turn_id: turn_id.clone(),
                item_id: "answer".into(),
                caller_id: "planner".into(),
                output: Output::text("complete"),
            })
            .await?;
            peer.finished(turn_id, TurnDisposition::Finished).await
        },
    ));
    let spec = zhir::tool::RuntimeToolSpec {
        name: "echo".into(),
        description: "echo".into(),
        input: zhir::tool::InputSpec::Structured {
            schema: json!({"type":"object"}),
        },
        output_schema: None,
        execution: Default::default(),
    };
    let tool = zhir::runtime_tools::function::structured(spec, |_: serde_json::Value, _| async {
        Ok(zhir::runtime_tools::reply::json(json!("ok")))
    })
    .unwrap();
    let registry = zhir::runtime_tools::RuntimeToolRegistry::from_tools([
        Arc::new(tool) as Arc<dyn zhir::tool::RuntimeTool>
    ])
    .unwrap();
    let runtime = Runtime::builder(model)
        .runtime_tools(Arc::new(registry))
        .build()
        .unwrap();
    let checkpoint = result(&runtime).await;
    assert!(
        matches!(checkpoint.state, State::Completed { .. }),
        "{:?}",
        checkpoint.state
    );
    assert!(checkpoint.active.operations.is_empty());
    assert_eq!(checkpoint.metrics.runtime_tool_calls, 1);
    assert!(
        zhir::output::provider_calls(&checkpoint)
            .iter()
            .any(|record| record.call.status == zhir::message::ProviderToolStatus::Completed)
    );
}

#[tokio::test]
async fn native_voice_video_stream_is_lossless_and_sealed_before_delivery() {
    use zhir::core::resource::{MediaChunk, MediaReceiver, ResourceStore, SealedMedia};
    use zhir::model::{Capability, SessionCommandBody, TurnDisposition};
    let mut caps = zhir_testing::model_capabilities();
    caps.features.insert(Capability::Duplex);
    let model = Arc::new(zhir_testing::SessionModel::new(
        caps,
        |_, mut peer| async move {
            let command = peer.commands.recv().await.unwrap();
            let SessionCommandBody::StartTurn { turn_id, .. } = &command.body else {
                panic!("start")
            };
            let turn_id = turn_id.clone();
            peer.acknowledge(&command, None).await?;
            for sequence in 0..8 {
                peer.media_output
                    .send(MediaChunk {
                        stream_id: "voice".into(),
                        turn_id: turn_id.clone(),
                        epoch: 0,
                        sequence,
                        timestamp_us: sequence * 1000,
                        media_type: "audio/pcm".into(),
                        bytes: vec![sequence as u8; 4],
                        end: sequence == 7,
                    })
                    .await?;
            }
            peer.finished(turn_id, TurnDisposition::Finished).await?;
            while let Some(command) = peer.commands.recv().await {
                peer.acknowledge(&command, None).await?;
                if matches!(command.body, SessionCommandBody::Close) {
                    peer.event(zhir::model::SessionEventBody::Closed).await?;
                    break;
                }
            }
            Ok(())
        },
    ));
    let resources = Arc::new(zhir::stores::MemoryResourceStore::new());
    let runtime = Runtime::builder(model)
        .resources(resources.clone())
        .defaults(|mut options| {
            options.limits.max_media_chunk_bytes = 4;
            options.limits.max_buffered_media_bytes = 8;
            options
        })
        .build()
        .unwrap();
    let mut invocation = runtime
        .start(RunRequest::new(vec![Message::user("speak")]))
        .unwrap();
    let mut output = invocation.media_output().unwrap();
    let receive = async move {
        let mut chunks = vec![];
        while let Some(chunk) = output.receive().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(2)).await;
            chunks.push(chunk);
        }
        chunks
    };
    let (completion, chunks) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(invocation.result(), receive)
    })
    .await
    .expect("media stalled");
    let checkpoint = completion.unwrap().into_checkpoint();
    assert!(
        matches!(checkpoint.state, State::Completed { .. }),
        "{:?}",
        checkpoint.state
    );
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| chunk.sequence)
            .collect::<Vec<_>>(),
        (0..8).collect::<Vec<_>>()
    );
    assert!(checkpoint.active.media.is_empty());
    let mut reader = resources
        .open(checkpoint.active.session.media_archive.clone().unwrap())
        .await
        .unwrap();
    let mut archive = vec![];
    loop {
        let bytes = reader.read(4096).await.unwrap();
        if bytes.is_empty() {
            break;
        }
        archive.extend(bytes);
    }
    let archive: zhir::resource::ArchivedMedia = serde_json::from_slice(&archive).unwrap();
    assert!(archive.complete);
    assert_eq!(archive.stream_key, "output:voice:0");
    let mut next = Some(archive.sealed);
    let mut sequences = vec![];
    while let Some(reference) = next {
        let mut reader = resources.open(reference).await.unwrap();
        let mut data = vec![];
        loop {
            let bytes = reader.read(17).await.unwrap();
            if bytes.is_empty() {
                break;
            }
            data.extend(bytes);
        }
        let node: SealedMedia = serde_json::from_slice(&data).unwrap();
        sequences.push(node.sequence);
        next = node.previous;
    }
    assert_eq!(sequences, (0..8).rev().collect::<Vec<_>>());
}
