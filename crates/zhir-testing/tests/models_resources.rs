use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    message::{Content, Message, Output},
    model::{GenerationOutput, ModelContext, ModelRequest},
    resource::*,
};
use zhir_models::{FunctionModel, ResourceModel};
use zhir_testing::ModelTestExt;
fn reference(source: ResourceSource) -> ResourceRef {
    ResourceRef {
        id: "image".into(),
        media_type: "image/png".into(),
        name: Some("image.png".into()),
        source,
        metadata: Default::default(),
    }
}
fn request(messages: Vec<Message>) -> ModelRequest {
    ModelRequest {
        messages,
        runtime_tools: vec![],
        provider_tools: vec![],
        profile: Default::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream: false,
    }
}
fn context() -> ModelContext {
    ModelContext {
        run: zhir_kernel::defaults::context(),
        cancellation: Default::default(),
        deltas: None,
    }
}
struct BrokenStore;
impl ResourceStore for BrokenStore {
    fn open(&self, _: ResourceRef) -> BoxFuture<'_, Result<Box<dyn ResourceReader>>> {
        Box::pin(async { Err(Error::Storage("fixture read failed".into())) })
    }
    fn create(&self, _: String, _: String) -> BoxFuture<'_, Result<Box<dyn ResourceWriter>>> {
        Box::pin(async { Ok(Box::new(BrokenWriter) as Box<dyn ResourceWriter>) })
    }
}
struct BrokenWriter;
impl ResourceWriter for BrokenWriter {
    fn append(&mut self, _: u64, _: Vec<u8>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn finish(self: Box<Self>) -> BoxFuture<'static, Result<ResourceRef>> {
        Box::pin(async {
            Ok(ResourceRef {
                media_type: "audio/wav".into(),
                ..reference(ResourceSource::Stored {
                    key: "wrong".into(),
                })
            })
        })
    }
}
#[tokio::test]
async fn invalid_replay_and_storage_failure_stop_before_model() {
    let calls = Arc::new(AtomicUsize::new(0));
    for marker in [false, true] {
        let seen = calls.clone();
        let mut caps = zhir_testing::model_capabilities();
        caps.input_modalities.push("image".into());
        let inner = FunctionModel::new(caps, move |_, _| {
            seen.fetch_add(1, Ordering::SeqCst);
            async { Ok(GenerationOutput::text("unexpected")) }
        });
        let model = ResourceModel::new(Arc::new(inner), Arc::new(BrokenStore), 1024).unwrap();
        let message = if marker {
            Message::Assistant {
                output: vec![Output::text("old")],
                provider_data: json!({"$zhir_resource":reference(ResourceSource::Stored { key:"image".into() }),"unexpected":true}),
            }
        } else {
            Message::User {
                content: vec![Content::resource(reference(ResourceSource::Stored {
                    key: "image".into(),
                }))],
            }
        };
        let error = model
            .generate(request(vec![message]), context())
            .await
            .unwrap_err();
        if marker {
            assert!(matches!(error, Error::Protocol(_)));
        } else {
            assert!(matches!(error,Error::Storage(message) if message=="fixture read failed"));
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn output_store_cannot_change_resource_media_type() {
    let inner = FunctionModel::new(zhir_testing::model_capabilities(), |_, _| async {
        Ok(GenerationOutput {
            output: vec![Output::Content {
                content: Content::resource(reference(ResourceSource::Inline {
                    bytes: vec![1, 2, 3],
                })),
            }],
            ..GenerationOutput::text("")
        })
    });
    let model = ResourceModel::new(Arc::new(inner), Arc::new(BrokenStore), 1024).unwrap();
    assert!(
        matches!(model.generate(request(vec![Message::user("generate")]),context()).await,Err(Error::Protocol(message)) if message.contains("media type"))
    );
}
#[tokio::test]
async fn stored_resources_resolve_in_chunks_with_an_aggregate_input_budget() {
    let store = Arc::new(zhir_storage::MemoryResourceStore::new());
    let mut writer = store
        .create("image".into(), "image/png".into())
        .await
        .unwrap();
    writer.append(0, vec![1, 2]).await.unwrap();
    writer.append(1, vec![3]).await.unwrap();
    let saved = writer.finish().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let mut caps = zhir_testing::model_capabilities();
    caps.input_modalities.push("image".into());
    let inner = FunctionModel::new(caps, move |request, _| {
        seen.fetch_add(1, Ordering::SeqCst);
        async move {
            assert!(
                matches!(&request.messages[0],Message::User { content } if matches!(&content[0],Content::Resource { input } if input.resource.source==ResourceSource::Inline { bytes:vec![1,2,3] }))
            );
            Ok(GenerationOutput::text("done"))
        }
    });
    let model = ResourceModel::new(Arc::new(inner), store, 4).unwrap();
    model
        .generate(
            request(vec![Message::User {
                content: vec![Content::resource(saved.clone())],
            }]),
            context(),
        )
        .await
        .unwrap();
    assert!(
        model
            .generate(
                request(vec![Message::User {
                    content: vec![Content::resource(saved.clone()), Content::resource(saved)]
                }]),
                context()
            )
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn sealed_output_retains_identity_and_rejects_duplicate_native_payloads() {
    for duplicate in [false, true] {
        let inner =
            FunctionModel::new(zhir_testing::model_capabilities(), move |_, _| async move {
                Ok(GenerationOutput {
                    output: vec![Output::Content {
                        content: Content::resource(reference(ResourceSource::Inline {
                            bytes: vec![1, 2, 3],
                        })),
                    }],
                    provider_data: if duplicate {
                        json!({"unbound":"AQID"})
                    } else {
                        json!(null)
                    },
                    ..GenerationOutput::text("")
                })
            });
        let model = ResourceModel::new(
            Arc::new(inner),
            Arc::new(zhir_storage::MemoryResourceStore::new()),
            1024,
        )
        .unwrap();
        let result = model
            .generate(request(vec![Message::user("generate")]), context())
            .await;
        if duplicate {
            assert!(matches!(result, Err(Error::Protocol(_))));
        } else {
            let output = result.unwrap();
            assert!(
                matches!(&output.output[0],Output::Content { content:Content::Resource { input } } if input.resource.id=="image" && input.resource.name.as_deref()==Some("image.png") && matches!(input.resource.source,ResourceSource::Stored { .. }))
            );
        }
    }
}

#[tokio::test]
async fn native_input_and_async_results_resolve_resources_and_seal_operation_output() {
    use zhir_core::{model::*, operation::*};
    let store = Arc::new(zhir_storage::MemoryResourceStore::new());
    let mut writer = store
        .create("live".into(), "image/png".into())
        .await
        .unwrap();
    writer.append(0, vec![1, 2, 3]).await.unwrap();
    let saved = writer.finish().await.unwrap();
    let mut caps = zhir_testing::model_capabilities();
    caps.features
        .extend([Capability::Steering, Capability::AsyncResults]);
    let inner = zhir_testing::SessionModel::new(caps, |_, mut peer| async move {
        for index in 0..2 {
            let command = peer.commands.recv().await.unwrap();
            let content = match &command.body {
                SessionCommandBody::Append {
                    entry:
                        zhir_core::run::HistoryEntry {
                            message: Message::User { content },
                            ..
                        },
                    ..
                } if index == 0 => content,
                SessionCommandBody::Append {
                    entry:
                        zhir_core::run::HistoryEntry {
                            message:
                                Message::RuntimeTool {
                                    outcome: OperationOutcome::Success { content, .. },
                                    ..
                                },
                            ..
                        },
                    ..
                } if index == 1 => content,
                _ => panic!("unexpected command"),
            };
            assert!(
                matches!(&content[0], Content::Resource { input } if input.resource.source == ResourceSource::Inline { bytes: vec![1,2,3] })
            );
            peer.acknowledge(&command, None).await?;
        }
        peer.event(SessionEventBody::Operation {
            origin: CallRef {
                session_id: "s".into(),
                item_id: "job".into(),
                generation_id: Some("t".into()),
                caller_id: "model".into(),
                call_id: "job".into(),
            },
            event: OperationEvent {
                sequence: 0,
                update: OperationUpdate::Finished {
                    outcome: OperationOutcome::Success {
                        content: vec![Content::resource(reference(ResourceSource::Inline {
                            bytes: vec![4, 5, 6],
                        }))],
                        structured: json!(null),
                    },
                },
            },
        })
        .await
    });
    let model = ResourceModel::new(Arc::new(inner), store.clone(), 3).unwrap();
    let mut session = model
        .open_session(SessionOpen {
            binding: None,
            context_revision: 0,
            input_position: 0,
            profile_revision: 0,
            mode: zhir_core::run::RunMode::Interactive,
            session_id: "s".into(),
            after_sequence: None,
            output_epoch: 0,
            limits: zhir_kernel::defaults::limits(),
            request: request(vec![Message::user("begin")]),
            recovery: None,
            context: context(),
        })
        .await
        .unwrap();
    session
        .control
        .submit(SessionCommand {
            id: "input".into(),
            body: SessionCommandBody::Append {
                context_revision: 1,
                input_position: 1,
                source: AppendSource::Submitted,
                entry: zhir_core::run::HistoryEntry {
                    id: "input".into(),
                    origin: None,
                    message: Message::User {
                        content: vec![Content::resource(saved.clone())],
                    },
                },
            },
        })
        .await
        .unwrap();
    session
        .control
        .submit(SessionCommand {
            id: "result".into(),
            body: SessionCommandBody::Append {
                context_revision: 2,
                input_position: 1,
                source: AppendSource::Submitted,
                entry: zhir_core::run::HistoryEntry {
                    id: "result".into(),
                    origin: Some(CallRef {
                        session_id: "s".into(),
                        item_id: "local".into(),
                        generation_id: Some("t".into()),
                        caller_id: "model".into(),
                        call_id: "local".into(),
                    }),
                    message: Message::RuntimeTool {
                        name: "tool".into(),
                        call_id: "local".into(),
                        outcome: OperationOutcome::Success {
                            content: vec![Content::resource(saved)],
                            structured: json!(null),
                        },
                    },
                },
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        session.events.receive().await.unwrap().unwrap().body,
        SessionEventBody::Ready { .. }
    ));
    for _ in 0..2 {
        assert!(matches!(
            session.events.receive().await.unwrap().unwrap().body,
            SessionEventBody::Acknowledged { .. }
        ));
    }
    let event = session.events.receive().await.unwrap().unwrap();
    let SessionEventBody::Operation {
        event:
            OperationEvent {
                update:
                    OperationUpdate::Finished {
                        outcome: OperationOutcome::Success { content, .. },
                    },
                ..
            },
        ..
    } = event.body
    else {
        panic!("missing result");
    };
    let Content::Resource { input } = &content[0] else {
        panic!("missing resource");
    };
    assert!(matches!(
        input.resource.source,
        ResourceSource::Stored { .. }
    ));
    let mut reader = store.open(input.resource.clone()).await.unwrap();
    assert_eq!(reader.read(10).await.unwrap(), vec![4, 5, 6]);
}
