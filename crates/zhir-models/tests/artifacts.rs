use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use zhir_core::{
    BoxFuture, Cancellation, Result,
    artifact::{ArtifactContent, ArtifactRef, ArtifactStore},
    error::{ArtifactError, Error},
    message::{Content, MediaSource, Message, Output},
    model::{Model, ModelContext, ModelRequest, ModelResponse},
    run::RunContext,
};
use zhir_models::{ArtifactModel, FunctionModel, capabilities};

struct InvalidStore {
    io_failure: bool,
}
impl ArtifactStore for InvalidStore {
    fn get(&self, _: ArtifactRef) -> BoxFuture<'_, Result<ArtifactContent>> {
        Box::pin(async move {
            if self.io_failure {
                Err(Error::Storage("fixture read failed".into()))
            } else {
                Ok(ArtifactContent {
                    mime_type: "audio/wav".into(),
                    base64: "YQ==".into(),
                })
            }
        })
    }
    fn put(&self, _: String, _: ArtifactContent) -> BoxFuture<'_, Result<ArtifactRef>> {
        Box::pin(async {
            Ok(ArtifactRef {
                id: "invalid-reference".into(),
                mime_type: "audio/wav".into(),
            })
        })
    }
}
fn request(messages: Vec<Message>) -> ModelRequest {
    ModelRequest {
        messages,
        runtime_tools: vec![],
        provider_tools: vec![],
        options: Default::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream: false,
    }
}
fn context() -> ModelContext {
    ModelContext {
        run: RunContext::new("artifacts", 0),
        cancellation: Cancellation::default(),
        deltas: None,
    }
}

#[tokio::test]
async fn invalid_contents_and_replay_markers_stop_before_model_and_preserve_error_kind() {
    let calls = Arc::new(AtomicUsize::new(0));
    for case in 0..3 {
        let observed = calls.clone();
        let mut capabilities = capabilities::text_tool_calling();
        capabilities.input_modalities.push("image".into());
        let inner = FunctionModel::new(capabilities, move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok(ModelResponse::text("unexpected")) }
        });
        let model = ArtifactModel::new(
            Arc::new(inner),
            Arc::new(InvalidStore {
                io_failure: case == 1,
            }),
        );
        let message = if case == 2 {
            Message::Assistant {
                output: vec![Output::text("previous")],
                provider_data: json!({
                    "$zhir_artifact": {"id":"image", "mime_type":"image/png"},
                    "unexpected": true
                }),
            }
        } else {
            Message::User {
                content: vec![Content::Image {
                    source: MediaSource::Artifact {
                        id: "image".into(),
                        mime_type: "image/png".into(),
                    },
                }],
            }
        };
        let error = model
            .invoke(request(vec![message]), context())
            .await
            .unwrap_err();
        match case {
            0 => assert!(
                matches!(error, Error::Artifact(ArtifactError::Invalid { id, .. }) if id == "image")
            ),
            1 => assert!(
                matches!(error, Error::Storage(message) if message == "fixture read failed")
            ),
            _ => assert!(matches!(error, Error::Protocol(_))),
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invalid_reference_returned_by_store_is_an_artifact_error() {
    let inner = FunctionModel::new(capabilities::text_tool_calling(), |_, _| async {
        let mut response = ModelResponse::text("");
        response.output = vec![Output::Content {
            content: Content::Image {
                source: MediaSource::Inline {
                    mime_type: "image/png".into(),
                    base64: "YQ==".into(),
                },
            },
        }];
        response.provider_data = Value::Null;
        Ok(response)
    });
    let model = ArtifactModel::new(
        Arc::new(inner),
        Arc::new(InvalidStore { io_failure: false }),
    );
    let error = model
        .invoke(request(vec![Message::user("generate")]), context())
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Artifact(ArtifactError::Invalid { id, .. }) if id == "invalid-reference")
    );
}
