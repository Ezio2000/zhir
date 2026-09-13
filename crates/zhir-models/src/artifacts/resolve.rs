use super::{invalid, json_error};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use zhir_core::{
    BoxFuture, Cancellation, Result,
    artifact::{ArtifactContent, ArtifactRef, ArtifactStore},
    message::{Content, MediaSource, Output},
};
async fn load(
    reference: ArtifactRef,
    store: &dyn ArtifactStore,
    cancellation: &Cancellation,
    cache: &mut BTreeMap<String, ArtifactContent>,
) -> Result<ArtifactContent> {
    cancellation.check()?;
    if reference.id.is_empty() || reference.mime_type.is_empty() {
        return Err(zhir_core::error::ArtifactError::Invalid {
            id: reference.id.clone(),
            message: "empty artifact reference".into(),
        }
        .into());
    }
    let content = if let Some(content) = cache.get(&reference.id) {
        content.clone()
    } else {
        let content = store.get(reference.clone()).await?;
        cancellation.check()?;
        cache.insert(reference.id.clone(), content.clone());
        content
    };
    if content.mime_type != reference.mime_type || content.base64.is_empty() {
        return Err(zhir_core::error::ArtifactError::Invalid {
            id: reference.id.clone(),
            message: "artifact contents do not match reference".into(),
        }
        .into());
    }
    Ok(content)
}
async fn resolve_content(
    content: &mut Content,
    store: &dyn ArtifactStore,
    cancellation: &Cancellation,
    cache: &mut BTreeMap<String, ArtifactContent>,
) -> Result<()> {
    let source = match content {
        Content::Image { source }
        | Content::Audio { source }
        | Content::Video { source }
        | Content::File { source, .. } => source,
        _ => return Ok(()),
    };
    if let MediaSource::Artifact { id, mime_type } = source {
        let content = load(
            ArtifactRef {
                id: id.clone(),
                mime_type: mime_type.clone(),
            },
            store,
            cancellation,
            cache,
        )
        .await?;
        *source = MediaSource::Inline {
            mime_type: content.mime_type,
            base64: content.base64,
        };
    }
    Ok(())
}
pub(super) async fn message(
    message: &mut zhir_core::message::Message,
    store: &dyn ArtifactStore,
    cancellation: &Cancellation,
    cache: &mut BTreeMap<String, ArtifactContent>,
) -> Result<()> {
    use zhir_core::{message::Message, tool::RuntimeToolOutcome};
    match message {
        Message::System { content } | Message::User { content } | Message::External { content } => {
            for part in content {
                resolve_content(part, store, cancellation, cache).await?;
            }
        }
        Message::Assistant {
            output,
            provider_data,
        } => {
            for item in output {
                match item {
                    Output::Content { content } => {
                        resolve_content(content, store, cancellation, cache).await?
                    }
                    Output::ProviderToolCall { call } => {
                        for content in &mut call.output {
                            resolve_content(content, store, cancellation, cache).await?;
                        }
                        resolve(&mut call.data, store, cancellation, cache).await?;
                    }
                    Output::RuntimeToolCall { .. } => {}
                }
            }
            resolve(provider_data, store, cancellation, cache).await?;
        }
        Message::RuntimeTool { outcome, .. } => match outcome {
            RuntimeToolOutcome::Success { content, .. }
            | RuntimeToolOutcome::Accepted { content, .. }
            | RuntimeToolOutcome::Waiting { content, .. } => {
                for part in content {
                    resolve_content(part, store, cancellation, cache).await?;
                }
            }
            RuntimeToolOutcome::Failure { .. } => {}
        },
    }
    Ok(())
}
fn resolve<'a>(
    value: &'a mut Value,
    store: &'a dyn ArtifactStore,
    cancellation: &'a Cancellation,
    cache: &'a mut BTreeMap<String, ArtifactContent>,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        cancellation.check()?;
        if let Some(reference) = value.get("$zhir_artifact").cloned() {
            if value.as_object().is_none_or(|object| object.len() != 1) {
                return Err(invalid("invalid artifact replay marker"));
            }
            let reference = serde_json::from_value::<ArtifactRef>(reference).map_err(json_error)?;
            let content = load(reference, store, cancellation, cache).await?;
            *value = json!(content.base64);
        } else {
            match value {
                Value::Array(values) => {
                    for value in values {
                        resolve(value, store, cancellation, cache).await?;
                    }
                }
                Value::Object(values) => {
                    for value in values.values_mut() {
                        resolve(value, store, cancellation, cache).await?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    })
}
