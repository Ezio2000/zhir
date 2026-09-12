//! Durable outputs and asynchronous reference resolution before protocol encoding.
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use zhir_core::{
    BoxFuture, Cancellation, Result,
    artifact::{ArtifactContent, ArtifactRef, ArtifactStore},
    error::Error,
    message::{Content, MediaSource, Output},
    model::{Capabilities, Model, ModelContext, ModelRequest, ModelResponse},
};

pub struct ArtifactModel {
    inner: Arc<dyn Model>,
    store: Arc<dyn ArtifactStore>,
}
impl ArtifactModel {
    pub fn new(inner: Arc<dyn Model>, store: Arc<dyn ArtifactStore>) -> Self {
        Self { inner, store }
    }
}
pub(crate) fn source(content: &Content) -> Option<&MediaSource> {
    match content {
        Content::Image { source }
        | Content::Audio { source }
        | Content::Video { source }
        | Content::File { source, .. } => Some(source),
        _ => None,
    }
}
fn invalid(message: impl Into<String>) -> Error {
    Error::Storage(message.into())
}
fn json_error(error: serde_json::Error) -> Error {
    invalid(error.to_string())
}

impl Model for ArtifactModel {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }
    fn invoke(
        &self,
        mut request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            context.cancellation.check()?;
            request.validate(self.capabilities())?;
            let mut cache = BTreeMap::new();
            for message in &mut request.messages {
                resolve_message(
                    message,
                    self.store.as_ref(),
                    &context.cancellation,
                    &mut cache,
                )
                .await?;
            }
            context.cancellation.check()?;
            let response = self.inner.invoke(request, context.clone()).await?;
            response.validate()?;
            context.cancellation.check()?;
            let mut locations = BTreeMap::new();
            for (index, output) in response.output.iter().enumerate() {
                if let Output::ProviderToolCall { call } = output
                    && let Some(replay) = crate::provider_tools::output::replay(call)?
                {
                    for binding in replay.media {
                        if !binding.pointer.starts_with("/items/") {
                            return Err(invalid("media binding must address native replay items"));
                        }
                        if locations
                            .insert(
                                (index, binding.content_index),
                                format!(
                                    "/output/{index}/call/data/{}/{}",
                                    crate::provider_tools::output::REPLAY_KEY,
                                    binding.pointer.trim_start_matches('/')
                                ),
                            )
                            .is_some()
                        {
                            return Err(invalid("duplicate provider media binding"));
                        }
                    }
                }
            }
            let mut value = serde_json::to_value(&response).map_err(json_error)?;
            let mut saved = BTreeMap::<String, ArtifactRef>::new();
            let mut payloads = Vec::new();
            let mut claimed = std::collections::BTreeSet::new();
            for (output_index, output) in response.output.iter().enumerate() {
                let contents: Vec<_> = match output {
                    Output::Content { content } => {
                        vec![(format!("/output/{output_index}/content/source"), content)]
                    }
                    Output::ProviderToolCall { call } => call
                        .output
                        .iter()
                        .enumerate()
                        .map(|(index, content)| {
                            (
                                format!("/output/{output_index}/call/output/{index}/source"),
                                content,
                            )
                        })
                        .collect(),
                    _ => Vec::new(),
                };
                for (content_index, (pointer, content)) in contents.into_iter().enumerate() {
                    let Some(MediaSource::Inline { mime_type, base64 }) = source(content) else {
                        continue;
                    };
                    let key = format!(
                        "{:x}",
                        Sha256::digest(
                            serde_json::to_vec(&(&context.run.run_id, mime_type, base64))
                                .map_err(json_error)?
                        )
                    );
                    let reference = if let Some(reference) = saved.get(&key) {
                        reference.clone()
                    } else {
                        context.cancellation.check()?;
                        let reference = self
                            .store
                            .put(
                                key.clone(),
                                ArtifactContent {
                                    mime_type: mime_type.clone(),
                                    base64: base64.clone(),
                                },
                            )
                            .await?;
                        context.cancellation.check()?;
                        if reference.id.is_empty() || reference.mime_type != *mime_type {
                            return Err(invalid("artifact store returned an invalid reference"));
                        }
                        saved.insert(key, reference.clone());
                        reference
                    };
                    *value
                        .pointer_mut(&pointer)
                        .ok_or_else(|| invalid("artifact output path not found"))? = json!({"kind":"artifact","id":reference.id,"mime_type":reference.mime_type});
                    if let Some(path) = locations.remove(&(output_index, content_index)) {
                        if !claimed.insert(path.clone()) {
                            return Err(invalid(
                                "artifact replay bindings must identify unique protocol payload fields",
                            ));
                        }
                        let target = value.pointer_mut(&path).ok_or_else(|| {
                            invalid(format!("artifact replay path not found: {path}"))
                        })?;
                        if target.as_str() != Some(base64) {
                            return Err(invalid(format!(
                                "artifact replay payload mismatch at {path}"
                            )));
                        }
                        *target = json!({"$zhir_artifact":reference});
                    }
                    payloads.push(base64.as_str());
                }
            }
            if !locations.is_empty() {
                return Err(invalid(
                    "artifact binding does not address an inline output",
                ));
            }
            // Raw snapshots must not silently retain a second inline copy.
            if contains_payload(&value["provider_data"], &payloads)
                || value["output"].as_array().is_some_and(|items| {
                    items
                        .iter()
                        .any(|item| contains_payload(&item["call"]["data"], &payloads))
                })
            {
                return Err(invalid(
                    "inline artifact remains in protocol replay data; declare media in ProviderOutput",
                ));
            }
            let response: ModelResponse = serde_json::from_value(value).map_err(json_error)?;
            response.validate()?;
            Ok(response)
        })
    }
}
fn contains_payload(value: &Value, payloads: &[&str]) -> bool {
    match value {
        Value::String(value) => payloads.contains(&value.as_str()),
        Value::Array(values) => values.iter().any(|value| contains_payload(value, payloads)),
        Value::Object(values) if !values.contains_key("$zhir_artifact") => values
            .values()
            .any(|value| contains_payload(value, payloads)),
        _ => false,
    }
}

async fn load(
    reference: ArtifactRef,
    store: &dyn ArtifactStore,
    cancellation: &Cancellation,
    cache: &mut BTreeMap<String, ArtifactContent>,
) -> Result<ArtifactContent> {
    cancellation.check()?;
    if reference.id.is_empty() || reference.mime_type.is_empty() {
        return Err(invalid("empty artifact reference"));
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
        return Err(invalid("artifact contents do not match reference"));
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
async fn resolve_message(
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
