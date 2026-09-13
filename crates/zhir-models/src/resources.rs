//! Resolve stored inputs and seal output resources before they reach the execution kernel.
use base64::Engine as _;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use zhir_core::{
    BoxFuture, Cancellation, Result,
    error::Error,
    message::{Content, Message, Output},
    model::*,
    profile::NegotiatedProfile,
    resource::*,
};
pub struct ResourceModel {
    inner: crate::TransformModel,
    store: Arc<dyn ResourceStore>,
}
impl ResourceModel {
    pub fn new(
        model: Arc<dyn Model>,
        store: Arc<dyn ResourceStore>,
        max_input_bytes: usize,
    ) -> Result<Self> {
        if max_input_bytes == 0 {
            return Err(Error::Invalid(
                "resource materialization limit must be positive".into(),
            ));
        }
        let inputs = store.clone();
        let inner = crate::TransformModel::new(model, move |mut request, context| {
            let store = inputs.clone();
            async move {
                let mut budget = max_input_bytes;
                for message in &mut request.messages {
                    resolve_message(message, store.as_ref(), &context.cancellation, &mut budget)
                        .await?;
                }
                Ok(request)
            }
        });
        let commands = store.clone();
        let inner = inner.map_command(move |mut command, context| {
            let store = commands.clone();
            async move {
                let mut budget = max_input_bytes;
                match &mut command.body {
                    SessionCommandBody::Input { message } => {
                        resolve_message(message, store.as_ref(), &context.cancellation, &mut budget)
                            .await?
                    }
                    SessionCommandBody::ToolResult {
                        outcome: zhir_core::tool::RuntimeToolOutcome::Success { content, .. },
                        ..
                    } => {
                        for content in content {
                            resolve_content(
                                content,
                                store.as_ref(),
                                &context.cancellation,
                                &mut budget,
                            )
                            .await?;
                        }
                    }
                    _ => (),
                }
                Ok(command)
            }
        });
        Ok(Self { inner, store })
    }
}
impl Model for ResourceModel {
    fn capabilities(&self) -> &CapabilitySet {
        self.inner.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn open_session(&self, open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            let cancellation = open.context.cancellation.clone();
            let mut session = self.inner.open_session(open).await?;
            session.output = Box::new(SealedOutput {
                inner: session.output,
                store: self.store.clone(),
                cancellation,
                payloads: Default::default(),
            });
            Ok(session)
        })
    }
}
struct SealedOutput {
    inner: Box<dyn SessionReceiver>,
    store: Arc<dyn ResourceStore>,
    cancellation: Cancellation,
    payloads: std::collections::BTreeSet<[u8; 32]>,
}
impl SessionReceiver for SealedOutput {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        Box::pin(async move {
            let Some(mut event) = self.inner.receive().await? else {
                return Ok(None);
            };
            match &mut event.body {
                SessionEventBody::Output { output, .. } => {
                    let contents: Vec<_> = match output {
                        Output::Content { content } => vec![&*content],
                        Output::ProviderToolCall { call } => call.output.iter().collect(),
                        _ => vec![],
                    };
                    for content in contents {
                        if let Content::Resource { input } = content
                            && let ResourceSource::Inline { bytes } = &input.resource.source
                        {
                            self.payloads.insert(
                                Sha256::digest(
                                    base64::engine::general_purpose::STANDARD
                                        .encode(bytes)
                                        .as_bytes(),
                                )
                                .into(),
                            );
                        }
                    }
                    save_output(output, self.store.as_ref(), &self.cancellation).await?;
                }
                SessionEventBody::Operation {
                    event:
                        zhir_core::operation::OperationEvent {
                            update:
                                zhir_core::operation::OperationUpdate::Finished {
                                    outcome:
                                        zhir_core::tool::RuntimeToolOutcome::Success { content, .. },
                                },
                            ..
                        },
                    ..
                } => {
                    for content in content {
                        save_content(content, self.store.as_ref(), &self.cancellation).await?;
                    }
                }
                SessionEventBody::TurnFinished { provider_data, .. } => {
                    if contains_sealed_payload(provider_data, &self.payloads) {
                        return Err(Error::Protocol("inline resource remains in native turn data; declare its replay binding".into()));
                    }
                    self.payloads.clear();
                }
                _ => (),
            }
            Ok(Some(event))
        })
    }
}
fn contains_sealed_payload(value: &Value, payloads: &std::collections::BTreeSet<[u8; 32]>) -> bool {
    match value {
        Value::String(value) => {
            payloads.contains(&<[u8; 32]>::from(Sha256::digest(value.as_bytes())))
        }
        Value::Array(values) => values
            .iter()
            .any(|value| contains_sealed_payload(value, payloads)),
        Value::Object(values) => values
            .values()
            .any(|value| contains_sealed_payload(value, payloads)),
        _ => false,
    }
}
async fn load(
    reference: ResourceRef,
    store: &dyn ResourceStore,
    cancellation: &Cancellation,
    budget: &mut usize,
) -> Result<Vec<u8>> {
    cancellation.check()?;
    reference.validate()?;
    let limit = *budget;
    let mut reader = store.open(reference).await?;
    let mut bytes = vec![];
    loop {
        cancellation.check()?;
        let chunk = reader
            .read((limit - bytes.len()).saturating_add(1).min(1024 * 1024))
            .await?;
        if chunk.is_empty() {
            break;
        }
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(Error::Invalid(
                "resource exceeds input materialization limit; use media streaming".into(),
            ));
        }
        bytes.extend(chunk);
    }
    *budget -= bytes.len();
    Ok(bytes)
}
async fn resolve_content(
    content: &mut Content,
    store: &dyn ResourceStore,
    cancellation: &Cancellation,
    budget: &mut usize,
) -> Result<()> {
    if let Content::Resource { input } = content
        && matches!(input.resource.source, ResourceSource::Stored { .. })
    {
        input.resource.source = ResourceSource::Inline {
            bytes: load(input.resource.clone(), store, cancellation, budget).await?,
        };
    } else if let Content::Resource { input } = content
        && let ResourceSource::Inline { bytes } = &input.resource.source
    {
        *budget = budget
            .checked_sub(bytes.len())
            .ok_or_else(|| Error::Invalid("resource exceeds input materialization limit".into()))?;
    }
    Ok(())
}
fn resolve_value<'a>(
    value: &'a mut Value,
    store: &'a dyn ResourceStore,
    cancellation: &'a Cancellation,
    budget: &'a mut usize,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        if let Some(reference) = value.get("$zhir_resource") {
            if value.as_object().is_none_or(|o| o.len() != 1) {
                return Err(Error::Protocol("invalid resource replay marker".into()));
            }
            let reference = serde_json::from_value(reference.clone())
                .map_err(|e| Error::Protocol(e.to_string()))?;
            *value = json!(
                base64::engine::general_purpose::STANDARD
                    .encode(load(reference, store, cancellation, budget).await?)
            );
        } else {
            match value {
                Value::Array(values) => {
                    for value in values {
                        resolve_value(value, store, cancellation, budget).await?;
                    }
                }
                Value::Object(values) => {
                    for value in values.values_mut() {
                        resolve_value(value, store, cancellation, budget).await?;
                    }
                }
                _ => (),
            }
        }
        Ok(())
    })
}
async fn resolve_message(
    message: &mut Message,
    store: &dyn ResourceStore,
    cancellation: &Cancellation,
    budget: &mut usize,
) -> Result<()> {
    match message {
        Message::System { content } | Message::User { content } | Message::External { content } => {
            for content in content {
                resolve_content(content, store, cancellation, budget).await?;
            }
        }
        Message::Assistant {
            output,
            provider_data,
        } => {
            for output in output {
                match output {
                    Output::Content { content } => {
                        resolve_content(content, store, cancellation, budget).await?
                    }
                    Output::ProviderToolCall { call } => {
                        for content in &mut call.output {
                            resolve_content(content, store, cancellation, budget).await?;
                        }
                        resolve_value(&mut call.data, store, cancellation, budget).await?;
                    }
                    _ => (),
                }
            }
            resolve_value(provider_data, store, cancellation, budget).await?;
        }
        Message::RuntimeTool {
            outcome: zhir_core::tool::RuntimeToolOutcome::Success { content, .. },
            ..
        } => {
            for content in content {
                resolve_content(content, store, cancellation, budget).await?;
            }
        }
        _ => (),
    }
    Ok(())
}
async fn save_content(
    content: &mut Content,
    store: &dyn ResourceStore,
    cancellation: &Cancellation,
) -> Result<Option<(String, ResourceRef)>> {
    let Content::Resource { input } = content else {
        return Ok(None);
    };
    let ResourceSource::Inline { bytes } = &input.resource.source else {
        return Ok(None);
    };
    let mut hash = Sha256::new();
    hash.update(input.resource.media_type.as_bytes());
    hash.update([0]);
    hash.update(bytes);
    let key = format!("{:x}", hash.finalize());
    let mut writer = store.create(key, input.resource.media_type.clone()).await?;
    for (sequence, bytes) in bytes.chunks(1024 * 1024).enumerate() {
        cancellation.check()?;
        writer.append(sequence as u64, bytes.to_vec()).await?;
    }
    let reference = writer.finish().await?;
    reference.validate()?;
    if reference.media_type != input.resource.media_type {
        return Err(Error::Protocol("resource store changed media type".into()));
    }
    let payload = base64::engine::general_purpose::STANDARD.encode(bytes);
    if matches!(reference.source, ResourceSource::Inline { .. }) {
        return Err(Error::Protocol("resource store did not seal output".into()));
    }
    input.resource.source = reference.source;
    Ok(Some((payload, input.resource.clone())))
}
async fn save_output(
    output: &mut Output,
    store: &dyn ResourceStore,
    cancellation: &Cancellation,
) -> Result<()> {
    match output {
        Output::Content { content } => {
            save_content(content, store, cancellation).await?;
        }
        Output::ProviderToolCall { call } => {
            let replay = crate::provider_tools::output::replay(call)?;
            let mut bindings = std::collections::BTreeMap::new();
            if let Some(replay) = replay {
                for binding in replay.media {
                    if !binding.pointer.starts_with("/items/")
                        || bindings
                            .insert(binding.content_index, binding.pointer)
                            .is_some()
                    {
                        return Err(Error::Protocol(
                            "invalid or duplicate resource replay binding".into(),
                        ));
                    }
                }
            }
            let mut sealed_payloads = Vec::new();
            for (index, content) in call.output.iter_mut().enumerate() {
                if let Some((payload, reference)) =
                    save_content(content, store, cancellation).await?
                {
                    sealed_payloads.push(payload.clone());
                    if let Some(pointer) = bindings.remove(&index) {
                        let full =
                            format!("/{}{}", crate::provider_tools::output::REPLAY_KEY, pointer);
                        let target = call.data.pointer_mut(&full).ok_or_else(|| {
                            Error::Protocol("resource replay pointer not found".into())
                        })?;
                        if target.as_str() != Some(&payload) {
                            return Err(Error::Protocol("resource replay payload mismatch".into()));
                        }
                        *target = json!({"$zhir_resource":reference});
                    } else if contains(&call.data, &payload) {
                        return Err(Error::Protocol(
                            "unbound resource remains in provider replay".into(),
                        ));
                    }
                }
            }
            if sealed_payloads
                .iter()
                .any(|payload| !payload.is_empty() && contains(&call.data, payload))
            {
                return Err(Error::Protocol(
                    "unbound resource remains in provider replay".into(),
                ));
            }
            if !bindings.is_empty() {
                return Err(Error::Protocol(
                    "resource binding has no inline output".into(),
                ));
            }
        }
        _ => (),
    }
    Ok(())
}
fn contains(value: &Value, payload: &str) -> bool {
    match value {
        Value::String(s) => s == payload,
        Value::Array(values) => values.iter().any(|v| contains(v, payload)),
        Value::Object(values) => values.values().any(|v| contains(v, payload)),
        _ => false,
    }
}
