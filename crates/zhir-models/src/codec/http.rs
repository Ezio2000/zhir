use crate::{Protocol, ProtocolExtension};
use base64::Engine as _;
use serde_json::{Value, json};
use zhir_core::resource::{ResourceRef, ResourceSource};
use zhir_core::{
    Result,
    error::Error,
    message::{Content, Message, Output, ProviderToolCall, ProviderToolStatus},
    model::{ModelRequest, ResponseFormat, ToolChoice, TurnOutput, Usage},
    tool::{InputSpec, RuntimeToolCall, RuntimeToolInput},
};
fn protocol_error(s: impl Into<String>) -> Error {
    Error::Protocol(s.into())
}
fn text(v: &Value, key: &str) -> Result<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| protocol_error(format!("missing {key}")))
}
fn data_url(resource: &ResourceRef) -> Result<String> {
    match &resource.source {
        ResourceSource::Url { url } => Ok(url.clone()),
        ResourceSource::Inline { bytes } => Ok(format!(
            "data:{};base64,{}",
            resource.media_type,
            base64::engine::general_purpose::STANDARD.encode(bytes)
        )),
        _ => Err(Error::Invalid(
            "resource requires resolution or provider-native encoding".into(),
        )),
    }
}
fn fidelity(input: &zhir_core::resource::ResourceInput) -> Option<&'static str> {
    input.usage.fidelity.as_ref().map(|r| match r.value() {
        zhir_core::profile::Fidelity::Economy => "low",
        zhir_core::profile::Fidelity::High => "high",
        zhir_core::profile::Fidelity::Original => "original",
    })
}
fn resource_extensions(
    input: &zhir_core::resource::ResourceInput,
    protocol: &str,
    target: &mut Value,
) -> Result<()> {
    if let Some(values) = input.usage.extensions.get(protocol) {
        for (key, value) in values {
            if target.get(key).is_some() {
                return Err(Error::Invalid(format!(
                    "resource extension overrides controlled field {key}"
                )));
            }
            target[key] = value.clone();
        }
    }
    Ok(())
}

#[path = "chat.rs"]
mod chat;
#[path = "messages.rs"]
mod messages;
#[path = "replay.rs"]
mod replay;
#[path = "responses.rs"]
mod responses;
use replay::{assistant_replay, decode_items, provider_history};

#[path = "adapter.rs"]
mod adapter;
use adapter::{ProtocolAdapter, UsageFields};
impl Protocol {
    pub(crate) fn adapter(self) -> &'static dyn ProtocolAdapter {
        match self {
            Self::Chat => &chat::Adapter,
            Self::Responses => &responses::Adapter,
            Self::Messages => &messages::Adapter,
        }
    }
    pub(crate) fn key(self) -> &'static str {
        self.adapter().key()
    }
}
pub(crate) fn encode(
    protocol: Protocol,
    model: &str,
    request: &ModelRequest,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Value> {
    protocol.adapter().encode(model, request, extension)
}
fn parts(
    values: &[Content],
    output: bool,
    encode: fn(&Content, bool) -> Result<Value>,
) -> Result<Vec<Value>> {
    values.iter().map(|v| encode(v, output)).collect()
}
fn call_input(call: &RuntimeToolCall) -> String {
    match &call.input {
        RuntimeToolInput::Structured(v) => v.to_string(),
        RuntimeToolInput::Freeform(s) => s.clone(),
    }
}
fn result_content(
    outcome: &zhir_core::tool::RuntimeToolOutcome,
    encode: fn(&Content, bool) -> Result<Value>,
) -> Result<Value> {
    let content = outcome_content(outcome);
    if content.iter().all(|c| matches!(c, Content::Text { .. })) {
        return Ok(json!(
            content
                .iter()
                .filter_map(Content::as_text)
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }
    Ok(json!(parts(&content, false, encode)?))
}
fn finish_request(
    protocol: Protocol,
    request: &ModelRequest,
    mut body: Value,
    mut tools: Vec<Value>,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Value> {
    for tool in &request.provider_tools {
        let encoded = extension
            .as_mut()
            .map(|e| e.encode_provider_tool(protocol, tool))
            .transpose()?
            .flatten()
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "no provider adapter for {}/{}",
                    tool.provider, tool.name
                ))
            })?;
        if !encoded.is_object() {
            return Err(Error::Invalid(
                "provider declaration must be an object".into(),
            ));
        }
        tools.push(encoded);
    }
    if !tools.is_empty() {
        body["tools"] = json!(tools);
        body["tool_choice"] = tool_choice(protocol, request, extension)?;
    }
    if let Some(value) = request.profile.generation.temperature {
        body["temperature"] = json!(value);
    }
    if let Some(value) = request.profile.generation.seed {
        body["seed"] = json!(value);
    }
    if let Some(value) = request.profile.generation.parallel_runtime_tools {
        protocol.adapter().parallel_tools(&mut body, value);
    }
    for (key, value) in request
        .profile
        .extensions
        .get(protocol.key())
        .into_iter()
        .flatten()
    {
        if ["input", "messages", "tools", "tool_choice", "system"].contains(&key.as_str()) {
            return Err(Error::Invalid(format!(
                "extra option overrides controlled field {key}"
            )));
        }
        match body.get_mut(key) {
            Some(existing) => merge_extra(existing, value, key)?,
            None => body[key] = value.clone(),
        }
    }
    Ok(body)
}
fn tool_choice(
    protocol: Protocol,
    request: &ModelRequest,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Value> {
    if let ToolChoice::ProviderTool { provider, name } = &request.tool_choice {
        let spec = request
            .provider_tools
            .iter()
            .find(|s| &s.provider == provider && &s.name == name)
            .ok_or_else(|| Error::Invalid("selected provider tool is unavailable".into()))?;
        return extension
            .as_mut()
            .map(|e| e.encode_provider_choice(protocol, spec))
            .transpose()?
            .flatten()
            .ok_or_else(|| {
                Error::Invalid("provider adapter does not encode explicit selection".into())
            });
    }
    protocol.adapter().choice(request)
}
fn merge_extra(encoded: &mut Value, extra: &Value, path: &str) -> Result<()> {
    if let (Some(encoded), Some(extra)) = (encoded.as_object_mut(), extra.as_object()) {
        for (key, value) in extra {
            if let Some(existing) = encoded.get_mut(key) {
                merge_extra(existing, value, &format!("{path}.{key}"))?;
            } else {
                encoded.insert(key.clone(), value.clone());
            }
        }
        Ok(())
    } else {
        Err(Error::Invalid(format!(
            "extra option overrides controlled field {path}"
        )))
    }
}
pub(crate) fn usage(protocol: Protocol, value: &Value) -> Usage {
    protocol.adapter().usage(value)
}
pub(crate) struct Decoded {
    output: Vec<Output>,
    replay: Value,
    pending: bool,
}
pub(crate) fn decode(
    protocol: Protocol,
    value: &Value,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<TurnOutput> {
    if let Some(error) = value.get("error").filter(|e| !e.is_null()) {
        return Err(protocol_error(error.to_string()));
    }
    let decoded = protocol.adapter().decode(value, extension)?;
    let pending = decoded.pending
        || decoded.output.iter().any(|output| match output {
            Output::ProviderToolCall { call } => matches!(
                call.status,
                ProviderToolStatus::Pending | ProviderToolStatus::Running
            ),
            _ => false,
        });
    Ok(TurnOutput {
        output: decoded.output,
        usage: usage(protocol, &value["usage"]),
        provider_turn_pending: pending,
        provider_data: json!({"protocol":protocol.key(),"response":decoded.replay}),
        model_id: value
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned),
        response_id: value.get("id").and_then(Value::as_str).map(str::to_owned),
        finish_reason: protocol
            .adapter()
            .finish_reason(value)
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}
fn parse_content(provider: &str, value: &Value) -> Result<Output> {
    Ok(
        match value.get("type").and_then(Value::as_str).unwrap_or("") {
            "text" | "output_text" => Output::text(text(value, "text")?),
            "refusal" => Output::text(text(value, "refusal")?),
            _ => Output::Content {
                content: Content::Opaque {
                    provider: provider.into(),
                    data: value.clone(),
                },
            },
        },
    )
}

fn outcome_content(
    outcome: &zhir_core::tool::RuntimeToolOutcome,
) -> std::borrow::Cow<'_, [Content]> {
    match outcome {
        zhir_core::tool::RuntimeToolOutcome::Failure { error } => {
            std::borrow::Cow::Owned(vec![Content::text(&error.message)])
        }
        _ => std::borrow::Cow::Borrowed(outcome.content()),
    }
}
