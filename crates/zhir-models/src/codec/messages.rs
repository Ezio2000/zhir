use super::*;

pub(super) fn encode(
    model: &str,
    request: &ModelRequest,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Value> {
    let mut messages = Vec::new();
    let mut results = Vec::new();
    let mut system = Vec::new();
    for message in &request.messages {
        if !matches!(message, Message::RuntimeTool { .. }) {
            flush_results(&mut messages, &mut results);
        }
        match message {
            Message::DelegationResult { .. } => return Err(Error::Invalid("protocol does not accept delegation history".into())),
            Message::System { content: values } => system.extend(parts(values,false,content)?),
            Message::User { content: values } | Message::External { content: values } => messages.push(json!({"role":"user","content":parts(values,false,content)?})),
            Message::Assistant { output, provider_data } => {
                let encoded = match assistant_replay(Protocol::Messages, output, provider_data, extension)? {
                    Some(values) => values, None => assistant(output, extension)?,
                };
                messages.push(json!({"role":"assistant","content":encoded}));
            }
            Message::RuntimeTool { call_id, outcome, .. } => results.push(json!({"type":"tool_result","tool_use_id":call_id,
                "content":parts(&outcome_content(outcome),false,content)?,"is_error":matches!(outcome,zhir_core::operation::OperationOutcome::Failure{..})})),
        }
    }
    flush_results(&mut messages, &mut results);
    let tools = request
        .runtime_tools
        .iter()
        .map(|spec| {
            let InputSpec::Structured { schema } = &spec.input else {
                return Err(Error::Invalid(
                    "freeform tool unsupported by protocol".into(),
                ));
            };
            Ok(json!({"name":spec.name,"description":spec.description,"input_schema":schema}))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut body = json!({"model":model,"stream":request.stream,"messages":messages,"max_tokens":request.profile.generation.max_output_tokens.unwrap_or(4096)});
    if !system.is_empty() {
        body["system"] = json!(system);
    }
    if let Some(format) = &request.response_format {
        let ResponseFormat::Schema { schema, .. } = format else {
            return Err(Error::Invalid("JSON mode not available".into()));
        };
        body["output_config"] = json!({"format":{"type":"json_schema","schema":schema}});
    }
    finish_request(Protocol::Messages, request, body, tools, extension)
}
fn flush_results(messages: &mut Vec<Value>, results: &mut Vec<Value>) {
    if !results.is_empty() {
        messages.push(json!({"role":"user","content":std::mem::take(results)}));
    }
}
fn assistant(
    output: &[Output],
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Vec<Value>> {
    let mut values = Vec::new();
    for item in output {
        match item {
            Output::Delegation { .. } => {
                return Err(Error::Invalid(
                    "protocol does not accept delegation calls".into(),
                ));
            }
            Output::Content { content: value } => values.push(content(value, true)?),
            Output::RuntimeToolCall { call } => {
                let RuntimeToolInput::Structured(input) = &call.input else {
                    return Err(Error::Invalid("Anthropic requires structured tools".into()));
                };
                values.push(json!({"type":"tool_use","id":call.id,"name":call.name,"input":input}));
            }
            Output::ProviderToolCall { call } => {
                values.extend(provider_history(Protocol::Messages, call, extension)?)
            }
        }
    }
    Ok(values)
}
fn content(value: &Content, _: bool) -> Result<Value> {
    Ok(match value {
        Content::Text { text } => json!({"type":"text","text":text}),
        Content::Opaque { data, .. } => data.clone(),
        Content::Resource { input } => {
            let resource = &input.resource;
            let source = match &resource.source {
                ResourceSource::Url { url } => json!({"type":"url","url":url}),
                ResourceSource::Inline { bytes } => {
                    json!({"type":"base64","media_type":resource.media_type,"data":base64::engine::general_purpose::STANDARD.encode(bytes)})
                }
                _ => {
                    return Err(Error::Invalid(
                        "resource must be resolved before encoding".into(),
                    ));
                }
            };
            let mut value = match resource.modality() {
                "image" => json!({"type":"image","source":source}),
                "file" => json!({"type":"document","source":source,"title":resource.name}),
                _ => return Err(Error::Invalid("unsupported resource modality".into())),
            };
            resource_extensions(input, "messages", &mut value)?;
            value
        }
    })
}

pub(super) fn choice(choice: &ToolChoice) -> Result<Value> {
    Ok(match choice {
        ToolChoice::Auto => json!({"type":"auto"}),
        ToolChoice::None => json!({"type":"none"}),
        ToolChoice::Required => json!({"type":"any"}),
        ToolChoice::RuntimeTool { name } => json!({"type":"tool","name":name}),
        ToolChoice::ProviderTool { .. } => {
            return Err(Error::Invalid("unsupported tool selection".into()));
        }
    })
}
pub(super) fn decode(
    value: &Value,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Decoded> {
    let mut decoded = decode_items(Protocol::Messages, "content", value, extension, decode_item)?;
    decoded.pending = value.get("stop_reason").and_then(Value::as_str) == Some("pause_turn");
    Ok(decoded)
}
fn decode_item(item: &Value) -> Result<Vec<Output>> {
    let output = match item.get("type").and_then(Value::as_str).unwrap_or("") {
        "tool_use" => Output::RuntimeToolCall {
            call: RuntimeToolCall {
                id: text(item, "id")?,
                name: text(item, "name")?,
                input: RuntimeToolInput::Structured(
                    item.get("input")
                        .cloned()
                        .ok_or_else(|| protocol_error("missing tool input"))?,
                ),
            },
        },
        "text" | "thinking" | "redacted_thinking" | "image" | "document" => {
            parse_content("anthropic", item)?
        }
        kind if kind == "server_tool_use" || kind.ends_with("_tool_result") => {
            return Err(protocol_error(format!("unmapped execution item: {kind}")));
        }
        kind => {
            return Err(protocol_error(format!(
                "unmapped execution or content item: {kind}"
            )));
        }
    };
    Ok(vec![output])
}

pub(super) struct Adapter;
impl ProtocolAdapter for Adapter {
    fn key(&self) -> &'static str {
        "messages"
    }
    fn endpoint(&self) -> &'static str {
        "messages"
    }
    fn capabilities(&self) -> zhir_core::model::CapabilitySet {
        use zhir_core::model::Capability::*;
        adapter::capabilities(&[StructuredOutput, ProviderTools], false, true)
    }
    fn encode(
        &self,
        model: &str,
        request: &ModelRequest,
        extension: &mut Option<Box<dyn ProtocolExtension>>,
    ) -> Result<Value> {
        encode(model, request, extension)
    }
    fn decode(
        &self,
        value: &Value,
        extension: &mut Option<Box<dyn ProtocolExtension>>,
    ) -> Result<Decoded> {
        decode(value, extension)
    }
    fn choice(&self, request: &ModelRequest) -> Result<Value> {
        choice(&request.tool_choice)
    }
    fn usage(&self, value: &Value) -> Usage {
        UsageFields {
            input: "input_tokens",
            output: "output_tokens",
            reasoning: "/output_tokens_details/reasoning_tokens",
            cached: "/input_tokens_details/cached_tokens",
            input_excludes_cache: true,
        }
        .read(value)
    }
    fn replay_items<'a>(&self, response: &'a Value) -> Option<&'a [Value]> {
        response
            .get("content")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
    }
    fn finish_reason<'a>(&self, response: &'a Value) -> Option<&'a Value> {
        response.get("stop_reason")
    }
    fn stream(&self) -> Box<dyn crate::streaming::StreamState> {
        Box::new(crate::streaming::messages::State::new())
    }

    fn supports_fidelity(&self) -> bool {
        false
    }
    fn authorize(
        &self,
        builder: reqwest::RequestBuilder,
        credential: &zhir_core::credential::Credential,
    ) -> reqwest::RequestBuilder {
        builder
            .header("x-api-key", &credential.value)
            .header("anthropic-version", "2023-06-01")
    }
    fn parallel_tools(&self, body: &mut Value, parallel: bool) {
        if body.get("tool_choice").is_some() {
            body["tool_choice"]["disable_parallel_tool_use"] = json!(!parallel);
        }
    }
}
