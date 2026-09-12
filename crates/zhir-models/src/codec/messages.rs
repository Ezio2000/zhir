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
            Message::System { content: values } => system.extend(parts(values,false,content)?),
            Message::User { content: values } | Message::External { content: values } => messages.push(json!({"role":"user","content":parts(values,false,content)?})),
            Message::Assistant { output, provider_data } => {
                let encoded = match assistant_replay(Protocol::Messages, output, provider_data, extension)? {
                    Some(values) => values, None => assistant(output, extension)?,
                };
                messages.push(json!({"role":"assistant","content":encoded}));
            }
            Message::RuntimeTool { call_id, outcome, .. } => results.push(json!({"type":"tool_result","tool_use_id":call_id,
                "content":parts(&outcome.content(),false,content)?,"is_error":matches!(outcome,zhir_core::tool::RuntimeToolOutcome::Failure{..})})),
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
    let mut body = json!({"model":model,"stream":request.stream,"messages":messages,"max_tokens":request.options.max_output_tokens.unwrap_or(4096)});
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
        Content::Image { source } => json!({"type":"image","source":media_source(source)?}),
        Content::File { source, name } => {
            json!({"type":"document","source":media_source(source)?,"title":name})
        }
        Content::Opaque { data, .. } => data.clone(),
        _ => {
            return Err(Error::Invalid(
                "unsupported media for selected protocol".into(),
            ));
        }
    })
}
fn media_source(source: &MediaSource) -> Result<Value> {
    match source {
        MediaSource::Url { url } => Ok(json!({"type":"url","url":url})),
        MediaSource::Inline { mime_type, base64 } => {
            Ok(json!({"type":"base64","media_type":mime_type,"data":base64}))
        }
        MediaSource::Artifact { .. } => Err(Error::Invalid(
            "resolve artifact before model invocation".into(),
        )),
    }
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
