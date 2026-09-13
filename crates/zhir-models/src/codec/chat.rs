use super::*;

pub(super) fn encode(
    model: &str,
    request: &ModelRequest,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Value> {
    let mut messages = Vec::new();
    for message in &request.messages {
        messages.extend(encode_message(message, extension)?);
    }
    let tools = request.runtime_tools.iter().map(|spec| {
        let InputSpec::Structured { schema } = &spec.input else { return Err(Error::Invalid("freeform tool unsupported by protocol".into())); };
        Ok(json!({"type":"function","function":{"name":spec.name,"description":spec.description,"parameters":schema}}))
    }).collect::<Result<Vec<_>>>()?;
    let mut body = json!({"model":model,"stream":request.stream,"messages":messages});
    if request.stream {
        body["stream_options"] = json!({"include_usage":true});
    }
    if let Some(n) = request.profile.generation.max_output_tokens {
        body["max_completion_tokens"] = json!(n);
    }
    if let Some(format) = &request.response_format {
        body["response_format"] = match format {
            ResponseFormat::Json => json!({"type":"json_object"}),
            ResponseFormat::Schema { name, schema } => {
                json!({"type":"json_schema","json_schema":{"name":name,"schema":schema,"strict":true}})
            }
        };
    }
    finish_request(Protocol::Chat, request, body, tools, extension)
}
fn encode_message(
    message: &Message,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Vec<Value>> {
    let value = match message {
        Message::System { content: values } => {
            json!({"role":"system","content":parts(values,false,content)?})
        }
        Message::User { content: values } | Message::External { content: values } => {
            json!({"role":"user","content":parts(values,false,content)?})
        }
        Message::Assistant {
            output,
            provider_data,
        } => {
            return match assistant_replay(Protocol::Chat, output, provider_data, extension)? {
                Some(values) => Ok(values),
                None => assistant(output, extension),
            };
        }
        Message::RuntimeTool {
            call_id, outcome, ..
        } => {
            json!({"role":"tool","tool_call_id":call_id,"content":result_content(outcome,content)?})
        }
    };
    Ok(vec![value])
}
fn assistant(
    output: &[Output],
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Vec<Value>> {
    let mut contents = Vec::new();
    let mut calls = Vec::new();
    let mut values = Vec::new();
    for item in output {
        match item {
            Output::Content { content: value } => contents.push(content(value, true)?),
            Output::RuntimeToolCall { call } => calls.push(json!({"id":call.id,"type":"function","function":{"name":call.name,"arguments":call_input(call)}})),
            Output::ProviderToolCall { call } => values.extend(provider_history(Protocol::Chat, call, extension)?),
        }
    }
    let mut message = json!({"role":"assistant","content":contents});
    if !calls.is_empty() {
        message["tool_calls"] = json!(calls);
    }
    values.push(message);
    Ok(values)
}
fn content(value: &Content, _: bool) -> Result<Value> {
    Ok(match value {
        Content::Text { text } => json!({"type":"text","text":text}),
        Content::Opaque { data, .. } => data.clone(),
        Content::Resource { input } => {
            let resource = &input.resource;
            let mut value = match resource.modality() {
                "image" => {
                    let mut image = json!({"url":data_url(resource)?});
                    if let Some(detail) = fidelity(input) {
                        image["detail"] = json!(detail);
                    }
                    json!({"type":"image_url","image_url":image})
                }
                "file" => {
                    json!({"type":"file","file":{"filename":resource.name,"file_data":data_url(resource)?}})
                }
                "audio" => {
                    let ResourceSource::Inline { bytes } = &resource.source else {
                        return Err(Error::Invalid(
                            "chat audio requires inline binary input".into(),
                        ));
                    };
                    json!({"type":"input_audio","input_audio":{"data":base64::engine::general_purpose::STANDARD.encode(bytes),"format":resource.media_type.split('/').next_back().unwrap_or("wav")}})
                }
                _ => return Err(Error::Invalid("unsupported resource modality".into())),
            };
            resource_extensions(input, "chat", &mut value)?;
            value
        }
    })
}

pub(super) fn choice(choice: &ToolChoice) -> Result<Value> {
    Ok(match choice {
        ToolChoice::Auto => json!("auto"),
        ToolChoice::None => json!("none"),
        ToolChoice::Required => json!("required"),
        ToolChoice::RuntimeTool { name } => json!({"type":"function","function":{"name":name}}),
        ToolChoice::ProviderTool { .. } => {
            return Err(Error::Invalid("unsupported tool selection".into()));
        }
    })
}
pub(super) fn decode(value: &Value) -> Result<Decoded> {
    let message = value
        .pointer("/choices/0/message")
        .ok_or_else(|| protocol_error("missing chat message"))?;
    let mut output = Vec::new();
    if let Some(reasoning) = message.get("reasoning_content").and_then(Value::as_str) {
        output.push(Output::Content {
            content: Content::Opaque {
                provider: "openai".into(),
                data: json!({"type":"reasoning","text":reasoning}),
            },
        });
    }
    match message.get("content") {
        Some(Value::String(text)) => output.push(Output::text(text)),
        Some(Value::Array(parts)) => {
            for part in parts {
                output.push(parse_content("openai", part)?);
            }
        }
        _ => {}
    }
    if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
        for call in calls {
            let function = call
                .get("function")
                .ok_or_else(|| protocol_error("missing function"))?;
            output.push(Output::RuntimeToolCall {
                call: RuntimeToolCall {
                    id: text(call, "id")?,
                    name: text(function, "name")?,
                    input: RuntimeToolInput::Structured(
                        serde_json::from_str(&text(function, "arguments")?)
                            .map_err(|e| protocol_error(e.to_string()))?,
                    ),
                },
            });
        }
    }
    Ok(Decoded {
        output,
        replay: value.clone(),
        pending: false,
    })
}
