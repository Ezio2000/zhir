use crate::{Protocol, ProtocolExtension};
use serde_json::{Value, json};
use zhir_core::{
    Result,
    error::Error,
    message::{Content, MediaSource, Message, Output, ProviderToolCall, ProviderToolStatus},
    model::{ModelRequest, ModelResponse, ResponseFormat, ToolChoice, Usage},
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
fn data_url(source: &MediaSource) -> Result<String> {
    match source {
        MediaSource::Url { url } => Ok(url.clone()),
        MediaSource::Inline { mime_type, base64 } => {
            Ok(format!("data:{mime_type};base64,{base64}"))
        }
        MediaSource::Artifact { .. } => Err(Error::Invalid(
            "host must resolve artifact references before model invocation".into(),
        )),
    }
}
fn content(protocol: Protocol, c: &Content, output: bool) -> Result<Value> {
    Ok(match c {
        Content::Text { text } => {
            json!({"type":if protocol==Protocol::Responses {if output {"output_text"} else {"input_text"}} else {"text"},"text":text})
        }
        Content::Image { source } => match protocol {
            Protocol::Chat => json!({"type":"image_url","image_url":{"url":data_url(source)?}}),
            Protocol::Responses => json!({"type":"input_image","image_url":data_url(source)?}),
            Protocol::Messages => json!({"type":"image","source":anthropic_source(source)?}),
        },
        Content::File { source, name } => match protocol {
            Protocol::Chat => {
                json!({"type":"file","file":{"filename":name,"file_data":data_url(source)?}})
            }
            Protocol::Responses => match source {
                MediaSource::Url { url } => json!({"type":"input_file","file_url":url}),
                _ => json!({"type":"input_file","filename":name,"file_data":data_url(source)?}),
            },
            Protocol::Messages => {
                json!({"type":"document","source":anthropic_source(source)?,"title":name})
            }
        },
        Content::Audio {
            source: MediaSource::Inline { mime_type, base64 },
        } if protocol == Protocol::Chat => {
            json!({"type":"input_audio","input_audio":{"data":base64,"format":mime_type.split('/').next_back().unwrap_or("wav")}})
        }
        Content::Opaque { data, .. } => data.clone(),
        _ => {
            return Err(Error::Invalid(
                "unsupported media for selected protocol".into(),
            ));
        }
    })
}
fn anthropic_source(source: &MediaSource) -> Result<Value> {
    Ok(match source {
        MediaSource::Url { url } => json!({"type":"url","url":url}),
        MediaSource::Inline { mime_type, base64 } => {
            json!({"type":"base64","media_type":mime_type,"data":base64})
        }
        MediaSource::Artifact { .. } => {
            return Err(Error::Invalid(
                "resolve artifact before model invocation".into(),
            ));
        }
    })
}
fn parts(protocol: Protocol, values: &[Content], output: bool) -> Result<Vec<Value>> {
    values
        .iter()
        .map(|v| content(protocol, v, output))
        .collect()
}
fn call_input(call: &RuntimeToolCall) -> String {
    match &call.input {
        RuntimeToolInput::Structured(v) => v.to_string(),
        RuntimeToolInput::Freeform(s) => s.clone(),
    }
}
fn assistant(
    protocol: Protocol,
    output: &[Output],
    provider_data: &Value,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Vec<Value>> {
    let key = match protocol {
        Protocol::Chat => "chat",
        Protocol::Responses => "responses",
        Protocol::Messages => "messages",
    };
    if provider_data.get("protocol").and_then(Value::as_str) == Some(key) {
        let response = &provider_data["response"];
        let raw = match protocol {
            Protocol::Chat => response
                .pointer("/choices/0/message")
                .map(|v| vec![v.clone()]),
            Protocol::Responses => response.get("output").and_then(Value::as_array).cloned(),
            Protocol::Messages => response.get("content").and_then(Value::as_array).cloned(),
        };
        if let Some(raw) = raw {
            let mut values = Vec::new();
            let mut emitted = std::collections::HashSet::new();
            for item in raw {
                if let Some(ids) = item.get("$zhir_provider_calls") {
                    if item.as_object().is_none_or(|object| object.len() != 1) {
                        return Err(protocol_error("invalid provider replay position"));
                    }
                    for id in ids.as_array().ok_or_else(|| {
                        protocol_error("provider replay mapping must contain call ids")
                    })? {
                        let id = id.as_str().ok_or_else(|| {
                            protocol_error("provider replay call id must be a string")
                        })?;
                        let call = output
                            .iter()
                            .find_map(|output| match output {
                                Output::ProviderToolCall { call } if call.id == id => Some(call),
                                _ => None,
                            })
                            .ok_or_else(|| {
                                protocol_error("provider replay mapping references an absent call")
                            })?;
                        if !emitted.insert(call.id.clone()) {
                            return Err(protocol_error(
                                "provider call has multiple native replay positions",
                            ));
                        }
                        values.extend(provider_history(protocol, call, extension)?);
                    }
                } else {
                    values.push(item);
                }
            }
            if output.iter().any(|output| matches!(output, Output::ProviderToolCall {call} if !emitted.contains(&call.id))) {
                return Err(protocol_error("provider call has no native replay position"));
            }
            return Ok(values);
        }
    }
    let mut values = Vec::new();
    let mut chat_content = Vec::new();
    let mut chat_calls = Vec::new();
    for item in output {
        match item {
            Output::Content {content:c}=>{let p=content(protocol,c,true)?;match protocol {Protocol::Chat=>chat_content.push(p),Protocol::Responses=>values.push(json!({"type":"message","role":"assistant","content":[p]})),Protocol::Messages=>values.push(p)}},
            Output::RuntimeToolCall {call}=>match protocol {
                Protocol::Chat=>chat_calls.push(json!({"id":call.id,"type":"function","function":{"name":call.name,"arguments":call_input(call)}})),
                Protocol::Responses=>values.push(match &call.input {RuntimeToolInput::Structured(_)=>json!({"type":"function_call","call_id":call.id,"name":call.name,"arguments":call_input(call)}),RuntimeToolInput::Freeform(s)=>json!({"type":"custom_tool_call","call_id":call.id,"name":call.name,"input":s})}),
                Protocol::Messages=>{let RuntimeToolInput::Structured(input)=&call.input else {return Err(Error::Invalid("Anthropic requires structured tools".into()));};values.push(json!({"type":"tool_use","id":call.id,"name":call.name,"input":input}));},
            },
            Output::ProviderToolCall {call}=>values.extend(provider_history(protocol, call, extension)?),
        }
    }
    if protocol == Protocol::Chat {
        let mut message = json!({"role":"assistant","content":chat_content});
        if !chat_calls.is_empty() {
            message["tool_calls"] = json!(chat_calls);
        }
        values.push(message);
    }
    Ok(values)
}
pub(crate) fn encode(
    protocol: Protocol,
    model: &str,
    r: &ModelRequest,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Value> {
    let mut messages = Vec::new();
    let mut tool_results = Vec::new();
    let mut system = Vec::new();
    let mut freeform_calls = std::collections::HashSet::new();
    for message in &r.messages {
        if !matches!(message, Message::RuntimeTool { .. }) {
            flush_tool_results(&mut messages, &mut tool_results);
        }
        match message {
            Message::System { content } if protocol == Protocol::Messages => {
                system.extend(parts(protocol, content, false)?)
            }
            Message::System { content }
            | Message::User { content }
            | Message::External { content } => {
                let role = if matches!(message, Message::System { .. }) {
                    "system"
                } else {
                    "user"
                };
                messages.push(json!({"role":role,"content":parts(protocol,content,false)?}));
            }
            Message::Assistant {
                output,
                provider_data,
            } => {
                for item in output {
                    if let Output::RuntimeToolCall { call } = item
                        && matches!(call.input, RuntimeToolInput::Freeform(_))
                    {
                        freeform_calls.insert(call.id.as_str());
                    }
                }
                let encoded = assistant(protocol, output, provider_data, extension)?;
                if protocol == Protocol::Messages {
                    messages.push(json!({"role":"assistant","content":encoded}));
                } else {
                    messages.extend(encoded);
                }
            }
            Message::RuntimeTool {
                call_id, outcome, ..
            } => {
                let result = outcome.content();
                let result_text = result
                    .iter()
                    .filter_map(Content::as_text)
                    .collect::<Vec<_>>()
                    .join("\n");
                let tool_content = if protocol == Protocol::Messages
                    || result.iter().any(|c| !matches!(c, Content::Text { .. }))
                {
                    json!(parts(protocol, &result, false)?)
                } else {
                    json!(result_text)
                };
                if protocol == Protocol::Messages {
                    tool_results.push(json!({"type":"tool_result","tool_use_id":call_id,"content":tool_content,"is_error":outcome.kind()=="failure"}));
                    continue;
                }
                messages.push(match protocol {
                    Protocol::Chat => json!({"role":"tool","tool_call_id":call_id,"content":tool_content}),
                    Protocol::Responses => json!({
                        "type": if freeform_calls.contains(call_id.as_str()) { "custom_tool_call_output" } else { "function_call_output" },
                        "call_id": call_id, "output": tool_content
                    }),
                    Protocol::Messages => unreachable!("tool results are grouped above"),
                });
            }
        }
    }
    flush_tool_results(&mut messages, &mut tool_results);
    let mut tools = Vec::new();
    for spec in &r.runtime_tools {
        match &spec.input {
            InputSpec::Structured {schema}=>tools.push(match protocol {Protocol::Chat=>json!({"type":"function","function":{"name":spec.name,"description":spec.description,"parameters":schema}}),Protocol::Responses=>json!({"type":"function","name":spec.name,"description":spec.description,"parameters":schema,"strict":false}),Protocol::Messages=>json!({"name":spec.name,"description":spec.description,"input_schema":schema})}),
            InputSpec::Freeform {format} if protocol==Protocol::Responses=>{let mut v=json!({"type":"custom","name":spec.name,"description":spec.description});if let Some(format)=format {v["format"]=format.clone();}tools.push(v);},
            _=>return Err(Error::Invalid("freeform tool unsupported by protocol".into())),
        }
    }
    for tool in &r.provider_tools {
        let encoded = match extension.as_mut() {
            Some(extension) => extension.encode_provider_tool(protocol, tool)?,
            None => None,
        }
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
    let mut body = json!({"model":model,"stream":r.stream});
    body[if protocol == Protocol::Responses {
        "input"
    } else {
        "messages"
    }] = json!(messages);
    if protocol == Protocol::Messages {
        body["max_tokens"] = json!(r.options.max_output_tokens.unwrap_or(4096));
        if !system.is_empty() {
            body["system"] = json!(system);
        }
    }
    if !tools.is_empty() {
        body["tools"] = json!(tools);
        body["tool_choice"] = match &r.tool_choice {
            ToolChoice::ProviderTool { provider, name } => {
                let spec = r
                    .provider_tools
                    .iter()
                    .find(|spec| &spec.provider == provider && &spec.name == name)
                    .ok_or_else(|| {
                        Error::Invalid("selected provider tool is unavailable".into())
                    })?;
                match extension.as_mut() {
                    Some(extension) => extension.encode_provider_choice(protocol, spec)?,
                    None => None,
                }
                .ok_or_else(|| {
                    Error::Invalid("provider adapter does not encode explicit selection".into())
                })?
            }
            choice_value => choice(protocol, choice_value, &r.runtime_tools)?,
        };
    }
    if let Some(t) = r.options.temperature {
        body["temperature"] = json!(t);
    }
    if let Some(n) = r.options.max_output_tokens {
        body[match protocol {
            Protocol::Chat => "max_completion_tokens",
            Protocol::Responses => "max_output_tokens",
            Protocol::Messages => "max_tokens",
        }] = json!(n);
    }
    if let Some(seed) = r.options.seed {
        body["seed"] = json!(seed);
    }
    if let Some(parallel) = r.options.parallel_runtime_tools {
        if protocol == Protocol::Messages {
            if body.get("tool_choice").is_some() {
                body["tool_choice"]["disable_parallel_tool_use"] = json!(!parallel);
            }
        } else {
            body["parallel_tool_calls"] = json!(parallel);
        }
    }
    if protocol == Protocol::Chat && r.stream {
        body["stream_options"] = json!({"include_usage":true});
    }
    if let Some(format) = &r.response_format {
        let value = match format {
            ResponseFormat::Json => json!({"type":"json_object"}),
            ResponseFormat::Schema { name, schema } => match protocol {
                Protocol::Chat => {
                    json!({"type":"json_schema","json_schema":{"name":name,"schema":schema,"strict":true}})
                }
                _ => json!({"type":"json_schema","name":name,"schema":schema,"strict":true}),
            },
        };
        match protocol {
            Protocol::Chat => body["response_format"] = value,
            Protocol::Responses => body["text"] = json!({"format":value}),
            Protocol::Messages => {
                let ResponseFormat::Schema { schema, .. } = format else {
                    return Err(Error::Invalid("JSON mode not available".into()));
                };
                body["output_config"] = json!({"format":{"type":"json_schema","schema":schema}});
            }
        }
    }
    for (key, value) in &r.options.extra {
        if ["input", "messages", "tools", "tool_choice", "system"].contains(&key.as_str()) {
            return Err(Error::Invalid(format!(
                "extra option overrides controlled field {key}"
            )));
        }
        if let Some(existing) = body.get_mut(key) {
            merge_extra(existing, value, key)?;
        } else {
            body[key] = value.clone();
        }
    }
    Ok(body)
}
fn flush_tool_results(messages: &mut Vec<Value>, results: &mut Vec<Value>) {
    if !results.is_empty() {
        messages.push(json!({"role":"user","content":std::mem::take(results)}));
    }
}
// Extend nested option objects without replacing anything already encoded.
// Arrays and scalar values have no unambiguous additive merge operation.
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
fn choice(
    protocol: Protocol,
    choice: &ToolChoice,
    runtime_tools: &[zhir_core::tool::RuntimeToolSpec],
) -> Result<Value> {
    Ok(match (protocol, choice) {
        (Protocol::Messages, ToolChoice::Auto) => json!({"type":"auto"}),
        (Protocol::Messages, ToolChoice::None) => json!({"type":"none"}),
        (Protocol::Messages, ToolChoice::Required) => json!({"type":"any"}),
        (Protocol::Messages, ToolChoice::RuntimeTool { name }) => {
            json!({"type":"tool","name":name})
        }
        (_, ToolChoice::Auto) => json!("auto"),
        (_, ToolChoice::None) => json!("none"),
        (_, ToolChoice::Required) => json!("required"),
        (Protocol::Chat, ToolChoice::RuntimeTool { name }) => {
            json!({"type":"function","function":{"name":name}})
        }
        (Protocol::Responses, ToolChoice::RuntimeTool { name }) => {
            json!({"type":if runtime_tools.iter().any(|t|&t.name==name && matches!(t.input,InputSpec::Freeform {..})) {"custom"} else {"function"},"name":name})
        }
        _ => return Err(Error::Invalid("unsupported tool selection".into())),
    })
}
pub(crate) fn usage(protocol: Protocol, v: &Value) -> Usage {
    let mut input = v
        .get(if protocol == Protocol::Chat {
            "prompt_tokens"
        } else {
            "input_tokens"
        })
        .and_then(Value::as_u64);
    // Messages reports uncached input separately. Normalize to total processed
    // input, matching Chat/Responses; cache fields remain a breakdown of input.
    if protocol == Protocol::Messages {
        input = input.map(|tokens| {
            tokens
                .saturating_add(
                    v.get("cache_read_input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                )
                .saturating_add(
                    v.get("cache_creation_input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                )
        });
    }
    let output = v
        .get(if protocol == Protocol::Chat {
            "completion_tokens"
        } else {
            "output_tokens"
        })
        .and_then(Value::as_u64);
    Usage {
        input_tokens: input,
        output_tokens: output,
        total_tokens: v
            .get("total_tokens")
            .and_then(Value::as_u64)
            .or_else(|| input.zip(output).map(|(a, b)| a.saturating_add(b))),
        reasoning_tokens: v
            .pointer(if protocol == Protocol::Chat {
                "/completion_tokens_details/reasoning_tokens"
            } else {
                "/output_tokens_details/reasoning_tokens"
            })
            .and_then(Value::as_u64),
        cache_read_tokens: v
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
            .or_else(|| {
                v.pointer(if protocol == Protocol::Chat {
                    "/prompt_tokens_details/cached_tokens"
                } else {
                    "/input_tokens_details/cached_tokens"
                })
                .and_then(Value::as_u64)
            }),
        cache_write_tokens: v.get("cache_creation_input_tokens").and_then(Value::as_u64),
    }
}
pub(crate) fn decode(
    protocol: Protocol,
    value: &Value,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<ModelResponse> {
    if let Some(error) = value.get("error").filter(|e| !e.is_null()) {
        return Err(protocol_error(error.to_string()));
    }
    let mut output = Vec::new();
    let mut replay_response = value.clone();
    let mut pending = false;
    match protocol {
        Protocol::Chat => {
            let message = value
                .pointer("/choices/0/message")
                .ok_or_else(|| protocol_error("missing chat message"))?;
            if let Some(reasoning) = message.get("reasoning_content").and_then(Value::as_str) {
                output.push(Output::Content {
                    content: Content::Opaque {
                        provider: "openai".into(),
                        data: json!({"type":"reasoning","text":reasoning}),
                    },
                });
            }
            if let Some(content) = message.get("content") {
                if let Some(text) = content.as_str() {
                    output.push(Output::text(text));
                } else if let Some(parts) = content.as_array() {
                    for part in parts {
                        output.push(parse_content("openai", part)?);
                    }
                }
            }
            if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
                for c in calls {
                    let f = c
                        .get("function")
                        .ok_or_else(|| protocol_error("missing function"))?;
                    let arguments = text(f, "arguments")?;
                    output.push(Output::RuntimeToolCall {
                        call: RuntimeToolCall {
                            id: text(c, "id")?,
                            name: text(f, "name")?,
                            input: RuntimeToolInput::Structured(
                                serde_json::from_str(&arguments)
                                    .map_err(|e| protocol_error(e.to_string()))?,
                            ),
                        },
                    });
                }
            }
        }
        Protocol::Responses => {
            let items = value
                .get("output")
                .and_then(Value::as_array)
                .ok_or_else(|| protocol_error("missing Responses output"))?;
            for (native_index, item) in items.iter().enumerate() {
                let kind = text(item, "type")?;
                if let Some(mapped) = match extension.as_mut() {
                    Some(extension) => extension.decode_output_item(protocol, item, value)?,
                    None => None,
                } {
                    let ids = mapped
                        .iter()
                        .filter_map(|output| match output {
                            Output::ProviderToolCall { call } => Some(call.id.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    if mapped.is_empty() || !ids.is_empty() {
                        if !mapped.is_empty() && ids.len() != mapped.len() {
                            return Err(protocol_error(
                                "one native item cannot mix provider and other output ownership",
                            ));
                        }
                        let key = if protocol == Protocol::Responses {
                            "output"
                        } else {
                            "content"
                        };
                        replay_response[key][native_index] = json!({"$zhir_provider_calls":ids});
                    }
                    output.extend(mapped);
                    continue;
                }
                match kind.as_str() {
                    "message" => {
                        for p in item
                            .get("content")
                            .and_then(Value::as_array)
                            .ok_or_else(|| protocol_error("missing message content"))?
                        {
                            output.push(parse_content("openai", p)?);
                        }
                    }
                    "function_call" => {
                        let arguments = text(item, "arguments")?;
                        output.push(Output::RuntimeToolCall {
                            call: RuntimeToolCall {
                                id: text(item, "call_id")?,
                                name: text(item, "name")?,
                                input: RuntimeToolInput::Structured(
                                    serde_json::from_str(&arguments)
                                        .map_err(|e| protocol_error(e.to_string()))?,
                                ),
                            },
                        });
                    }
                    "custom_tool_call" => output.push(Output::RuntimeToolCall {
                        call: RuntimeToolCall {
                            id: text(item, "call_id")?,
                            name: text(item, "name")?,
                            input: RuntimeToolInput::Freeform(text(item, "input")?),
                        },
                    }),
                    "reasoning" | "compaction" => output.push(Output::Content {
                        content: Content::Opaque {
                            provider: "openai".into(),
                            data: item.clone(),
                        },
                    }),
                    kind => {
                        return Err(protocol_error(format!(
                            "unmapped execution or output item: {kind}"
                        )));
                    }
                }
            }
        }
        Protocol::Messages => {
            let items = value
                .get("content")
                .and_then(Value::as_array)
                .ok_or_else(|| protocol_error("missing Anthropic content"))?;
            pending = value.get("stop_reason").and_then(Value::as_str) == Some("pause_turn");
            for (native_index, item) in items.iter().enumerate() {
                if let Some(mapped) = match extension.as_mut() {
                    Some(extension) => extension.decode_output_item(protocol, item, value)?,
                    None => None,
                } {
                    let ids = mapped
                        .iter()
                        .filter_map(|output| match output {
                            Output::ProviderToolCall { call } => Some(call.id.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    if mapped.is_empty() || !ids.is_empty() {
                        if !mapped.is_empty() && ids.len() != mapped.len() {
                            return Err(protocol_error(
                                "one native item cannot mix provider and other output ownership",
                            ));
                        }
                        let key = if protocol == Protocol::Responses {
                            "output"
                        } else {
                            "content"
                        };
                        replay_response[key][native_index] = json!({"$zhir_provider_calls":ids});
                    }
                    output.extend(mapped);
                    continue;
                }
                match item.get("type").and_then(Value::as_str).unwrap_or("") {
                    "tool_use" => output.push(Output::RuntimeToolCall {
                        call: RuntimeToolCall {
                            id: text(item, "id")?,
                            name: text(item, "name")?,
                            input: RuntimeToolInput::Structured(
                                item.get("input")
                                    .cloned()
                                    .ok_or_else(|| protocol_error("missing tool input"))?,
                            ),
                        },
                    }),
                    k if k == "server_tool_use" || k.ends_with("_tool_result") => {
                        return Err(protocol_error(format!("unmapped execution item: {k}")));
                    }
                    "text" | "thinking" | "redacted_thinking" | "image" | "document" => {
                        output.push(parse_content("anthropic", item)?)
                    }
                    kind => {
                        return Err(protocol_error(format!(
                            "unmapped execution or content item: {kind}"
                        )));
                    }
                }
            }
        }
    };
    pending |= output.iter().any(|item| matches!(item, Output::ProviderToolCall {call} if matches!(call.status, ProviderToolStatus::Pending | ProviderToolStatus::Running)));
    Ok(ModelResponse {
        output,
        usage: usage(protocol, &value["usage"]),
        provider_turn_pending: pending,
        provider_data: json!({"protocol":match protocol {Protocol::Chat=>"chat",Protocol::Responses=>"responses",Protocol::Messages=>"messages"},"response":replay_response}),
        model_id: value
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned),
        response_id: value.get("id").and_then(Value::as_str).map(str::to_owned),
        finish_reason: match protocol {
            Protocol::Chat => value.pointer("/choices/0/finish_reason"),
            Protocol::Responses => value.get("status"),
            Protocol::Messages => value.get("stop_reason"),
        }
        .and_then(Value::as_str)
        .map(str::to_owned),
    })
}
fn parse_content(provider: &str, value: &Value) -> Result<Output> {
    let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
    Ok(match kind {
        "text" | "output_text" => Output::text(text(value, "text")?),
        "refusal" => Output::text(text(value, "refusal")?),
        _ => Output::Content {
            content: Content::Opaque {
                provider: provider.into(),
                data: value.clone(),
            },
        },
    })
}

fn provider_history(
    protocol: Protocol,
    call: &ProviderToolCall,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Vec<Value>> {
    match extension.as_mut() {
        Some(extension) => extension.encode_provider_history(protocol, call)?,
        None => None,
    }
    .ok_or_else(|| {
        Error::Invalid(format!(
            "no replay adapter for {}/{}",
            call.provider, call.name
        ))
    })
}
