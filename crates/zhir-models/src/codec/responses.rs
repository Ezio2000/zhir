use super::*;
use std::collections::HashSet;

pub(super) fn encode(
    model: &str,
    request: &ModelRequest,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Value> {
    let mut messages = Vec::new();
    let mut freeform = HashSet::new();
    for message in &request.messages {
        match message {
            Message::DelegationResult { .. } => return Err(Error::Invalid("protocol does not accept delegation history".into())),
            Message::System { content: values } => messages.push(json!({"role":"system","content":parts(values,false,content)?})),
            Message::User { content: values } | Message::External { content: values } => messages.push(json!({"role":"user","content":parts(values,false,content)?})),
            Message::Assistant { output, provider_data } => {
                for item in output {
                    if let Output::RuntimeToolCall { call } = item && matches!(call.input, RuntimeToolInput::Freeform(_)) { freeform.insert(call.id.as_str()); }
                }
                let encoded = match assistant_replay(Protocol::Responses, output, provider_data, extension)? {
                    Some(values) => values, None => assistant(output, extension)?,
                };
                messages.extend(encoded);
            }
            Message::RuntimeTool { call_id, outcome, .. } => messages.push(json!({
                "type":if freeform.contains(call_id.as_str()) {"custom_tool_call_output"} else {"function_call_output"},
                "call_id":call_id,"output":result_content(outcome,content)?
            })),
        }
    }
    let tools = request.runtime_tools.iter().map(tool).collect();
    let mut body = json!({"model":model,"stream":request.stream,"input":messages});
    if let Some(n) = request.profile.generation.max_output_tokens {
        body["max_output_tokens"] = json!(n);
    }
    if let Some(format) = &request.response_format {
        let format = match format {
            ResponseFormat::Json => json!({"type":"json_object"}),
            ResponseFormat::Schema { name, schema } => {
                json!({"type":"json_schema","name":name,"schema":schema,"strict":true})
            }
        };
        body["text"] = json!({"format":format});
    }
    finish_request(Protocol::Responses, request, body, tools, extension)
}
fn tool(spec: &zhir_core::tool::RuntimeToolSpec) -> Value {
    match &spec.input {
        InputSpec::Structured { schema } => {
            json!({"type":"function","name":spec.name,"description":spec.description,"parameters":schema,"strict":false})
        }
        InputSpec::Freeform { format } => {
            let mut value =
                json!({"type":"custom","name":spec.name,"description":spec.description});
            if let Some(format) = format {
                value["format"] = format.clone();
            }
            value
        }
    }
}
fn assistant(
    output: &[Output],
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Vec<Value>> {
    let mut values = Vec::new();
    for item in output {
        match item {
            Output::Delegation { .. } => return Err(Error::Invalid("protocol does not accept delegation calls".into())),
            Output::Content { content: value } => values.push(json!({"type":"message","role":"assistant","content":[content(value,true)?]})),
            Output::RuntimeToolCall { call } => values.push(match &call.input {
                RuntimeToolInput::Structured(_) => json!({"type":"function_call","call_id":call.id,"name":call.name,"arguments":call_input(call)}),
                RuntimeToolInput::Freeform(input) => json!({"type":"custom_tool_call","call_id":call.id,"name":call.name,"input":input}),
            }),
            Output::ProviderToolCall { call } => values.extend(provider_history(Protocol::Responses, call, extension)?),
        }
    }
    Ok(values)
}
fn content(value: &Content, output: bool) -> Result<Value> {
    Ok(match value {
        Content::Text { text } => {
            json!({"type":if output {"output_text"} else {"input_text"},"text":text})
        }
        Content::Opaque { data, .. } => data.clone(),
        Content::Resource { input } => {
            let resource = &input.resource;
            let mut value = match resource.modality() {
                "image" => {
                    let mut value = json!({"type":"input_image","image_url":data_url(resource)?});
                    if let Some(detail) = fidelity(input) {
                        value["detail"] = json!(detail);
                    }
                    value
                }
                "file" => match &resource.source {
                    ResourceSource::Url { url } => json!({"type":"input_file","file_url":url}),
                    ResourceSource::Provider { reference, .. } => {
                        json!({"type":"input_file","file_id":reference})
                    }
                    _ => {
                        json!({"type":"input_file","filename":resource.name,"file_data":data_url(resource)?})
                    }
                },
                _ => return Err(Error::Invalid("unsupported resource modality".into())),
            };
            resource_extensions(input, "responses", &mut value)?;
            value
        }
    })
}

pub(super) fn choice(
    choice: &ToolChoice,
    tools: &[zhir_core::tool::RuntimeToolSpec],
) -> Result<Value> {
    Ok(match choice {
        ToolChoice::Auto => json!("auto"),
        ToolChoice::None => json!("none"),
        ToolChoice::Required => json!("required"),
        ToolChoice::RuntimeTool { name } => {
            let freeform = tools
                .iter()
                .any(|t| &t.name == name && matches!(t.input, InputSpec::Freeform { .. }));
            json!({"type":if freeform {"custom"} else {"function"},"name":name})
        }
        ToolChoice::ProviderTool { .. } => {
            return Err(Error::Invalid("unsupported tool selection".into()));
        }
    })
}
pub(super) fn decode(
    value: &Value,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Decoded> {
    decode_items(Protocol::Responses, "output", value, extension, decode_item)
}
fn decode_item(item: &Value) -> Result<Vec<Output>> {
    let output = match text(item, "type")?.as_str() {
        "message" => {
            return item
                .get("content")
                .and_then(Value::as_array)
                .ok_or_else(|| protocol_error("missing message content"))?
                .iter()
                .map(|p| parse_content("openai", p))
                .collect();
        }
        "function_call" => Output::RuntimeToolCall {
            call: RuntimeToolCall {
                id: text(item, "call_id")?,
                name: text(item, "name")?,
                input: RuntimeToolInput::Structured(
                    serde_json::from_str(&text(item, "arguments")?)
                        .map_err(|e| protocol_error(e.to_string()))?,
                ),
            },
        },
        "custom_tool_call" => Output::RuntimeToolCall {
            call: RuntimeToolCall {
                id: text(item, "call_id")?,
                name: text(item, "name")?,
                input: RuntimeToolInput::Freeform(text(item, "input")?),
            },
        },
        "reasoning" | "compaction" => Output::Content {
            content: Content::Opaque {
                provider: "openai".into(),
                data: item.clone(),
            },
        },
        kind => {
            return Err(protocol_error(format!(
                "unmapped execution or output item: {kind}"
            )));
        }
    };
    Ok(vec![output])
}

pub(super) struct Adapter;
impl ProtocolAdapter for Adapter {
    fn key(&self) -> &'static str {
        "responses"
    }
    fn endpoint(&self) -> &'static str {
        "responses"
    }
    fn capabilities(&self) -> zhir_core::model::CapabilitySet {
        use zhir_core::model::Capability::*;
        adapter::capabilities(
            &[StructuredOutput, JsonMode, ProviderTools, FreeformTools],
            false,
            true,
        )
    }
    fn encode(
        &self,
        model: &str,
        request: &ModelRequest,
        extension: &mut Option<Box<dyn ProtocolExtension>>,
    ) -> Result<Value> {
        encode(model, request, extension)
    }
    fn validate_output_item(&self, item: &Value) -> Result<()> {
        text(item, "type").map(|_| ())
    }
    fn decode(
        &self,
        value: &Value,
        extension: &mut Option<Box<dyn ProtocolExtension>>,
    ) -> Result<Decoded> {
        decode(value, extension)
    }
    fn choice(&self, request: &ModelRequest) -> Result<Value> {
        choice(&request.tool_choice, &request.runtime_tools)
    }
    fn usage(&self, value: &Value) -> Usage {
        UsageFields {
            input: "input_tokens",
            output: "output_tokens",
            reasoning: "/output_tokens_details/reasoning_tokens",
            cached: "/input_tokens_details/cached_tokens",
            input_excludes_cache: false,
        }
        .read(value)
    }
    fn replay_items<'a>(&self, response: &'a Value) -> Option<&'a [Value]> {
        response
            .get("output")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
    }
    fn finish_reason<'a>(&self, response: &'a Value) -> Option<&'a Value> {
        response.get("status")
    }
    fn stream(&self) -> Box<dyn crate::streaming::StreamState> {
        Box::new(crate::streaming::responses::State::default())
    }
}
