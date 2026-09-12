use crate::{
    BoxFuture, Cancellation, Result,
    error::Error,
    message::{Message, Output, validate_output},
    run::RunContext,
    tool::RuntimeToolSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
}
impl Usage {
    pub fn add(&mut self, rhs: &Self) {
        fn add(a: &mut Option<u64>, b: Option<u64>) {
            if let Some(b) = b {
                *a = Some(a.unwrap_or(0).saturating_add(b));
            }
        }
        add(&mut self.input_tokens, rhs.input_tokens);
        add(&mut self.output_tokens, rhs.output_tokens);
        add(&mut self.total_tokens, rhs.total_tokens);
        add(&mut self.reasoning_tokens, rhs.reasoning_tokens);
        add(&mut self.cache_read_tokens, rhs.cache_read_tokens);
        add(&mut self.cache_write_tokens, rhs.cache_write_tokens);
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    pub input_modalities: Vec<String>,
    pub output_modalities: Vec<String>,
    pub structured_runtime_tools: bool,
    pub freeform_runtime_tools: bool,
    pub provider_tools: bool,
    pub parallel_runtime_tools: bool,
    pub parallel_control: bool,
    pub streaming: bool,
    pub usage: bool,
    pub structured_output: bool,
    pub json_mode: bool,
    pub seed: bool,
    pub tool_choices: Vec<String>,
}
impl Default for Capabilities {
    fn default() -> Self {
        Self {
            input_modalities: vec!["text".into()],
            output_modalities: vec!["text".into()],
            structured_runtime_tools: true,
            freeform_runtime_tools: false,
            provider_tools: false,
            parallel_runtime_tools: true,
            parallel_control: true,
            streaming: true,
            usage: true,
            structured_output: false,
            json_mode: false,
            seed: false,
            tool_choices: vec![
                "auto".into(),
                "none".into(),
                "required".into(),
                "runtime_tool".into(),
            ],
        }
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelOptions {
    #[serde(default, deserialize_with = "finite_option")]
    pub temperature: Option<f64>,
    pub max_output_tokens: Option<u64>,
    pub seed: Option<i64>,
    pub parallel_runtime_tools: Option<bool>,
    #[serde(default)]
    pub extra: serde_json::Map<String, Value>,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolChoice {
    #[default]
    Auto,
    None,
    Required,
    RuntimeTool {
        name: String,
    },
    ProviderTool {
        provider: String,
        name: String,
    },
}
impl ToolChoice {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::None => "none",
            Self::Required => "required",
            Self::RuntimeTool { .. } => "runtime_tool",
            Self::ProviderTool { .. } => "provider_tool",
        }
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderToolSpec {
    pub provider: String,
    pub name: String,
    #[serde(default)]
    pub options: Value,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResponseFormat {
    Json,
    Schema { name: String, schema: Value },
}
#[derive(Debug, Clone)]
pub struct ModelRequest {
    pub messages: Vec<Message>,
    pub runtime_tools: Vec<RuntimeToolSpec>,
    pub provider_tools: Vec<ProviderToolSpec>,
    pub options: ModelOptions,
    pub tool_choice: ToolChoice,
    pub response_format: Option<ResponseFormat>,
    pub stream: bool,
}
impl ModelRequest {
    pub fn validate(&self, c: &Capabilities) -> Result<()> {
        let invalid = |s: &str| Err(Error::Invalid(s.into()));
        if self.stream && !c.streaming {
            return invalid("model does not support streaming");
        }
        if !c.tool_choices.iter().any(|s| s == self.tool_choice.kind()) {
            return invalid("unsupported tool choice");
        }
        if self.options.parallel_runtime_tools == Some(false) && !c.parallel_control {
            return invalid("model cannot disable parallel tools");
        }
        if self.options.seed.is_some() && !c.seed {
            return invalid("unsupported seed");
        }
        if self.options.temperature.is_some_and(|n| !n.is_finite()) {
            return invalid("nonfinite temperature");
        }
        if !self.provider_tools.is_empty() && !c.provider_tools {
            return invalid("unsupported provider tools");
        }
        let mut provider_ids = std::collections::HashSet::new();
        for tool in &self.provider_tools {
            if tool.provider.is_empty()
                || tool.name.is_empty()
                || !provider_ids.insert((&tool.provider, &tool.name))
            {
                return invalid("provider tool declarations require unique nonempty identities");
            }
        }
        let mut runtime_names = std::collections::HashSet::new();
        for tool in &self.runtime_tools {
            if tool.name.is_empty() || !runtime_names.insert(&tool.name) {
                return invalid("runtime tool names must be nonempty and unique");
            }
            tool.execution.validate()?;
        }
        if matches!(self.response_format, Some(ResponseFormat::Schema { .. }))
            && !c.structured_output
        {
            return invalid("unsupported structured output");
        }
        if matches!(self.response_format, Some(ResponseFormat::Json)) && !c.json_mode {
            return invalid("unsupported JSON mode");
        }
        for t in &self.runtime_tools {
            match t.input {
                crate::tool::InputSpec::Structured { .. } if !c.structured_runtime_tools => {
                    return invalid("unsupported structured tools");
                }
                crate::tool::InputSpec::Freeform { .. } if !c.freeform_runtime_tools => {
                    return invalid("unsupported freeform tools");
                }
                _ => {}
            }
        }
        if let ToolChoice::RuntimeTool { name } = &self.tool_choice
            && !self.runtime_tools.iter().any(|s| &s.name == name)
        {
            return invalid("selected tool is unavailable");
        }
        if let ToolChoice::ProviderTool { provider, name } = &self.tool_choice
            && !self
                .provider_tools
                .iter()
                .any(|s| &s.provider == provider && &s.name == name)
        {
            return invalid("selected provider tool is unavailable");
        }
        for m in &self.messages {
            m.validate()?;
            let content = match m {
                Message::System { content }
                | Message::User { content }
                | Message::External { content } => content.clone(),
                // Provider outputs are replayed by their adapter; they are not native
                // media inputs to the model. Only ordinary assistant content is checked.
                Message::Assistant { output, .. } => output
                    .iter()
                    .filter_map(|item| {
                        if let Output::Content { content } = item {
                            Some(content.clone())
                        } else {
                            None
                        }
                    })
                    .collect(),
                Message::RuntimeTool { outcome, .. } => outcome.content(),
            };
            for part in content {
                if part
                    .modality()
                    .is_some_and(|modality| !c.input_modalities.iter().any(|v| v == modality))
                {
                    return invalid("unsupported input modality");
                }
            }
        }
        Ok(())
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponse {
    pub output: Vec<Output>,
    #[serde(default)]
    pub usage: Usage,
    #[serde(default)]
    pub provider_turn_pending: bool,
    #[serde(default)]
    pub provider_data: Value,
    pub model_id: Option<String>,
    pub response_id: Option<String>,
    pub finish_reason: Option<String>,
}
impl ModelResponse {
    pub fn text(s: impl Into<String>) -> Self {
        Self {
            output: vec![Output::text(s)],
            usage: Usage::default(),
            provider_turn_pending: false,
            provider_data: Value::Null,
            model_id: None,
            response_id: None,
            finish_reason: None,
        }
    }
    pub fn validate(&self) -> Result<()> {
        validate_output(&self.output)?;
        if self.output.iter().any(|o| matches!(o,Output::ProviderToolCall {call} if matches!(call.status,crate::message::ProviderToolStatus::Pending|crate::message::ProviderToolStatus::Running))) && !self.provider_turn_pending {return Err(Error::Protocol("unfinished provider call requires continuation".into()));}
        Ok(())
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelDelta {
    Text {
        output_index: usize,
        text: String,
    },
    Reasoning {
        output_index: usize,
        text: String,
    },
    RuntimeTool {
        output_index: usize,
        id: Option<String>,
        name: Option<String>,
        input: String,
    },
    ProtocolEvent {
        output_index: usize,
        data: Value,
    },
    ProviderToolProgress {
        output_index: usize,
        provider: String,
        name: String,
        id: Option<String>,
        status: Option<crate::message::ProviderToolStatus>,
        data: Value,
    },
    Usage {
        usage: Usage,
    },
}
pub trait DeltaSink: Send + Sync {
    fn emit(&self, delta: ModelDelta) -> BoxFuture<'_, Result<()>>;
}
#[derive(Clone)]
pub struct ModelContext {
    pub run: RunContext,
    pub cancellation: Cancellation,
    pub deltas: Option<Arc<dyn DeltaSink>>,
}
pub trait Model: Send + Sync {
    fn capabilities(&self) -> &Capabilities;
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>>;
}

fn finite_option<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<f64>, D::Error> {
    let value = Option::<serde_json::Number>::deserialize(deserializer)?;
    value
        .map(|n| {
            if (n.is_i64() || n.is_u64())
                && n.as_f64().is_some_and(|v| v.abs() > 9007199254740991.0)
            {
                return Err(serde::de::Error::custom(
                    "integer cannot be represented as a portable floating point number",
                ));
            }
            n.as_f64()
                .filter(|v| v.is_finite())
                .ok_or_else(|| serde::de::Error::custom("nonfinite number"))
        })
        .transpose()
}
