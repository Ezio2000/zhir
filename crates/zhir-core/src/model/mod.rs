mod session;
use crate::{
    BoxFuture, Cancellation, Result,
    error::Error,
    message::{Message, Output, validate_output},
    run::RunContext,
    tool::RuntimeToolSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub use session::*;
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilitySet {
    pub input_modalities: Vec<String>,
    pub output_modalities: Vec<String>,
    pub features: std::collections::BTreeSet<Capability>,
    pub tool_choices: Vec<String>,
    pub constraints: std::collections::BTreeMap<String, Vec<Value>>,
    pub extensions: crate::profile::Extensions,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    StructuredTools,
    FreeformTools,
    ProviderTools,
    ParallelTools,
    ParallelControl,
    Streaming,
    Usage,
    StructuredOutput,
    JsonMode,
    Seed,
    Duplex,
    Steering,
    InterruptOutput,
    FlushInput,
    InputAudioControl,
    ConversationItems,
    Delegation,
    ProfileUpdates,
    AsyncResults,
    Resume,
    /// The host explicitly requests generation; opening never generates.
    ExplicitGeneration,
    /// The protocol exposes real start/finish events and a verified input boundary.
    ResponseEvents,
    /// Context is local; rebuilding an idle projection has no remote session-creation effect.
    LocalProjection,
    /// Context replacement is acknowledged before another generation may start.
    ReplaceContext,
    /// A generation consumes a stream until the host seals its user input.
    StreamingInput,
}
impl CapabilitySet {
    pub fn supports(&self, capability: Capability) -> bool {
        self.features.contains(&capability)
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationProfile {
    #[serde(default, deserialize_with = "finite_option")]
    pub temperature: Option<f64>,
    pub max_output_tokens: Option<u64>,
    pub seed: Option<i64>,
    pub parallel_runtime_tools: Option<bool>,
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
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequest {
    pub messages: Vec<Message>,
    pub runtime_tools: Vec<RuntimeToolSpec>,
    pub provider_tools: Vec<ProviderToolSpec>,
    pub profile: crate::profile::RequestProfile,
    pub tool_choice: ToolChoice,
    pub response_format: Option<ResponseFormat>,
    pub stream: bool,
}
impl ModelRequest {
    pub fn validate(&self, c: &CapabilitySet) -> Result<()> {
        crate::profile::validate_extensions(&self.profile.extensions)?;
        let invalid = |s: &str| Err(Error::Invalid(s.into()));
        if self.stream && !c.supports(Capability::Streaming) {
            return invalid("model does not support streaming");
        }
        if !c.tool_choices.iter().any(|s| s == self.tool_choice.kind()) {
            return invalid("unsupported tool choice");
        }
        if self.profile.generation.parallel_runtime_tools == Some(false)
            && !c.supports(Capability::ParallelControl)
        {
            return invalid("model cannot disable parallel tools");
        }
        if self.profile.generation.seed.is_some() && !c.supports(Capability::Seed) {
            return invalid("unsupported seed");
        }
        if self
            .profile
            .generation
            .temperature
            .is_some_and(|n| !n.is_finite())
        {
            return invalid("nonfinite temperature");
        }
        if !self.provider_tools.is_empty() && !c.supports(Capability::ProviderTools) {
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
            && !c.supports(Capability::StructuredOutput)
        {
            return invalid("unsupported structured output");
        }
        if matches!(self.response_format, Some(ResponseFormat::Json))
            && !c.supports(Capability::JsonMode)
        {
            return invalid("unsupported JSON mode");
        }
        for t in &self.runtime_tools {
            match t.input {
                crate::tool::InputSpec::Structured { .. }
                    if !c.supports(Capability::StructuredTools) =>
                {
                    return invalid("unsupported structured tools");
                }
                crate::tool::InputSpec::Freeform { .. }
                    if !c.supports(Capability::FreeformTools) =>
                {
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
            if !c.supports(Capability::Delegation)
                && (matches!(m, Message::DelegationResult { .. })
                    || matches!(m, Message::Assistant { output, .. } if output.iter().any(|o| matches!(o, Output::Delegation { .. }))))
            {
                return invalid("unsupported delegation history");
            }
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
                Message::RuntimeTool { outcome, .. } => outcome.content().to_vec(),
                Message::DelegationResult { outcome, .. } => outcome.content().to_vec(),
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
pub struct GenerationOutput {
    pub output: Vec<Output>,
    #[serde(default)]
    pub usage: Usage,
    pub status: ResponseStatus,
    #[serde(default)]
    pub provider_data: Value,
    pub model_id: Option<String>,
    pub response_id: Option<String>,
    pub finish_reason: Option<String>,
}
impl GenerationOutput {
    pub fn text(s: impl Into<String>) -> Self {
        Self {
            output: vec![Output::text(s)],
            usage: Usage::default(),
            status: ResponseStatus::Completed,
            provider_data: Value::Null,
            model_id: None,
            response_id: None,
            finish_reason: None,
        }
    }
    pub fn validate(&self) -> Result<()> {
        validate_output(&self.output)?;
        let provider_pending = self.output.iter().any(|output| match output {
            Output::ProviderToolCall { call } => matches!(
                call.status,
                crate::message::ProviderToolStatus::Pending
                    | crate::message::ProviderToolStatus::Running
            ),
            _ => false,
        });
        if provider_pending && self.status != ResponseStatus::Continuation {
            return Err(Error::Protocol(
                "unfinished provider call requires continuation".into(),
            ));
        }

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
    fn capabilities(&self) -> &CapabilitySet;
    fn negotiate(&self, request: &ModelRequest) -> Result<crate::profile::NegotiatedProfile>;
    fn open_session(&self, open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>>;
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

mod conversation;
pub use conversation::conversation;
