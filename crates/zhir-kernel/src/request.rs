use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use zhir_core::{
    Result,
    message::Message,
    model::{ModelOptions, ProviderToolSpec, ResponseFormat, ToolChoice},
    run::{
        Checkpoint, Limits, ResumeTarget, RunContext, RunOptions, SuspensionSelector,
        SuspensionTicket,
    },
};

#[derive(Debug, Clone, Default)]
struct Overrides {
    runtime_tools: Option<zhir_core::tool::RuntimeToolSelection>,
    limits: Option<Limits>,
    model: Option<ModelOptions>,
    provider_tools: Option<Vec<ProviderToolSpec>>,
    tool_choice: Option<ToolChoice>,
    response_format: Option<Option<ResponseFormat>>,
    stream: Option<bool>,
}
#[derive(Debug, Clone)]
pub struct RunRequest {
    pub messages: Vec<Message>,
    pub context: RunContext,
    overrides: Overrides,
}
impl RunRequest {
    pub fn context_value<T: serde::Serialize>(
        mut self,
        key: zhir_core::run::ContextKey<T>,
        value: T,
    ) -> Result<Self> {
        self.context.insert(key, value)?;
        Ok(self)
    }
    pub fn runtime_tools(mut self, value: zhir_core::tool::RuntimeToolSelection) -> Self {
        self.overrides.runtime_tools = Some(value);
        self
    }
    pub fn new(messages: impl IntoIterator<Item = Message>) -> Self {
        Self {
            messages: messages.into_iter().collect(),
            context: crate::defaults::context(),
            overrides: Default::default(),
        }
    }
    pub fn context(mut self, value: RunContext) -> Self {
        self.context = value;
        self
    }
    pub fn run_options(mut self, value: RunOptions) -> Self {
        self.overrides = Overrides {
            runtime_tools: Some(value.runtime_tools),
            limits: Some(value.limits),
            model: Some(value.model),
            provider_tools: Some(value.provider_tools),
            tool_choice: Some(value.tool_choice),
            response_format: Some(value.response_format),
            stream: Some(value.stream),
        };
        self
    }
    pub fn limits(mut self, value: Limits) -> Self {
        self.overrides.limits = Some(value);
        self
    }
    pub fn options(mut self, value: ModelOptions) -> Self {
        self.overrides.model = Some(value);
        self
    }
    pub fn provider_tools(mut self, value: Vec<ProviderToolSpec>) -> Self {
        self.overrides.provider_tools = Some(value);
        self
    }
    pub fn tool_choice(mut self, value: ToolChoice) -> Self {
        self.overrides.tool_choice = Some(value);
        self
    }
    pub fn response_format(mut self, value: ResponseFormat) -> Self {
        self.overrides.response_format = Some(Some(value));
        self
    }
    pub fn without_response_format(mut self) -> Self {
        self.overrides.response_format = Some(None);
        self
    }
    pub fn stream(mut self, value: bool) -> Self {
        self.overrides.stream = Some(value);
        self
    }
    /// Resolve explicit whole-field overrides against immutable runtime defaults.
    pub fn into_parts(self, defaults: &RunOptions) -> (Vec<Message>, RunContext, RunOptions) {
        let o = self.overrides;
        let options = RunOptions {
            runtime_tools: o
                .runtime_tools
                .unwrap_or_else(|| defaults.runtime_tools.clone()),
            limits: o.limits.unwrap_or_else(|| defaults.limits.clone()),
            model: o.model.unwrap_or_else(|| defaults.model.clone()),
            provider_tools: o
                .provider_tools
                .unwrap_or_else(|| defaults.provider_tools.clone()),
            tool_choice: o
                .tool_choice
                .unwrap_or_else(|| defaults.tool_choice.clone()),
            response_format: o
                .response_format
                .unwrap_or_else(|| defaults.response_format.clone()),
            stream: o.stream.unwrap_or(defaults.stream),
        };
        (self.messages, self.context, options)
    }
}

#[derive(Debug, Clone)]
pub struct ResumeRequest {
    pub target: ResumeTarget,
    pub messages: Vec<Message>,
    pub metadata: BTreeMap<String, Value>,
    pub selector: Option<SuspensionSelector>,
}
impl ResumeRequest {
    pub fn context_value<T: serde::Serialize>(
        mut self,
        key: zhir_core::run::ContextKey<T>,
        value: T,
    ) -> Result<Self> {
        key.insert(&mut self.metadata, value)?;
        Ok(self)
    }
    pub fn from_checkpoint(checkpoint: Arc<Checkpoint>) -> Self {
        Self::new(ResumeTarget::Checkpoint(checkpoint))
    }
    pub fn from_ticket(ticket: SuspensionTicket) -> Self {
        Self::new(ResumeTarget::Ticket(ticket))
    }
    fn new(target: ResumeTarget) -> Self {
        Self {
            target,
            messages: Vec::new(),
            metadata: BTreeMap::new(),
            selector: None,
        }
    }
    pub fn message(mut self, value: Message) -> Self {
        self.messages.push(value);
        self
    }
    pub fn messages(mut self, values: impl IntoIterator<Item = Message>) -> Self {
        self.messages.extend(values);
        self
    }
    pub fn metadata(mut self, value: BTreeMap<String, Value>) -> Self {
        self.metadata.extend(value);
        self
    }
    pub fn matching(mut self, value: SuspensionSelector) -> Self {
        self.selector = Some(value);
        self
    }
}
