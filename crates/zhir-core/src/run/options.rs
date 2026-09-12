use super::Limits;
use crate::{
    Result,
    error::Error,
    model::{ModelOptions, ProviderToolSpec, ResponseFormat, ToolChoice},
};
use serde::{Deserialize, Serialize};

/// Effective parameters frozen when a run starts and persisted with every checkpoint.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunOptions {
    pub limits: Limits,
    pub model: ModelOptions,
    pub provider_tools: Vec<ProviderToolSpec>,
    pub tool_choice: ToolChoice,
    pub response_format: Option<ResponseFormat>,
    pub stream: bool,
}
impl RunOptions {
    pub fn limits(mut self, value: Limits) -> Self {
        self.limits = value;
        self
    }
    pub fn options(mut self, value: ModelOptions) -> Self {
        self.model = value;
        self
    }
    pub fn provider_tools(mut self, value: Vec<ProviderToolSpec>) -> Self {
        self.provider_tools = value;
        self
    }
    pub fn tool_choice(mut self, value: ToolChoice) -> Self {
        self.tool_choice = value;
        self
    }
    pub fn response_format(mut self, value: ResponseFormat) -> Self {
        self.response_format = Some(value);
        self
    }
    pub fn without_response_format(mut self) -> Self {
        self.response_format = None;
        self
    }
    pub fn stream(mut self, value: bool) -> Self {
        self.stream = value;
        self
    }
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        if self.model.temperature.is_some_and(|v| !v.is_finite()) {
            return Err(Error::Invalid("nonfinite model temperature".into()));
        }
        let mut identities = std::collections::HashSet::new();
        for tool in &self.provider_tools {
            if tool.provider.is_empty()
                || tool.name.is_empty()
                || !identities.insert((&tool.provider, &tool.name))
            {
                return Err(Error::Invalid(
                    "provider declarations require unique nonempty identities".into(),
                ));
            }
        }
        if let ToolChoice::ProviderTool { provider, name } = &self.tool_choice
            && !identities.contains(&(provider, name))
        {
            return Err(Error::Invalid(
                "selected provider tool is unavailable".into(),
            ));
        }
        Ok(())
    }
}
