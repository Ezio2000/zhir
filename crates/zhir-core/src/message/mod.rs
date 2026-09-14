use crate::operation::OperationOutcome;
use crate::{Result, error::Error, tool::RuntimeToolCall};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Content {
    Text {
        text: String,
    },
    Resource {
        input: crate::resource::ResourceInput,
    },
    Opaque {
        provider: String,
        data: Value,
    },
}
impl Content {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }
    pub fn resource(resource: crate::resource::ResourceRef) -> Self {
        Self::Resource {
            input: crate::resource::ResourceInput {
                resource,
                usage: Default::default(),
            },
        }
    }
    pub fn as_text(&self) -> Option<&str> {
        if let Self::Text { text } = self {
            Some(text)
        } else {
            None
        }
    }
    pub fn source(&self) -> Option<&crate::resource::ResourceRef> {
        if let Self::Resource { input } = self {
            Some(&input.resource)
        } else {
            None
        }
    }
    pub fn modality(&self) -> Option<&str> {
        match self {
            Self::Text { .. } => Some("text"),
            Self::Resource { input } => Some(input.resource.modality()),
            Self::Opaque { .. } => None,
        }
    }
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Resource { input } => {
                input.resource.validate()?;
                crate::profile::validate_extensions(&input.usage.extensions)
            }
            Self::Opaque { provider, data }
                if provider.is_empty() || data.as_object().is_none_or(|v| v.is_empty()) =>
            {
                Err(Error::Invalid(
                    "opaque content requires provider and nonempty object".into(),
                ))
            }
            _ => Ok(()),
        }
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderToolStatus {
    Cancelled,
    Pending,
    Running,
    Completed,
    Incomplete,
    Failed,
}
/// Settlement metadata owned by the runtime. Content is stored once, in the
/// provider call's output, so resource normalization cannot create two versions.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderToolOutcome {
    Success { structured: Value },
    Failure { error: crate::error::Failure },
    Cancelled { reason: String },
}
impl From<&OperationOutcome> for ProviderToolOutcome {
    fn from(outcome: &OperationOutcome) -> Self {
        match outcome {
            OperationOutcome::Success { structured, .. } => Self::Success {
                structured: structured.clone(),
            },
            OperationOutcome::Failure { error } => Self::Failure {
                error: error.clone(),
            },
            OperationOutcome::Cancelled { reason } => Self::Cancelled {
                reason: reason.clone(),
            },
        }
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderToolCall {
    pub id: String,
    pub provider: String,
    pub name: String,
    pub status: ProviderToolStatus,
    /// Kernel-owned operation settlement. Adapter data remains unchanged.
    pub outcome: Option<ProviderToolOutcome>,
    #[serde(default)]
    pub output: Vec<Content>,
    #[serde(default)]
    pub data: Value,
}
impl ProviderToolCall {
    pub fn matches_outcome(&self, outcome: &OperationOutcome) -> bool {
        let metadata_matches = match (&self.outcome, outcome) {
            (
                Some(ProviderToolOutcome::Success {
                    structured: previous,
                }),
                OperationOutcome::Success { structured, .. },
            ) => previous == structured,
            (
                Some(ProviderToolOutcome::Failure { error: previous }),
                OperationOutcome::Failure { error },
            ) => previous == error,
            (
                Some(ProviderToolOutcome::Cancelled { reason: previous }),
                OperationOutcome::Cancelled { reason },
            ) => previous == reason,
            _ => false,
        };
        metadata_matches && self.output == outcome.content()
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Output {
    Delegation {
        request: crate::operation::DelegationRequest,
    },
    Content {
        content: Content,
    },
    RuntimeToolCall {
        call: RuntimeToolCall,
    },
    ProviderToolCall {
        call: ProviderToolCall,
    },
}
impl Output {
    pub fn text(s: impl Into<String>) -> Self {
        Self::Content {
            content: Content::text(s),
        }
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
pub enum Message {
    DelegationResult {
        id: String,
        outcome: OperationOutcome,
    },
    System {
        content: Vec<Content>,
    },
    User {
        content: Vec<Content>,
    },
    External {
        content: Vec<Content>,
    },
    Assistant {
        output: Vec<Output>,
        #[serde(default)]
        provider_data: Value,
    },
    RuntimeTool {
        call_id: String,
        name: String,
        outcome: OperationOutcome,
    },
}
impl Message {
    pub fn user(text: impl Into<String>) -> Self {
        Self::User {
            content: vec![Content::text(text)],
        }
    }
    pub fn system(text: impl Into<String>) -> Self {
        Self::System {
            content: vec![Content::text(text)],
        }
    }
    pub fn external(text: impl Into<String>) -> Self {
        Self::External {
            content: vec![Content::text(text)],
        }
    }
    pub fn role(&self) -> &'static str {
        match self {
            Self::DelegationResult { .. } => "delegation_result",
            Self::System { .. } => "system",
            Self::User { .. } => "user",
            Self::External { .. } => "external",
            Self::Assistant { .. } => "assistant",
            Self::RuntimeTool { .. } => "runtime_tool",
        }
    }
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::DelegationResult { id, outcome } => {
                if id.is_empty() {
                    return Err(Error::Invalid("empty delegation identity".into()));
                }
                outcome.validate()?;
            }
            Self::System { content } | Self::User { content } | Self::External { content } => {
                for c in content {
                    c.validate()?;
                }
            }
            Self::Assistant { output, .. } => validate_output(output)?,
            Self::RuntimeTool {
                call_id,
                name,
                outcome,
            } => {
                if call_id.is_empty() || name.is_empty() {
                    return Err(Error::Invalid("empty tool message identity".into()));
                }
                outcome.validate()?;
            }
        }
        Ok(())
    }
}
pub fn validate_output(output: &[Output]) -> Result<()> {
    let mut ids = std::collections::HashSet::new();
    for item in output {
        match item {
            Output::Delegation { request } => {
                if request.id.is_empty()
                    || request.prompt.trim().is_empty()
                    || !ids.insert(&request.id)
                {
                    return Err(Error::Invalid("invalid delegation".into()));
                }
            }
            Output::Content { content } => content.validate()?,
            Output::RuntimeToolCall { call } => {
                call.validate()?;
                if !ids.insert(&call.id) {
                    return Err(Error::Invalid("duplicate tool call id".into()));
                }
            }
            Output::ProviderToolCall { call } => {
                if !ids.insert(&call.id) {
                    return Err(Error::Invalid("duplicate output call id".into()));
                }
                if call.id.is_empty() || call.provider.is_empty() || call.name.is_empty() {
                    return Err(Error::Invalid("empty provider call identity".into()));
                }
                for c in &call.output {
                    c.validate()?;
                }
                if let Some(outcome) = &call.outcome
                    && (call.status != ProviderToolStatus::from(outcome)
                        || (!matches!(outcome, ProviderToolOutcome::Success { .. })
                            && !call.output.is_empty()))
                {
                    return Err(Error::Invalid(
                        "provider settlement disagrees with output".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}
impl From<&ProviderToolOutcome> for ProviderToolStatus {
    fn from(outcome: &ProviderToolOutcome) -> Self {
        match outcome {
            ProviderToolOutcome::Success { .. } => Self::Completed,
            ProviderToolOutcome::Failure { .. } => Self::Failed,
            ProviderToolOutcome::Cancelled { .. } => Self::Cancelled,
        }
    }
}
pub fn visible_content(output: &[Output]) -> Vec<Content> {
    output
        .iter()
        .flat_map(|o| match o {
            Output::Content { content } => vec![content.clone()],
            Output::ProviderToolCall { call }
                if matches!(
                    call.status,
                    ProviderToolStatus::Completed
                        | ProviderToolStatus::Incomplete
                        | ProviderToolStatus::Failed
                ) =>
            {
                call.output.clone()
            }
            _ => vec![],
        })
        .collect()
}
