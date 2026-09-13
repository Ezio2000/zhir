use crate::{
    Result,
    error::Error,
    tool::{RuntimeToolCall, RuntimeToolOutcome},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MediaSource {
    Url { url: String },
    Inline { mime_type: String, base64: String },
    Artifact { id: String, mime_type: String },
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Content {
    Text {
        text: String,
    },
    Image {
        source: MediaSource,
    },
    Audio {
        source: MediaSource,
    },
    Video {
        source: MediaSource,
    },
    File {
        source: MediaSource,
        name: Option<String>,
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
    pub fn as_text(&self) -> Option<&str> {
        if let Self::Text { text } = self {
            Some(text)
        } else {
            None
        }
    }
    pub fn source(&self) -> Option<&MediaSource> {
        match self {
            Self::Image { source }
            | Self::Audio { source }
            | Self::Video { source }
            | Self::File { source, .. } => Some(source),
            _ => None,
        }
    }
    pub fn modality(&self) -> Option<&'static str> {
        match self {
            Self::Text { .. } => Some("text"),
            Self::Image { .. } => Some("image"),
            Self::Audio { .. } => Some("audio"),
            Self::Video { .. } => Some("video"),
            Self::File { .. } => Some("file"),
            Self::Opaque { .. } => None,
        }
    }
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Opaque { provider, data }
                if provider.is_empty() || data.as_object().is_none_or(|v| v.is_empty()) =>
            {
                Err(Error::Invalid(
                    "opaque content requires a provider and nonempty object".into(),
                ))
            }
            Self::Image { source }
            | Self::Audio { source }
            | Self::Video { source }
            | Self::File { source, .. } => match source {
                MediaSource::Url { url } if url.is_empty() => {
                    Err(Error::Invalid("empty media URL".into()))
                }
                MediaSource::Artifact { id, mime_type }
                    if id.is_empty() || mime_type.is_empty() =>
                {
                    Err(Error::Invalid("invalid artifact reference".into()))
                }
                MediaSource::Inline { mime_type, base64 }
                    if mime_type.is_empty() || base64.is_empty() =>
                {
                    Err(Error::Invalid("empty inline media".into()))
                }
                _ => Ok(()),
            },
            _ => Ok(()),
        }
    }
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderToolStatus {
    Pending,
    Running,
    Completed,
    Incomplete,
    Failed,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderToolCall {
    pub id: String,
    pub provider: String,
    pub name: String,
    pub status: ProviderToolStatus,
    #[serde(default)]
    pub output: Vec<Content>,
    #[serde(default)]
    pub data: Value,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Output {
    Content { content: Content },
    RuntimeToolCall { call: RuntimeToolCall },
    ProviderToolCall { call: ProviderToolCall },
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
        outcome: RuntimeToolOutcome,
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
            Self::System { .. } => "system",
            Self::User { .. } => "user",
            Self::External { .. } => "external",
            Self::Assistant { .. } => "assistant",
            Self::RuntimeTool { .. } => "runtime_tool",
        }
    }
    pub fn validate(&self) -> Result<()> {
        match self {
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
            }
        }
    }
    Ok(())
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
