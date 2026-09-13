use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use zhir_core::resource::{ResourceRef, ResourceSource};
use zhir_core::{
    Result,
    error::Error,
    message::{Content, Output, ProviderToolCall, ProviderToolStatus},
};

/// Builds normalized output and its canonical native replay together in the decoder.
/// Each media path is local to the most recently appended native item.
pub struct ProviderOutput {
    call: ProviderToolCall,
    replay: Replay,
    media_paths: std::collections::HashSet<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Replay {
    pub items: Vec<Value>,
    pub media: Vec<MediaBinding>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MediaBinding {
    pub content_index: usize,
    pub pointer: String,
}
pub(crate) const REPLAY_KEY: &str = "$zhir_provider_replay";
pub(crate) fn replay(call: &ProviderToolCall) -> Result<Option<Replay>> {
    call.data
        .get(REPLAY_KEY)
        .map(|value| {
            serde_json::from_value(value.clone())
                .map_err(|e| Error::Protocol(format!("invalid provider replay: {e}")))
        })
        .transpose()
}
impl ProviderOutput {
    pub fn new(
        provider: impl Into<String>,
        name: impl Into<String>,
        id: impl Into<String>,
        status: ProviderToolStatus,
    ) -> Self {
        Self {
            call: ProviderToolCall {
                outcome: None,
                provider: provider.into(),
                name: name.into(),
                id: id.into(),
                status,
                output: Vec::new(),
                data: Value::Null,
            },
            media_paths: std::collections::HashSet::new(),
            replay: Replay {
                items: Vec::new(),
                media: Vec::new(),
            },
        }
    }
    /// Append a native replay item, including any opaque fields owned by the caller.
    pub fn native(mut self, item: Value) -> Self {
        self.replay.items.push(item);
        self
    }
    pub fn content(mut self, content: Content) -> Self {
        self.call.output.push(content);
        self
    }
    /// Copy a base64 field from the last native item into normalized media and
    /// remember its local binding. No response-wide array positions are exposed.
    pub fn media(mut self, pointer: &str, mime_type: impl Into<String>) -> Result<Self> {
        let index =
            self.replay.items.len().checked_sub(1).ok_or_else(|| {
                Error::Invalid("append a native item before binding media".into())
            })?;
        let base64 = self.replay.items[index]
            .pointer(pointer)
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                Error::Invalid(
                    "media path must address a nonempty base64 string in the native item".into(),
                )
            })?
            .to_owned();
        let source = ResourceRef {
            id: format!("{}:{index}:{pointer}", self.call.id),
            media_type: mime_type.into(),
            name: None,
            source: ResourceSource::Inline {
                bytes: base64::engine::general_purpose::STANDARD
                    .decode(&base64)
                    .map_err(|e| Error::Protocol(e.to_string()))?,
            },
            metadata: Default::default(),
        };
        let content = Content::resource(source);
        let pointer = format!("/items/{index}{pointer}");
        if !self.media_paths.insert(pointer.clone()) {
            return Err(Error::Invalid("native media path is already bound".into()));
        }
        self.replay.media.push(MediaBinding {
            content_index: self.call.output.len(),
            pointer,
        });
        self.call.output.push(content);
        Ok(self)
    }
    pub fn finish(mut self) -> Result<Output> {
        self.call.data = json!({ REPLAY_KEY: self.replay });
        let output = Output::ProviderToolCall { call: self.call };
        zhir_core::message::validate_output(std::slice::from_ref(&output))?;
        Ok(output)
    }
    /// Default adapter replay. A custom adapter may instead encode its own history.
    pub fn replay(call: &ProviderToolCall) -> Result<Vec<Value>> {
        replay(call)?
            .map(|r| r.items)
            .ok_or_else(|| Error::Invalid("provider call has no canonical replay payload".into()))
    }
}
