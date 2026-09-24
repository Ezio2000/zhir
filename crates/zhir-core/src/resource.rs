//! Immutable resources and bounded streaming ports, independent of storage and transport.
use crate::{BoxFuture, Result, error::Error, profile::ResourceUsage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceSource {
    Inline { bytes: Vec<u8> },
    Url { url: String },
    Stored { key: String },
    Provider { provider: String, reference: Value },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRef {
    pub id: String,
    pub media_type: String,
    pub name: Option<String>,
    pub source: ResourceSource,
    pub metadata: BTreeMap<String, Value>,
}
impl ResourceRef {
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.media_type.is_empty() {
            return Err(Error::Invalid(
                "resource identity and media type are required".into(),
            ));
        }
        match &self.source {
            ResourceSource::Url { url } if url.is_empty() => {
                Err(Error::Invalid("empty resource URL".into()))
            }
            ResourceSource::Stored { key } if key.is_empty() => {
                Err(Error::Invalid("empty stored resource key".into()))
            }
            ResourceSource::Provider {
                provider,
                reference,
            } if provider.is_empty() || reference.is_null() => {
                Err(Error::Invalid("invalid provider resource".into()))
            }
            _ => Ok(()),
        }
    }
    pub fn modality(&self) -> &'static str {
        match self.media_type.split('/').next() {
            Some("image") => "image",
            Some("audio") => "audio",
            Some("video") => "video",
            _ => "file",
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceInput {
    pub resource: ResourceRef,
    pub usage: ResourceUsage,
}

pub trait ResourceReader: Send {
    /// Returns at most max_bytes; an empty chunk indicates EOF.
    fn read(&mut self, max_bytes: usize) -> BoxFuture<'_, Result<Vec<u8>>>;
}
pub trait ResourceWriter: Send {
    fn append(&mut self, sequence: u64, bytes: Vec<u8>) -> BoxFuture<'_, Result<()>>;
    fn finish(self: Box<Self>) -> BoxFuture<'static, Result<ResourceRef>>;
}
pub trait ResourceStore: Send + Sync {
    fn create(
        &self,
        key: String,
        media_type: String,
    ) -> BoxFuture<'_, Result<Box<dyn ResourceWriter>>>;
    fn open(&self, reference: ResourceRef) -> BoxFuture<'_, Result<Box<dyn ResourceReader>>>;
    /// Removes a stored resource. Deleting an unknown resource succeeds; the host decides
    /// which resources are no longer reachable.
    fn delete(&self, reference: ResourceRef) -> BoxFuture<'_, Result<()>>;
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Stream identity is unique per session direction and epoch. `end` seals it.
/// Input epoch is zero; output uses the session's current output_epoch.
/// Payload encoding and packet pacing belong to the selected model and host.
pub struct MediaChunk {
    pub stream_id: String,
    pub session_id: String,
    pub epoch: u64,
    pub sequence: u64,
    pub timestamp_us: u64,
    pub media_type: String,
    pub bytes: Vec<u8>,
    pub end: bool,
}
impl MediaChunk {
    pub fn validate(&self, max_bytes: usize) -> Result<()> {
        if self.stream_id.is_empty()
            || self.session_id.is_empty()
            || self.media_type.is_empty()
            || self.bytes.len() > max_bytes
        {
            return Err(Error::Invalid(
                "invalid media chunk or chunk limit exceeded".into(),
            ));
        }
        Ok(())
    }
}
pub trait MediaSender: Send + Sync {
    fn send(&self, chunk: MediaChunk) -> BoxFuture<'_, Result<()>>;
}
pub trait MediaReceiver: Send {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<MediaChunk>>>;
}

/// Independently optional media endpoints, with directions relative to the model.
/// This value groups ownership only; it adds no buffering or lifecycle behavior.
#[derive(Default)]
pub struct MediaPorts {
    pub input: Option<std::sync::Arc<dyn MediaSender>>,
    pub output: Option<Box<dyn MediaReceiver>>,
}

/// One immutable segment of a sealed media stream: consecutive chunks of one stream,
/// epoch and media type stored in one resource. `previous` links to the prior segment,
/// keeping checkpoint size independent of the stream duration.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedMedia {
    pub stream_id: String,
    pub session_id: String,
    pub epoch: u64,
    /// Chunks in increasing sequence order; only the last one can end the stream.
    pub chunks: Vec<SealedChunk>,
    pub resource: ResourceRef,
    pub previous: Option<ResourceRef>,
}
/// One chunk within a sealed segment. `offset` and `length` locate its bytes in the
/// segment resource.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedChunk {
    pub sequence: u64,
    pub timestamp_us: u64,
    pub offset: u64,
    pub length: u64,
    pub end: bool,
}

/// An immutable archive node. Completed or interrupted streams leave the active
/// cursor table; their resources remain reachable from the session archive root.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivedMedia {
    pub stream_key: String,
    pub sealed: ResourceRef,
    pub complete: bool,
    pub previous: Option<ResourceRef>,
}
