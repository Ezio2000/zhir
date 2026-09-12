//! Caller-owned durable artifact storage. No filesystem or transport implementation.
use crate::{BoxFuture, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub id: String,
    pub mime_type: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactContent {
    pub mime_type: String,
    pub base64: String,
}
/// Repeating put with the same key and contents must return the same durable reference.
/// The caller owns cleanup of artifacts saved before an uncommitted response.
pub trait ArtifactStore: Send + Sync {
    fn put(&self, key: String, content: ArtifactContent) -> BoxFuture<'_, Result<ArtifactRef>>;
    fn get(&self, reference: ArtifactRef) -> BoxFuture<'_, Result<ArtifactContent>>;
}
