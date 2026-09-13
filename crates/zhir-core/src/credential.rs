//! Credentials are resolved by adapters; execution does not depend on authentication.
use crate::{BoxFuture, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub generation: String,
    pub scheme: String,
    pub value: String,
    pub expires_at_ms: Option<u64>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct CredentialContext {
    pub audience: String,
    pub now_ms: u64,
}

pub trait CredentialProvider: Send + Sync {
    fn resolve(&self, context: CredentialContext) -> BoxFuture<'_, Result<Credential>>;
    /// Invalidate only the generation actually rejected by the server.
    fn invalidate(&self, generation: &str) -> BoxFuture<'_, Result<()>>;
}
