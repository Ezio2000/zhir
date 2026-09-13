//! Shared credential instances are injected into adapters, never managed by kernel.
use std::sync::Arc;
use tokio::sync::Mutex;
use zhir_core::{
    BoxFuture, Result,
    credential::{Credential, CredentialContext, CredentialProvider},
    error::Error,
};
pub struct StaticCredential {
    credential: Credential,
}
impl StaticCredential {
    pub fn new(scheme: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            credential: Credential {
                generation: "static".into(),
                scheme: scheme.into(),
                value: value.into(),
                expires_at_ms: None,
                metadata: Default::default(),
            },
        }
    }
}
impl CredentialProvider for StaticCredential {
    fn resolve(&self, _: CredentialContext) -> BoxFuture<'_, Result<Credential>> {
        Box::pin(async { Ok(self.credential.clone()) })
    }
    fn invalidate(&self, _: &str) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
type Refresh = dyn Fn(CredentialContext) -> BoxFuture<'static, Result<Credential>> + Send + Sync;
pub struct RefreshingCredential {
    current: Mutex<std::collections::BTreeMap<String, Credential>>,
    refresh: Arc<Refresh>,
}
impl RefreshingCredential {
    pub fn new(
        refresh: impl Fn(CredentialContext) -> BoxFuture<'static, Result<Credential>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            current: Mutex::new(Default::default()),
            refresh: Arc::new(refresh),
        }
    }
}
impl CredentialProvider for RefreshingCredential {
    fn resolve(&self, context: CredentialContext) -> BoxFuture<'_, Result<Credential>> {
        Box::pin(async move {
            let mut current = self.current.lock().await;
            if let Some(value) = current.get(&context.audience)
                && value
                    .expires_at_ms
                    .is_none_or(|expiry| expiry > context.now_ms)
            {
                return Ok(value.clone());
            }
            let value = (self.refresh)(context.clone()).await?;
            if value.generation.is_empty()
                || value.value.is_empty()
                || value
                    .expires_at_ms
                    .is_some_and(|expiry| expiry <= context.now_ms)
            {
                return Err(Error::Invalid(
                    "refresher returned invalid or expired credentials".into(),
                ));
            }
            current.insert(context.audience, value.clone());
            Ok(value)
        })
    }
    fn invalidate(&self, generation: &str) -> BoxFuture<'_, Result<()>> {
        let generation = generation.to_owned();
        Box::pin(async move {
            let mut current = self.current.lock().await;
            current.retain(|_, value| value.generation != generation);
            Ok(())
        })
    }
}
