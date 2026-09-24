use crate::{Protocol, ProtocolExtension, codec, streaming, transport};
use serde_json::Value;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    model::{CapabilitySet, GenerationOutput, Model, ModelContext, ModelRequest},
};

#[derive(Clone)]
pub struct ModelConfig {
    pub base_url: String,
    pub credentials: Arc<dyn zhir_core::credential::CredentialProvider>,
    pub model: String,
    pub client: reqwest::Client,
    pub timeout: Duration,
}
impl ModelConfig {
    pub fn new(
        base_url: impl Into<String>,
        credentials: Arc<dyn zhir_core::credential::CredentialProvider>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            credentials,
            model: model.into(),
            client: reqwest::Client::new(),
            timeout: Duration::from_secs(120),
        }
    }
}
type ExtensionFactory =
    dyn Fn(crate::ExtensionContext<'_>) -> Result<Box<dyn ProtocolExtension>> + Send + Sync;
struct PreparedExchange {
    request: ModelRequest,
    body: Value,
    extension: Option<Box<dyn ProtocolExtension>>,
}
#[derive(Clone)]
pub struct HttpModel {
    config: ModelConfig,
    protocol: Protocol,
    capabilities: CapabilitySet,
    extension: Option<Arc<ExtensionFactory>>,
    mappings: Vec<crate::profiles::ProfileMapping>,
    retry: zhir_policies::RetryPolicy,
    request_limit: Option<crate::RequestLimit>,
}
impl HttpModel {
    #[cfg(any(
        feature = "openai-chat",
        feature = "openai-responses",
        feature = "anthropic"
    ))]
    pub(crate) fn new(config: ModelConfig, protocol: Protocol) -> Result<Self> {
        if config.model.is_empty() || config.base_url.is_empty() {
            return Err(zhir_core::error::Error::Invalid(
                "model and base URL are required".into(),
            ));
        }
        let mut capabilities = protocol.adapter().capabilities();
        capabilities.features.extend([
            zhir_core::model::Capability::ExplicitGeneration,
            zhir_core::model::Capability::ResponseEvents,
            zhir_core::model::Capability::LocalProjection,
            zhir_core::model::Capability::ReplaceContext,
            zhir_core::model::Capability::ProfileUpdates,
        ]);
        Ok(Self {
            config,
            protocol,
            capabilities,
            extension: None,
            mappings: vec![],
            retry: zhir_policies::RetryPolicy::new(3)?.backoff(
                zhir_policies::Backoff::exponential(
                    Duration::from_millis(500),
                    Duration::from_secs(8),
                )?,
            ),
            request_limit: None,
        })
    }
    pub fn with_profile_mapping(
        mut self,
        mapping: crate::profiles::ProfileMapping,
    ) -> Result<Self> {
        if self
            .mappings
            .iter()
            .any(|old| old.key == mapping.key && old.value == mapping.value)
        {
            return Err(zhir_core::error::Error::Invalid(
                "duplicate profile mapping".into(),
            ));
        }
        self.capabilities
            .constraints
            .entry(mapping.key.clone())
            .or_default()
            .push(mapping.value.clone());
        self.mappings.push(mapping);
        Ok(self)
    }
    pub fn with_capabilities(mut self, mut capabilities: CapabilitySet) -> Self {
        capabilities.features.extend([
            zhir_core::model::Capability::ExplicitGeneration,
            zhir_core::model::Capability::ResponseEvents,
            zhir_core::model::Capability::LocalProjection,
            zhir_core::model::Capability::ReplaceContext,
            zhir_core::model::Capability::ProfileUpdates,
        ]);
        self.capabilities = capabilities;
        self
    }
    /// Replaces the request retry policy. Only retryable rejections received before the
    /// response body is read are retried; the default makes 3 attempts with exponential
    /// backoff from 500ms to 8s, and a longer `Retry-After` delay takes precedence.
    pub fn with_retry(mut self, policy: zhir_policies::RetryPolicy) -> Self {
        self.retry = policy;
        self
    }
    /// Shares one request bound with every model holding the same limit.
    pub fn with_request_limit(mut self, limit: crate::RequestLimit) -> Self {
        self.request_limit = Some(limit);
        self
    }
    /// Install a factory for an independent extension session on each invocation.
    /// Calling this again replaces the factory; compose policies in your session.
    pub fn with_extension<E: ProtocolExtension + 'static>(
        mut self,
        factory: impl Fn(crate::ExtensionContext<'_>) -> Result<E> + Send + Sync + 'static,
    ) -> Self {
        self.extension = Some(Arc::new(move |context| {
            factory(context).map(|session| Box::new(session) as Box<dyn ProtocolExtension>)
        }));
        self
    }
}
impl Model for HttpModel {
    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<zhir_core::profile::NegotiatedProfile> {
        crate::session::validate_capabilities(&self.capabilities)?;
        crate::profiles::negotiate(
            request,
            &self.capabilities,
            &self.mappings,
            self.protocol.adapter().supports_fidelity(),
        )
    }
    fn open_session(
        &self,
        open: zhir_core::model::SessionOpen,
    ) -> BoxFuture<'_, Result<zhir_core::model::ModelSession>> {
        let model = self.clone();
        Box::pin(async move {
            self.negotiate(&open.request)?;
            crate::session::open(
                open,
                Arc::new(move |request, context| {
                    let model = model.clone();
                    Box::pin(async move { model.exchange(request, context).await })
                }),
                self.capabilities.clone(),
                {
                    let model = self.clone();
                    Arc::new(move |request| model.negotiate(request))
                },
            )
        })
    }
}
impl HttpModel {
    fn exchange(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<GenerationOutput>> {
        Box::pin(async move {
            let PreparedExchange {
                request,
                body,
                mut extension,
            } = self.prepare(request, &context)?;
            let deadline = zhir_policies::timing::deadline(&context.run)?;
            let _permit = match &self.request_limit {
                Some(limit) => Some(limit.acquire(&context.cancellation, deadline).await?),
                None => None,
            };
            let response = self.send(&body, &context, deadline).await?;
            let value: Value = if request.stream {
                streaming::receive(self.protocol, response, &context, &mut extension).await?
            } else {
                response.json().await.map_err(transport::request_error)?
            };
            context.cancellation.check()?;
            let decoded = codec::decode(self.protocol, &value, &mut extension);
            let decoded = if let Some(extension) = &mut extension {
                extension.decode_response(self.protocol, &value, decoded)?
            } else {
                decoded?
            };
            context.cancellation.check()?;
            decoded.validate()?;
            Ok(decoded)
        })
    }
}

impl HttpModel {
    fn prepare(
        &self,
        mut request: ModelRequest,
        context: &ModelContext,
    ) -> Result<PreparedExchange> {
        let selected = self.negotiate(&request)?;
        crate::profiles::apply(&mut request, &selected)?;
        if !self
            .capabilities
            .input_modalities
            .iter()
            .any(|m| m == "text")
            && request.messages.iter().any(|m| {
                matches!(
                    m,
                    zhir_core::message::Message::RuntimeTool {
                        outcome: zhir_core::operation::OperationOutcome::Failure { .. },
                        ..
                    }
                )
            })
        {
            return Err(zhir_core::error::Error::Invalid(
                "model cannot receive textual tool failures".into(),
            ));
        }
        context.cancellation.check()?;
        let mut extension = self
            .extension
            .as_ref()
            .map(|factory| {
                factory(crate::ExtensionContext {
                    protocol: self.protocol,
                    request: &request,
                    run: &context.run,
                })
            })
            .transpose()?;
        let mut body = codec::encode(self.protocol, &self.config.model, &request, &mut extension)?;
        let controlled = crate::profiles::fields(&mut body, &selected, &self.mappings)?;
        if let Some(extension) = &mut extension {
            extension.encode_request(self.protocol, &request, &mut body)?;
        }
        if controlled
            .iter()
            .any(|(field, value)| body.get(field) != Some(value))
        {
            return Err(zhir_core::error::Error::Invalid(
                "extension changed negotiated profile".into(),
            ));
        }
        context.cancellation.check()?;
        Ok(PreparedExchange {
            request,
            body,
            extension,
        })
    }
    async fn send(
        &self,
        body: &Value,
        context: &ModelContext,
        deadline: Option<Instant>,
    ) -> Result<reqwest::Response> {
        let mut failed = 0;
        loop {
            let (error, retry_after) = match self.attempt(body).await {
                Ok(response) => return Ok(response),
                Err(rejection) => rejection,
            };
            failed += 1;
            let retryable = matches!(&error, Error::Model(failure) if failure.retryable);
            let Some(delay) = self.retry.delay_after(failed).filter(|_| retryable) else {
                return Err(error);
            };
            let delay = retry_after.map_or(delay, |after| after.max(delay));
            // A retry that cannot start before the deadline reports the real rejection.
            if deadline.is_some_and(|at| Instant::now().checked_add(delay).is_none_or(|t| t >= at))
            {
                return Err(error);
            }
            zhir_policies::timing::wait(delay, &context.cancellation, deadline, |at| {
                tokio::time::sleep_until(at.into())
            })
            .await?;
        }
    }
    /// Sends one request, refreshing a rejected credential once. A rejection carries the
    /// server's `Retry-After` delay in seconds when present.
    async fn attempt(
        &self,
        body: &Value,
    ) -> std::result::Result<reqwest::Response, (Error, Option<Duration>)> {
        let path = self.protocol.adapter().endpoint();
        for attempt in 0..2 {
            let credential = self
                .config
                .credentials
                .resolve(zhir_core::credential::CredentialContext {
                    audience: self.config.base_url.clone(),
                    now_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|e| (Error::Invalid(e.to_string()), None))?
                        .as_millis() as u64,
                })
                .await
                .map_err(|e| (e, None))?;
            let mut builder = self
                .config
                .client
                .post(format!(
                    "{}/{}",
                    self.config.base_url.trim_end_matches('/'),
                    path
                ))
                .timeout(self.config.timeout)
                .json(body);
            builder = self.protocol.adapter().authorize(builder, &credential);
            for (key, value) in &credential.metadata {
                if let Some(header) = key.strip_prefix("header:") {
                    builder = builder.header(header, value);
                }
            }
            let received = builder
                .send()
                .await
                .map_err(|e| (transport::request_error(e), None))?;
            if received.status() == reqwest::StatusCode::UNAUTHORIZED && attempt == 0 {
                self.config
                    .credentials
                    .invalidate(&credential.generation)
                    .await
                    .map_err(|e| (e, None))?;
                continue;
            }
            if !received.status().is_success() {
                let retry_after = received
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.trim().parse().ok())
                    .map(Duration::from_secs);
                return Err((transport::http_error(received).await, retry_after));
            }
            return Ok(received);
        }
        unreachable!("authentication has a fixed positive attempt budget")
    }
}
