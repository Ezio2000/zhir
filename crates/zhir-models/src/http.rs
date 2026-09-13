use crate::{Protocol, ProtocolExtension, codec, streaming, transport};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use zhir_core::{
    BoxFuture, Result,
    model::{CapabilitySet, Model, ModelContext, ModelRequest, TurnOutput},
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
#[derive(Clone)]
pub struct HttpModel {
    config: ModelConfig,
    protocol: Protocol,
    capabilities: CapabilitySet,
    extension: Option<Arc<ExtensionFactory>>,
    mappings: Vec<crate::profiles::ProfileMapping>,
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
        use zhir_core::model::Capability;
        let mut capabilities = crate::capabilities::text_tool_calling();
        capabilities
            .input_modalities
            .extend(["image".into(), "file".into()]);
        capabilities.features.insert(Capability::StructuredOutput);
        if protocol == Protocol::Chat {
            capabilities.input_modalities.push("audio".into());
            capabilities.features.insert(Capability::Seed);
        }
        if protocol != Protocol::Messages {
            capabilities.features.insert(Capability::JsonMode);
        }
        if protocol != Protocol::Chat {
            capabilities.features.insert(Capability::ProviderTools);
            capabilities.tool_choices.push("provider_tool".into());
        }
        if protocol == Protocol::Responses {
            capabilities.features.insert(Capability::FreeformTools);
        }
        Ok(Self {
            config,
            protocol,
            capabilities,
            extension: None,
            mappings: vec![],
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
    pub fn with_capabilities(mut self, capabilities: CapabilitySet) -> Self {
        self.capabilities = capabilities;
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
            self.protocol != Protocol::Messages,
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
        mut request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<TurnOutput>> {
        Box::pin(async move {
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
                            outcome: zhir_core::tool::RuntimeToolOutcome::Failure { .. },
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
            let mut body =
                codec::encode(self.protocol, &self.config.model, &request, &mut extension)?;
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
            let path = match self.protocol {
                Protocol::Chat => "chat/completions",
                Protocol::Responses => "responses",
                Protocol::Messages => "messages",
            };
            let mut response = None;
            for attempt in 0..2 {
                let credential = self
                    .config
                    .credentials
                    .resolve(zhir_core::credential::CredentialContext {
                        audience: self.config.base_url.clone(),
                        now_ms: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_err(|e| zhir_core::error::Error::Invalid(e.to_string()))?
                            .as_millis() as u64,
                    })
                    .await?;
                let mut builder = self
                    .config
                    .client
                    .post(format!(
                        "{}/{}",
                        self.config.base_url.trim_end_matches('/'),
                        path
                    ))
                    .timeout(self.config.timeout)
                    .json(&body);
                builder = if self.protocol == Protocol::Messages {
                    builder
                        .header("x-api-key", &credential.value)
                        .header("anthropic-version", "2023-06-01")
                } else {
                    builder.header(
                        reqwest::header::AUTHORIZATION,
                        format!("{} {}", credential.scheme, credential.value),
                    )
                };
                for (key, value) in &credential.metadata {
                    if let Some(header) = key.strip_prefix("header:") {
                        builder = builder.header(header, value);
                    }
                }
                let received = builder.send().await.map_err(transport::request_error)?;
                if received.status() == reqwest::StatusCode::UNAUTHORIZED && attempt == 0 {
                    self.config
                        .credentials
                        .invalidate(&credential.generation)
                        .await?;
                    continue;
                }
                if !received.status().is_success() {
                    return Err(transport::http_error(received).await);
                }
                response = Some(received);
                break;
            }
            let response = response.ok_or_else(|| {
                zhir_core::error::Error::Protocol(
                    "authentication retry did not produce a response".into(),
                )
            })?;
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
