use crate::{Protocol, ProtocolExtension, codec, streaming, transport};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use zhir_core::{
    BoxFuture, Result,
    model::{Capabilities, Model, ModelContext, ModelRequest, ModelResponse},
};

#[derive(Clone)]
pub struct ModelConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub client: reqwest::Client,
    pub timeout: Duration,
}
impl ModelConfig {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
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
    capabilities: Capabilities,
    extension: Option<Arc<ExtensionFactory>>,
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
        let mut capabilities = Capabilities {
            input_modalities: vec!["text".into(), "image".into(), "file".into()],
            structured_output: true,
            json_mode: true,
            seed: protocol == Protocol::Chat,
            ..crate::capabilities::text_tool_calling()
        };
        if protocol == Protocol::Chat {
            capabilities.input_modalities.push("audio".into());
        }
        capabilities.provider_tools = protocol != Protocol::Chat;
        capabilities.freeform_runtime_tools = protocol == Protocol::Responses;
        if capabilities.provider_tools {
            capabilities.tool_choices.push("provider_tool".into());
        }
        if protocol == Protocol::Messages {
            capabilities.json_mode = false;
        }
        Ok(Self {
            config,
            protocol,
            capabilities,
            extension: None,
        })
    }
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
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
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            request.validate(&self.capabilities)?;
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
            if let Some(extension) = &mut extension {
                extension.encode_request(self.protocol, &request, &mut body)?;
            }
            context.cancellation.check()?;
            let path = match self.protocol {
                Protocol::Chat => "chat/completions",
                Protocol::Responses => "responses",
                Protocol::Messages => "messages",
            };
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
                    .header("x-api-key", &self.config.api_key)
                    .header("anthropic-version", "2023-06-01")
            } else {
                builder.bearer_auth(&self.config.api_key)
            };
            let response = builder.send().await.map_err(transport::request_error)?;
            if !response.status().is_success() {
                return Err(transport::http_error(response).await);
            }
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
