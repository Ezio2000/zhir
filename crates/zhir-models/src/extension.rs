use crate::{Protocol, transport::SseEvent};
use serde_json::Value;
use zhir_core::{
    Result,
    error::Error,
    message::{Output, ProviderToolCall},
    model::{ModelDelta, ModelRequest, ModelResponse, ProviderToolSpec},
};

/// Read-only context for creating one invocation-local extension session.
pub struct ExtensionContext<'a> {
    pub protocol: Protocol,
    pub request: &'a ModelRequest,
    pub run: &'a zhir_core::run::RunContext,
}

/// User-owned protocol customization, instantiated once per model invocation.
///
/// A session may accumulate stream state without sharing it with concurrent calls
/// or retries. Hooks run in order on the invocation task; errors abort that attempt.
/// The standard HTTP transport, protocol codec and response validation remain in use.
pub trait ProtocolExtension: Send {
    fn encode_provider_tool(
        &mut self,
        _protocol: Protocol,
        _tool: &ProviderToolSpec,
    ) -> Result<Option<Value>> {
        Ok(None)
    }
    fn encode_provider_choice(
        &mut self,
        _protocol: Protocol,
        _tool: &ProviderToolSpec,
    ) -> Result<Option<Value>> {
        Ok(None)
    }
    /// None leaves the item to the standard codec. Some(empty) explicitly consumes it.
    fn decode_output_item(
        &mut self,
        _protocol: Protocol,
        _item: &Value,
        _response: &Value,
    ) -> Result<Option<Vec<Output>>> {
        Ok(None)
    }
    fn encode_provider_history(
        &mut self,
        _protocol: Protocol,
        _call: &ProviderToolCall,
    ) -> Result<Option<Vec<Value>>> {
        Ok(None)
    }
    /// Modify the encoded body, including nested or controlled fields. The caller
    /// owns the resulting protocol semantics. `ModelOptions::extra` remains the
    /// simpler path for additive top-level and nested options.
    fn encode_request(
        &mut self,
        _protocol: Protocol,
        _request: &ModelRequest,
        _body: &mut Value,
    ) -> Result<()> {
        Ok(())
    }

    /// Normalize a wire event before standard stream assembly and optionally emit
    /// additional deltas. The original frame is also forwarded as a ProtocolEvent delta.
    /// This hook sees terminal markers as well as JSON events.
    fn decode_event(
        &mut self,
        _protocol: Protocol,
        _event: &mut SseEvent,
    ) -> Result<Vec<ModelDelta>> {
        Ok(Vec::new())
    }

    /// Map the decoded response, attach accumulated state, or handle a response
    /// shape the standard codec cannot decode. `raw` is the JSON response (or the
    /// assembled stream response). The returned response is always validated.
    fn decode_response(
        &mut self,
        _protocol: Protocol,
        _raw: &Value,
        decoded: Result<ModelResponse>,
    ) -> Result<ModelResponse> {
        decoded
    }
}

/// Compose invocation-local extensions in declaration order for every hook.
///
/// Request/event errors stop that hook immediately. Event deltas are concatenated
/// in order and returned only when the complete hook succeeds. Response hooks pass
/// Result forward, including errors: a later decoder may recover a standard decode
/// error, just as a single ProtocolExtension can. The final response is validated
/// by HttpModel. Construct the chain inside with_extension's invocation factory.
#[derive(Default)]
pub struct ExtensionChain {
    extensions: Vec<Box<dyn ProtocolExtension>>,
}
impl ExtensionChain {
    pub fn new() -> Self {
        Self::default()
    }
    #[must_use]
    pub fn push(mut self, extension: impl ProtocolExtension + 'static) -> Self {
        self.extensions.push(Box::new(extension));
        self
    }
}
impl ProtocolExtension for ExtensionChain {
    fn encode_provider_tool(
        &mut self,
        protocol: Protocol,
        tool: &ProviderToolSpec,
    ) -> Result<Option<Value>> {
        one_owner(
            self.extensions
                .iter_mut()
                .map(|e| e.encode_provider_tool(protocol, tool)),
        )
    }
    fn encode_provider_choice(
        &mut self,
        protocol: Protocol,
        tool: &ProviderToolSpec,
    ) -> Result<Option<Value>> {
        one_owner(
            self.extensions
                .iter_mut()
                .map(|e| e.encode_provider_choice(protocol, tool)),
        )
    }
    fn decode_output_item(
        &mut self,
        protocol: Protocol,
        item: &Value,
        response: &Value,
    ) -> Result<Option<Vec<Output>>> {
        one_owner(
            self.extensions
                .iter_mut()
                .map(|e| e.decode_output_item(protocol, item, response)),
        )
    }
    fn encode_provider_history(
        &mut self,
        protocol: Protocol,
        call: &ProviderToolCall,
    ) -> Result<Option<Vec<Value>>> {
        one_owner(
            self.extensions
                .iter_mut()
                .map(|e| e.encode_provider_history(protocol, call)),
        )
    }
    fn encode_request(
        &mut self,
        protocol: Protocol,
        request: &ModelRequest,
        body: &mut Value,
    ) -> Result<()> {
        for extension in &mut self.extensions {
            extension.encode_request(protocol, request, body)?;
        }
        Ok(())
    }
    fn decode_event(
        &mut self,
        protocol: Protocol,
        event: &mut SseEvent,
    ) -> Result<Vec<ModelDelta>> {
        let mut deltas = Vec::new();
        for extension in &mut self.extensions {
            deltas.extend(extension.decode_event(protocol, event)?);
        }
        Ok(deltas)
    }
    fn decode_response(
        &mut self,
        protocol: Protocol,
        raw: &Value,
        mut decoded: Result<ModelResponse>,
    ) -> Result<ModelResponse> {
        for extension in &mut self.extensions {
            decoded = extension.decode_response(protocol, raw, decoded);
        }
        decoded
    }
}

pub(crate) fn one_owner<T>(
    values: impl IntoIterator<Item = Result<Option<T>>>,
) -> Result<Option<T>> {
    let mut result = None;
    for value in values {
        if let Some(value) = value? {
            if result.is_some() {
                return Err(Error::Invalid(
                    "multiple extensions claimed the same protocol item".into(),
                ));
            }
            result = Some(value);
        }
    }
    Ok(result)
}
