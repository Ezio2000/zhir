//! User-owned provider tool codecs, composed within one model invocation.
pub(crate) mod output;
use crate::{Protocol, ProtocolExtension, extension::one_owner, transport::SseEvent};
pub use output::ProviderOutput;
use serde_json::Value;
use std::collections::BTreeMap;
use zhir_core::{
    Result,
    error::Error,
    message::{Output, ProviderToolCall},
    model::{ModelDelta, ProviderToolSpec},
};

/// One provider capability. Native types, status semantics and replay belong to the caller.
pub trait ProviderToolAdapter: Send {
    fn identity(&self) -> (&str, &str);
    fn encode(&mut self, protocol: Protocol, tool: &ProviderToolSpec) -> Result<Value>;
    fn decode(
        &mut self,
        protocol: Protocol,
        item: &Value,
        response: &Value,
    ) -> Result<Option<Vec<Output>>>;
    fn replay(&mut self, _protocol: Protocol, call: &ProviderToolCall) -> Result<Vec<Value>> {
        ProviderOutput::replay(call)
    }
    fn choice(&mut self, _protocol: Protocol, _tool: &ProviderToolSpec) -> Result<Value> {
        Err(Error::Invalid(
            "provider adapter does not define explicit tool selection".into(),
        ))
    }
    fn event(&mut self, _protocol: Protocol, _event: &SseEvent) -> Result<Vec<ModelDelta>> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
pub struct ProviderTools {
    adapters: BTreeMap<(String, String), Box<dyn ProviderToolAdapter>>,
    enabled: std::collections::BTreeSet<(String, String)>,
}
impl ProviderTools {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register(&mut self, adapter: impl ProviderToolAdapter + 'static) -> Result<()> {
        let (provider, name) = adapter.identity();
        let key = (provider.to_owned(), name.to_owned());
        if provider.is_empty() || name.is_empty() || self.adapters.contains_key(&key) {
            return Err(Error::Invalid(
                "provider adapter identities must be nonempty and unique".into(),
            ));
        }
        self.adapters.insert(key, Box::new(adapter));
        Ok(())
    }
}
impl ProtocolExtension for ProviderTools {
    fn encode_provider_tool(
        &mut self,
        protocol: Protocol,
        tool: &ProviderToolSpec,
    ) -> Result<Option<Value>> {
        let key = (tool.provider.clone(), tool.name.clone());
        let Some(adapter) = self.adapters.get_mut(&key) else {
            return Ok(None);
        };
        let value = adapter.encode(protocol, tool)?;
        if !value.is_object() {
            return Err(Error::Invalid(
                "provider declaration must encode an object".into(),
            ));
        }
        self.enabled.insert(key);
        Ok(Some(value))
    }
    fn encode_provider_choice(
        &mut self,
        protocol: Protocol,
        tool: &ProviderToolSpec,
    ) -> Result<Option<Value>> {
        self.adapters
            .get_mut(&(tool.provider.clone(), tool.name.clone()))
            .map(|a| a.choice(protocol, tool))
            .transpose()
    }
    fn decode_output_item(
        &mut self,
        protocol: Protocol,
        item: &Value,
        response: &Value,
    ) -> Result<Option<Vec<Output>>> {
        one_owner(self.adapters.iter_mut().map(|(key, adapter)| {
            let decoded = adapter.decode(protocol, item, response)?;
            if let Some(output) = &decoded {
                if !self.enabled.contains(key) { return Err(Error::Protocol("provider returned a tool that was not enabled".into())); }
                for item in output {
                    if !matches!(item, Output::ProviderToolCall {call} if (&call.provider, &call.name) == (&key.0, &key.1)) {
                        return Err(Error::Protocol("provider adapter returned another execution identity".into()));
                    }
                }
            }
            Ok(decoded)
        }))
    }
    fn encode_provider_history(
        &mut self,
        protocol: Protocol,
        call: &ProviderToolCall,
    ) -> Result<Option<Vec<Value>>> {
        self.adapters
            .get_mut(&(call.provider.clone(), call.name.clone()))
            .map(|a| a.replay(protocol, call))
            .transpose()
    }
    fn decode_event(
        &mut self,
        protocol: Protocol,
        event: &mut SseEvent,
    ) -> Result<Vec<ModelDelta>> {
        let mut deltas = Vec::new();
        for (key, adapter) in &mut self.adapters {
            if self.enabled.contains(key) {
                for delta in adapter.event(protocol, event)? {
                    if !matches!(&delta, ModelDelta::ProviderToolProgress {provider, name, ..} if (provider, name) == (&key.0, &key.1))
                    {
                        return Err(Error::Protocol(
                            "provider adapter emitted another execution identity".into(),
                        ));
                    }
                    deltas.push(delta);
                }
            }
        }
        Ok(deltas)
    }
}
