use super::*;
use crate::streaming::StreamState;
use zhir_core::{credential::Credential, model::CapabilitySet};

/// The shared HTTP pipeline delegates all protocol-specific behavior here.
/// These are the three built-in protocols; custom models implement core::Model.
pub(crate) trait ProtocolAdapter: Sync {
    fn key(&self) -> &'static str;
    fn endpoint(&self) -> &'static str;
    fn capabilities(&self) -> CapabilitySet;
    fn supports_fidelity(&self) -> bool {
        true
    }
    fn authorize(
        &self,
        builder: reqwest::RequestBuilder,
        credential: &Credential,
    ) -> reqwest::RequestBuilder {
        builder.header(
            reqwest::header::AUTHORIZATION,
            format!("{} {}", credential.scheme, credential.value),
        )
    }
    fn encode(
        &self,
        model: &str,
        request: &ModelRequest,
        extension: &mut Option<Box<dyn ProtocolExtension>>,
    ) -> Result<Value>;
    fn decode(
        &self,
        value: &Value,
        extension: &mut Option<Box<dyn ProtocolExtension>>,
    ) -> Result<Decoded>;
    fn validate_output_item(&self, _item: &Value) -> Result<()> {
        Ok(())
    }
    fn choice(&self, request: &ModelRequest) -> Result<Value>;
    fn usage(&self, value: &Value) -> Usage;
    fn replay_items<'a>(&self, response: &'a Value) -> Option<&'a [Value]>;
    fn finish_reason<'a>(&self, response: &'a Value) -> Option<&'a Value>;
    fn stream(&self) -> Box<dyn StreamState>;
    fn parallel_tools(&self, body: &mut Value, parallel: bool) {
        body["parallel_tool_calls"] = json!(parallel);
    }
}

pub(super) fn capabilities(
    features: &[zhir_core::model::Capability],
    audio: bool,
    provider_tools: bool,
) -> CapabilitySet {
    let mut caps = crate::capabilities::text_tool_calling();
    caps.input_modalities
        .extend(["image".into(), "file".into()]);
    caps.features.extend(features.iter().cloned());
    if audio {
        caps.input_modalities.push("audio".into());
    }
    if provider_tools {
        caps.tool_choices.push("provider_tool".into());
    }
    caps
}

pub(super) struct UsageFields {
    pub input: &'static str,
    pub output: &'static str,
    pub reasoning: &'static str,
    pub cached: &'static str,
    pub input_excludes_cache: bool,
}
impl UsageFields {
    pub fn read(&self, value: &Value) -> Usage {
        let count = |key: &str| value.get(key).and_then(Value::as_u64);
        let input = count(self.input).map(|input| {
            if self.input_excludes_cache {
                input
                    .saturating_add(count("cache_read_input_tokens").unwrap_or(0))
                    .saturating_add(count("cache_creation_input_tokens").unwrap_or(0))
            } else {
                input
            }
        });
        let output = count(self.output);
        Usage {
            input_tokens: input,
            output_tokens: output,
            total_tokens: count("total_tokens")
                .or_else(|| input.zip(output).map(|(a, b)| a.saturating_add(b))),
            reasoning_tokens: value.pointer(self.reasoning).and_then(Value::as_u64),
            cache_read_tokens: count("cache_read_input_tokens")
                .or_else(|| value.pointer(self.cached).and_then(Value::as_u64)),
            cache_write_tokens: count("cache_creation_input_tokens"),
        }
    }
}
