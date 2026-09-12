//! Strict decoding of completed textual JSON, without modifying its checkpoint.
use serde::de::DeserializeOwned;
use zhir_core::{
    Result,
    error::Error,
    message::Content,
    run::{Checkpoint, State},
};

/// Locations are scoped to the checkpoint; native call ids need not be unique across turns.
#[derive(Debug, Clone)]
pub struct ProviderToolRecord {
    pub message_index: usize,
    pub output_index: usize,
    pub call: zhir_core::message::ProviderToolCall,
}

/// Provider calls in committed history order, retaining their exact output positions.
pub fn provider_calls(checkpoint: &Checkpoint) -> Vec<ProviderToolRecord> {
    checkpoint
        .history
        .messages()
        .into_iter()
        .enumerate()
        .flat_map(|(message_index, message)| match message {
            zhir_core::message::Message::Assistant { output, .. } => output
                .into_iter()
                .enumerate()
                .filter_map(move |(output_index, item)| {
                    if let zhir_core::message::Output::ProviderToolCall { call } = item {
                        Some(ProviderToolRecord {
                            message_index,
                            output_index,
                            call,
                        })
                    } else {
                        None
                    }
                })
                .collect(),
            _ => Vec::new(),
        })
        .collect()
}

/// Concatenate text parts and deserialize the complete JSON document. Non-text
/// content, non-completed states, Markdown fences and trailing text are errors.
/// This does not request a format, validate an external schema, repair output,
/// or retry execution. Serde controls the selected type's unknown-field behavior.
pub fn decode<T: DeserializeOwned>(checkpoint: &Checkpoint) -> Result<T> {
    let text = completed_text(checkpoint)?;
    serde_json::from_str(&text).map_err(|e| Error::Protocol(format!("completed output JSON: {e}")))
}

fn completed_text(checkpoint: &Checkpoint) -> Result<String> {
    let State::Completed { content } = &checkpoint.state else {
        return Err(Error::Invalid(
            "output decoding requires a completed checkpoint".into(),
        ));
    };
    let mut text = String::new();
    for part in content {
        let Content::Text { text: part } = part else {
            return Err(Error::Invalid(
                "JSON output contains non-text content".into(),
            ));
        };
        text.push_str(part);
    }
    Ok(text)
}

/// One request schema and compiled local validator, following T's input contract.
#[cfg(feature = "typed-output")]
pub struct JsonOutput<T> {
    format: zhir_core::model::ResponseFormat,
    validator: jsonschema::Validator,
    marker: std::marker::PhantomData<fn() -> T>,
}

#[cfg(feature = "typed-output")]
impl<T: DeserializeOwned + schemars::JsonSchema> JsonOutput<T> {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::Invalid("empty output contract name".into()));
        }
        let schema = schemars::generate::SchemaSettings::draft2020_12()
            .for_deserialize()
            .into_generator()
            .into_root_schema_for::<T>();
        let schema = serde_json::to_value(schema)
            .map_err(|e| Error::Invalid(format!("output schema: {e}")))?;
        let validator = jsonschema::validator_for(&schema)
            .map_err(|e| Error::Invalid(format!("output schema: {e}")))?;
        Ok(Self {
            format: zhir_core::model::ResponseFormat::Schema { name, schema },
            validator,
            marker: std::marker::PhantomData,
        })
    }
    pub fn format(&self) -> zhir_core::model::ResponseFormat {
        self.format.clone()
    }
    pub fn decode(&self, checkpoint: &Checkpoint) -> Result<T> {
        let text = completed_text(checkpoint)?;
        let value: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| Error::Protocol(format!("output JSON: {e}")))?;
        self.validator
            .validate(&value)
            .map_err(|e| Error::Protocol(format!("output schema at {}: {e}", e.instance_path())))?;
        serde_json::from_value(value)
            .map_err(|e| Error::Protocol(format!("output deserialization: {e}")))
    }
}
