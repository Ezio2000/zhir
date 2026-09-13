//! Explicit endpoint mappings for negotiated semantics. Providers own their wire values.
use serde_json::Value;
use zhir_core::profile::keys;
use zhir_core::{
    Result,
    error::Error,
    model::{CapabilitySet, ModelRequest},
    profile::{NegotiatedProfile, Requirement},
};
#[derive(Clone, Debug)]
pub struct ProfileMapping {
    pub key: String,
    pub value: Value,
    pub field: String,
    pub wire_value: Value,
}
impl ProfileMapping {
    pub fn new(
        key: impl Into<String>,
        value: Value,
        field: impl Into<String>,
        wire_value: Value,
    ) -> Result<Self> {
        let mapping = Self {
            key: key.into(),
            value,
            field: field.into(),
            wire_value,
        };
        if mapping.key.is_empty()
            || mapping.field.is_empty()
            || matches!(
                mapping.field.as_str(),
                "model" | "messages" | "input" | "tools" | "stream"
            )
        {
            return Err(Error::Invalid("invalid profile mapping".into()));
        }
        Ok(mapping)
    }
}
pub(crate) fn negotiate(
    request: &ModelRequest,
    caps: &CapabilitySet,
    mappings: &[ProfileMapping],
    images: bool,
) -> Result<NegotiatedProfile> {
    let selected = zhir_policies::negotiation::negotiate(request, caps)?;
    for (key, value) in &selected.selected {
        if key == keys::INTERACTION && value == "turn_based" {
            continue;
        }
        if images && keys::is_resource_fidelity(key) {
            continue;
        }
        if !mappings
            .iter()
            .any(|mapping| &mapping.key == key && &mapping.value == value)
        {
            return Err(Error::Invalid(format!(
                "negotiated {key} has no endpoint mapping"
            )));
        }
    }
    Ok(selected)
}
pub(crate) fn apply(request: &mut ModelRequest, selected: &NegotiatedProfile) -> Result<()> {
    use zhir_core::message::{Content, Message, Output};
    fn content(content: &mut Content, selected: &NegotiatedProfile) -> Result<()> {
        if let Content::Resource { input } = content
            && input.usage.fidelity.is_some()
        {
            input.usage.fidelity = selected
                .selected
                .get(&keys::resource_fidelity(&input.resource.id))
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| Error::Invalid(e.to_string()))?
                .map(Requirement::Required);
        }
        Ok(())
    }
    for message in &mut request.messages {
        match message {
            Message::System { content: contents }
            | Message::User { content: contents }
            | Message::External { content: contents } => {
                for c in contents {
                    content(c, selected)?;
                }
            }
            Message::Assistant { output, .. } => {
                for item in output {
                    if let Output::Content { content: c } = item {
                        content(c, selected)?;
                    }
                }
            }
            Message::RuntimeTool {
                outcome:
                    zhir_core::tool::RuntimeToolOutcome::Success {
                        content: contents, ..
                    },
                ..
            } => {
                for c in contents {
                    content(c, selected)?;
                }
            }
            _ => (),
        }
    }
    Ok(())
}
pub(crate) fn fields(
    body: &mut Value,
    selected: &NegotiatedProfile,
    mappings: &[ProfileMapping],
) -> Result<Vec<(String, Value)>> {
    let mut controlled = vec![];
    for (key, value) in &selected.selected {
        if let Some(mapping) = mappings
            .iter()
            .find(|mapping| &mapping.key == key && &mapping.value == value)
        {
            if body
                .get(&mapping.field)
                .is_some_and(|existing| existing != &mapping.wire_value)
            {
                return Err(Error::Invalid(format!(
                    "profile conflicts with {}",
                    mapping.field
                )));
            }
            body[&mapping.field] = mapping.wire_value.clone();
            controlled.push((mapping.field.clone(), mapping.wire_value.clone()));
        }
    }
    Ok(controlled)
}
