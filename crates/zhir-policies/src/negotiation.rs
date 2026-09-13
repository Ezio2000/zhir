//! Pure request negotiation. Concrete protocol mappings remain in adapters.
use serde_json::{Value, json};
use zhir_core::{
    Result,
    error::Error,
    model::{Capability, CapabilitySet, ModelRequest},
    profile::{NegotiatedProfile, Requirement},
};

pub fn negotiate(
    request: &ModelRequest,
    capabilities: &CapabilitySet,
) -> Result<NegotiatedProfile> {
    request.validate(capabilities)?;
    let mut result = NegotiatedProfile::default();
    let profile = &request.profile;
    fn choose<T: serde::Serialize>(
        key: &str,
        requested: &Option<Requirement<T>>,
        request: &ModelRequest,
        caps: &CapabilitySet,
        result: &mut NegotiatedProfile,
    ) -> Result<()> {
        let Some(requirement) = requested else {
            return Ok(());
        };
        let value =
            serde_json::to_value(requirement.value()).map_err(|e| Error::Invalid(e.to_string()))?;
        let values = caps.constraints.get(key);
        if values.is_some_and(|values| values.contains(&value)) {
            result.selected.insert(key.into(), value);
            return Ok(());
        }
        if requirement.required() {
            return Err(Error::Invalid(format!("required {key} is unsupported")));
        }
        let alternatives = request.profile.alternatives.get(key).ok_or_else(|| {
            Error::Invalid(format!("{key} cannot degrade without an explicit policy"))
        })?;
        if let Some(value) = alternatives
            .iter()
            .find(|value| values.is_some_and(|supported| supported.contains(value)))
        {
            result.selected.insert(key.into(), value.clone());
        }
        result
            .unmet_preferences
            .insert(key.into(), "requested value unavailable".into());
        Ok(())
    }
    choose(
        "serving",
        &profile.serving,
        request,
        capabilities,
        &mut result,
    )?;
    choose(
        "reasoning",
        &profile.reasoning,
        request,
        capabilities,
        &mut result,
    )?;
    choose(
        "language",
        &profile.language,
        request,
        capabilities,
        &mut result,
    )?;
    let mut caps = capabilities.clone();
    caps.constraints.insert(
        "interaction".into(),
        if caps.supports(Capability::Duplex) {
            vec![json!("turn_based"), json!("duplex")]
        } else {
            vec![json!("turn_based")]
        },
    );
    choose(
        "interaction",
        &profile.interaction,
        request,
        &caps,
        &mut result,
    )?;
    for message in &request.messages {
        let contents: Vec<_> = match message {
            zhir_core::message::Message::System { content }
            | zhir_core::message::Message::User { content }
            | zhir_core::message::Message::External { content } => content.iter().collect(),
            zhir_core::message::Message::Assistant { output, .. } => output
                .iter()
                .filter_map(|o| match o {
                    zhir_core::message::Output::Content { content } => Some(content),
                    _ => None,
                })
                .collect(),
            zhir_core::message::Message::RuntimeTool { outcome, .. } => {
                outcome.content().iter().collect()
            }
        };
        for content in contents {
            if let zhir_core::message::Content::Resource { input } = content {
                if !input.usage.transforms.is_empty() {
                    return Err(Error::Invalid("resource transforms must be executed as explicit operations before model input".into()));
                }
                let key = format!("resource.{}.fidelity", input.resource.modality());
                let mut selected = NegotiatedProfile::default();
                choose(
                    &key,
                    &input.usage.fidelity,
                    request,
                    capabilities,
                    &mut selected,
                )?;
                if let Some(value) = selected.selected.remove(&key) {
                    result
                        .selected
                        .insert(format!("resource.{}.fidelity", input.resource.id), value);
                }
                for (_, reason) in selected.unmet_preferences {
                    result
                        .unmet_preferences
                        .insert(format!("resource.{}.fidelity", input.resource.id), reason);
                }
            }
        }
    }
    Ok(result)
}

pub fn selected<'a>(profile: &'a NegotiatedProfile, key: &str) -> Option<&'a Value> {
    profile.selected.get(key)
}
