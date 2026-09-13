use super::{ModelResponse, invalid};
use serde_json::Value;
use std::collections::BTreeMap;
use zhir_core::{Result, message::Output};
pub(super) fn bindings(response: &ModelResponse) -> Result<BTreeMap<(usize, usize), String>> {
    let mut locations = BTreeMap::new();
    for (index, output) in response.output.iter().enumerate() {
        if let Output::ProviderToolCall { call } = output
            && let Some(replay) = crate::provider_tools::output::replay(call)?
        {
            for binding in replay.media {
                if !binding.pointer.starts_with("/items/") {
                    return Err(invalid("media binding must address native replay items"));
                }
                if locations
                    .insert(
                        (index, binding.content_index),
                        format!(
                            "/output/{index}/call/data/{}/{}",
                            crate::provider_tools::output::REPLAY_KEY,
                            binding.pointer.trim_start_matches('/')
                        ),
                    )
                    .is_some()
                {
                    return Err(invalid("duplicate provider media binding"));
                }
            }
        }
    }
    Ok(locations)
}
pub(super) fn contains_payload(value: &Value, payloads: &std::collections::HashSet<&str>) -> bool {
    match value {
        Value::String(value) => payloads.contains(&value.as_str()),
        Value::Array(values) => values.iter().any(|value| contains_payload(value, payloads)),
        Value::Object(values) if !values.contains_key("$zhir_artifact") => values
            .values()
            .any(|value| contains_payload(value, payloads)),
        _ => false,
    }
}
