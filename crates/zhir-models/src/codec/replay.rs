use super::*;
use std::collections::{HashMap, HashSet};

const POSITION: &str = "$zhir_provider_calls";
pub(super) fn assistant_replay(
    protocol: Protocol,
    output: &[Output],
    data: &Value,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Option<Vec<Value>>> {
    if data.get("protocol").and_then(Value::as_str) != Some(protocol.key()) {
        return Ok(None);
    }
    let response = &data["response"];
    let raw = match protocol {
        Protocol::Chat => response
            .pointer("/choices/0/message")
            .map(std::slice::from_ref),
        Protocol::Responses => response
            .get("output")
            .and_then(Value::as_array)
            .map(Vec::as_slice),
        Protocol::Messages => response
            .get("content")
            .and_then(Value::as_array)
            .map(Vec::as_slice),
    };
    raw.map(|items| replay_items(protocol, items, output, extension))
        .transpose()
}
fn replay_items(
    protocol: Protocol,
    items: &[Value],
    output: &[Output],
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Vec<Value>> {
    let calls: HashMap<_, _> = output
        .iter()
        .filter_map(|o| match o {
            Output::ProviderToolCall { call } => Some((call.id.as_str(), call)),
            _ => None,
        })
        .collect();
    let mut emitted = HashSet::new();
    let mut values = Vec::new();
    for item in items {
        let Some(ids) = item.get(POSITION) else {
            values.push(item.clone());
            continue;
        };
        if item.as_object().is_none_or(|object| object.len() != 1) {
            return Err(protocol_error("invalid provider replay position"));
        }
        let ids = ids
            .as_array()
            .ok_or_else(|| protocol_error("provider replay mapping must contain call ids"))?;
        for id in ids {
            let id = id
                .as_str()
                .ok_or_else(|| protocol_error("provider replay call id must be a string"))?;
            let call = calls.get(id).ok_or_else(|| {
                protocol_error("provider replay mapping references an absent call")
            })?;
            if !emitted.insert(id) {
                return Err(protocol_error(
                    "provider call has multiple native replay positions",
                ));
            }
            values.extend(provider_history(protocol, call, extension)?);
        }
    }
    if emitted.len() != calls.len() {
        return Err(protocol_error(
            "provider call has no native replay position",
        ));
    }
    Ok(values)
}
pub(super) fn provider_history(
    protocol: Protocol,
    call: &ProviderToolCall,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
) -> Result<Vec<Value>> {
    extension
        .as_mut()
        .map(|e| e.encode_provider_history(protocol, call))
        .transpose()?
        .flatten()
        .ok_or_else(|| {
            Error::Invalid(format!(
                "no replay adapter for {}/{}",
                call.provider, call.name
            ))
        })
}
pub(super) fn decode_items(
    protocol: Protocol,
    key: &str,
    value: &Value,
    extension: &mut Option<Box<dyn ProtocolExtension>>,
    native: fn(&Value) -> Result<Vec<Output>>,
) -> Result<Decoded> {
    let items = value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| protocol_error(format!("missing {key}")))?;
    let mut replay = value.clone();
    let mut output = Vec::new();
    for (index, item) in items.iter().enumerate() {
        if protocol == Protocol::Responses {
            text(item, "type")?;
        }
        let mapped = extension
            .as_mut()
            .map(|e| e.decode_output_item(protocol, item, value))
            .transpose()?
            .flatten();
        match mapped {
            Some(mapped) => {
                mark_position(&mapped, &mut replay[key][index])?;
                output.extend(mapped);
            }
            None => output.extend(native(item)?),
        }
    }
    Ok(Decoded {
        output,
        replay,
        pending: false,
    })
}
fn mark_position(output: &[Output], position: &mut Value) -> Result<()> {
    let ids: Vec<_> = output
        .iter()
        .filter_map(|o| match o {
            Output::ProviderToolCall { call } => Some(&call.id),
            _ => None,
        })
        .collect();
    if ids.is_empty() && !output.is_empty() {
        return Ok(());
    }
    if ids.len() != output.len() {
        return Err(protocol_error(
            "one native item cannot mix provider and other output ownership",
        ));
    }
    *position = json!({POSITION: ids});
    Ok(())
}
