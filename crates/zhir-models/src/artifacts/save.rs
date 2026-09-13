use super::{
    invalid, json_error,
    replay::{bindings, contains_payload},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use zhir_core::{
    Cancellation, Result,
    artifact::{ArtifactContent, ArtifactRef, ArtifactStore},
    message::{MediaSource, Output},
    model::ModelResponse,
    run::RunContext,
};
pub(super) async fn response(
    response: ModelResponse,
    run: &RunContext,
    store: &dyn ArtifactStore,
    cancellation: &Cancellation,
) -> Result<ModelResponse> {
    let mut locations = bindings(&response)?;
    let mut value = serde_json::to_value(&response).map_err(json_error)?;
    let mut saved = BTreeMap::<String, ArtifactRef>::new();
    let mut payloads = std::collections::HashSet::new();
    let mut claimed = std::collections::BTreeSet::new();
    for (output_index, output) in response.output.iter().enumerate() {
        let contents: Vec<_> = match output {
            Output::Content { content } => {
                vec![(format!("/output/{output_index}/content/source"), content)]
            }
            Output::ProviderToolCall { call } => call
                .output
                .iter()
                .enumerate()
                .map(|(index, content)| {
                    (
                        format!("/output/{output_index}/call/output/{index}/source"),
                        content,
                    )
                })
                .collect(),
            _ => Vec::new(),
        };
        for (content_index, (pointer, content)) in contents.into_iter().enumerate() {
            let Some(MediaSource::Inline { mime_type, base64 }) = content.source() else {
                continue;
            };
            let key = format!(
                "{:x}",
                Sha256::digest(
                    serde_json::to_vec(&(&run.run_id, mime_type, base64)).map_err(json_error)?
                )
            );
            let reference = if let Some(reference) = saved.get(&key) {
                reference.clone()
            } else {
                cancellation.check()?;
                let reference = store
                    .put(
                        key.clone(),
                        ArtifactContent {
                            mime_type: mime_type.clone(),
                            base64: base64.clone(),
                        },
                    )
                    .await?;
                cancellation.check()?;
                if reference.id.is_empty() || reference.mime_type != *mime_type {
                    return Err(zhir_core::error::ArtifactError::Invalid {
                        id: reference.id.clone(),
                        message: "artifact store returned an invalid reference".into(),
                    }
                    .into());
                }
                saved.insert(key, reference.clone());
                reference
            };
            *value
                .pointer_mut(&pointer)
                .ok_or_else(|| invalid("artifact output path not found"))? =
                json!({"kind":"artifact","id":reference.id,"mime_type":reference.mime_type});
            if let Some(path) = locations.remove(&(output_index, content_index)) {
                if !claimed.insert(path.clone()) {
                    return Err(invalid(
                        "artifact replay bindings must identify unique protocol payload fields",
                    ));
                }
                let target = value
                    .pointer_mut(&path)
                    .ok_or_else(|| invalid(format!("artifact replay path not found: {path}")))?;
                if target.as_str() != Some(base64) {
                    return Err(invalid(format!(
                        "artifact replay payload mismatch at {path}"
                    )));
                }
                *target = json!({"$zhir_artifact":reference});
            }
            payloads.insert(base64.as_str());
        }
    }
    if !locations.is_empty() {
        return Err(invalid(
            "artifact binding does not address an inline output",
        ));
    }
    // Raw snapshots must not silently retain a second inline copy.
    if contains_payload(&value["provider_data"], &payloads)
        || value["output"].as_array().is_some_and(|items| {
            items
                .iter()
                .any(|item| contains_payload(&item["call"]["data"], &payloads))
        })
    {
        return Err(invalid(
            "inline artifact remains in protocol replay data; declare media in ProviderOutput",
        ));
    }
    let response: ModelResponse = serde_json::from_value(value).map_err(json_error)?;
    response.validate()?;
    Ok(response)
}
