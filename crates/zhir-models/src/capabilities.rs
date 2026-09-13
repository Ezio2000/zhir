//! Protocol-independent capability preset; callers declare endpoint constraints explicitly.
use zhir_core::model::{Capability, CapabilitySet};
pub fn text_tool_calling() -> CapabilitySet {
    CapabilitySet {
        input_modalities: vec!["text".into()],
        output_modalities: vec!["text".into()],
        features: [
            Capability::StructuredTools,
            Capability::ParallelTools,
            Capability::ParallelControl,
            Capability::Streaming,
            Capability::Usage,
        ]
        .into_iter()
        .collect(),
        tool_choices: ["auto", "none", "required", "runtime_tool"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        constraints: Default::default(),
        extensions: Default::default(),
    }
}
