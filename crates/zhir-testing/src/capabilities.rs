use zhir_core::model::CapabilitySet;
pub fn model_capabilities() -> CapabilitySet {
    let mut caps = zhir_models::capabilities::text_tool_calling();
    caps.features
        .insert(zhir_core::model::Capability::ExplicitGeneration);
    caps.features
        .insert(zhir_core::model::Capability::ResponseEvents);
    caps
}
