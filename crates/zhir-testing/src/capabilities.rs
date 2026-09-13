use zhir_core::model::CapabilitySet;
pub fn model_capabilities() -> CapabilitySet {
    zhir_models::capabilities::text_tool_calling()
}
