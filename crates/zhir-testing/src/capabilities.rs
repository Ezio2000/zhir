use zhir_core::model::Capabilities;

/// Capabilities for text and structured-tool consumer fixtures.
pub fn model_capabilities() -> Capabilities {
    Capabilities {
        input_modalities: vec!["text".into()],
        output_modalities: vec!["text".into()],
        structured_runtime_tools: true,
        freeform_runtime_tools: false,
        provider_tools: false,
        parallel_runtime_tools: true,
        parallel_control: true,
        streaming: true,
        usage: true,
        structured_output: false,
        json_mode: false,
        seed: false,
        tool_choices: vec![
            "auto".into(),
            "none".into(),
            "required".into(),
            "runtime_tool".into(),
        ],
    }
}
