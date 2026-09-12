//! Explicit capability presets; callers must match the selected model.
use zhir_core::model::Capabilities;

/// Text input/output with structured tool calls, streaming, usage and parallel control.
pub fn text_tool_calling() -> Capabilities {
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
