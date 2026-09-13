//! Default configuration and run creation owned by the execution layer.
/// Create a fresh run identity and capture its start time.
pub fn context() -> zhir_core::run::RunContext {
    zhir_core::run::RunContext::new(crate::environment::new_id(), crate::environment::now_ms())
}
/// Default execution budgets for a new run.
pub fn limits() -> zhir_core::run::Limits {
    zhir_core::run::Limits {
        max_model_turns: 100,
        max_runtime_tool_calls: 1000,
        max_inflight_operations: 64,
        max_control_commands: 256,
        max_session_events: 256,
        max_media_streams: 64,
        max_media_chunk_bytes: 1024 * 1024,
        max_buffered_media_bytes: 16 * 1024 * 1024,
        max_runtime_tool_concurrency: 8,
        max_observer_events: 256,
        max_total_tokens: None,
        elapsed_ms: None,
        commit_timeout_ms: 5000,
    }
}
/// Fully resolved defaults persisted with each new run.
pub fn run_options() -> zhir_core::run::RunOptions {
    zhir_core::run::RunOptions {
        runtime_tools: zhir_core::tool::RuntimeToolSelection::All,
        limits: limits(),
        profile: Default::default(),
        mode: zhir_core::run::RunMode::Task,
        provider_tools: vec![],
        tool_choice: Default::default(),
        response_format: None,
        stream: false,
    }
}

mod catalog;
mod scheduling;
mod store;
pub(crate) use catalog::EmptyTools;
pub(crate) use scheduling::DefaultScheduling;
pub(crate) use store::Ephemeral;

/// Host-requested pause preset.
pub fn pause() -> zhir_core::run::Suspension {
    zhir_core::run::Suspension {
        reason: "pause".into(),
        source: "host".into(),
        wait_id: None,
        metadata: Default::default(),
    }
}
