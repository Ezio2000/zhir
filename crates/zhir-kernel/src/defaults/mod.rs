//! Default configuration and run creation owned by the execution layer.
/// Create a fresh run identity and capture its start time.
pub fn context() -> zhir_core::run::RunContext {
    zhir_core::run::RunContext::new(crate::environment::new_id(), crate::environment::now_ms())
}
/// Default execution budgets for a new run.
pub fn limits() -> zhir_core::run::Limits {
    zhir_core::run::Limits {
        max_planning_steps: 100,
        max_runtime_tool_calls: 1000,
        max_runtime_tool_batch_size: 32,
        max_runtime_tool_concurrency: 8,
        max_progress_events: 256,
        max_buffered_progress: 256,
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
        model: Default::default(),
        provider_tools: vec![],
        tool_choice: Default::default(),
        response_format: None,
        stream: false,
    }
}

mod batch;
mod catalog;
mod store;
pub(crate) use batch::DefaultBatch;
pub(crate) use catalog::EmptyTools;
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
