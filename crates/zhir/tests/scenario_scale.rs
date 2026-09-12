//! Consumer integration and scale audit. No production adapters are added here.
#![cfg(all(
    feature = "tools",
    feature = "openai-chat",
    feature = "openai-responses",
    feature = "anthropic",
    feature = "memory",
    feature = "sqlite",
    feature = "agent"
))]
#[cfg(feature = "typed-tools")]
#[path = "scenario_scale/ergonomics.rs"]
mod ergonomics;
#[path = "scenario_scale/harness.rs"]
mod harness;
#[path = "scenario_scale/live.rs"]
mod live;
#[path = "scenario_scale/transport.rs"]
mod transport;
