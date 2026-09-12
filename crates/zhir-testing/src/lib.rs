//! Consumer test fixtures. All recordings are in-memory and unbounded.
#[cfg(feature = "http")]
pub mod http;
mod model;
mod sink;
mod store;
pub use model::{
    ModelCase, ModelRecord, RecordedRequest, RecordingModel, ScriptStep, ScriptedModel,
};
pub use sink::RecordingSink;
pub use store::RecordingStore;
