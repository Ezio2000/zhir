//! Consumer test fixtures. All recordings are in-memory and unbounded.
mod capabilities;
#[cfg(feature = "http")]
pub mod http;
mod model;
pub use capabilities::model_capabilities;
pub use model::ModelTestExt;
mod sink;
mod store;
mod trace;
pub use model::{
    ModelCase, RecordedRequest, RecordingModel, ScriptStep, ScriptedModel, SessionRecord,
};
pub use sink::RecordingSink;
pub use store::{CrashingStore, RecordingStore};
pub use trace::verify_trace;

pub mod session;
pub use session::{SessionModel, SessionPeer};

mod checkpoint;
pub use checkpoint::{checkpoint, checkpoint_with_history};

mod tool;
pub use tool::{FinalExecution, WaitingTool, waiting_operation};
