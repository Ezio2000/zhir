use crate::{
    catalog::SelectedCatalog,
    control::{Control, ControlBody, ControlReceipt},
    environment::{new_id, now_ms},
    invocation::{Emitter, EngineResult, MediaInput, Packet, RunError},
    runtime::{Config, Request},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::Instant,
};
use zhir_core::{
    Cancellation, Result,
    error::{Error, Failure},
    message::{Message, Output, ProviderToolStatus},
    model::*,
    operation::*,
    resource::{MediaChunk, MediaSender},
    run::*,
    storage::{Commit, HistoryDelta, RunStore},
    tool::*,
};

enum Work {
    Model(Result<Option<SessionEvent>>),
    Started(String, Result<ToolExecution>),
    CancelFailed(String, Error),
    Operation(String, Result<Option<OperationEvent>>),
    Admitted(
        Vec<(String, Arc<dyn RuntimeToolBinding>)>,
        Result<Vec<ApprovalDecision>>,
    ),
    MediaReady(
        MediaChunk,
        zhir_core::resource::ResourceRef,
        oneshot::Sender<bool>,
    ),
    InputReady(
        MediaChunk,
        zhir_core::resource::ResourceRef,
        oneshot::Sender<bool>,
    ),
    InputSent(Result<()>),
    MediaDone,
    ReplyDone(String, Result<()>),
    Progress(String, serde_json::Value),
    MediaError(Error),
    CommandSent(Result<()>),
}
struct Engine {
    config: Arc<Config>,
    store: Arc<dyn RunStore>,
    current: Arc<Checkpoint>,
    catalog: Arc<SelectedCatalog>,
    deadline: Option<Instant>,
    cancellation: Cancellation,
    emitter: Emitter,
    controls: mpsc::Receiver<Control>,
    media: mpsc::Receiver<Packet>,
    media_output: MediaInput,
    work_tx: mpsc::Sender<Work>,
    work: mpsc::Receiver<Work>,
    /// Work taken from the queue while batching session events, handled next. Only
    /// accessed through `get_mut`; the mutex keeps the engine `Sync`.
    stashed: std::sync::Mutex<Option<Work>>,
    tasks: JoinSet<()>,
    session_control: Option<Arc<dyn SessionControl>>,
    model_media_input: Option<Arc<dyn MediaSender>>,
    operation_controls: BTreeMap<String, Arc<dyn OperationControl>>,
    tokens: BTreeMap<String, Cancellation>,
    admitting: BTreeSet<String>,
    sent: BTreeSet<String>,
    sending: bool,
    input_sending: bool,
    media_pending: bool,
    pending_replies: BTreeSet<String>,
    pending_starts: BTreeSet<String>,
    session_sequence: Option<u64>,
}

mod admission;
mod checkpoint;
mod commands;
mod completion;
mod controls;
mod lifecycle;
mod media;
mod model_events;
mod operations;
mod session;
mod workers;

use checkpoint::WaitReason;
use completion::provider_operation_id;
pub(crate) use lifecycle::execute;
use lifecycle::{interruptible, interruption};

struct ToolProgress {
    operation_id: String,
    sender: mpsc::Sender<Work>,
}
impl ProgressSink for ToolProgress {
    fn emit(&self, value: serde_json::Value) -> zhir_core::BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.sender
                .send(Work::Progress(self.operation_id.clone(), value))
                .await
                .map_err(|_| Error::Cancelled)
        })
    }
}
