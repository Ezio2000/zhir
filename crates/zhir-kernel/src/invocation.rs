use crate::environment::new_id;
use crate::{
    control::{Control, ControlHandle},
    runtime::{Config, Request},
};
use futures::Stream;
use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tokio::sync::{mpsc, watch};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    model::{DeltaSink, ModelDelta},
    run::{Checkpoint, Event, EventData, RunCompletion},
    tool::ProgressSink,
};

#[derive(Debug, Clone, thiserror::Error)]
#[error("{error}")]
pub struct RunError {
    pub error: Error,
    pub last_checkpoint: Option<Arc<Checkpoint>>,
}
pub type RunResult = std::result::Result<RunCompletion, RunError>;
pub(crate) type EngineResult = std::result::Result<Arc<Checkpoint>, RunError>;
struct EventState {
    sequence: u64,
    sender: Option<mpsc::UnboundedSender<Event>>,
}
#[derive(Clone)]
pub(crate) struct Emitter {
    state: Arc<Mutex<EventState>>,
    pub queued: Arc<AtomicUsize>,
    limit: usize,
    run_id: String,
    invocation_id: String,
}
impl Emitter {
    pub fn emit(&self, data: EventData) {
        let mut state = self.state.lock().expect("event sender lock");
        let Some(sender) = state.sender.as_ref() else {
            return;
        };
        if data.lossy() && self.queued.load(Ordering::Acquire) >= self.limit {
            return;
        }
        if data.lossy() {
            self.queued.fetch_add(1, Ordering::AcqRel);
        }
        let event = Event {
            run_id: self.run_id.clone(),
            invocation_id: self.invocation_id.clone(),
            sequence: state.sequence + 1,
            data,
        };
        let _ = sender.send(event);
        state.sequence += 1;
    }
    fn close(&self) {
        self.state.lock().expect("event sender lock").sender = None;
    }
}
impl DeltaSink for Emitter {
    fn emit(&self, delta: ModelDelta) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.emit(EventData::ModelDelta { delta });
            Ok(())
        })
    }
}
pub(crate) struct Progress {
    pub sender: mpsc::Sender<serde_json::Value>,
}
impl ProgressSink for Progress {
    fn emit(&self, value: serde_json::Value) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.sender.try_send(value).map_err(|e| {
                Error::RuntimeTool(zhir_core::error::Failure::new(
                    "progress_overflow",
                    e.to_string(),
                ))
            })
        })
    }
}

pub struct Invocation {
    pending: Option<(Arc<Config>, Request, mpsc::UnboundedReceiver<Control>)>,
    control: ControlHandle,
    events: Option<mpsc::UnboundedReceiver<Event>>,
    emitter: Emitter,
    result: watch::Receiver<Option<RunResult>>,
    result_sender: Option<watch::Sender<Option<RunResult>>>,
    observed: bool,
}
impl Invocation {
    pub(crate) fn new(config: Arc<Config>, request: Request) -> Self {
        let run_id = match &request {
            Request::Start { context, .. } => context.run_id.clone(),
            Request::Continue(c) | Request::Resume { checkpoint: c, .. } => {
                c.context.run_id.clone()
            }
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (result_tx, result_rx) = watch::channel(None);
        let emitter = Emitter {
            state: Arc::new(Mutex::new(EventState {
                sequence: 0,
                sender: Some(events_tx),
            })),
            queued: Arc::new(AtomicUsize::new(0)),
            limit: request.options().limits.max_progress_events,
            run_id,
            invocation_id: new_id(),
        };
        Self {
            pending: Some((config, request, rx)),
            control: ControlHandle { sender: tx },
            events: Some(events_rx),
            emitter,
            result: result_rx,
            result_sender: Some(result_tx),
            observed: false,
        }
    }
    pub fn control(&self) -> ControlHandle {
        self.control.clone()
    }
    fn start(&mut self) {
        if let Some((config, request, receiver)) = self.pending.take() {
            if !self.observed {
                self.emitter.close();
                self.events.take();
            }
            let emitter = self.emitter.clone();
            let sender = self.result_sender.take().expect("single invocation start");
            tokio::spawn(async move {
                let result =
                    crate::engine::execute(config, request, receiver, emitter.clone()).await;
                let result = result.and_then(|checkpoint| {
                    RunCompletion::new(checkpoint.clone()).map_err(|error| RunError {
                        error,
                        last_checkpoint: Some(checkpoint),
                    })
                });
                sender.send_replace(Some(result));
                emitter.close();
            });
        }
    }
    pub fn events(&mut self) -> Result<EventStream> {
        if self.pending.is_none() || self.observed {
            return Err(Error::Invalid(
                "events must be selected once, before result".into(),
            ));
        }
        self.observed = true;
        let receiver = self.events.take().expect("unselected events");
        self.start();
        Ok(EventStream {
            receiver,
            queued: self.emitter.queued.clone(),
            control: self.control.clone(),
            completed: false,
        })
    }
    pub async fn result(&mut self) -> RunResult {
        self.start();
        loop {
            if let Some(result) = self.result.borrow().clone() {
                return result;
            }
            if self.result.changed().await.is_err() {
                return Err(RunError {
                    error: Error::Protocol("invocation worker stopped unexpectedly".into()),
                    last_checkpoint: None,
                });
            }
        }
    }
}
impl Drop for Invocation {
    fn drop(&mut self) {
        if !self.observed {
            self.control.cancel();
        }
    }
}
pub struct EventStream {
    receiver: mpsc::UnboundedReceiver<Event>,
    queued: Arc<AtomicUsize>,
    control: ControlHandle,
    completed: bool,
}
impl Stream for EventStream {
    type Item = Event;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(event)) => {
                if event.data.lossy() {
                    self.queued.fetch_sub(1, Ordering::AcqRel);
                }
                Poll::Ready(Some(event))
            }
            Poll::Ready(None) => {
                self.completed = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl Drop for EventStream {
    fn drop(&mut self) {
        if !self.completed {
            self.control.cancel();
        }
    }
}
