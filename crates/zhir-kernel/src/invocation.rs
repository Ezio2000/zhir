use crate::{
    control::{Control, ControlHandle},
    runtime::{Config, Request},
};
use futures::Stream;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use zhir_core::{
    BoxFuture, Cancellation, Result,
    error::Error,
    resource::{MediaChunk, MediaReceiver, MediaSender},
    run::{Checkpoint, Event, EventData, RunCompletion},
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
    lost: usize,
    sender: Option<mpsc::Sender<Event>>,
}
#[derive(Clone)]
pub(crate) struct Emitter {
    state: Arc<Mutex<EventState>>,
    run_id: String,
    invocation_id: String,
}
impl Emitter {
    pub fn emit(&self, data: EventData) {
        let mut state = self.state.lock().expect("observer lock");
        let Some(sender) = state.sender.clone() else {
            return;
        };
        state.sequence += 1;
        if state.lost > 0 {
            let gap = Event {
                run_id: self.run_id.clone(),
                invocation_id: self.invocation_id.clone(),
                sequence: state.sequence,
                data: EventData::ObservationGap { count: state.lost },
            };
            if sender.try_send(gap).is_ok() {
                state.lost = 0;
                state.sequence += 1;
            }
        }
        let event = Event {
            run_id: self.run_id.clone(),
            invocation_id: self.invocation_id.clone(),
            sequence: state.sequence,
            data,
        };
        if sender.try_send(event).is_err() {
            state.lost += 1;
        }
    }
    fn close(&self) {
        self.state.lock().expect("observer lock").sender = None;
    }
}
pub(crate) struct Packet {
    pub chunk: MediaChunk,
    _permit: OwnedSemaphorePermit,
}
#[derive(Clone)]
pub(crate) struct MediaInput {
    sender: mpsc::Sender<Packet>,
    bytes: Arc<Semaphore>,
    max_chunk: usize,
    min_epoch: Arc<std::sync::atomic::AtomicU64>,
}
impl MediaInput {
    pub(crate) fn invalidate_before(&self, epoch: u64) {
        self.min_epoch
            .store(epoch, std::sync::atomic::Ordering::Release);
    }
}
impl MediaSender for MediaInput {
    fn send(&self, chunk: MediaChunk) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            chunk.validate(self.max_chunk)?;
            let size = u32::try_from(chunk.bytes.len().max(1))
                .map_err(|_| Error::Invalid("media chunk too large".into()))?;
            let permit = self
                .bytes
                .clone()
                .acquire_many_owned(size)
                .await
                .map_err(|_| Error::Cancelled)?;
            self.sender
                .send(Packet {
                    chunk,
                    _permit: permit,
                })
                .await
                .map_err(|_| Error::Cancelled)
        })
    }
}
pub struct MediaOutput {
    receiver: mpsc::Receiver<Packet>,
    min_epoch: Arc<std::sync::atomic::AtomicU64>,
}
impl MediaReceiver for MediaOutput {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<MediaChunk>>> {
        Box::pin(async move {
            while let Some(packet) = self.receiver.recv().await {
                if packet.chunk.epoch >= self.min_epoch.load(std::sync::atomic::Ordering::Acquire) {
                    return Ok(Some(packet.chunk));
                }
            }
            Ok(None)
        })
    }
}
pub(crate) fn media_pipe(
    limit: usize,
    packets: usize,
    max_chunk: usize,
) -> (MediaInput, mpsc::Receiver<Packet>) {
    let (sender, receiver) = mpsc::channel(packets);
    (
        MediaInput {
            sender,
            bytes: Arc::new(Semaphore::new(limit)),
            max_chunk,
            min_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        },
        receiver,
    )
}
type PendingInvocation = (
    Arc<Config>,
    Request,
    mpsc::Receiver<Control>,
    mpsc::Receiver<Packet>,
    MediaInput,
);
pub struct Invocation {
    pending: Option<PendingInvocation>,
    control: ControlHandle,
    media_input: Arc<MediaInput>,
    media_output: Option<MediaOutput>,
    events: Option<mpsc::Receiver<Event>>,
    emitter: Emitter,
    result: watch::Receiver<Option<RunResult>>,
    result_sender: Option<watch::Sender<Option<RunResult>>>,
}
impl Invocation {
    pub(crate) fn new(config: Arc<Config>, request: Request) -> Self {
        let limits = &request.options().limits;
        let (control_tx, control_rx) = mpsc::channel(limits.max_control_commands);
        let (events_tx, events_rx) = mpsc::channel(limits.max_observer_events);
        let (result_tx, result_rx) = watch::channel(None);
        let (media_input, media_rx) = media_pipe(
            limits.max_buffered_media_bytes,
            limits.max_buffered_media_packets,
            limits.max_media_chunk_bytes,
        );
        let (media_tx, media_output) = media_pipe(
            limits.max_buffered_media_bytes,
            limits.max_buffered_media_packets,
            limits.max_media_chunk_bytes,
        );
        let output_epoch = media_tx.min_epoch.clone();
        let emitter = Emitter {
            state: Arc::new(Mutex::new(EventState {
                sequence: 0,
                lost: 0,
                sender: Some(events_tx),
            })),
            run_id: request.context().run_id.clone(),
            invocation_id: crate::environment::new_id(),
        };
        Self {
            pending: Some((config, request, control_rx, media_rx, media_tx)),
            control: ControlHandle {
                sender: control_tx,
                cancellation: Cancellation::default(),
            },
            media_input: Arc::new(media_input),
            media_output: Some(MediaOutput {
                receiver: media_output,
                min_epoch: output_epoch,
            }),
            events: Some(events_rx),
            emitter,
            result: result_rx,
            result_sender: Some(result_tx),
        }
    }
    pub fn control(&self) -> ControlHandle {
        self.control.clone()
    }
    pub fn media_input(&mut self) -> Arc<dyn MediaSender> {
        self.start();
        self.media_input.clone()
    }
    pub fn media_output(&mut self) -> Result<MediaOutput> {
        self.start();
        self.media_output
            .take()
            .ok_or_else(|| Error::Invalid("media output already selected".into()))
    }
    pub fn start(&mut self) {
        if let Some((config, request, controls, media, media_output)) = self.pending.take() {
            let emitter = self.emitter.clone();
            let cancellation = self.control.cancellation.clone();
            let sender = self.result_sender.take().expect("single start");
            tokio::spawn(async move {
                let result = crate::engine::execute(
                    config,
                    request,
                    controls,
                    media,
                    media_output,
                    cancellation,
                    emitter.clone(),
                )
                .await;
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
        let receiver = self
            .events
            .take()
            .ok_or_else(|| Error::Invalid("events already selected".into()))?;
        self.start();
        Ok(EventStream {
            receiver,
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
                    error: Error::Protocol("worker stopped without settlement".into()),
                    last_checkpoint: None,
                });
            }
        }
    }
}
impl Drop for Invocation {
    fn drop(&mut self) {
        self.control.cancel();
    }
}
pub struct EventStream {
    receiver: mpsc::Receiver<Event>,
    control: ControlHandle,
    completed: bool,
}
impl Stream for EventStream {
    type Item = Event;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        let poll = self.receiver.poll_recv(cx);
        if matches!(poll, Poll::Ready(None)) {
            self.completed = true;
        }
        poll
    }
}
impl Drop for EventStream {
    fn drop(&mut self) {
        if !self.completed {
            self.control.cancel();
        }
    }
}
