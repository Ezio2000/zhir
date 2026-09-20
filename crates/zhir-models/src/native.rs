//! Shared native-session ports and bounded output scheduling. Protocol states stay
//! in adapters; execution, persistence and control intent stay in the kernel.
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
#[path = "native/media.rs"]
mod media;
pub(crate) use media::{Buffered, MediaBudget, Reservation};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    model::*,
    profile::NegotiatedProfile,
    resource::{MediaChunk, MediaPorts, MediaReceiver, MediaSender},
    run::Limits,
};

pub(crate) struct Ports {
    pub commands: mpsc::Receiver<SessionCommand>,
    pub audio_input: mpsc::UnboundedReceiver<Buffered<MediaChunk>>,
    pub outputs: Outputs,
    pub terminal: oneshot::Sender<Terminal>,
}

pub(crate) struct Terminal {
    events: VecDeque<SessionEvent>,
    result: Result<()>,
}

pub(crate) fn ports(
    model: Arc<dyn Model>,
    limits: &Limits,
    audio_input: bool,
    epoch: u64,
) -> (ModelSession, Ports) {
    let (sender, commands) = mpsc::channel(limits.max_control_commands);
    let (events, receiver) = mpsc::channel(limits.max_session_events);
    let (terminal, result) = oneshot::channel();
    // Queue entries retain byte/slot reservations until the consumer takes them.
    let (input, audio) = mpsc::unbounded_channel();
    let (media, media_receiver) = mpsc::unbounded_channel();
    let min_epoch = Arc::new(AtomicU64::new(epoch));
    (
        ModelSession {
            control: Arc::new(Control { model, sender }),
            events: Box::new(Events {
                receiver,
                terminal: Some(result),
                remaining: VecDeque::new(),
                result: None,
            }),
            media: MediaPorts {
                input: audio_input.then(|| {
                    Arc::new(AudioInput {
                        sender: input,
                        limit: limits.max_media_chunk_bytes,
                        budget: MediaBudget::new(limits.max_buffered_media_bytes),
                    }) as Arc<dyn MediaSender>
                }),
                output: Some(Box::new(AudioOutput {
                    receiver: media_receiver,
                    min_epoch: min_epoch.clone(),
                })),
            },
        },
        Ports {
            commands,
            audio_input: audio,
            terminal,
            outputs: Outputs {
                events,
                media: Some(media),
                pending_events: VecDeque::new(),
                pending_media: VecDeque::new(),
                sequence: 0,
                event_limit: limits
                    .max_session_events
                    .saturating_add(limits.max_control_commands)
                    .saturating_add(8),
                media_limit: limits.max_buffered_media_bytes,
                chunk_limit: limits.max_media_chunk_bytes,
                media_bytes: 0,
                media_budget: MediaBudget::new(limits.max_buffered_media_bytes),
                min_epoch,
                media_ending: false,
            },
        },
    )
}
struct Control {
    model: Arc<dyn Model>,
    sender: mpsc::Sender<SessionCommand>,
}
impl SessionControl for Control {
    fn capabilities(&self) -> &CapabilitySet {
        self.model.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.model.negotiate(request)
    }
    fn submit(&self, command: SessionCommand) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.sender
                .send(command)
                .await
                .map_err(|_| Error::Cancelled)
        })
    }
}
struct Events {
    receiver: mpsc::Receiver<SessionEvent>,
    terminal: Option<oneshot::Receiver<Terminal>>,
    remaining: VecDeque<SessionEvent>,
    result: Option<Result<()>>,
}
impl SessionEvents for Events {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        Box::pin(async move {
            if self.terminal.is_some() {
                if let Some(event) = self.receiver.recv().await {
                    return Ok(Some(event));
                }
                let done = self
                    .terminal
                    .take()
                    .expect("unsettled receiver")
                    .await
                    .map_err(|_| {
                        Error::Uncertain("native session worker stopped without settlement".into())
                    })?;
                self.remaining = done.events;
                self.result = Some(done.result);
            }
            if let Some(event) = self.remaining.pop_front() {
                return Ok(Some(event));
            }
            if let Some(result) = self.result.take() {
                result?;
            }
            Ok(None)
        })
    }
}
struct AudioInput {
    sender: mpsc::UnboundedSender<Buffered<MediaChunk>>,
    limit: usize,
    budget: MediaBudget,
}
impl MediaSender for AudioInput {
    fn send(&self, chunk: MediaChunk) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            chunk.validate(self.limit)?;
            let reservation = tokio::select! {
                result = self.budget.reserve(chunk.bytes.len()) => result?,
                _ = self.sender.closed() => return Err(Error::Cancelled),
            };
            self.sender
                .send(Buffered {
                    value: chunk,
                    reservation,
                })
                .map_err(|_| Error::Cancelled)
        })
    }
}
struct AudioOutput {
    receiver: mpsc::UnboundedReceiver<Buffered<MediaChunk>>,
    min_epoch: Arc<AtomicU64>,
}
impl MediaReceiver for AudioOutput {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<MediaChunk>>> {
        Box::pin(async move {
            while let Some(packet) = self.receiver.recv().await {
                let chunk = packet.into_inner();
                if chunk.epoch >= self.min_epoch.load(Ordering::Acquire) {
                    return Ok(Some(chunk));
                }
            }
            Ok(None)
        })
    }
}

pub(crate) struct Outputs {
    pub events: mpsc::Sender<SessionEvent>,
    media: Option<mpsc::UnboundedSender<Buffered<MediaChunk>>>,
    pending_events: VecDeque<SessionEvent>,
    pending_media: VecDeque<PendingMedia>,
    sequence: u64,
    event_limit: usize,
    media_limit: usize,
    chunk_limit: usize,
    media_bytes: usize,
    media_budget: MediaBudget,
    min_epoch: Arc<AtomicU64>,
    media_ending: bool,
}
struct PendingMedia {
    chunk: MediaChunk,
    // WebRTC ingress already reserves before queuing. Decoded WebSocket output
    // reserves when flushed; at most one wire frame's decoded batch is pending.
    reservation: Option<Reservation>,
}
impl Outputs {
    /// Preserve already accepted control events before reporting worker failure.
    /// Ownership moves through the terminal port, so a blocked consumer cannot
    /// delay worker teardown or lose the final provider diagnostic.
    pub fn settle(&mut self, result: Result<()>) -> Terminal {
        Terminal {
            events: std::mem::take(&mut self.pending_events),
            result,
        }
    }
    pub fn event(&mut self, body: SessionEventBody) -> Result<()> {
        if self.pending_events.len() >= self.event_limit {
            return Err(Error::Uncertain("native event capacity exceeded".into()));
        }
        let sequence = self.sequence;
        self.sequence = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("session event sequence overflow".into()))?;
        self.pending_events
            .push_back(SessionEvent { sequence, body });
        Ok(())
    }
    pub fn media(&mut self, chunk: MediaChunk) -> Result<()> {
        self.queue_media(chunk, None)
    }
    #[cfg(feature = "webrtc")]
    pub fn media_budget(&self) -> MediaBudget {
        self.media_budget.clone()
    }
    #[cfg(feature = "webrtc")]
    pub fn received_media(&mut self, packet: Buffered<MediaChunk>) -> Result<()> {
        self.queue_media(packet.value, Some(packet.reservation))
    }
    fn queue_media(&mut self, chunk: MediaChunk, reservation: Option<Reservation>) -> Result<()> {
        chunk.validate(self.chunk_limit)?;
        if chunk.epoch < self.min_epoch.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.media_ending {
            return Err(Error::Protocol("media after native output closure".into()));
        }
        let size = chunk.bytes.len();
        if size > self.media_limit.saturating_sub(self.media_bytes)
            || self.pending_media.len() >= media::MAX_BUFFERED_CHUNKS
        {
            return Err(Error::Uncertain("native media capacity exceeded".into()));
        }
        self.media_bytes += size;
        self.pending_media
            .push_back(PendingMedia { chunk, reservation });
        Ok(())
    }
    #[cfg(feature = "websocket")]
    pub fn invalidate_before(&mut self, epoch: u64) {
        self.min_epoch.fetch_max(epoch, Ordering::AcqRel);
        self.pending_media
            .retain(|packet| packet.chunk.epoch >= epoch);
        self.media_bytes = self
            .pending_media
            .iter()
            .map(|packet| packet.chunk.bytes.len())
            .sum();
    }
    pub fn end_media(&mut self) {
        self.media_ending = true;
        if self.pending_media.is_empty() {
            self.media.take();
        }
    }
    pub fn pending(&self) -> bool {
        !self.pending_events.is_empty() || !self.pending_media.is_empty()
    }
    #[cfg(feature = "websocket")]
    pub fn can_receive(&self) -> bool {
        self.pending_media.is_empty() && self.pending_events.is_empty()
    }
    #[cfg(feature = "webrtc")]
    pub fn commands_allowed(&self, headroom: usize) -> bool {
        // Keep bounded room for receipts and finalization after admitting work.
        self.pending_events.len() < self.event_limit.saturating_sub(headroom)
    }
    #[cfg(feature = "webrtc")]
    pub fn can_receive_media(&self) -> bool {
        self.pending_media.is_empty()
    }
    pub async fn flush_one(&mut self) -> Result<()> {
        enum Ready {
            Event(mpsc::OwnedPermit<SessionEvent>),
            Media(Option<Reservation>),
        }
        let media = self.media.clone();
        let ready = tokio::select! {
            permit = self.events.clone().reserve_owned(), if !self.pending_events.is_empty() => Ready::Event(permit.map_err(|_| Error::Cancelled)?),
            reservation = async {
                let packet = self.pending_media.front().expect("pending media");
                if packet.reservation.is_some() { return Ok(None); }
                tokio::select! {
                    result = self.media_budget.reserve(packet.chunk.bytes.len()) => result.map(Some),
                    _ = media.as_ref().expect("pending media has a sender").closed() => Err(Error::Cancelled),
                }
            }, if !self.pending_media.is_empty() => Ready::Media(reservation?),
        };
        match ready {
            Ready::Event(permit) => {
                permit.send(self.pending_events.pop_front().expect("queued event"));
            }
            Ready::Media(reservation) => {
                let packet = self.pending_media.pop_front().expect("queued media");
                self.media_bytes -= packet.chunk.bytes.len();
                media
                    .expect("pending media has a sender")
                    .send(Buffered {
                        value: packet.chunk,
                        reservation: packet.reservation.or(reservation).expect("reserved media"),
                    })
                    .map_err(|_| Error::Cancelled)?;
                if self.media_ending && self.pending_media.is_empty() {
                    self.media.take();
                }
            }
        }
        Ok(())
    }
}

pub(crate) async fn guarded<T>(
    work: impl std::future::Future<Output = Result<T>>,
    cancellation: &zhir_core::Cancellation,
    deadline: Option<std::time::Instant>,
) -> Result<T> {
    tokio::pin!(work);
    loop {
        zhir_policies::timing::check(cancellation, deadline)?;
        tokio::select! { result = &mut work => return result, _ = tokio::time::sleep(Duration::from_millis(10)) => () }
    }
}

/// A protocol supplies the deadline; the driver polls it independently of output
/// pressure. Idle speaking has no acknowledgement deadline.
pub(crate) async fn confirmation_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[path = "native/confirmation.rs"]
mod confirmation;
pub use confirmation::Confirmation;
