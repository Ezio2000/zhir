//! Incremental context projection and one bounded exchange/event scheduler.
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::{Mutex, mpsc, oneshot};
use zhir_core::run::History;
use zhir_core::{BoxFuture, Result, error::Error, model::*};
pub(crate) type Exchange = dyn Fn(ModelRequest, ModelContext) -> BoxFuture<'static, Result<GenerationOutput>>
    + Send
    + Sync;
pub(crate) type Negotiate =
    dyn Fn(&ModelRequest) -> Result<zhir_core::profile::NegotiatedProfile> + Send + Sync;
struct Control(mpsc::Sender<SessionCommand>, CapabilitySet, Arc<Negotiate>);
impl SessionControl for Control {
    fn capabilities(&self) -> &CapabilitySet {
        &self.1
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<zhir_core::profile::NegotiatedProfile> {
        (self.2)(request)
    }
    fn submit(&self, command: SessionCommand) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.0.send(command).await.map_err(|_| Error::Cancelled) })
    }
}
struct EventsReceiver {
    events: mpsc::Receiver<SessionEvent>,
    terminal: oneshot::Receiver<Result<()>>,
    stopped: bool,
    error: Option<Error>,
}
impl EventsReceiver {
    fn settle(&mut self, result: std::result::Result<Result<()>, oneshot::error::RecvError>) {
        self.stopped = true;
        self.events.close();
        self.error = match result {
            Ok(result) => result.err(),
            Err(_) => Some(Error::Protocol(
                "exchange driver lost its terminal result".into(),
            )),
        };
    }
}
impl SessionEvents for EventsReceiver {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        Box::pin(async move {
            loop {
                if self.stopped {
                    return match self.events.recv().await {
                        Some(event) => Ok(Some(event)),
                        None => self.error.take().map_or(Ok(None), Err),
                    };
                }
                tokio::select! {
                    biased;
                    result = &mut self.terminal => self.settle(result),
                    event = self.events.recv() => match event {
                        Some(event) => return Ok(Some(event)),
                        None => {
                            let result = (&mut self.terminal).await;
                            self.settle(result);
                        }
                    },
                }
            }
        })
    }
}
#[derive(Clone)]
struct Events {
    tx: mpsc::Sender<SessionEvent>,
    // Serialize sequence allocation and delivery, including concurrent user delta sinks.
    sequence: Arc<Mutex<u64>>,
}
impl Events {
    async fn send(&self, body: SessionEventBody) -> Result<()> {
        let mut sequence = self.sequence.lock().await;
        self.tx
            .send(SessionEvent {
                sequence: *sequence,
                body,
            })
            .await
            .map_err(|_| Error::Cancelled)?;
        *sequence = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("event sequence overflow".into()))?;
        Ok(())
    }
}
struct Deltas {
    events: Events,
    generation_id: String,
    downstream: Option<Arc<dyn DeltaSink>>,
}
impl DeltaSink for Deltas {
    fn emit(&self, delta: ModelDelta) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if let Some(sink) = &self.downstream {
                sink.emit(delta.clone()).await?;
            }
            self.events
                .send(SessionEventBody::Delta {
                    generation_id: Some(self.generation_id.clone()),
                    delta,
                })
                .await
        })
    }
}
pub(crate) fn open(
    open: SessionOpen,
    exchange: Arc<Exchange>,
    mut capabilities: CapabilitySet,
    negotiate: Arc<Negotiate>,
) -> Result<ModelSession> {
    validate_capabilities(&capabilities)?;
    open.limits.validate()?;
    open.context.cancellation.check()?;
    negotiate(&open.request)?;
    capabilities.features.extend([
        Capability::ExplicitGeneration,
        Capability::ResponseEvents,
        Capability::LocalProjection,
        Capability::ReplaceContext,
        Capability::ProfileUpdates,
    ]);
    let deadline = zhir_policies::timing::deadline(&open.context.run)?;
    if open.binding.is_some() {
        return Err(Error::Invalid(
            "binding does not belong to an exchange adapter".into(),
        ));
    }
    if open.recovery.is_some() {
        return Err(Error::Protocol(
            "exchange session has no persistent session resume".into(),
        ));
    }
    let (tx, rx) = mpsc::channel::<SessionCommand>(open.limits.max_control_commands);
    let (event_tx, event_rx) = mpsc::channel(open.limits.max_session_events);
    let (terminal_tx, terminal_rx) = oneshot::channel();
    let task = SessionTask {
        events: Events {
            tx: event_tx,
            sequence: Arc::new(Mutex::new(0)),
        },
        exchange,
        negotiate: negotiate.clone(),
        context: open.context,
        deadline,
        seed: open.request.messages.clone(),
        request: open.request,
        history: History::from_entries(vec![])?,
        context_revision: open.context_revision,
        input_position: open.input_position,
        profile_revision: open.profile_revision,
        sealed: false,
        active: None,
    };
    tokio::spawn(async move {
        let result = task.run(rx).await;
        // Teardown never waits for public output capacity. The receiver drains admitted
        // events before returning the terminal error exactly once.
        let _ = terminal_tx.send(result);
    });
    Ok(ModelSession {
        control: Arc::new(Control(tx, capabilities, negotiate)),
        events: Box::new(EventsReceiver {
            events: event_rx,
            terminal: terminal_rx,
            stopped: false,
            error: None,
        }),
        media: zhir_core::resource::MediaPorts::default(),
    })
}

pub(crate) fn validate_capabilities(capabilities: &CapabilitySet) -> Result<()> {
    if [
        Capability::Duplex,
        Capability::ConversationItems,
        Capability::Delegation,
        Capability::InterruptOutput,
        Capability::FlushInput,
        Capability::InputAudioControl,
        Capability::Steering,
        Capability::AsyncResults,
        Capability::Resume,
    ]
    .iter()
    .any(|capability| capabilities.supports(*capability))
    {
        return Err(Error::Invalid(
            "exchange session advertises unsupported session capabilities".into(),
        ));
    }
    Ok(())
}

struct SessionTask {
    events: Events,
    exchange: Arc<Exchange>,
    negotiate: Arc<Negotiate>,
    context: ModelContext,
    deadline: Option<std::time::Instant>,
    request: ModelRequest,
    seed: Vec<zhir_core::message::Message>,
    history: History,
    context_revision: u64,
    input_position: u64,
    profile_revision: u64,
    sealed: bool,
    active: Option<(String, u64, BoxFuture<'static, Result<GenerationOutput>>)>,
}
#[derive(Clone, Copy)]
enum Emission {
    Control,
    Start,
    Output,
    Finish,
}
impl Emission {
    fn control(self) -> bool {
        matches!(self, Self::Control | Self::Start)
    }
}
impl SessionTask {
    async fn run(mut self, mut commands: mpsc::Receiver<SessionCommand>) -> Result<()> {
        // At most one two-event command batch and one in-flight emission are staged.
        // A completed response retains its original output iterator, not another event
        // transcript. No command handler awaits the shared bounded output channel.
        let mut controls = VecDeque::from([SessionEventBody::Ready {
            context_revision: self.context_revision,
        }]);
        let mut sending: Option<BoxFuture<'static, Result<()>>> = None;
        let mut emission = Emission::Control;
        let mut generating = false;
        let mut finishing: Option<FinishEvents> = None;
        let mut closing = false;
        let closed = self.events.tx.clone();
        loop {
            zhir_policies::timing::check(&self.context.cancellation, self.deadline)?;
            if sending.is_none() {
                let next = if let Some(body) = controls.pop_front() {
                    let kind = if matches!(body, SessionEventBody::ResponseStarted { .. }) {
                        Emission::Start
                    } else {
                        Emission::Control
                    };
                    Some((body, kind))
                } else {
                    finishing.as_mut().map(FinishEvents::next)
                };
                if let Some((body, kind)) = next {
                    let events = self.events.clone();
                    sending = Some(Box::pin(async move { events.send(body).await }));
                    emission = kind;
                } else if closing {
                    return Ok(());
                }
            }
            tokio::select! {
                _ = closed.closed() => return Err(Error::Cancelled),
                result = async { sending.as_mut().expect("pending emission").as_mut().await }, if sending.is_some() => {
                    result?;
                    sending = None;
                    match emission {
                        Emission::Start => generating = true,
                        Emission::Finish => finishing = None,
                        _ => (),
                    }
                }
                result = async { self.active.as_mut().expect("active generation").2.as_mut().await }, if generating => {
                    let (generation_id, input_position, _) = self.active.take().expect("active generation");
                    generating = false;
                    let response = result?;
                    response.validate()?;
                    finishing = Some(FinishEvents::new(generation_id, input_position, response)?);
                }
                command = commands.recv(), if !closing && controls.is_empty() && (sending.is_none() || !emission.control()) => {
                    let Some(command) = command else { return Err(Error::Cancelled) };
                    let busy = self.active.is_some() || finishing.is_some();
                    closing = matches!(command.body, SessionCommandBody::Close);
                    controls.extend(self.command(command, busy)?);
                }
                error = zhir_policies::timing::interrupted(&self.context.cancellation, self.deadline, |at| tokio::time::sleep_until(at.into())) => return Err(error),
            }
        }
    }

    fn command(&mut self, command: SessionCommand, busy: bool) -> Result<Vec<SessionEventBody>> {
        self.context.cancellation.check()?;
        if command.id.is_empty() {
            return Err(Error::Invalid("empty command identity".into()));
        }
        let acknowledge = SessionEventBody::Acknowledged {
            command_id: command.id,
            recovery: None,
            level: Acknowledgement::Projection,
        };
        match command.body {
            SessionCommandBody::Generate {
                generation_id,
                context_revision,
                input_position,
                profile_revision,
            } => {
                if busy
                    || generation_id.is_empty()
                    || context_revision != self.context_revision
                    || input_position != self.input_position
                    || profile_revision != self.profile_revision
                {
                    return Err(Error::Protocol("generation does not match the acknowledged projection or another generation is active".into()));
                }
                self.request.messages = self.seed.clone();
                self.request
                    .messages
                    .extend(conversation(self.history.entries()));
                (self.negotiate)(&self.request)?;
                let mut context = self.context.clone();
                context.deltas = Some(Arc::new(Deltas {
                    events: self.events.clone(),
                    generation_id: generation_id.clone(),
                    downstream: context.deltas.clone(),
                }));
                let exchange = self.exchange.clone();
                let request = self.request.clone();
                self.active = Some((
                    generation_id.clone(),
                    input_position,
                    // Invoking the callback can itself emit deltas. Defer invocation
                    // until the scheduler has delivered ResponseStarted.
                    Box::pin(async move { exchange(request, context).await }),
                ));
                Ok(vec![
                    acknowledge,
                    SessionEventBody::ResponseStarted {
                        generation_id,
                        input_position,
                    },
                ])
            }
            SessionCommandBody::Append {
                entry,
                context_revision,
                input_position,
                source,
            } => {
                if source == AppendSource::Submitted
                    && self.sealed
                    && matches!(entry.message, zhir_core::message::Message::User { .. })
                {
                    return Err(Error::Invalid("user input is sealed".into()));
                }
                if context_revision != self.context_revision + 1
                    || input_position < self.input_position
                {
                    return Err(Error::Protocol("non-contiguous context revision".into()));
                }
                entry.validate()?;
                self.history = self.history.append(vec![entry])?;
                self.context_revision = context_revision;
                self.input_position = input_position;
                Ok(vec![acknowledge])
            }
            SessionCommandBody::ReplaceContext {
                entries,
                context_revision,
            } => {
                if busy || context_revision != self.context_revision + 1 {
                    return Err(Error::Protocol(
                        "context replacement crosses a generation or revision".into(),
                    ));
                }
                self.seed = conversation(History::from_entries(entries)?.entries());
                self.history = History::from_entries(vec![])?;
                self.context_revision = context_revision;
                Ok(vec![acknowledge])
            }
            SessionCommandBody::UpdateProfile { revision, profile } => {
                if revision != self.profile_revision + 1 {
                    return Err(Error::Protocol("non-contiguous profile revision".into()));
                }
                self.request.profile = profile;
                (self.negotiate)(&self.request)?;
                self.profile_revision = revision;
                Ok(vec![acknowledge])
            }
            SessionCommandBody::SealUserInput => {
                self.sealed = true;
                Ok(vec![acknowledge])
            }
            SessionCommandBody::Close => {
                if busy {
                    return Err(Error::Protocol("cannot close an active generation".into()));
                }
                Ok(vec![
                    acknowledge,
                    SessionEventBody::Closed {
                        reason: "host_request".into(),
                        provider_data: serde_json::Value::Null,
                    },
                ])
            }
            _ => Err(Error::Invalid(
                "unsupported exchange session command".into(),
            )),
        }
    }
}

struct FinishEvents {
    generation_id: String,
    output: std::iter::Enumerate<std::vec::IntoIter<zhir_core::message::Output>>,
    finished: Option<SessionEventBody>,
}
impl FinishEvents {
    fn new(generation_id: String, input_position: u64, response: GenerationOutput) -> Result<Self> {
        let response_status = response_status(&response);
        let effective = response
            .provider_data
            .get("effective")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| Error::Protocol(format!("effective profile: {e}")))?
            .unwrap_or_default();
        let finished = SessionEventBody::ResponseFinished {
            generation_id: generation_id.clone(),
            input_position,
            response_status,
            usage: response.usage,
            model_id: response.model_id,
            response_id: response.response_id,
            finish_reason: response.finish_reason,
            provider_data: response.provider_data,
            effective,
        };
        Ok(Self {
            generation_id,
            output: response.output.into_iter().enumerate(),
            finished: Some(finished),
        })
    }
    fn next(&mut self) -> (SessionEventBody, Emission) {
        if let Some((index, output)) = self.output.next() {
            (
                SessionEventBody::Output {
                    generation_id: Some(self.generation_id.clone()),
                    item_id: format!("{}:{index}", self.generation_id),
                    caller_id: "model".into(),
                    output,
                },
                Emission::Output,
            )
        } else {
            (
                self.finished.take().expect("one response completion"),
                Emission::Finish,
            )
        }
    }
}
fn response_status(response: &GenerationOutput) -> ResponseStatus {
    if response.status != ResponseStatus::Completed {
        return response.status.clone();
    }
    if response
        .output
        .iter()
        .any(|item| matches!(item, zhir_core::message::Output::RuntimeToolCall { .. }))
    {
        return ResponseStatus::RequiresResults;
    }
    ResponseStatus::Completed
}
