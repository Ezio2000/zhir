//! Incremental context projection and bounded, single-generation exchange driver.
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::mpsc;
use zhir_core::run::History;
use zhir_core::{BoxFuture, Result, error::Error, model::*};
pub(crate) type Exchange = dyn Fn(ModelRequest, ModelContext) -> BoxFuture<'static, Result<GenerationOutput>>
    + Send
    + Sync;
pub(crate) type Negotiate =
    dyn Fn(&ModelRequest) -> Result<zhir_core::profile::NegotiatedProfile> + Send + Sync;
struct Sender(mpsc::Sender<SessionCommand>, CapabilitySet, Arc<Negotiate>);
impl SessionSender for Sender {
    fn capabilities(&self) -> &CapabilitySet {
        &self.1
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<zhir_core::profile::NegotiatedProfile> {
        (self.2)(request)
    }
    fn send(&self, command: SessionCommand) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.0.send(command).await.map_err(|_| Error::Cancelled) })
    }
}
struct Receiver(mpsc::Receiver<Result<SessionEvent>>);
impl SessionReceiver for Receiver {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        Box::pin(async move { self.0.recv().await.transpose() })
    }
}
#[derive(Clone)]
struct Events {
    tx: mpsc::Sender<Result<SessionEvent>>,
    sequence: Arc<AtomicU64>,
}
impl Events {
    async fn send(&self, body: SessionEventBody) -> Result<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        self.tx
            .send(Ok(SessionEvent { sequence, body }))
            .await
            .map_err(|_| Error::Cancelled)
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
    if open.recovery.is_some() {
        return Err(Error::Protocol(
            "exchange session has no persistent session resume".into(),
        ));
    }
    let (tx, mut rx) = mpsc::channel::<SessionCommand>(open.limits.max_control_commands);
    let (event_tx, event_rx) = mpsc::channel(open.limits.max_session_events);
    let events = Events {
        tx: event_tx,
        sequence: Arc::new(AtomicU64::new(0)),
    };
    let mut task = SessionTask {
        events,
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
        let result: Result<()> = async {
            task.events.send(SessionEventBody::Ready { context_revision: task.context_revision }).await?;
            loop {
                tokio::select! {
                    command = rx.recv() => {
                        let Some(command) = command else { break };
                        if !task.command(command).await? { break; }
                    }
                    result = async { task.active.as_mut().expect("active generation").2.as_mut().await }, if task.active.is_some() => {
                        let (id, input_position, _) = task.active.take().expect("active generation");
                        let response = result?;
                        zhir_policies::timing::check(&task.context.cancellation, task.deadline)?;
                        response.validate()?;
                        task.finish(id, input_position, response).await?;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                        zhir_policies::timing::check(&task.context.cancellation, task.deadline)?;
                    }
                }
            }
            Ok(())
        }.await;
        if let Err(error) = result {
            let _ = task.events.tx.send(Err(error)).await;
        }
    });
    Ok(ModelSession {
        input: Arc::new(Sender(tx, capabilities, negotiate)),
        output: Box::new(Receiver(event_rx)),
        media_input: None,
        media_output: None,
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
impl SessionTask {
    async fn command(&mut self, command: SessionCommand) -> Result<bool> {
        self.context.cancellation.check()?;
        match command.body {
            SessionCommandBody::Generate {
                generation_id,
                context_revision,
                input_position,
                profile_revision,
            } => {
                if self.active.is_some()
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
                self.acknowledge(command.id).await?;
                self.events
                    .send(SessionEventBody::ResponseStarted {
                        generation_id: generation_id.clone(),
                        input_position,
                    })
                    .await?;
                let mut context = self.context.clone();
                context.deltas = Some(Arc::new(Deltas {
                    events: self.events.clone(),
                    generation_id: generation_id.clone(),
                    downstream: context.deltas.clone(),
                }));
                self.active = Some((
                    generation_id,
                    input_position,
                    (self.exchange)(self.request.clone(), context),
                ));
                Ok(true)
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
                self.acknowledge(command.id).await?;
                Ok(true)
            }
            SessionCommandBody::ReplaceContext {
                entries,
                context_revision,
            } => {
                if self.active.is_some() || context_revision != self.context_revision + 1 {
                    return Err(Error::Protocol(
                        "context replacement crosses a generation or revision".into(),
                    ));
                }
                self.seed = conversation(History::from_entries(entries)?.entries());
                self.history = History::from_entries(vec![])?;
                self.context_revision = context_revision;
                self.acknowledge(command.id).await?;
                Ok(true)
            }
            SessionCommandBody::UpdateProfile { revision, profile } => {
                if revision != self.profile_revision + 1 {
                    return Err(Error::Protocol("non-contiguous profile revision".into()));
                }
                self.request.profile = profile;
                (self.negotiate)(&self.request)?;
                self.profile_revision = revision;
                self.acknowledge(command.id).await?;
                Ok(true)
            }
            SessionCommandBody::InterruptOutput { .. } => Err(Error::Invalid(
                "exchange session cannot interrupt natively".into(),
            )),
            SessionCommandBody::FlushInput | SessionCommandBody::SetInputAudio { .. } => Err(
                Error::Invalid("exchange session cannot flush an active input".into()),
            ),
            SessionCommandBody::DelegationContext { .. } => Err(Error::Invalid(
                "exchange session cannot accept native delegation".into(),
            )),
            SessionCommandBody::Close => {
                if self.active.is_some() {
                    return Err(Error::Protocol("cannot close an active generation".into()));
                }
                self.acknowledge(command.id).await?;
                self.events
                    .send(SessionEventBody::Closed {
                        reason: "host_request".into(),
                        provider_data: serde_json::Value::Null,
                    })
                    .await?;
                Ok(false)
            }
            SessionCommandBody::SealUserInput => {
                self.sealed = true;
                self.acknowledge(command.id).await?;
                Ok(true)
            }
        }
    }
    async fn acknowledge(&mut self, command_id: String) -> Result<()> {
        self.events
            .send(SessionEventBody::Acknowledged {
                command_id,
                recovery: None,
                level: Acknowledgement::Projection,
            })
            .await
    }
    async fn finish(
        &mut self,
        generation_id: String,
        input_position: u64,
        response: GenerationOutput,
    ) -> Result<()> {
        let response_status = response_status(&response);
        for (index, output) in response.output.into_iter().enumerate() {
            self.events
                .send(SessionEventBody::Output {
                    generation_id: Some(generation_id.clone()),
                    item_id: format!("{generation_id}:{index}"),
                    caller_id: "model".into(),
                    output,
                })
                .await?;
        }
        let effective = response
            .provider_data
            .get("effective")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| Error::Protocol(format!("effective profile: {e}")))?
            .unwrap_or_default();
        self.events
            .send(SessionEventBody::ResponseFinished {
                generation_id,
                input_position,
                response_status,
                usage: response.usage,
                model_id: response.model_id,
                response_id: response.response_id,
                finish_reason: response.finish_reason,
                provider_data: response.provider_data,
                effective,
            })
            .await
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
