//! Bounded session machinery for protocols whose exchange boundary is one model turn.
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::mpsc;
use zhir_core::{BoxFuture, Result, error::Error, model::*};
pub(crate) type Exchange =
    dyn Fn(ModelRequest, ModelContext) -> BoxFuture<'static, Result<TurnOutput>> + Send + Sync;
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
    turn_id: String,
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
                    turn_id: self.turn_id.clone(),
                    delta,
                })
                .await
        })
    }
}
pub(crate) fn open(
    open: SessionOpen,
    exchange: Arc<Exchange>,
    capabilities: CapabilitySet,
    negotiate: Arc<Negotiate>,
) -> Result<ModelSession> {
    validate_capabilities(&capabilities)?;
    open.limits.validate()?;
    open.context.cancellation.check()?;
    negotiate(&open.request)?;
    let deadline = zhir_policies::timing::deadline(&open.context.run)?;
    if open.recovery.is_some() {
        return Err(Error::Protocol(
            "turn protocol has no persistent session resume".into(),
        ));
    }
    let (tx, mut rx) = mpsc::channel::<SessionCommand>(open.limits.max_control_commands);
    let (event_tx, event_rx) = mpsc::channel(open.limits.max_session_events);
    let events = Events {
        tx: event_tx,
        sequence: Arc::new(AtomicU64::new(0)),
    };
    let task = SessionTask {
        events,
        exchange,
        negotiate: negotiate.clone(),
        context: open.context,
        deadline,
    };
    tokio::spawn(async move {
        while let Some(command) = rx.recv().await {
            match task.command(command).await {
                Ok(true) => (),
                Ok(false) => break,
                Err(error) => {
                    let _ = task.events.tx.send(Err(error)).await;
                    break;
                }
            }
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
        Capability::Steering,
        Capability::ProfileUpdates,
        Capability::AsyncResults,
        Capability::Resume,
    ]
    .iter()
    .any(|capability| capabilities.supports(*capability))
    {
        return Err(Error::Invalid(
            "turn protocol advertises unsupported session capabilities".into(),
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
}
impl SessionTask {
    async fn command(&self, command: SessionCommand) -> Result<bool> {
        self.context.cancellation.check()?;
        match command.body {
            SessionCommandBody::StartTurn { turn_id, request } => {
                self.turn(command.id, turn_id, *request).await
            }
            SessionCommandBody::UpdateProfile { .. } => Err(Error::Invalid(
                "turn protocol cannot update an active profile".into(),
            )),
            SessionCommandBody::InterruptOutput { .. } => Err(Error::Invalid(
                "turn protocol cannot interrupt natively".into(),
            )),
            SessionCommandBody::DelegationContext { .. }
            | SessionCommandBody::DelegationResult { .. } => Err(Error::Invalid(
                "turn protocol cannot accept native delegation".into(),
            )),
            SessionCommandBody::Close => {
                self.acknowledge(command.id).await?;
                self.events.send(SessionEventBody::Closed).await?;
                Ok(false)
            }
            _ => {
                self.acknowledge(command.id).await?;
                Ok(true)
            }
        }
    }
    async fn acknowledge(&self, command_id: String) -> Result<()> {
        self.events
            .send(SessionEventBody::Acknowledged {
                command_id,
                recovery: None,
            })
            .await
    }
    async fn turn(
        &self,
        command_id: String,
        turn_id: String,
        request: ModelRequest,
    ) -> Result<bool> {
        if command_id.is_empty() || turn_id.is_empty() {
            return Err(Error::Invalid("empty command or turn identity".into()));
        }
        (self.negotiate)(&request)?;
        zhir_policies::timing::check(&self.context.cancellation, self.deadline)?;
        self.acknowledge(command_id).await?;
        let mut context = self.context.clone();
        context.deltas = Some(Arc::new(Deltas {
            events: self.events.clone(),
            turn_id: turn_id.clone(),
            downstream: context.deltas.clone(),
        }));
        let exchanging = (self.exchange)(request, context);
        tokio::pin!(exchanging);
        let response = loop {
            tokio::select! {
                result = &mut exchanging => break result?,
                _ = self.events.tx.closed() => return Ok(false),
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => zhir_policies::timing::check(&self.context.cancellation, self.deadline)?,
            }
        };
        zhir_policies::timing::check(&self.context.cancellation, self.deadline)?;
        response.validate()?;
        self.finish(turn_id, response).await?;
        Ok(true)
    }
    async fn finish(&self, turn_id: String, response: TurnOutput) -> Result<()> {
        let disposition = disposition(&response);
        for (index, output) in response.output.into_iter().enumerate() {
            self.events
                .send(SessionEventBody::Output {
                    turn_id: turn_id.clone(),
                    item_id: format!("{turn_id}:{index}"),
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
            .send(SessionEventBody::TurnFinished {
                turn_id,
                disposition,
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
fn disposition(response: &TurnOutput) -> TurnDisposition {
    if response.provider_turn_pending {
        return TurnDisposition::Continue;
    }
    if response
        .output
        .iter()
        .any(|item| matches!(item, zhir_core::message::Output::RuntimeToolCall { .. }))
    {
        return TurnDisposition::AwaitingTools;
    }
    TurnDisposition::Finished
}
