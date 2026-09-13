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
    tasks: JoinSet<()>,
    session: Option<Arc<dyn SessionSender>>,
    model_media: Option<Arc<dyn MediaSender>>,
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
    needs_turn: bool,
    model_closed: bool,
}

pub(crate) async fn execute(
    config: Arc<Config>,
    request: Request,
    controls: mpsc::Receiver<Control>,
    media: mpsc::Receiver<Packet>,
    media_output: MediaInput,
    cancellation: Cancellation,
    emitter: Emitter,
) -> EngineResult {
    let initial = match &request {
        Request::Recover { checkpoint, .. } => Some(checkpoint.clone()),
        _ => None,
    };
    let context = request.context().clone();
    let deadline = context
        .deadline_at_ms
        .map(|d| Instant::now() + Duration::from_millis(d.saturating_sub(now_ms())));
    let store = config
        .store
        .clone()
        .unwrap_or_else(|| Arc::new(crate::defaults::Ephemeral::new(initial.clone())));
    let fresh = initial.is_none();
    let (mut next, delta) = match &request {
        Request::Start {
            history, options, ..
        } => {
            let session = SessionSnapshot {
                capabilities: None,
                id: new_id(),
                turn_id: None,
                turn_start: 0,
                last_sequence: None,
                disposition: None,
                recovery: None,
                epoch: 0,
                input_closed: options.mode == RunMode::Task,
                closing: false,
                closed: false,
                profile_revision: 0,
                profile: options.profile.clone(),
                negotiated: Default::default(),
                effective: Default::default(),
            };
            (
                Checkpoint {
                    options: *options.clone(),
                    id: new_id(),
                    parent_id: None,
                    revision: 0,
                    context: context.clone(),
                    history: history.clone(),
                    state: State::Running,
                    active: ActiveState {
                        session,
                        operations: BTreeMap::new(),
                        commands: vec![],
                        media: BTreeMap::new(),
                    },
                    metrics: Metrics::default(),
                    fact: Fact::Started,
                },
                HistoryDelta::Initial(history.entries()),
            )
        }
        Request::Recover {
            checkpoint,
            metadata,
            ..
        } => {
            let mut next = checkpoint.as_ref().clone();
            next.id = new_id();
            next.parent_id = Some(checkpoint.id.clone());
            next.revision += 1;
            next.state = State::Running;
            next.context.metadata.extend(metadata.clone());
            next.fact = Fact::Attached;
            (next, HistoryDelta::Unchanged)
        }
    };
    let expired = deadline.is_some_and(|d| Instant::now() >= d);
    if expired {
        next.state = State::Limited {
            reason: LimitReason::Deadline,
        };
    }
    let mut commit = Commit::new(Arc::new(next), delta);
    commit.deadline = Some(
        std::time::Instant::now()
            + Duration::from_millis(request.options().limits.commit_timeout_ms),
    );
    let first = commit.checkpoint.clone();
    tokio::time::timeout(
        Duration::from_millis(request.options().limits.commit_timeout_ms),
        store.commit(commit),
    )
    .await
    .map_err(|_| RunError {
        error: Error::Storage(
            "initial commit timed out; reload the durable head before retrying".into(),
        ),
        last_checkpoint: initial.clone(),
    })?
    .map_err(|error| RunError {
        error,
        last_checkpoint: initial,
    })?;
    let (work_tx, work) = mpsc::channel(first.options.limits.max_session_events);
    let sent = first
        .active
        .commands
        .iter()
        .filter(|command| command.sent)
        .map(|command| command.id.clone())
        .collect();
    let mut engine = Engine {
        config,
        store,
        current: first,
        catalog: crate::catalog::select_catalog(
            &RuntimeToolSelection::All,
            Arc::new(crate::defaults::EmptyTools),
        )
        .expect("empty catalog"),
        deadline,
        cancellation,
        emitter,
        controls,
        media,
        media_output,
        work_tx,
        work,
        tasks: JoinSet::new(),
        session: None,
        model_media: None,
        operation_controls: BTreeMap::new(),
        tokens: BTreeMap::new(),
        admitting: BTreeSet::new(),
        sent,
        sending: false,
        input_sending: false,
        media_pending: false,
        pending_replies: BTreeSet::new(),
        pending_starts: BTreeSet::new(),
        session_sequence: None,
        needs_turn: fresh,
        model_closed: false,
    };
    let result = engine.run(request).await;
    engine.stop_workers().await;
    match result {
        Ok(()) => Ok(engine.current),
        Err(error) if matches!(error, Error::Storage(_) | Error::Conflict { .. }) => {
            Err(RunError {
                error,
                last_checkpoint: Some(engine.current),
            })
        }
        Err(error) => {
            let state = if matches!(error, Error::Cancelled) {
                State::Cancelled
            } else if matches!(error, Error::Deadline) {
                State::Limited {
                    reason: LimitReason::Deadline,
                }
            } else {
                State::Failed {
                    error: crate::failure::failure(&error),
                }
            };
            let action = if matches!(state, State::Cancelled) {
                ControlAction::Cancelled
            } else if matches!(state, State::Limited { .. }) {
                ControlAction::Limited
            } else {
                ControlAction::Failed
            };
            let mut next = engine.current.as_ref().clone();
            next.state = state;
            for operation in next.active.operations.values_mut() {
                if matches!(
                    operation.state,
                    OperationState::Running | OperationState::Cancelling
                ) {
                    operation.state = OperationState::Unknown;
                }
            }
            match engine
                .commit(next, Fact::Control { action }, HistoryDelta::Unchanged)
                .await
            {
                Ok(()) => Ok(engine.current),
                Err(error) => Err(RunError {
                    error,
                    last_checkpoint: Some(engine.current),
                }),
            }
        }
    }
}
impl Engine {
    fn session_capabilities(&self) -> &CapabilitySet {
        self.session.as_ref().map_or_else(
            || self.config.model.capabilities(),
            |input| input.capabilities(),
        )
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<zhir_core::profile::NegotiatedProfile> {
        self.session.as_ref().map_or_else(
            || self.config.model.negotiate(request),
            |input| input.negotiate(request),
        )
    }
    fn check(&self) -> Result<()> {
        if self.deadline.is_some_and(|d| Instant::now() >= d) {
            return Err(Error::Deadline);
        }
        self.cancellation.check()
    }
    async fn commit(
        &mut self,
        mut next: Checkpoint,
        fact: Fact,
        history: HistoryDelta,
    ) -> Result<()> {
        next.id = new_id();
        next.parent_id = Some(self.current.id.clone());
        next.revision = self.current.revision + 1;
        next.fact = fact;
        let mut commit = Commit::new(Arc::new(next), history);
        commit.deadline = Some(
            std::time::Instant::now()
                + Duration::from_millis(self.current.options.limits.commit_timeout_ms),
        );
        let next = commit.checkpoint.clone();
        tokio::time::timeout(
            Duration::from_millis(self.current.options.limits.commit_timeout_ms),
            self.store.commit(commit),
        )
        .await
        .map_err(|_| {
            Error::Storage(
                "checkpoint commit timed out; reload the durable head before retrying".into(),
            )
        })??;
        self.current = next;
        self.emitter.emit(EventData::CheckpointCommitted {
            checkpoint_id: self.current.id.clone(),
            revision: self.current.revision,
            state: self.current.state.kind(),
            fact: self.current.fact.clone(),
        });
        Ok(())
    }
    fn model_request(&self, history_count: usize) -> ModelRequest {
        let messages = zhir_core::model::conversation(
            self.current
                .history
                .entries()
                .into_iter()
                .take(history_count),
        );
        ModelRequest {
            messages,
            runtime_tools: self.catalog.specs(),
            provider_tools: self.current.options.provider_tools.clone(),
            profile: self.current.active.session.profile.clone(),
            tool_choice: self.current.options.tool_choice.clone(),
            response_format: self.current.options.response_format.clone(),
            stream: self.current.options.stream,
        }
    }
    async fn suspend(&mut self, reason: &str) -> Result<()> {
        let mut next = self.current.as_ref().clone();
        next.state = State::Suspended {
            suspension: Suspension {
                reason: reason.into(),
                source: "runtime".into(),
                wait_id: None,
                metadata: Default::default(),
            },
        };
        self.commit(
            next,
            Fact::Control {
                action: ControlAction::Suspended,
            },
            HistoryDelta::Unchanged,
        )
        .await
    }
    async fn run(&mut self, request: Request) -> Result<()> {
        if self.current.state.terminal() {
            return Ok(());
        }
        self.check()?;
        let provider = self.config.runtime_tools.clone();
        let opening = provider.open_catalog(CatalogContext {
            run: self.current.context.clone(),
            cancellation: self.cancellation.clone(),
        });
        tokio::pin!(opening);
        loop {
            tokio::select! {
                result = &mut opening => {
                    self.catalog = crate::catalog::select_catalog(&self.current.options.runtime_tools, result?)?;
                    break;
                },
                control = self.controls.recv(), if !self.controls.is_closed() || !self.controls.is_empty() => if let Some(control) = control {self.control(control).await?;if !self.current.state.active() {return Ok(());}},
                _ = tokio::time::sleep(Duration::from_millis(10)) => self.check()?,
            }
        }
        if let Request::Recover {
            resolutions,
            messages,
            ..
        } = request
        {
            for resolution in resolutions {
                self.resolve(resolution).await?;
            }
            for message in messages {
                self.insert(message, "resume".into()).await?;
            }
            if self
                .current
                .active
                .operations
                .values()
                .any(|o| o.state == OperationState::Unknown)
            {
                return self.suspend("RecoveryRequired").await;
            }
            let in_turn = self.current.active.session.turn_id.is_some()
                && self.current.active.session.disposition.is_none();
            if (in_turn || self.current.active.commands.iter().any(|c| c.sent))
                && self.current.active.session.recovery.is_none()
            {
                return self.suspend("RecoveryRequired").await;
            }
            if !self.current.active.media.is_empty()
                && self.current.active.session.recovery.is_none()
            {
                return self.suspend("RecoveryRequired").await;
            }
            self.needs_turn = !in_turn;
        }
        let open = SessionOpen {
            request: self.model_request(self.current.history.len()),
            session_id: self.current.active.session.id.clone(),
            after_sequence: self
                .current
                .active
                .session
                .recovery
                .as_ref()
                .and(self.current.active.session.last_sequence),
            epoch: self.current.active.session.epoch,
            limits: self.current.options.limits.clone(),
            recovery: self.current.active.session.recovery.clone(),
            context: ModelContext {
                run: self.current.context.clone(),
                cancellation: self.cancellation.clone(),
                deltas: None,
            },
        };
        let model = self.config.model.clone();
        let opening = model.open_session(open);
        tokio::pin!(opening);
        let session = loop {
            tokio::select! {
                result = &mut opening => match result { Ok(session) => break session, Err(_) if self.current.active.session.recovery.is_some() => return self.suspend("RecoveryRequired").await, Err(error) => return Err(error) },
                control = self.controls.recv(), if !self.controls.is_closed() || !self.controls.is_empty() => if let Some(control) = control { self.control(control).await?; if !self.current.state.active() { return Ok(()); } },
                _ = tokio::time::sleep(Duration::from_millis(10)) => self.check()?,
            }
        };
        let mut next = self.current.as_ref().clone();
        if next.active.session.recovery.is_some()
            && next
                .active
                .session
                .capabilities
                .as_ref()
                .is_some_and(|caps| caps != session.input.capabilities())
        {
            return self.suspend("RecoveryRequired").await;
        }
        next.active.session.capabilities = Some(session.input.capabilities().clone());
        if next.active.session.recovery.is_none() {
            next.active.session.last_sequence = None;
        }
        self.commit(
            next,
            Fact::Command {
                command_id: new_id(),
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        self.session = Some(session.input);
        self.model_media = session.media_input;
        let tx = self.work_tx.clone();
        let mut receiver = session.output;
        self.tasks.spawn(async move {
            loop {
                let event = receiver.receive().await;
                let stop = !matches!(event, Ok(Some(_)));
                if tx.send(Work::Model(event)).await.is_err() || stop {
                    break;
                }
            }
        });
        if let Some(mut media) = session.media_output {
            self.media_pending = true;
            let mut previous: BTreeMap<_, _> = self
                .current
                .active
                .media
                .iter()
                .map(|(key, cursor)| (key.clone(), cursor.sealed.clone()))
                .collect();
            let resources =
                self.config.resources.clone().ok_or_else(|| {
                    Error::Invalid("media session requires a resource store".into())
                })?;
            let tx = self.work_tx.clone();
            let output = self.media_output.clone();
            let limit = self.current.options.limits.max_media_chunk_bytes;
            self.tasks.spawn(async move {
                loop {
                    let result: Result<bool> = async {
                        let Some(chunk) = media.receive().await? else {
                            return Ok(false);
                        };
                        chunk.validate(limit)?;
                        let key = media_key("output", &chunk);
                        let reference =
                            seal_media(resources.as_ref(), &chunk, previous.get(&key).cloned())
                                .await?;
                        previous.insert(key, reference.clone());
                        let (ack, rx) = oneshot::channel();
                        tx.send(Work::MediaReady(chunk.clone(), reference, ack))
                            .await
                            .map_err(|_| Error::Cancelled)?;
                        if rx.await.map_err(|_| Error::Cancelled)? {
                            output.send(chunk).await?;
                        }
                        Ok(true)
                    }
                    .await;
                    match result {
                        Ok(true) => (),
                        Ok(false) => {
                            let _ = tx.send(Work::MediaDone).await;
                            break;
                        }
                        Err(error) => {
                            let _ = tx.send(Work::MediaError(error)).await;
                            break;
                        }
                    }
                }
            });
        }
        let recovering: Vec<_> = self
            .current
            .active
            .operations
            .values()
            .filter(|o| {
                matches!(
                    o.state,
                    OperationState::Running | OperationState::Waiting | OperationState::Cancelling
                )
            })
            .cloned()
            .collect();
        for record in recovering {
            if let OperationOwner::RuntimeTool { .. } = &record.owner {
                let call = self.call(&record)?;
                match self.catalog.bind(&call) {
                    Ok(binding) => self.spawn_tool(record.id.clone(), binding, Some(record))?,
                    Err(error) => {
                        self.unknown(&record.id, format!("recovery binding failed: {error}"))
                            .await?
                    }
                }
            }
        }
        loop {
            self.check()?;
            if !self.current.state.active() {
                return Ok(());
            }
            self.dispatch_commands().await?;
            self.admit().await?;
            let busy = self
                .current
                .active
                .operations
                .values()
                .any(|o| !o.state.terminal());
            let runtime_busy = self.current.active.operations.values().any(|op| {
                !op.state.terminal() && matches!(op.owner, OperationOwner::RuntimeTool { .. })
            });
            if (!busy
                || (self.needs_turn
                    && !runtime_busy
                    && self.current.active.session.disposition == Some(TurnDisposition::Continue)))
                && self.current.active.commands.is_empty()
                && !self.sending
            {
                if self.needs_turn {
                    self.start_turn().await?;
                    continue;
                }
                if self.current.active.session.disposition == Some(TurnDisposition::Finished)
                    && (self.current.options.mode == RunMode::Task
                        || self.current.active.session.input_closed)
                {
                    if self.media_pending || self.input_sending {
                        if !self.model_closed
                            && !self.current.active.session.closing
                            && !self.input_sending
                        {
                            self.prepare_command(CommandIntent::Close).await?;
                        }
                    } else {
                        let content = conversation(
                            self.current
                                .history
                                .entries()
                                .into_iter()
                                .skip(self.current.active.session.turn_start),
                        )
                        .into_iter()
                        .filter_map(|message| match message {
                            Message::Assistant { output, .. } => {
                                Some(zhir_core::message::visible_content(&output))
                            }
                            _ => None,
                        })
                        .flatten()
                        .collect();
                        let mut next = self.current.as_ref().clone();
                        next.state = State::Completed { content };
                        return self
                            .commit(
                                next,
                                Fact::Control {
                                    action: ControlAction::Finished,
                                },
                                HistoryDelta::Unchanged,
                            )
                            .await;
                    }
                }
            }
            if busy
                && (self.current.options.mode == RunMode::Task
                    || self
                        .current
                        .active
                        .operations
                        .values()
                        .any(|op| op.state == OperationState::Unknown))
                && self.pending_replies.is_empty()
                && self.pending_starts.is_empty()
                && self
                    .current
                    .active
                    .operations
                    .values()
                    .filter(|o| !o.state.terminal())
                    .all(|o| matches!(o.state, OperationState::Waiting | OperationState::Unknown))
                && self.current.active.session.disposition.is_some()
            {
                return self
                    .suspend(
                        if self
                            .current
                            .active
                            .operations
                            .values()
                            .any(|o| o.state == OperationState::Unknown)
                        {
                            "RecoveryRequired"
                        } else {
                            "InputRequired"
                        },
                    )
                    .await;
            }
            tokio::select! { biased;
                control = self.controls.recv(), if !self.controls.is_closed() || !self.controls.is_empty() => if let Some(control) = control { self.control(control).await?; },
                work = self.work.recv() => if let Some(work) = work { self.handle(work).await?; },
                packet = self.media.recv(), if self.model_media.is_some() && !self.input_sending && (!self.media.is_closed() || !self.media.is_empty()) => if let Some(packet) = packet { self.input_media(packet)?; },
                _ = tokio::time::sleep(Duration::from_millis(10)) => (),
            }
        }
    }
    async fn prepare_command(&mut self, intent: CommandIntent) -> Result<String> {
        if self.current.active.commands.len() >= self.current.options.limits.max_control_commands {
            return Err(Error::Invalid("pending command capacity exceeded".into()));
        }
        let end_input = matches!(intent, CommandIntent::EndInput);
        let id = new_id();
        let mut next = self.current.as_ref().clone();
        match &intent {
            CommandIntent::Close => next.active.session.closing = true,
            CommandIntent::Interrupt { .. } => next.active.session.epoch += 1,
            CommandIntent::EndInput => next.active.session.input_closed = true,
            _ => (),
        }
        next.active.commands.push(PendingCommand {
            id: id.clone(),
            intent,
            sent: false,
        });
        self.commit(
            next,
            Fact::Command {
                command_id: id.clone(),
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        if end_input {
            self.media.close();
        }
        Ok(id)
    }
    async fn dispatch_commands(&mut self) -> Result<()> {
        let Some(input) = self.session.clone() else {
            return Ok(());
        };
        if self.sending {
            return Ok(());
        }
        for command in self.current.active.commands.clone() {
            if self.sent.contains(&command.id) {
                continue;
            }
            if matches!(
                command.intent,
                CommandIntent::EndInput | CommandIntent::Close
            ) && (self.input_sending || !self.media.is_empty())
            {
                continue;
            }
            let busy_turn = self.current.active.session.disposition.is_none()
                && self.current.active.session.turn_id.is_some();
            if busy_turn
                && matches!(command.intent, CommandIntent::ToolResult { .. })
                && !self
                    .session_capabilities()
                    .supports(Capability::AsyncResults)
            {
                continue;
            }
            if busy_turn
                && matches!(command.intent, CommandIntent::Input { .. })
                && !self.session_capabilities().supports(Capability::Steering)
            {
                continue;
            }
            let body = match command.intent {
                CommandIntent::StartTurn {
                    turn_id,
                    history_count,
                    profile,
                    runtime_tools,
                } => {
                    let mut request = self.model_request(history_count);
                    request.profile = profile;
                    request.runtime_tools = runtime_tools;
                    SessionCommandBody::StartTurn {
                        turn_id,
                        request: Box::new(request),
                    }
                }
                CommandIntent::Input { entry } => SessionCommandBody::Input {
                    message: self
                        .current
                        .history
                        .get(entry)
                        .ok_or_else(|| Error::Protocol("missing input entry".into()))?
                        .message
                        .clone(),
                },
                CommandIntent::ToolResult {
                    operation_id,
                    entry,
                } => {
                    let Message::RuntimeTool { outcome, .. } = &self
                        .current
                        .history
                        .get(entry)
                        .ok_or_else(|| Error::Protocol("missing result entry".into()))?
                        .message
                    else {
                        return Err(Error::Protocol("command result entry mismatch".into()));
                    };
                    SessionCommandBody::ToolResult {
                        origin: self
                            .current
                            .active
                            .operations
                            .get(&operation_id)
                            .ok_or_else(|| Error::Protocol("result origin missing".into()))?
                            .origin
                            .clone(),
                        operation_id,
                        outcome: outcome.clone(),
                    }
                }
                CommandIntent::UpdateProfile { revision, profile } => {
                    SessionCommandBody::UpdateProfile { revision, profile }
                }
                CommandIntent::Interrupt { turn_id } => SessionCommandBody::Interrupt { turn_id },
                CommandIntent::EndInput => SessionCommandBody::EndInput,
                CommandIntent::Close => SessionCommandBody::Close,
            };
            // A crash after this commit is an explicitly uncertain send, never an implicit retry.
            let mut next = self.current.as_ref().clone();
            next.active
                .commands
                .iter_mut()
                .find(|c| c.id == command.id)
                .expect("pending command")
                .sent = true;
            self.commit(
                next,
                Fact::Command {
                    command_id: command.id.clone(),
                },
                HistoryDelta::Unchanged,
            )
            .await?;
            self.check()?;
            self.sent.insert(command.id.clone());
            self.sending = true;
            let tx = self.work_tx.clone();
            let input = input.clone();
            self.tasks.spawn(async move {
                let result = input
                    .send(SessionCommand {
                        id: command.id,
                        body,
                    })
                    .await;
                let _ = tx.send(Work::CommandSent(result)).await;
            });
            break;
        }
        Ok(())
    }
    async fn start_turn(&mut self) -> Result<()> {
        if self.current.metrics.model_turns >= self.current.options.limits.max_model_turns {
            let mut next = self.current.as_ref().clone();
            next.state = State::Limited {
                reason: LimitReason::ModelTurns,
            };
            return self
                .commit(
                    next,
                    Fact::Control {
                        action: ControlAction::Limited,
                    },
                    HistoryDelta::Unchanged,
                )
                .await;
        }
        if self.current.active.operations.is_empty()
            && self.current.active.commands.is_empty()
            && self.current.active.media.is_empty()
            && let Some(reducer) = self.config.history_reducer.clone()
            && let Some(rewrite) = interruptible(
                reducer.reduce(self.current.clone()),
                self.cancellation.clone(),
                self.deadline,
            )
            .await?
        {
            let mut next = self.current.as_ref().clone();
            next.history = History::from_entries(rewrite.entries.clone())?;
            next.active.session.turn_start = next.history.len();
            self.commit(
                next,
                Fact::HistoryRewrite {
                    reason: rewrite.reason,
                },
                HistoryDelta::Replace(rewrite.entries),
            )
            .await?;
        }
        let turn_id = new_id();
        let request = self.model_request(self.current.history.len());
        let negotiated = self.negotiate(&request)?;
        let id = new_id();
        let mut next = self.current.as_ref().clone();
        next.active.session.turn_id = Some(turn_id.clone());
        next.active.session.turn_start = next.history.len();
        next.active.session.disposition = None;
        next.active.session.negotiated = negotiated;
        next.active.commands.push(PendingCommand {
            id: id.clone(),
            intent: CommandIntent::StartTurn {
                turn_id,
                history_count: next.history.len(),
                profile: next.active.session.profile.clone(),
                runtime_tools: self.catalog.specs(),
            },
            sent: false,
        });
        self.commit(
            next,
            Fact::Command { command_id: id },
            HistoryDelta::Unchanged,
        )
        .await?;
        self.needs_turn = false;
        Ok(())
    }
    fn call(&self, operation: &OperationRecord) -> Result<RuntimeToolCall> {
        let entry = self
            .current
            .history
            .get(operation.call_entry)
            .ok_or_else(|| Error::Protocol("missing operation call".into()))?;
        if let Message::Assistant { output, .. } = &entry.message {
            for item in output {
                if let Output::RuntimeToolCall { call } = item
                    && call.id == operation.origin.call_id
                {
                    return Ok(call.clone());
                }
            }
        }
        Err(Error::Protocol("operation call entry mismatch".into()))
    }
    fn tool_context(&self, id: String, cancellation: Cancellation) -> RuntimeToolContext {
        RuntimeToolContext {
            run: self.current.context.clone(),
            operation_id: id.clone(),
            cancellation,
            progress: Some(Arc::new(ToolProgress {
                operation_id: id.clone(),
                sender: self.work_tx.clone(),
            })),
        }
    }
    fn spawn_tool(
        &mut self,
        id: String,
        binding: Arc<dyn RuntimeToolBinding>,
        recovery: Option<OperationRecord>,
    ) -> Result<()> {
        self.check()?;
        self.pending_starts.insert(id.clone());
        let token = Cancellation::default();
        let context = self.tool_context(id.clone(), token.clone());
        self.tokens.insert(id.clone(), token);
        let tx = self.work_tx.clone();
        self.tasks.spawn(async move {
            let result = match recovery {
                Some(record) => binding
                    .recover(record, context)
                    .await
                    .map_err(|error| Error::Protocol(format!("recovery failed: {error}"))),
                None => binding.start(context).await,
            };
            let _ = tx.send(Work::Started(id, result)).await;
        });
        Ok(())
    }
    async fn admit(&mut self) -> Result<()> {
        let running = self
            .current
            .active
            .operations
            .values()
            .filter(|o| {
                matches!(
                    o.state,
                    OperationState::Running | OperationState::Cancelling
                )
            })
            .count();
        if running + self.admitting.len() + self.current.active.commands.len()
            >= self.current.options.limits.max_control_commands
            || running + self.admitting.len()
                >= self.current.options.limits.max_runtime_tool_concurrency
        {
            return Ok(());
        }
        let records: Vec<_> = self
            .current
            .active
            .operations
            .values()
            .filter(|o| o.state == OperationState::Queued && !self.admitting.contains(&o.id))
            .cloned()
            .collect();
        let mut candidates = vec![];
        let mut ids = BTreeMap::new();
        for record in records {
            if matches!(record.owner, OperationOwner::RuntimeTool { .. }) {
                let call = self.call(&record)?;
                let mut candidate = call.clone();
                candidate.id = record.id.clone();
                ids.insert(record.id, call);
                candidates.push(candidate);
            }
        }
        if candidates.is_empty() {
            return Ok(());
        }
        let specs = self
            .catalog
            .specs()
            .into_iter()
            .map(|s| (s.name.clone(), s))
            .collect();
        let admission = self.config.scheduler.select(&candidates, &specs)?;
        let mut bindings = vec![];
        let mut requests = vec![];
        let mut selected = BTreeSet::new();
        for call in admission.calls.into_iter().take(
            self.current.options.limits.max_runtime_tool_concurrency
                - running
                - self.admitting.len(),
        ) {
            if !candidates.contains(&call) || !selected.insert(call.id.clone()) {
                return Err(Error::Invalid(
                    "scheduler returned duplicate or unknown call".into(),
                ));
            }
            let id = call.id.clone();
            let call = ids[&id].clone();
            let binding = match self.catalog.bind(&call) {
                Ok(binding) => binding,
                Err(error) => {
                    self.finish(
                        &id,
                        RuntimeToolOutcome::Failure {
                            error: crate::failure::failure(&error),
                        },
                    )
                    .await?;
                    continue;
                }
            };
            if (running > 0 || !bindings.is_empty())
                && (!admission.parallel || !binding.spec().execution.parallel)
            {
                break;
            }
            if running > 0
                && self
                    .current
                    .active
                    .operations
                    .values()
                    .filter(|o| o.state == OperationState::Running)
                    .any(|o| match &o.owner {
                        OperationOwner::RuntimeTool { name } => !specs
                            .get(name)
                            .is_some_and(|s: &RuntimeToolSpec| s.execution.parallel),
                        _ => false,
                    })
            {
                break;
            }
            requests.push(ApprovalRequest {
                call,
                spec: binding.spec().clone(),
            });
            self.admitting.insert(id.clone());
            bindings.push((id, binding));
        }
        if bindings.is_empty() {
            return Ok(());
        }
        let approval = self.config.approval.clone();
        let context = self.current.context.clone();
        let tx = self.work_tx.clone();
        self.tasks.spawn(async move {
            let result = if let Some(approval) = approval {
                approval.decide(requests, context).await
            } else {
                Ok(vec![ApprovalDecision::Allow; bindings.len()])
            };
            let _ = tx.send(Work::Admitted(bindings, result)).await;
        });
        Ok(())
    }
    async fn handle(&mut self, work: Work) -> Result<()> {
        if let Work::Started(id, _) = &work {
            self.pending_starts.remove(id);
        }
        match work {
            Work::Model(Ok(Some(event))) => self.model_event(event).await,
            Work::Model(Ok(None)) => {
                self.model_closed = true;
                if self.current.active.session.disposition.is_none() {
                    self.suspend("RecoveryRequired").await
                } else {
                    Ok(())
                }
            }
            Work::Model(Err(error)) => {
                if matches!(error, Error::Uncertain(_))
                    || self.current.active.session.recovery.is_some()
                    || self
                        .current
                        .active
                        .operations
                        .values()
                        .any(|op| !op.state.terminal())
                {
                    self.suspend("RecoveryRequired").await
                } else {
                    Err(error)
                }
            }
            Work::Started(id, Ok(ToolExecution::Finished(outcome))) => {
                self.finish(&id, outcome).await
            }
            Work::Started(id, Ok(ToolExecution::Active(handle))) => {
                let mut next = self.current.as_ref().clone();
                let op = next
                    .active
                    .operations
                    .get_mut(&id)
                    .ok_or_else(|| Error::Protocol("unknown started operation".into()))?;
                op.recovery = handle.recovery.or(op.recovery.take());
                let state = op.state;
                self.commit(
                    next,
                    Fact::Operation {
                        operation_id: id.clone(),
                        state,
                    },
                    HistoryDelta::Unchanged,
                )
                .await?;
                if state == OperationState::Cancelling {
                    let control = handle.control.clone();
                    self.tasks.spawn(async move {
                        let _ = control.cancel().await;
                    });
                }
                self.operation_controls.insert(id.clone(), handle.control);
                let tx = self.work_tx.clone();
                let mut events = handle.events;
                self.tasks.spawn(async move {
                    loop {
                        let event = events.receive().await;
                        let stop = !matches!(event, Ok(Some(_)));
                        if tx.send(Work::Operation(id.clone(), event)).await.is_err() || stop {
                            break;
                        }
                    }
                });
                Ok(())
            }
            Work::Started(id, Err(Error::RuntimeTool(error))) => {
                self.finish(&id, RuntimeToolOutcome::Failure { error })
                    .await
            }
            Work::Started(id, Err(error)) => self.unknown(&id, error.to_string()).await,
            Work::Operation(id, Ok(Some(event))) => self.operation_event(&id, event).await,
            Work::Operation(id, _) => {
                if self
                    .current
                    .active
                    .operations
                    .get(&id)
                    .is_some_and(|op| !op.state.terminal())
                {
                    self.unknown(&id, "operation stream ended without a final result".into())
                        .await
                } else {
                    Ok(())
                }
            }
            Work::Admitted(bindings, decisions) => {
                let decisions = decisions?;
                if decisions.len() != bindings.len() {
                    return Err(Error::Protocol(
                        "approval decision count differs from requests".into(),
                    ));
                }
                for (id, _) in &bindings {
                    self.admitting.remove(id);
                }
                if let Some(suspension) = decisions.iter().find_map(|d| match d {
                    ApprovalDecision::Suspend(s) => Some(s.clone()),
                    _ => None,
                }) {
                    let mut next = self.current.as_ref().clone();
                    next.state = State::Suspended { suspension };
                    return self
                        .commit(
                            next,
                            Fact::Control {
                                action: ControlAction::Suspended,
                            },
                            HistoryDelta::Unchanged,
                        )
                        .await;
                }
                for ((id, binding), decision) in bindings.into_iter().zip(decisions) {
                    if self
                        .current
                        .active
                        .operations
                        .get(&id)
                        .is_none_or(|o| o.state != OperationState::Queued)
                    {
                        continue;
                    }
                    match decision {
                        ApprovalDecision::Allow => {
                            self.check()?;
                            if self.current.metrics.runtime_tool_calls
                                >= self.current.options.limits.max_runtime_tool_calls
                            {
                                let mut next = self.current.as_ref().clone();
                                next.state = State::Limited {
                                    reason: LimitReason::RuntimeToolCalls,
                                };
                                return self
                                    .commit(
                                        next,
                                        Fact::Control {
                                            action: ControlAction::Limited,
                                        },
                                        HistoryDelta::Unchanged,
                                    )
                                    .await;
                            }
                            let mut next = self.current.as_ref().clone();
                            next.active.operations.get_mut(&id).expect("queued").state =
                                OperationState::Running;
                            next.metrics.runtime_tool_calls += 1;
                            self.commit(
                                next,
                                Fact::Operation {
                                    operation_id: id.clone(),
                                    state: OperationState::Running,
                                },
                                HistoryDelta::Unchanged,
                            )
                            .await?;
                            self.spawn_tool(id, binding, None)?;
                        }
                        ApprovalDecision::Deny(reason) => {
                            self.finish(
                                &id,
                                RuntimeToolOutcome::Failure {
                                    error: Failure::new("approval_denied", reason),
                                },
                            )
                            .await?
                        }
                        ApprovalDecision::Suspend(_) => {
                            unreachable!("suspensions handled before admission")
                        }
                    }
                }
                Ok(())
            }
            Work::MediaReady(chunk, reference, reply) => {
                let valid = chunk.epoch == self.current.active.session.epoch
                    && self.current.active.session.turn_id.as_ref() == Some(&chunk.turn_id);
                if valid {
                    self.seal_cursor("output", &chunk, reference).await?;
                }
                let _ = reply.send(valid);
                Ok(())
            }
            Work::InputReady(chunk, reference, reply) => {
                let valid = chunk.epoch == self.current.active.session.epoch
                    && self.current.active.session.turn_id.as_ref() == Some(&chunk.turn_id);
                if valid {
                    self.seal_cursor("input", &chunk, reference).await?;
                }
                let _ = reply.send(valid);
                Ok(())
            }
            Work::InputSent(result) => {
                self.input_sending = false;
                if result.is_err() {
                    self.suspend("RecoveryRequired").await
                } else {
                    Ok(())
                }
            }
            Work::MediaDone => {
                self.media_pending = false;
                Ok(())
            }
            Work::ReplyDone(id, result) => {
                self.pending_replies.remove(&id);
                if self
                    .current
                    .active
                    .operations
                    .get(&id)
                    .is_some_and(|op| op.state == OperationState::Unknown)
                {
                    if let Err(error) = result {
                        self.unknown(&id, error.to_string()).await?;
                    } else {
                        self.local_operation_update(&id, OperationState::Running, None)
                            .await?;
                    }
                }
                Ok(())
            }
            Work::Progress(id, value) => {
                self.emitter.emit(EventData::OperationProgress {
                    operation_id: id,
                    value,
                });
                Ok(())
            }
            Work::MediaError(error) => Err(error),
            Work::CommandSent(result) => {
                self.sending = false;
                if result.is_err() {
                    self.suspend("RecoveryRequired").await
                } else {
                    Ok(())
                }
            }
        }
    }
    async fn model_event(&mut self, event: SessionEvent) -> Result<()> {
        if self
            .session_sequence
            .is_some_and(|sequence| event.sequence <= sequence)
        {
            return Err(Error::Protocol(
                "session event sequence did not increase".into(),
            ));
        }
        self.session_sequence = Some(event.sequence);
        if self
            .current
            .active
            .session
            .last_sequence
            .is_some_and(|sequence| event.sequence <= sequence)
        {
            return Ok(());
        }
        if let SessionEventBody::Delta { delta, .. } = event.body {
            self.emitter.emit(EventData::ModelDelta { delta });
            return Ok(());
        }
        let mut next = self.current.as_ref().clone();
        next.active.session.last_sequence = Some(event.sequence);
        let mut entries = vec![];
        match event.body {
            SessionEventBody::Acknowledged {
                command_id,
                recovery,
            } => {
                let index = next
                    .active
                    .commands
                    .iter()
                    .position(|c| c.id == command_id)
                    .ok_or_else(|| {
                        Error::Protocol("acknowledgement has no pending command".into())
                    })?;
                let command = next.active.commands.remove(index);
                if let CommandIntent::ToolResult { operation_id, .. } = &command.intent {
                    next.active.operations.remove(operation_id);
                }
                if let CommandIntent::UpdateProfile { revision, profile } = command.intent {
                    let mut request = self.model_request(next.history.len());
                    request.profile = profile.clone();
                    next.active.session.negotiated = self.negotiate(&request)?;
                    next.active.session.effective.values = next
                        .active
                        .session
                        .negotiated
                        .selected
                        .keys()
                        .map(|key| (key.clone(), zhir_core::profile::Confirmation::Unknown))
                        .collect();
                    next.active.session.profile_revision = revision;
                    next.active.session.profile = profile;
                }
                if let Some(reference) = recovery {
                    next.active.session.recovery = Some(reference);
                }
                self.sent.remove(&command_id);
            }
            SessionEventBody::Output {
                turn_id,
                item_id,
                caller_id,
                output,
            } => {
                if next.active.session.turn_id.as_ref() != Some(&turn_id) {
                    return Err(Error::Protocol("output belongs to another turn".into()));
                }
                zhir_core::message::validate_output(std::slice::from_ref(&output))?;
                let call_id = match &output {
                    Output::RuntimeToolCall { call } => call.id.clone(),
                    Output::ProviderToolCall { call } => call.id.clone(),
                    _ => item_id.clone(),
                };
                if item_id.is_empty() || caller_id.is_empty() {
                    return Err(Error::Protocol(
                        "output requires nonempty item and caller identities".into(),
                    ));
                }
                let output_turn = turn_id.clone();
                let mut origin = CallRef {
                    session_id: next.active.session.id.clone(),
                    turn_id,
                    caller_id,
                    call_id,
                };
                if let Output::ProviderToolCall { call } = &output
                    && let Some(existing) = next.active.operations.values().find(|op| !op.state.terminal() && op.origin.call_id == call.id && op.origin.caller_id == origin.caller_id && matches!(&op.owner, OperationOwner::Provider { provider } if provider == &call.provider)) {
                        origin = existing.origin.clone();
                    }
                let entry = HistoryEntry {
                    id: serde_json::json!([
                        next.active.session.id,
                        output_turn,
                        origin.caller_id,
                        item_id
                    ])
                    .to_string(),
                    origin: Some(origin.clone()),
                    message: Message::Assistant {
                        output: vec![output.clone()],
                        provider_data: serde_json::Value::Null,
                    },
                };
                if let Some(previous) = next.history.by_id(&entry.id) {
                    if previous != &entry {
                        return Err(Error::Protocol("conflicting output identity".into()));
                    }
                    return self
                        .commit(
                            next,
                            Fact::Session {
                                session_id: self.current.active.session.id.clone(),
                                sequence: event.sequence,
                            },
                            HistoryDelta::Unchanged,
                        )
                        .await;
                }
                if let Output::RuntimeToolCall { call } = output {
                    if next
                        .active
                        .operations
                        .values()
                        .filter(|op| !op.state.terminal())
                        .count()
                        >= next.options.limits.max_inflight_operations
                    {
                        return Err(Error::Invalid(
                            "inflight operation capacity exceeded".into(),
                        ));
                    }
                    let id = new_id();
                    next.active.operations.insert(
                        id.clone(),
                        OperationRecord {
                            id,
                            origin,
                            owner: OperationOwner::RuntimeTool { name: call.name },
                            state: OperationState::Queued,
                            call_entry: next.history.len(),
                            result_entry: None,
                            recovery: None,
                            last_sequence: None,
                            last_update: None,
                            wait: None,
                        },
                    );
                } else if let Output::ProviderToolCall { call } = output {
                    let id = provider_operation_id(&origin);
                    let state = match call.status {
                        ProviderToolStatus::Pending | ProviderToolStatus::Running => {
                            OperationState::Running
                        }
                        ProviderToolStatus::Failed | ProviderToolStatus::Incomplete => {
                            OperationState::Failed
                        }
                        ProviderToolStatus::Cancelled => OperationState::Cancelled,
                        _ => OperationState::Succeeded,
                    };
                    let record =
                        next.active
                            .operations
                            .entry(id.clone())
                            .or_insert(OperationRecord {
                                id,
                                origin,
                                owner: OperationOwner::Provider {
                                    provider: call.provider,
                                },
                                state,
                                call_entry: next.history.len(),
                                result_entry: None,
                                recovery: None,
                                last_sequence: None,
                                last_update: None,
                                wait: None,
                            });
                    record.state = state;
                    record.result_entry = state.terminal().then_some(next.history.len());
                }
                entries.push(entry);
            }
            SessionEventBody::TurnFinished {
                turn_id,
                disposition,
                usage,
                model_id,
                response_id,
                finish_reason,
                provider_data,
                effective,
            } => {
                if next.active.session.turn_id.as_ref() != Some(&turn_id)
                    || next.active.session.disposition.is_some()
                {
                    return Err(Error::Protocol(
                        "duplicate or foreign turn completion".into(),
                    ));
                }
                next.active.operations.retain(|_, op| {
                    !matches!(op.owner, OperationOwner::Provider { .. }) || !op.state.terminal()
                });
                next.active.session.disposition = Some(disposition.clone());
                next.active.session.effective.values = next
                    .active
                    .session
                    .negotiated
                    .selected
                    .keys()
                    .map(|key| (key.clone(), zhir_core::profile::Confirmation::Unknown))
                    .collect();
                next.active
                    .session
                    .effective
                    .values
                    .extend(effective.values);
                next.metrics.usage.add(&usage);
                next.metrics.model_turns += 1;
                let mut data = provider_data;
                if let Some(object) = data.as_object_mut() {
                    object.insert("completion".into(), serde_json::json!({"model_id":model_id,"response_id":response_id,"finish_reason":finish_reason}));
                } else {
                    data = serde_json::json!({"completion":{"model_id":model_id,"response_id":response_id,"finish_reason":finish_reason},"native":data});
                }
                entries.push(HistoryEntry {
                    id: format!("{}:{turn_id}:finished", next.active.session.id),
                    origin: Some(CallRef {
                        session_id: next.active.session.id.clone(),
                        turn_id,
                        caller_id: "model".into(),
                        call_id: "completion".into(),
                    }),
                    message: Message::Assistant {
                        output: vec![],
                        provider_data: data,
                    },
                });
                self.needs_turn |= disposition != TurnDisposition::Finished;
                if next.options.limits.max_total_tokens.is_some_and(|limit| {
                    next.metrics
                        .usage
                        .total_tokens
                        .is_some_and(|tokens| tokens >= limit)
                }) {
                    next.state = State::Limited {
                        reason: LimitReason::TotalTokens,
                    };
                }
            }
            SessionEventBody::Recovery { reference } => {
                next.active.session.recovery = Some(reference)
            }
            SessionEventBody::Closed => {
                self.model_closed = true;
                next.active.session.closed = true;
                if next.active.session.disposition.is_none() {
                    return self.suspend("RecoveryRequired").await;
                }
            }
            SessionEventBody::Operation {
                origin,
                event: operation_event,
            } => {
                let record = self
                    .current
                    .active
                    .operations
                    .values()
                    .find(|record| record.origin == origin)
                    .cloned();
                let Some(record) = record else {
                    if let OperationUpdate::Finished { outcome } = operation_event.update {
                        self.finish(&provider_operation_id(&origin), outcome)
                            .await?;
                        return self
                            .commit(
                                next,
                                Fact::Session {
                                    session_id: self.current.active.session.id.clone(),
                                    sequence: event.sequence,
                                },
                                HistoryDelta::Unchanged,
                            )
                            .await;
                    }
                    return Err(Error::Protocol("provider operation has no call".into()));
                };
                if !matches!(record.owner, OperationOwner::Provider { .. }) {
                    return Err(Error::Protocol(
                        "model cannot own local tool operations".into(),
                    ));
                }
                if !next.active.operations.contains_key(&record.id) {
                    return Err(Error::Protocol(
                        "provider operation must be introduced by an output item".into(),
                    ));
                }
                self.operation_event(&record.id, operation_event).await?;
                let mut next = self.current.as_ref().clone();
                next.active.session.last_sequence = Some(event.sequence);
                return self
                    .commit(
                        next,
                        Fact::Session {
                            session_id: self.current.active.session.id.clone(),
                            sequence: event.sequence,
                        },
                        HistoryDelta::Unchanged,
                    )
                    .await;
            }
            SessionEventBody::Delta { .. } => unreachable!(),
        }
        let history = if entries.is_empty() {
            HistoryDelta::Unchanged
        } else {
            next.history = next.history.append(entries.clone())?;
            HistoryDelta::Append(entries)
        };
        self.commit(
            next,
            Fact::Session {
                session_id: self.current.active.session.id.clone(),
                sequence: event.sequence,
            },
            history,
        )
        .await
    }
    async fn finish(&mut self, id: &str, outcome: RuntimeToolOutcome) -> Result<()> {
        self.finish_at(id, outcome, None).await
    }
    async fn finish_at(
        &mut self,
        id: &str,
        outcome: RuntimeToolOutcome,
        sequence: Option<u64>,
    ) -> Result<()> {
        outcome.validate()?;
        if !self.current.active.operations.contains_key(id) {
            return if self
                .current
                .history
                .by_id(&format!("operation:{id}:result"))
                .is_some_and(|entry| same_outcome(&entry.message, &outcome))
            {
                Ok(())
            } else {
                Err(Error::Protocol(
                    "unknown or conflicting operation completion".into(),
                ))
            };
        }
        let record = self
            .current
            .active
            .operations
            .get(id)
            .ok_or_else(|| Error::Protocol("result for unknown operation".into()))?
            .clone();
        if record.state.terminal() {
            if self
                .current
                .history
                .get(record.result_entry.expect("terminal result"))
                .is_some_and(|entry| same_outcome(&entry.message, &outcome))
            {
                return Ok(());
            }
            return Err(Error::Protocol("conflicting operation completion".into()));
        }
        let name = match &record.owner {
            OperationOwner::RuntimeTool { name } => name.clone(),
            OperationOwner::Provider { .. } => {
                return self.finish_provider(record, outcome, sequence).await;
            }
        };
        let update = sequence.map(|_| OperationUpdate::Finished {
            outcome: outcome.clone(),
        });
        let state = match outcome {
            RuntimeToolOutcome::Success { .. } => OperationState::Succeeded,
            RuntimeToolOutcome::Failure { .. } => OperationState::Failed,
            RuntimeToolOutcome::Cancelled { .. } => OperationState::Cancelled,
        };
        let entry = HistoryEntry {
            id: format!("operation:{id}:result"),
            origin: Some(record.origin.clone()),
            message: Message::RuntimeTool {
                call_id: record.origin.call_id,
                name,
                outcome,
            },
        };
        let mut next = self.current.as_ref().clone();
        let index = next.history.len();
        next.history = next.history.append(vec![entry.clone()])?;
        let operation = next.active.operations.get_mut(id).expect("operation");
        operation.state = state;
        operation.last_sequence = sequence.or(operation.last_sequence);
        operation.last_update = update.or(operation.last_update.take());
        operation.result_entry = Some(index);
        operation.wait = None;
        let command_id = new_id();
        next.active.commands.push(PendingCommand {
            id: command_id,
            intent: CommandIntent::ToolResult {
                operation_id: id.into(),
                entry: index,
            },
            sent: false,
        });
        self.commit(
            next,
            Fact::Operation {
                operation_id: id.into(),
                state,
            },
            HistoryDelta::Append(vec![entry]),
        )
        .await?;
        self.tokens.remove(id);
        self.operation_controls.remove(id);
        self.emitter.emit(EventData::OperationChanged {
            operation_id: id.into(),
            state,
        });
        Ok(())
    }
    async fn finish_provider(
        &mut self,
        record: OperationRecord,
        outcome: RuntimeToolOutcome,
        sequence: Option<u64>,
    ) -> Result<()> {
        let entry = self
            .current
            .history
            .get(record.call_entry)
            .expect("validated call entry");
        let Message::Assistant { output, .. } = &entry.message else {
            return Err(Error::Protocol("provider call entry mismatch".into()));
        };
        let mut call = output
            .iter()
            .find_map(|item| {
                if let Output::ProviderToolCall { call } = item {
                    Some(call.clone())
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::Protocol("provider call missing".into()))?;
        let state = match &outcome {
            RuntimeToolOutcome::Success { .. } => OperationState::Succeeded,
            RuntimeToolOutcome::Failure { .. } => OperationState::Failed,
            RuntimeToolOutcome::Cancelled { .. } => OperationState::Cancelled,
        };
        let update = sequence.map(|_| OperationUpdate::Finished {
            outcome: outcome.clone(),
        });
        call.status = match state {
            OperationState::Succeeded => ProviderToolStatus::Completed,
            OperationState::Cancelled => ProviderToolStatus::Cancelled,
            _ => ProviderToolStatus::Failed,
        };
        call.output = outcome.content().to_vec();
        call.data = serde_json::json!({"outcome": outcome, "native": call.data});
        let entry = HistoryEntry {
            id: format!("operation:{}:result", record.id),
            origin: Some(record.origin),
            message: Message::Assistant {
                output: vec![Output::ProviderToolCall { call }],
                provider_data: serde_json::Value::Null,
            },
        };
        let mut next = self.current.as_ref().clone();
        let operation = next
            .active
            .operations
            .get_mut(&record.id)
            .expect("registered provider operation");
        operation.state = state;
        operation.last_sequence = sequence.or(operation.last_sequence);
        operation.last_update = update.or(operation.last_update.take());
        operation.result_entry = Some(next.history.len());
        operation.wait = None;
        next.history = next.history.append(vec![entry.clone()])?;
        self.commit(
            next,
            Fact::Operation {
                operation_id: record.id,
                state,
            },
            HistoryDelta::Append(vec![entry]),
        )
        .await
    }
    async fn operation_event(&mut self, id: &str, event: OperationEvent) -> Result<()> {
        if let Some(record) = self.current.active.operations.get(id)
            && let Some(sequence) = record.last_sequence
        {
            if event.sequence == sequence {
                return if record.last_update.as_ref() == Some(&event.update) {
                    Ok(())
                } else {
                    Err(Error::Protocol(
                        "conflicting operation event sequence".into(),
                    ))
                };
            }
            if event.sequence < sequence {
                return Err(Error::Protocol(
                    "operation event sequence moved backwards".into(),
                ));
            }
        }
        if let OperationUpdate::Finished { outcome } = &event.update {
            return self
                .finish_at(id, outcome.clone(), Some(event.sequence))
                .await;
        }
        let record = self
            .current
            .active
            .operations
            .get(id)
            .ok_or_else(|| Error::Protocol("unknown operation event".into()))?;
        if let OperationUpdate::Progress { value } = event.update {
            self.emitter.emit(EventData::OperationProgress {
                operation_id: id.into(),
                value,
            });
            return Ok(());
        }
        if let OperationUpdate::Finished { outcome } = event.update {
            return self.finish(id, outcome).await;
        }
        if record.state.terminal() {
            return Err(Error::Protocol("update follows terminal operation".into()));
        }
        let mut next = self.current.as_ref().clone();
        let record = next.active.operations.get_mut(id).expect("record");
        record.last_sequence = Some(event.sequence);
        record.last_update = Some(event.update.clone());
        match event.update {
            OperationUpdate::Running { recovery } => {
                record.state = OperationState::Running;
                record.recovery = recovery.or(record.recovery.take());
                record.wait = None;
            }
            OperationUpdate::Waiting { prompt, recovery } => {
                record.state = OperationState::Waiting;
                record.wait = Some(prompt);
                record.recovery = recovery.or(record.recovery.take());
            }
            OperationUpdate::Unknown { reason } => {
                record.state = OperationState::Unknown;
                record.wait = Some(serde_json::json!({"reason":reason}));
            }
            _ => unreachable!(),
        }
        let state = record.state;
        self.commit(
            next,
            Fact::Operation {
                operation_id: id.into(),
                state,
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        self.emitter.emit(EventData::OperationChanged {
            operation_id: id.into(),
            state,
        });
        Ok(())
    }
    async fn unknown(&mut self, id: &str, reason: String) -> Result<()> {
        self.local_operation_update(
            id,
            OperationState::Unknown,
            Some(serde_json::json!({"reason":reason})),
        )
        .await
    }
    async fn local_operation_update(
        &mut self,
        id: &str,
        state: OperationState,
        wait: Option<serde_json::Value>,
    ) -> Result<()> {
        let mut next = self.current.as_ref().clone();
        let op = next
            .active
            .operations
            .get_mut(id)
            .ok_or_else(|| Error::Protocol("unknown operation".into()))?;
        if op.state.terminal() {
            return Err(Error::Protocol("update follows terminal operation".into()));
        }
        op.state = state;
        op.wait = wait;
        self.commit(
            next,
            Fact::Operation {
                operation_id: id.into(),
                state,
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        self.emitter.emit(EventData::OperationChanged {
            operation_id: id.into(),
            state,
        });
        Ok(())
    }
    async fn resolve(&mut self, resolution: RecoveryResolution) -> Result<()> {
        match resolution {
            RecoveryResolution::Complete {
                operation_id,
                outcome,
            } => self.finish(&operation_id, outcome).await,
            RecoveryResolution::Abandon {
                operation_id,
                reason,
            } => {
                self.finish(&operation_id, RuntimeToolOutcome::Cancelled { reason })
                    .await
            }
            RecoveryResolution::Attach {
                operation_id,
                reference,
            } => {
                let mut next = self.current.as_ref().clone();
                let op = next
                    .active
                    .operations
                    .get_mut(&operation_id)
                    .ok_or_else(|| Error::Invalid("unknown operation resolution".into()))?;
                if op.state.terminal() {
                    return Err(Error::Invalid("cannot attach terminal work".into()));
                }
                op.recovery = Some(reference);
                op.state = OperationState::Running;
                self.commit(
                    next,
                    Fact::Operation {
                        operation_id,
                        state: OperationState::Running,
                    },
                    HistoryDelta::Unchanged,
                )
                .await
            }
        }
    }
    async fn insert(&mut self, message: Message, source: String) -> Result<String> {
        message.validate()?;
        let id = new_id();
        let entry = HistoryEntry {
            id: id.clone(),
            origin: None,
            message,
        };
        let mut next = self.current.as_ref().clone();
        let index = next.history.len();
        next.history = next.history.append(vec![entry.clone()])?;
        next.active.commands.push(PendingCommand {
            id: id.clone(),
            intent: CommandIntent::Input { entry: index },
            sent: false,
        });
        self.commit(
            next,
            Fact::ConversationInsert { source },
            HistoryDelta::Append(vec![entry]),
        )
        .await?;
        self.needs_turn |= self.current.active.session.disposition.is_some()
            || !self
                .current
                .active
                .session
                .capabilities
                .as_ref()
                .is_some_and(|c| c.supports(Capability::Steering));
        Ok(id)
    }
    async fn control(&mut self, control: Control) -> Result<()> {
        let result: Result<String> = async {
            match control.body {
                ControlBody::Input(message, source) => self.insert(message, source).await,
                ControlBody::Pause(suspension) => {
                    let mut next = self.current.as_ref().clone();
                    next.state = State::Suspended { suspension };
                    self.commit(
                        next,
                        Fact::Control {
                            action: ControlAction::Suspended,
                        },
                        HistoryDelta::Unchanged,
                    )
                    .await?;
                    Ok(new_id())
                }
                ControlBody::UpdateProfile(profile) => {
                    let busy = self.current.active.session.disposition.is_none()
                        && self.current.active.session.turn_id.is_some();
                    if busy
                        && !self
                            .session_capabilities()
                            .supports(Capability::ProfileUpdates)
                    {
                        return Err(Error::Invalid(
                            "model does not support running profile updates".into(),
                        ));
                    }
                    let mut request = self.model_request(self.current.history.len());
                    request.profile = profile.clone();
                    let negotiated = self.negotiate(&request)?;
                    if !self
                        .session_capabilities()
                        .supports(Capability::ProfileUpdates)
                    {
                        let id = new_id();
                        let mut next = self.current.as_ref().clone();
                        next.active.session.profile_revision += 1;
                        next.active.session.profile = profile;
                        next.active.session.effective.values = negotiated
                            .selected
                            .keys()
                            .map(|key| (key.clone(), zhir_core::profile::Confirmation::Unknown))
                            .collect();
                        next.active.session.negotiated = negotiated;
                        self.commit(
                            next,
                            Fact::Command {
                                command_id: id.clone(),
                            },
                            HistoryDelta::Unchanged,
                        )
                        .await?;
                        return Ok(id);
                    }
                    self.prepare_command(CommandIntent::UpdateProfile {
                        revision: self
                            .current
                            .active
                            .commands
                            .iter()
                            .filter_map(|c| {
                                if let CommandIntent::UpdateProfile { revision, .. } = c.intent {
                                    Some(revision)
                                } else {
                                    None
                                }
                            })
                            .max()
                            .unwrap_or(self.current.active.session.profile_revision)
                            + 1,
                        profile,
                    })
                    .await
                }
                ControlBody::Interrupt => {
                    if !self.session_capabilities().supports(Capability::Steering) {
                        return Err(Error::Invalid(
                            "model does not support native interruption".into(),
                        ));
                    }
                    let turn_id = self
                        .current
                        .active
                        .session
                        .turn_id
                        .clone()
                        .ok_or_else(|| Error::Invalid("no turn to interrupt".into()))?;
                    self.prepare_command(CommandIntent::Interrupt { turn_id })
                        .await
                }
                ControlBody::EndInput => self.prepare_command(CommandIntent::EndInput).await,
                ControlBody::CancelOperation(id) => {
                    let record = self
                        .current
                        .active
                        .operations
                        .get(&id)
                        .ok_or_else(|| Error::Invalid("unknown operation".into()))?;
                    if record.state.terminal() {
                        return Ok(id);
                    }
                    if record.state == OperationState::Queued {
                        self.finish(
                            &id,
                            RuntimeToolOutcome::Cancelled {
                                reason: "cancelled before dispatch".into(),
                            },
                        )
                        .await?;
                        return Ok(id);
                    }
                    let mut next = self.current.as_ref().clone();
                    next.active
                        .operations
                        .get_mut(&id)
                        .expect("operation")
                        .state = OperationState::Cancelling;
                    self.commit(
                        next,
                        Fact::Operation {
                            operation_id: id.clone(),
                            state: OperationState::Cancelling,
                        },
                        HistoryDelta::Unchanged,
                    )
                    .await?;
                    if let Some(token) = self.tokens.get(&id) {
                        token.cancel();
                    }
                    if let Some(handle) = self.operation_controls.get(&id).cloned() {
                        let tx = self.work_tx.clone();
                        let opid = id.clone();
                        self.tasks.spawn(async move {
                            if let Err(error) = handle.cancel().await {
                                let _ = tx.send(Work::Started(opid, Err(error))).await;
                            }
                        });
                    }
                    Ok(id)
                }
                ControlBody::ReplyOperation(id, value) => {
                    let handle =
                        self.operation_controls.get(&id).cloned().ok_or_else(|| {
                            Error::Invalid("operation has no input channel".into())
                        })?;
                    // Reply is an external effect with a durable uncertain boundary.
                    self.unknown(&id, "reply awaiting external confirmation".into())
                        .await?;
                    let tx = self.work_tx.clone();
                    let opid = id.clone();
                    self.pending_replies.insert(id.clone());
                    self.tasks.spawn(async move {
                        let result = handle.reply(value).await;
                        let _ = tx.send(Work::ReplyDone(opid, result)).await;
                    });
                    Ok(id)
                }
            }
        }
        .await;
        let fatal = result
            .as_ref()
            .err()
            .filter(|e| matches!(e, Error::Storage(_) | Error::Conflict { .. }))
            .cloned();
        let _ = control.reply.send(result.map(|command_id| ControlReceipt {
            command_id,
            revision: self.current.revision,
        }));
        if let Some(error) = fatal {
            Err(error)
        } else {
            Ok(())
        }
    }
    async fn seal_cursor(
        &mut self,
        direction: &str,
        chunk: &MediaChunk,
        reference: zhir_core::resource::ResourceRef,
    ) -> Result<()> {
        let key = media_key(direction, chunk);
        if self
            .current
            .active
            .media
            .get(&key)
            .is_some_and(|c| chunk.sequence <= c.sequence)
        {
            return Err(Error::Protocol("media sequence did not increase".into()));
        }
        let mut next = self.current.as_ref().clone();
        next.active.media.insert(
            key,
            StreamCursor {
                sequence: chunk.sequence,
                epoch: chunk.epoch,
                sealed: reference,
            },
        );
        self.commit(
            next,
            Fact::Media {
                stream_id: chunk.stream_id.clone(),
                sequence: chunk.sequence,
            },
            HistoryDelta::Unchanged,
        )
        .await
    }
    fn input_media(&mut self, packet: Packet) -> Result<()> {
        let chunk = &packet.chunk;
        chunk.validate(self.current.options.limits.max_media_chunk_bytes)?;
        if chunk.epoch < self.current.active.session.epoch {
            return Ok(());
        }
        if chunk.epoch != self.current.active.session.epoch
            || self.current.active.session.turn_id.as_ref() != Some(&chunk.turn_id)
        {
            return Err(Error::Invalid("foreign media turn or epoch".into()));
        }
        let resources = self
            .config
            .resources
            .clone()
            .ok_or_else(|| Error::Invalid("media input requires resource storage".into()))?;
        let input = self
            .model_media
            .clone()
            .ok_or_else(|| Error::Invalid("model has no media input".into()))?;
        let previous = self
            .current
            .active
            .media
            .get(&media_key("input", chunk))
            .map(|cursor| cursor.sealed.clone());
        let tx = self.work_tx.clone();
        self.input_sending = true;
        self.tasks.spawn(async move {
            let result: Result<()> = async {
                let reference = seal_media(resources.as_ref(), &packet.chunk, previous).await?;
                let (ack, rx) = oneshot::channel();
                tx.send(Work::InputReady(packet.chunk.clone(), reference, ack))
                    .await
                    .map_err(|_| Error::Cancelled)?;
                if rx.await.map_err(|_| Error::Cancelled)? {
                    input.send(packet.chunk.clone()).await?;
                }
                Ok(())
            }
            .await;
            drop(packet);
            let _ = tx.send(Work::InputSent(result)).await;
        });
        Ok(())
    }
    async fn stop_workers(&mut self) {
        for token in self.tokens.values() {
            token.cancel();
        }
        if !matches!(self.current.state, State::Suspended { .. }) {
            for handle in self.operation_controls.values() {
                let _ = tokio::time::timeout(Duration::from_millis(50), handle.cancel()).await;
            }
        }
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }
}

async fn interruptible<T>(
    future: impl std::future::Future<Output = Result<T>>,
    cancellation: Cancellation,
    deadline: Option<Instant>,
) -> Result<T> {
    tokio::pin!(future);
    loop {
        tokio::select! {
            result = &mut future => return result,
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
                cancellation.check()?;
                if deadline.is_some_and(|d| Instant::now() >= d) { return Err(Error::Deadline); }
            }
        }
    }
}

fn media_key(direction: &str, chunk: &MediaChunk) -> String {
    format!("{direction}:{}:{}", chunk.stream_id, chunk.epoch)
}
async fn seal_media(
    store: &dyn zhir_core::resource::ResourceStore,
    chunk: &MediaChunk,
    previous: Option<zhir_core::resource::ResourceRef>,
) -> Result<zhir_core::resource::ResourceRef> {
    let mut writer = store.create(new_id(), chunk.media_type.clone()).await?;
    writer.append(0, chunk.bytes.clone()).await?;
    let resource = writer.finish().await?;
    let node = zhir_core::resource::SealedMedia {
        stream_id: chunk.stream_id.clone(),
        turn_id: chunk.turn_id.clone(),
        epoch: chunk.epoch,
        sequence: chunk.sequence,
        timestamp_us: chunk.timestamp_us,
        end: chunk.end,
        resource,
        previous,
    };
    let mut writer = store
        .create(new_id(), "application/vnd.zhir.sealed-media+json".into())
        .await?;
    writer
        .append(
            0,
            serde_json::to_vec(&node).map_err(|e| Error::Invalid(e.to_string()))?,
        )
        .await?;
    writer.finish().await
}

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

fn provider_operation_id(origin: &CallRef) -> String {
    format!(
        "provider:{}",
        serde_json::json!([
            origin.session_id,
            origin.turn_id,
            origin.caller_id,
            origin.call_id
        ])
    )
}
fn same_outcome(message: &Message, outcome: &RuntimeToolOutcome) -> bool {
    match message {
        Message::RuntimeTool { outcome: previous, .. } => previous == outcome,
        Message::Assistant { output, .. } => output.iter().any(|item| matches!(item, Output::ProviderToolCall { call } if call.data.get("outcome") == Some(&serde_json::to_value(outcome).expect("serializable outcome")))),
        _ => false,
    }
}
