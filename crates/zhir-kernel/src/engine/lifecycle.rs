use super::*;
use futures::{StreamExt, stream};

#[derive(Default)]
struct WorkStatus {
    busy: bool,
    runtime_busy: bool,
    unknown: bool,
    progressing: bool,
}
impl WorkStatus {
    fn read(operations: &BTreeMap<String, OperationRecord>) -> Self {
        let mut status = Self::default();
        for operation in operations.values().filter(|op| !op.state.terminal()) {
            status.busy = true;
            status.runtime_busy |= matches!(operation.owner, OperationOwner::RuntimeTool { .. });
            status.unknown |= operation.state == OperationState::Unknown;
            status.progressing |= !matches!(
                operation.state,
                OperationState::Waiting | OperationState::Unknown
            );
        }
        status
    }
}

impl Engine {
    pub(super) async fn run(&mut self, request: Request) -> Result<()> {
        if self.current.state.terminal() {
            return Ok(());
        }
        self.check()?;
        self.open_catalog().await?;
        if !self.current.state.active() {
            return Ok(());
        }
        self.apply_recovery(request).await?;
        if !self.current.state.active() {
            return Ok(());
        }
        self.open_model().await?;
        if !self.current.state.active() {
            return Ok(());
        }
        self.recover_tools().await?;
        while self.current.state.active() {
            self.check()?;
            self.dispatch_commands().await?;
            self.admit().await?;
            if self.advance().await? {
                continue;
            }
            self.receive_work().await?;
        }
        Ok(())
    }

    async fn receive_work(&mut self) -> Result<()> {
        tokio::select! { biased;
            Some(control) = self.controls.recv(), if !self.controls.is_closed() || !self.controls.is_empty() => {
                self.control(control).await?;
            }
            Some(work) = self.work.recv() => { self.handle(work).await?; }
            Some(packet) = self.media.recv(), if self.model_media.is_some() && !self.input_sending && (!self.media.is_closed() || !self.media.is_empty()) => {
                self.input_media(packet)?;
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => (),
        }
        Ok(())
    }

    // True means the state advanced and must be reconsidered before receiving more work.
    async fn advance(&mut self) -> Result<bool> {
        let status = WorkStatus::read(&self.current.active.operations);
        if self.can_advance_turn(&status) {
            if self.needs_turn {
                self.start_turn().await?;
                return Ok(true);
            }
            if self.input_finished() && self.finish_or_drain().await? {
                return Ok(true);
            }
        }
        if self.waiting_for_input(&status) {
            self.suspend(if status.unknown {
                WaitReason::Recovery
            } else {
                WaitReason::Input
            })
            .await?;
            return Ok(true);
        }
        Ok(false)
    }

    fn can_advance_turn(&self, status: &WorkStatus) -> bool {
        let provider_continuation = self.needs_turn
            && !status.runtime_busy
            && self.current.active.session.disposition == Some(TurnDisposition::Continue);
        (!status.busy || provider_continuation)
            && self.current.active.commands.is_empty()
            && !self.sending
    }

    fn input_finished(&self) -> bool {
        self.current.active.session.disposition == Some(TurnDisposition::Finished)
            && (self.current.options.mode == RunMode::Task
                || self.current.active.session.input_closed)
    }

    fn waiting_for_input(&self, status: &WorkStatus) -> bool {
        status.busy
            && !status.progressing
            && (self.current.options.mode == RunMode::Task || status.unknown)
            && self.pending_replies.is_empty()
            && self.pending_starts.is_empty()
            && self.current.active.session.disposition.is_some()
    }

    async fn finish_or_drain(&mut self) -> Result<bool> {
        if self.media_pending || self.input_sending {
            if !self.model_closed && !self.current.active.session.closing && !self.input_sending {
                self.prepare_command(CommandIntent::Close).await?;
            }
            return Ok(false);
        }
        let content = conversation(
            self.current
                .history
                .entries()
                .into_iter()
                .skip(self.current.active.session.turn_start),
        )
        .into_iter()
        .flat_map(|message| match message {
            Message::Assistant { output, .. } => zhir_core::message::visible_content(&output),
            _ => Vec::new(),
        })
        .collect();
        let mut next = self.current.as_ref().clone();
        next.state = State::Completed { content };
        self.commit(
            next,
            Fact::Control {
                action: ControlAction::Finished,
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        Ok(true)
    }

    pub(super) async fn stop_workers(&mut self) {
        for token in self.tokens.values() {
            token.cancel();
        }
        if !matches!(self.current.state, State::Suspended { .. }) {
            let handles: Vec<_> = self.operation_controls.values().cloned().collect();
            let futures: Vec<zhir_core::BoxFuture<'static, Result<()>>> = handles
                .into_iter()
                .map(|handle| {
                    Box::pin(async move { handle.cancel().await })
                        as zhir_core::BoxFuture<'static, Result<()>>
                })
                .collect();
            let cancellations = stream::iter(futures)
                .buffer_unordered(self.current.options.limits.max_runtime_tool_concurrency)
                .collect::<Vec<_>>();
            // One cleanup budget for all handles, independent of the number admitted.
            let _ = tokio::time::timeout(Duration::from_millis(50), cancellations).await;
        }
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }
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
    let (mut next, delta) = checkpoint::initial(&request);
    let expired = deadline.is_some_and(|d| Instant::now() >= d);
    if expired {
        next.state = State::Limited {
            reason: LimitReason::Deadline,
        };
    }
    let first = checkpoint::persist(store.as_ref(), next, delta)
        .await
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
    engine.settle(result).await
}

impl Engine {
    async fn settle(mut self, result: Result<()>) -> EngineResult {
        match result {
            Ok(()) => Ok(self.current),
            Err(error) if matches!(error, Error::Storage(_) | Error::Conflict { .. }) => {
                Err(RunError {
                    error,
                    last_checkpoint: Some(self.current),
                })
            }
            Err(error) => {
                let (state, action) = match error {
                    Error::Cancelled => (State::Cancelled, ControlAction::Cancelled),
                    Error::Deadline => (
                        State::Limited {
                            reason: LimitReason::Deadline,
                        },
                        ControlAction::Limited,
                    ),
                    error => (
                        State::Failed {
                            error: crate::failure::failure(&error),
                        },
                        ControlAction::Failed,
                    ),
                };
                let mut next = self.current.as_ref().clone();
                next.state = state;
                for operation in next.active.operations.values_mut() {
                    if matches!(
                        operation.state,
                        OperationState::Running | OperationState::Cancelling
                    ) {
                        operation.state = OperationState::Unknown;
                    }
                }
                match self
                    .commit(next, Fact::Control { action }, HistoryDelta::Unchanged)
                    .await
                {
                    Ok(()) => Ok(self.current),
                    Err(error) => Err(RunError {
                        error,
                        last_checkpoint: Some(self.current),
                    }),
                }
            }
        }
    }
}

pub(super) async fn interruptible<T>(
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
