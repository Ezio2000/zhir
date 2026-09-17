use super::*;

impl Engine {
    pub(super) fn check(&self) -> Result<()> {
        if self.deadline.is_some_and(|d| Instant::now() >= d) {
            return Err(Error::Deadline);
        }
        self.cancellation.check()
    }
    pub(super) async fn commit(
        &mut self,
        mut next: Checkpoint,
        fact: Fact,
        history: HistoryDelta,
    ) -> Result<()> {
        next.id = new_id();
        next.parent_id = Some(self.current.id.clone());
        next.revision = self.current.revision + 1;
        next.fact = fact;
        self.current = persist(self.store.as_ref(), next, history).await?;
        self.emitter.emit(EventData::CheckpointCommitted {
            checkpoint_id: self.current.id.clone(),
            revision: self.current.revision,
            state: self.current.state.kind(),
            fact: self.current.fact.clone(),
        });
        Ok(())
    }
    pub(super) async fn suspend(&mut self, reason: WaitReason) -> Result<()> {
        let mut next = self.current.as_ref().clone();
        next.state = State::Suspended {
            suspension: Suspension {
                reason: reason.as_str().into(),
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
}

#[derive(Clone, Copy)]
pub(super) enum WaitReason {
    Recovery,
    Input,
}
impl WaitReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Recovery => "RecoveryRequired",
            Self::Input => "InputRequired",
        }
    }
}

/// All checkpoint writes, including the initial checkpoint, share this timeout boundary.
pub(super) async fn persist(
    store: &dyn RunStore,
    next: Checkpoint,
    history: HistoryDelta,
) -> Result<Arc<Checkpoint>> {
    let timeout = Duration::from_millis(next.options.limits.commit_timeout_ms);
    let mut commit = Commit::new(Arc::new(next), history);
    commit.deadline = Some(std::time::Instant::now() + timeout);
    let checkpoint = commit.checkpoint.clone();
    tokio::time::timeout(timeout, store.commit(commit))
        .await
        .map_err(|_| {
            Error::Storage(
                "checkpoint commit timed out; reload the durable head before retrying".into(),
            )
        })??;
    Ok(checkpoint)
}

pub(super) fn initial(request: &Request) -> (Checkpoint, HistoryDelta) {
    match request {
        Request::Start {
            history, options, ..
        } => {
            let session = SessionSnapshot {
                capabilities: None,
                id: new_id(),
                generation_id: None,
                establishment: SessionEstablishment::New,
                ready: false,
                generation_started: false,
                context_revision: 0,
                acknowledged_context_revision: 0,
                reduced_context_revision: None,
                input_position: 0,
                generated_input_position: 0,
                needs_generation: true,
                response_start: 0,
                run_start: history.len(),
                last_sequence: None,
                response_status: None,
                recovery: None,
                output_epoch: 0,
                media_archive: None,
                input_audio_enabled: true,
                input_closed: options.mode == RunMode::Task,
                closing: false,
                closure: None,
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
                    context: request.context().clone(),
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
    }
}
