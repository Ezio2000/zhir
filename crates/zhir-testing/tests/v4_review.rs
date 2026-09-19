use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, oneshot};
use zhir_core::{
    BoxFuture, Cancellation, Result,
    error::{Error, Failure},
    message::{Message, Output},
    model::*,
    run::{HistoryEntry, RunMode, State},
};
use zhir_kernel::{ResumeRequest, RunRequest, Runtime};
use zhir_models::{
    FunctionModel,
    decorators::{FallbackCandidate, FallbackModel},
};
use zhir_testing::{RecordingStore, SessionModel};

fn open() -> SessionOpen {
    SessionOpen {
        binding: None,
        session_id: "review-session".into(),
        after_sequence: None,
        output_epoch: 0,
        context_revision: 0,
        input_position: 0,
        profile_revision: 0,
        mode: RunMode::Task,
        limits: zhir_kernel::defaults::limits(),
        request: ModelRequest {
            messages: vec![Message::user("A")],
            runtime_tools: vec![],
            provider_tools: vec![],
            profile: Default::default(),
            tool_choice: ToolChoice::Auto,
            response_format: None,
            stream: true,
        },
        recovery: None,
        context: ModelContext {
            run: zhir_kernel::defaults::context(),
            cancellation: Cancellation::default(),
            deltas: None,
        },
    }
}
fn command(id: &str, body: SessionCommandBody) -> SessionCommand {
    SessionCommand {
        id: id.into(),
        body,
    }
}
fn generate() -> SessionCommand {
    command(
        "generate",
        SessionCommandBody::Generate {
            generation_id: "g".into(),
            context_revision: 0,
            input_position: 0,
            profile_revision: 0,
        },
    )
}
async fn receive(session: &mut ModelSession) -> SessionEvent {
    tokio::time::timeout(Duration::from_secs(3), session.output.receive())
        .await
        .expect("session stalled")
        .expect("session error")
        .expect("unexpected EOF")
}
async fn finish(session: &mut ModelSession) {
    session
        .input
        .send(command("close", SessionCommandBody::Close))
        .await
        .unwrap();
    while !matches!(receive(session).await.body, SessionEventBody::Closed { .. }) {}
}

#[tokio::test]
async fn encoded_checkpoint_matches_published_schema_and_rejects_old_versions() {
    let runtime = Runtime::builder(Arc::new(FunctionModel::new(
        zhir_testing::model_capabilities(),
        |_, _| async { Ok(GenerationOutput::text("done")) },
    )))
    .build()
    .unwrap();
    let checkpoint = runtime
        .start(RunRequest::new([Message::user("A")]))
        .unwrap()
        .result()
        .await
        .unwrap()
        .into_checkpoint();
    assert!(matches!(checkpoint.state, State::Completed { .. }));
    let bytes = zhir_core::wire::encode_checkpoint(&checkpoint).unwrap();
    let mut value: Value = serde_json::from_slice(&bytes).unwrap();
    let schema: Value = serde_json::from_str(include_str!(
        "../../../contracts/v4/schemas/checkpoint.schema.json"
    ))
    .unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    assert_eq!(value["version"], zhir_core::wire::VERSION);
    validator.validate(&value).unwrap();
    let restored = zhir_core::wire::decode_checkpoint(&bytes).unwrap();
    assert_eq!(restored.history.digest(), checkpoint.history.digest());
    for version in [0, 3, 5] {
        value["version"] = json!(version);
        assert!(!validator.is_valid(&value));
        assert!(zhir_core::wire::decode_checkpoint(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    let mut invalid = checkpoint.as_ref().clone();
    invalid.active.session.input_position += 1;
    invalid.active.session.needs_generation = true;
    assert!(
        invalid.validate().is_err(),
        "stale input coverage must not validate as Completed"
    );
}

#[tokio::test]
async fn automatic_generation_cannot_close_over_unprocessed_late_input() {
    for mode in [RunMode::Task, RunMode::Interactive] {
        let started = Arc::new(Notify::new());
        let signal = started.clone();
        let mut caps = zhir_testing::model_capabilities();
        caps.features.remove(&Capability::ExplicitGeneration);
        caps.features.insert(Capability::Steering);
        let model = SessionModel::new(caps, move |open, mut peer| {
            let signal = signal.clone();
            async move {
                peer.event(SessionEventBody::ResponseStarted {
                    generation_id: "auto-a".into(),
                    input_position: open.input_position,
                })
                .await?;
                signal.notify_one();
                let mut latest = open.input_position;
                loop {
                    let c = peer.command().await?.expect("input or seal expected");
                    match &c.body {
                        SessionCommandBody::Append { input_position, .. } => {
                            latest = *input_position
                        }
                        SessionCommandBody::SealUserInput => {
                            peer.acknowledge(&c, None).await?;
                            break;
                        }
                        _ => {
                            return Err(Error::Protocol(
                                "unexpected explicit generation or early close".into(),
                            ));
                        }
                    }
                    peer.acknowledge(&c, None).await?;
                }
                assert!(latest > open.input_position);
                peer.finished("auto-a".into(), ResponseStatus::Completed)
                    .await?;
                // No provider response for B exists yet. An ACK is not evidence that B
                // was answered. Old code sends Close here in both Task and Interactive.
                tokio::select! {
                    unexpected = peer.command() => {
                        return Err(Error::Protocol(format!("command before B was processed: {:?}", unexpected?)));
                    }
                    _ = tokio::time::sleep(Duration::from_millis(50)) => (),
                }
                peer.event(SessionEventBody::ResponseStarted {
                    generation_id: "auto-b".into(),
                    input_position: latest,
                })
                .await?;
                peer.event(SessionEventBody::Output {
                    generation_id: Some("auto-b".into()),
                    item_id: "answer-b".into(),
                    caller_id: "model".into(),
                    output: Output::text("answer B"),
                })
                .await?;
                peer.event(SessionEventBody::ResponseFinished {
                    generation_id: "auto-b".into(),
                    input_position: latest,
                    response_status: ResponseStatus::Completed,
                    usage: Default::default(),
                    model_id: None,
                    response_id: None,
                    finish_reason: None,
                    provider_data: Value::Null,
                    effective: Default::default(),
                })
                .await?;
                peer.close().await
            }
        });
        let runtime = Runtime::builder(Arc::new(model)).build().unwrap();
        // Task seals user admission at creation, but host context is still admissible.
        let late_input = if mode == RunMode::Task {
            Message::external("B")
        } else {
            Message::user("B")
        };
        let mut invocation = runtime
            .start(RunRequest::new([Message::user("A")]).mode(mode))
            .unwrap();
        invocation.start();
        tokio::time::timeout(Duration::from_secs(3), async {
            started.notified().await;
            invocation
                .control()
                .input(late_input, "test")
                .await
                .unwrap();
            invocation.control().seal_user_input().await.unwrap();
            let checkpoint = invocation.result().await.unwrap().into_checkpoint();
            assert!(
                matches!(checkpoint.state, State::Completed { .. }),
                "{:?}",
                checkpoint.state
            );
            assert_eq!(checkpoint.metrics.generation_requests, 0);
            assert_eq!(checkpoint.metrics.observed_responses, Some(2));
            assert_eq!(
                checkpoint.active.session.generated_input_position,
                checkpoint.active.session.input_position
            );
        })
        .await
        .expect("automatic response stalled");
    }
}

#[tokio::test]
async fn eager_exchange_deltas_follow_acknowledgement_and_response_start() {
    use futures::FutureExt;

    let model = FunctionModel::new(zhir_testing::model_capabilities(), |_, context| {
        // Emit during callback invocation, before returning its generation future.
        // The default event capacity makes this immediately ready without threads
        // or timing assumptions about how a background producer is scheduled.
        let emitted = context
            .deltas
            .expect("delta sink")
            .emit(ModelDelta::Text {
                output_index: 0,
                text: "first".into(),
            })
            .now_or_never()
            .expect("event capacity available");
        async move {
            emitted?;
            Ok(GenerationOutput::text("done"))
        }
    });
    let mut session = model.open_session(open()).await.unwrap();
    assert!(matches!(
        receive(&mut session).await.body,
        SessionEventBody::Ready { .. }
    ));
    session.input.send(generate()).await.unwrap();
    let mut events = Vec::new();
    loop {
        let event = receive(&mut session).await;
        assert_eq!(event.sequence, events.len() as u64 + 1);
        let finished = matches!(event.body, SessionEventBody::ResponseFinished { .. });
        events.push(event.body);
        if finished {
            break;
        }
    }
    assert!(
        matches!(events.as_slice(), [
        SessionEventBody::Acknowledged { command_id, .. },
        SessionEventBody::ResponseStarted { generation_id: started, .. },
        SessionEventBody::Delta { generation_id: Some(delta), .. },
        SessionEventBody::Output { generation_id: Some(output), .. },
        SessionEventBody::ResponseFinished { generation_id: finished, .. },
    ] if command_id == "generate" && [started, delta, output, finished].iter().all(|id| *id == "g")),
        "unexpected event order: {events:?}"
    );
    finish(&mut session).await;
}

#[tokio::test]
async fn bounded_delta_and_command_ack_keep_each_other_runnable() {
    let blocked = Arc::new(Notify::new());
    let signal = blocked.clone();
    let model = FunctionModel::new(zhir_testing::model_capabilities(), move |_, context| {
        let signal = signal.clone();
        async move {
            let sink = context.deltas.expect("delta sink");
            sink.emit(ModelDelta::Text {
                output_index: 0,
                text: "first".into(),
            })
            .await?;
            // On the current-thread executor the second send reaches Pending before
            // the notified test can run: there is no intervening yield.
            signal.notify_one();
            sink.emit(ModelDelta::Text {
                output_index: 0,
                text: "second".into(),
            })
            .await?;
            Ok(GenerationOutput::text("done"))
        }
    });
    let mut settings = open();
    settings.limits.max_session_events = 1;
    settings.limits.max_control_commands = 1;
    let mut session = model.open_session(settings).await.unwrap();
    assert!(matches!(
        receive(&mut session).await.body,
        SessionEventBody::Ready { .. }
    ));
    session.input.send(generate()).await.unwrap();
    while !matches!(
        receive(&mut session).await.body,
        SessionEventBody::ResponseStarted { .. }
    ) {}
    tokio::time::timeout(Duration::from_secs(3), blocked.notified())
        .await
        .unwrap();
    session
        .input
        .send(command(
            "append",
            SessionCommandBody::Append {
                entry: HistoryEntry {
                    id: "late".into(),
                    origin: None,
                    message: Message::user("B"),
                },
                context_revision: 1,
                input_position: 1,
                source: AppendSource::Submitted,
            },
        ))
        .await
        .unwrap();
    // Capacity one makes admission of this command prove that Append was dequeued,
    // before the test starts draining the full public event queue.
    tokio::time::timeout(
        Duration::from_secs(3),
        session
            .input
            .send(command("seal", SessionCommandBody::SealUserInput)),
    )
    .await
    .unwrap()
    .unwrap();
    let mut delta_count = 0;
    let mut append_ack = false;
    let mut last = None;
    loop {
        let event = receive(&mut session).await;
        assert!(last.is_none_or(|previous| previous < event.sequence));
        last = Some(event.sequence);
        match event.body {
            SessionEventBody::Delta { .. } => delta_count += 1,
            SessionEventBody::Acknowledged { command_id, .. } if command_id == "append" => {
                append_ack = true
            }
            SessionEventBody::ResponseFinished { .. } => break,
            _ => (),
        }
    }
    assert_eq!(delta_count, 2);
    assert!(append_ack);
    finish(&mut session).await;
}

struct Dropped(Option<oneshot::Sender<()>>);
impl Drop for Dropped {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
#[tokio::test]
async fn cancellation_settles_even_when_output_is_not_consumed() {
    let entered = Arc::new(Notify::new());
    let signal = entered.clone();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let dropped = Arc::new(std::sync::Mutex::new(Some(dropped_tx)));
    let model = FunctionModel::new(zhir_testing::model_capabilities(), move |_, context| {
        let guard = Dropped(dropped.lock().unwrap().take());
        let signal = signal.clone();
        async move {
            let _guard = guard;
            let sink = context.deltas.expect("delta sink");
            sink.emit(ModelDelta::Text {
                output_index: 0,
                text: "first".into(),
            })
            .await?;
            signal.notify_one();
            sink.emit(ModelDelta::Text {
                output_index: 0,
                text: "blocked".into(),
            })
            .await?;
            std::future::pending::<Result<GenerationOutput>>().await
        }
    });
    let mut settings = open();
    settings.limits.max_session_events = 1;
    let cancellation = settings.context.cancellation.clone();
    let mut session = model.open_session(settings).await.unwrap();
    receive(&mut session).await;
    session.input.send(generate()).await.unwrap();
    while !matches!(
        receive(&mut session).await.body,
        SessionEventBody::ResponseStarted { .. }
    ) {}
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(3), dropped_rx)
        .await
        .unwrap()
        .unwrap();
    let mut errors = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(3), session.output.receive())
            .await
            .unwrap()
        {
            Ok(Some(_)) => (),
            Err(Error::Cancelled) => errors += 1,
            Ok(None) => break,
            other => panic!("unexpected termination: {other:?}"),
        }
    }
    assert_eq!(errors, 1);
}

struct Candidate {
    name: &'static str,
    unavailable: Arc<AtomicBool>,
    opens: Arc<AtomicUsize>,
    capabilities: CapabilitySet,
}
impl Model for Candidate {
    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<zhir_core::profile::NegotiatedProfile> {
        zhir_policies::negotiation::negotiate(request, &self.capabilities)
    }
    fn open_session(&self, open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            self.opens.fetch_add(1, Ordering::SeqCst);
            if self.unavailable.load(Ordering::SeqCst) {
                return Err(Error::Model(Failure {
                    code: "unavailable".into(),
                    message: "fixture".into(),
                    retryable: true,
                }));
            }
            let name = self.name;
            FunctionModel::new(self.capabilities.clone(), move |_, _| async move {
                Ok(GenerationOutput::text(name))
            })
            .open_session(open)
            .await
        })
    }
}
fn candidate(name: &'static str, unavailable: bool) -> Arc<Candidate> {
    let mut capabilities = zhir_testing::model_capabilities();
    capabilities.features.insert(Capability::LocalProjection);
    Arc::new(Candidate {
        name,
        unavailable: Arc::new(AtomicBool::new(unavailable)),
        opens: Arc::new(AtomicUsize::new(0)),
        capabilities,
    })
}
#[tokio::test]
async fn local_projection_binding_survives_checkpoint_recovery_and_reordering() {
    let a = candidate("A", false);
    let b = candidate("B", true);
    let fallback = FallbackModel::new(vec![
        FallbackCandidate::new("b", b.clone()),
        FallbackCandidate::new("a", a.clone()),
    ])
    .unwrap();
    let model = zhir_models::ConcurrencyLimitedModel::new(Arc::new(fallback), 1).unwrap();
    let store = Arc::new(RecordingStore::new(Arc::new(
        zhir_storage::MemoryRunStore::new(),
    )));
    let runtime = Runtime::builder(Arc::new(model))
        .store(store.clone())
        .build()
        .unwrap();
    let mut invocation = runtime
        .start(RunRequest::new([Message::user("A")]).mode(RunMode::Interactive))
        .unwrap();
    invocation.start();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let idle = store.commits().last().is_some_and(|commit| {
                let c = &commit.checkpoint;
                c.active.session.response_status == Some(ResponseStatus::Completed)
                    && c.active.commands.is_empty()
            });
            if idle {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        invocation
            .control()
            .pause(zhir_kernel::defaults::pause())
            .await
            .unwrap();
    })
    .await
    .unwrap();
    let checkpoint = invocation.result().await.unwrap().into_checkpoint();
    assert!(matches!(checkpoint.state, State::Suspended { .. }));
    assert!(checkpoint.active.session.recovery.is_none());
    let binding = checkpoint
        .active
        .session
        .binding
        .clone()
        .expect("durable candidate binding");
    assert_eq!(binding.data["candidate"], "a");
    let encoded = zhir_core::wire::encode_checkpoint(&checkpoint).unwrap();
    let restored = zhir_core::wire::decode_checkpoint(&encoded).unwrap();
    assert_eq!(restored.active.session.binding, Some(binding.clone()));
    b.unavailable.store(false, Ordering::SeqCst);
    let recovered_model = FallbackModel::new(vec![
        FallbackCandidate::new("b", b.clone()),
        FallbackCandidate::new("a", a.clone()),
    ])
    .unwrap();
    let recovered_runtime = Runtime::builder(Arc::new(recovered_model))
        .store(store.clone())
        .build()
        .unwrap();
    let before_b = b.opens.load(Ordering::SeqCst);
    let mut invocation = recovered_runtime
        .resume(ResumeRequest::from_checkpoint(Arc::new(restored)).message(Message::user("again")))
        .await
        .unwrap();
    invocation.start();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if store.commits().last().is_some_and(|commit| {
                commit.checkpoint.metrics.observed_responses == Some(2)
                    && commit.checkpoint.active.commands.is_empty()
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        invocation.control().seal_user_input().await.unwrap();
    })
    .await
    .unwrap();
    let completed = invocation.result().await.unwrap().into_checkpoint();
    assert!(
        matches!(completed.state, State::Completed { .. }),
        "{:?}",
        completed.state
    );
    assert_eq!(completed.active.session.binding, Some(binding.clone()));
    assert_eq!(
        b.opens.load(Ordering::SeqCst),
        before_b,
        "kernel recovery must not reselect a healthy B"
    );
    assert_eq!(a.opens.load(Ordering::SeqCst), 2);
    let reordered = FallbackModel::new(vec![
        FallbackCandidate::new("a", a.clone()),
        FallbackCandidate::new("b", b.clone()),
    ])
    .unwrap();
    let mut resumed = open();
    resumed.binding = Some(binding.clone());
    resumed.request.messages = conversation(completed.history.entries());
    let before_b = b.opens.load(Ordering::SeqCst);
    let mut session = reordered.open_session(resumed.clone()).await.unwrap();
    assert_eq!(session.input.binding(), Some(binding));
    assert_eq!(
        b.opens.load(Ordering::SeqCst),
        before_b,
        "recovery must not retry candidates"
    );
    assert_eq!(a.opens.load(Ordering::SeqCst), 3);
    receive(&mut session).await;
    finish(&mut session).await;
    let missing = FallbackModel::new(vec![FallbackCandidate::new("b", b.clone())]).unwrap();
    assert!(missing.open_session(resumed).await.is_err());
    assert_eq!(
        b.opens.load(Ordering::SeqCst),
        before_b,
        "missing binding must fail before opening another model"
    );
}

#[test]
fn persistent_call_index_settles_on_a_small_stack_without_mutating_snapshots() {
    std::thread::Builder::new()
        .name("bounded-history-stack".into())
        .stack_size(512 * 1024)
        .spawn(|| {
            use zhir_core::{
                operation::{CallRef, OperationOutcome},
                run::History,
                tool::{RuntimeToolCall, RuntimeToolInput},
            };
            let calls = (0..256)
                .map(|index| {
                    let id = format!("call-{index}");
                    HistoryEntry {
                        id: id.clone(),
                        origin: Some(CallRef {
                            session_id: "s".into(),
                            item_id: id.clone(),
                            generation_id: Some("g".into()),
                            caller_id: "model".into(),
                            call_id: id.clone(),
                        }),
                        message: Message::Assistant {
                            output: vec![Output::RuntimeToolCall {
                                call: RuntimeToolCall {
                                    id,
                                    name: "tool".into(),
                                    input: RuntimeToolInput::Structured(
                                        json!({"argument":"value"}),
                                    ),
                                },
                            }],
                            provider_data: Value::Null,
                        },
                    }
                })
                .collect();
            let original = History::from_entries(calls).unwrap();
            let digest = original.digest();
            let pending: Vec<_> = original
                .pending_calls()
                .map(|(origin, call)| (origin.clone(), call.id.clone()))
                .collect();
            let mut settled = original.clone();
            for (origin, id) in pending.into_iter().rev() {
                settled = settled
                    .append(vec![HistoryEntry {
                        id: format!("result:{id}"),
                        origin: Some(origin),
                        message: Message::RuntimeTool {
                            call_id: id,
                            name: "tool".into(),
                            outcome: OperationOutcome::Success {
                                content: vec![],
                                structured: Value::Null,
                            },
                        },
                    }])
                    .unwrap();
            }
            original.validate().unwrap();
            settled.validate().unwrap();
            assert_eq!(original.digest(), digest);
            assert_eq!(original.pending_calls().count(), 256);
            assert_eq!(settled.pending_calls().count(), 0);
            assert_eq!(settled.len(), 512);
        })
        .unwrap()
        .join()
        .unwrap();
}
