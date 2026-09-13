use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use zhir_core::{
    BoxFuture, Cancellation, Result,
    error::{Error, Failure},
    message::Message,
    model::{CapabilitySet, Model, ModelContext, ModelDelta, ModelRequest, ToolChoice, TurnOutput},
    run::RunContext,
};
use zhir_models::decorators::{FallbackCandidate, FallbackModel, RetryingModel};
use zhir_testing::ModelTestExt;

struct Flaky {
    calls: Arc<AtomicUsize>,
    failures: usize,
    retryable: bool,
    emit: bool,
    capabilities: CapabilitySet,
}
impl Flaky {
    fn new(failures: usize, retryable: bool, emit: bool) -> Arc<Self> {
        Arc::new(Self {
            calls: Arc::new(AtomicUsize::new(0)),
            failures,
            retryable,
            emit,
            capabilities: zhir_models::capabilities::text_tool_calling(),
        })
    }
}
impl Model for Flaky {
    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<zhir_core::profile::NegotiatedProfile> {
        zhir_policies::negotiation::negotiate(request, &self.capabilities)
    }
    fn open_session(
        &self,
        open: zhir_core::model::SessionOpen,
    ) -> BoxFuture<'_, Result<zhir_core::model::ModelSession>> {
        Box::pin(async move {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            let fail = attempt < self.failures;
            let emit = self.emit;
            let retryable = self.retryable;
            let failure = Failure {
                code: "fixture".into(),
                message: "failed".into(),
                retryable,
            };
            if fail && !emit {
                return Err(Error::Model(failure));
            }
            let model =
                zhir_models::FunctionModel::new(self.capabilities.clone(), move |_, context| {
                    let failure = failure.clone();
                    async move {
                        if fail {
                            if emit && let Some(sink) = context.deltas {
                                sink.emit(ModelDelta::Text {
                                    output_index: 0,
                                    text: "visible".into(),
                                })
                                .await?;
                            }
                            return Err(Error::Model(failure));
                        }
                        Ok(TurnOutput::text("done"))
                    }
                });
            model.open_session(open).await
        })
    }
}
fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![Message::user("hello")],
        runtime_tools: vec![],
        provider_tools: vec![],
        profile: Default::default(),
        tool_choice: ToolChoice::Auto,
        response_format: None,
        stream: true,
    }
}
fn context() -> ModelContext {
    ModelContext {
        run: RunContext::new("model-test", 0),
        cancellation: Cancellation::default(),
        deltas: None,
    }
}
#[tokio::test]
async fn retries_only_retryable_session_establishment_before_any_turn() {
    for (retryable, emit, expected) in [(true, false, 2), (false, false, 1), (true, true, 1)] {
        let inner = Flaky::new(1, retryable, emit);
        let model = RetryingModel::new(
            inner.clone(),
            zhir_policies::RetryPolicy::new(3)
                .unwrap()
                .backoff(zhir_policies::Backoff::fixed(Duration::ZERO)),
        )
        .unwrap();
        let result = model.turn(request(), context()).await;
        assert_eq!(result.is_ok(), expected == 2);
        assert_eq!(inner.calls.load(Ordering::SeqCst), expected);
    }
}
#[tokio::test]
async fn fallback_stops_at_permanent_errors_and_visible_output() {
    for (retryable, emit, expected) in [(true, false, 1), (false, false, 0), (true, true, 0)] {
        let first = Flaky::new(1, retryable, emit);
        let second = Flaky::new(0, true, false);
        let model = FallbackModel::new(vec![
            FallbackCandidate::new("first", first),
            FallbackCandidate::new("second", second.clone()),
        ])
        .unwrap();
        let result = model.turn(request(), context()).await;
        assert_eq!(result.is_ok(), expected == 1);
        assert_eq!(second.calls.load(Ordering::SeqCst), expected);
    }
}

#[derive(Default)]
struct Journal {
    values: std::sync::Mutex<Vec<ModelDelta>>,
    fail: bool,
}
impl zhir_core::model::DeltaSink for Journal {
    fn emit(&self, delta: ModelDelta) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.values.lock().unwrap().push(delta);
            if self.fail {
                return Err(Error::Model(Failure {
                    code: "observer_write".into(),
                    message: "fixture write failed".into(),
                    retryable: true,
                }));
            }
            Ok(())
        })
    }
}
struct Emitting {
    calls: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    capabilities: CapabilitySet,
}
impl Emitting {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: Arc::new(AtomicUsize::new(0)),
            completed: Arc::new(AtomicUsize::new(0)),
            capabilities: zhir_models::capabilities::text_tool_calling(),
        })
    }
}
impl Model for Emitting {
    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<zhir_core::profile::NegotiatedProfile> {
        zhir_policies::negotiation::negotiate(request, &self.capabilities)
    }
    fn open_session(
        &self,
        open: zhir_core::model::SessionOpen,
    ) -> BoxFuture<'_, Result<zhir_core::model::ModelSession>> {
        Box::pin(async move {
            let calls = self.calls.clone();
            let completed = self.completed.clone();
            let model = zhir_models::FunctionModel::new(
                self.capabilities.clone(),
                move |request, context| {
                    let calls = calls.clone();
                    let completed = completed.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        for ordinal in 0..2 {
                            tokio::task::yield_now().await;
                            if let Some(sink) = &context.deltas {
                                sink.emit(ModelDelta::ProtocolEvent { output_index:0,data:serde_json::json!({"ordinal":ordinal,"run_id":context.run.run_id,"tag":request.profile.extensions.get("fixture").and_then(|fields|fields.get("tag"))}) }).await?;
                            }
                        }
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(TurnOutput::text("done"))
                    }
                },
            );
            model.open_session(open).await
        })
    }
}
#[tokio::test]
async fn observers_are_isolated_per_concurrent_invocation_and_receive_context() {
    use zhir_models::decorators::ObservedModel;
    let inner = Emitting::new();
    let journals = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = journals.clone();
    let model = ObservedModel::new(inner.clone(), move |request, run| {
        let tag = request.profile.extensions["fixture"]["tag"].clone();
        assert_eq!(run.metadata["tag"], tag);
        let journal = Arc::new(Journal::default());
        captured
            .lock()
            .unwrap()
            .push((run.run_id.clone(), tag, journal.clone()));
        Ok(journal)
    });
    assert_eq!(
        model
            .capabilities()
            .supports(zhir_core::model::Capability::Streaming),
        inner
            .capabilities()
            .supports(zhir_core::model::Capability::Streaming)
    );
    let outputs = futures::future::join_all((0..32).map(|index| {
        let model = &model;
        async move {
            let mut request = request();
            request
                .profile
                .extensions
                .entry("fixture".into())
                .or_default()
                .insert("tag".into(), index.into());
            let mut context = context();
            context.run.metadata.insert("tag".into(), index.into());
            model.turn(request, context).await.unwrap()
        }
    }))
    .await;
    assert_eq!(outputs.len(), 32);
    let journals = journals.lock().unwrap();
    assert_eq!(journals.len(), 32);
    for (run_id, tag, journal) in journals.iter() {
        let values = journal.values.lock().unwrap();
        assert_eq!(values.len(), 2);
        for (ordinal, value) in values.iter().enumerate() {
            assert!(
                matches!(value, ModelDelta::ProtocolEvent {data,..} if data["run_id"]==*run_id && data["tag"]==*tag && data["ordinal"]==ordinal)
            );
        }
    }
}
struct GatedSink {
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    writes: AtomicUsize,
}
impl zhir_core::model::DeltaSink for GatedSink {
    fn emit(&self, _: ModelDelta) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}
#[tokio::test]
async fn observer_backpressure_is_awaited_and_dropped_calls_do_not_spawn_writes() {
    use zhir_models::decorators::ObservedModel;
    for cancel in [false, true] {
        let inner = Emitting::new();
        let sink = Arc::new(GatedSink {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
            writes: AtomicUsize::new(0),
        });
        let captured = sink.clone();
        let model = ObservedModel::new(inner.clone(), move |_, _| Ok(captured.clone()));
        let job = tokio::spawn(async move { model.turn(request(), context()).await });
        tokio::time::timeout(Duration::from_secs(2), sink.entered.notified())
            .await
            .unwrap();
        assert!(!job.is_finished());
        assert_eq!(sink.writes.load(Ordering::SeqCst), 0);
        if cancel {
            job.abort();
            assert!(job.await.unwrap_err().is_cancelled());
            sink.release.add_permits(2);
            tokio::task::yield_now().await;
            assert_eq!(sink.writes.load(Ordering::SeqCst), 0);
        } else {
            sink.release.add_permits(2);
            job.await.unwrap().unwrap();
            assert_eq!(sink.writes.load(Ordering::SeqCst), 2);
            assert_eq!(inner.completed.load(Ordering::SeqCst), 1);
        }
    }
}
#[tokio::test]
async fn observer_errors_after_side_effects_never_retry_in_either_nesting_order() {
    use zhir_models::decorators::ObservedModel;
    for observer_inside in [false, true] {
        let inner = Emitting::new();
        let sink = Arc::new(Journal {
            fail: true,
            ..Default::default()
        });
        let captured = sink.clone();
        let model: Arc<dyn Model> = if observer_inside {
            Arc::new(
                RetryingModel::new(
                    Arc::new(ObservedModel::new(inner.clone(), move |_, _| {
                        Ok(captured.clone())
                    })),
                    zhir_policies::RetryPolicy::new(3)
                        .unwrap()
                        .backoff(zhir_policies::Backoff::fixed(Duration::ZERO)),
                )
                .unwrap(),
            )
        } else {
            Arc::new(ObservedModel::new(
                Arc::new(
                    RetryingModel::new(
                        inner.clone(),
                        zhir_policies::RetryPolicy::new(3)
                            .unwrap()
                            .backoff(zhir_policies::Backoff::fixed(Duration::ZERO)),
                    )
                    .unwrap(),
                ),
                move |_, _| Ok(captured.clone()),
            ))
        };
        assert!(
            matches!(model.turn(request(), context()).await, Err(Error::Model(f)) if f.code=="observer_write")
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(sink.values.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn observer_factory_scope_tracks_wrapper_invocations_across_retries() {
    use zhir_models::decorators::ObservedModel;
    for observer_inside in [false, true] {
        let inner = Flaky::new(1, true, false);
        let factories = Arc::new(AtomicUsize::new(0));
        let captured = factories.clone();
        let factory = move |_: &ModelRequest,
                            _: &RunContext|
              -> Result<Arc<dyn zhir_core::model::DeltaSink>> {
            captured.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(Journal::default()))
        };
        let model: Arc<dyn Model> = if observer_inside {
            Arc::new(
                RetryingModel::new(
                    Arc::new(ObservedModel::new(inner.clone(), factory)),
                    zhir_policies::RetryPolicy::new(3)
                        .unwrap()
                        .backoff(zhir_policies::Backoff::fixed(Duration::ZERO)),
                )
                .unwrap(),
            )
        } else {
            Arc::new(ObservedModel::new(
                Arc::new(
                    RetryingModel::new(
                        inner.clone(),
                        zhir_policies::RetryPolicy::new(3)
                            .unwrap()
                            .backoff(zhir_policies::Backoff::fixed(Duration::ZERO)),
                    )
                    .unwrap(),
                ),
                factory,
            ))
        };
        model.turn(request(), context()).await.unwrap();
        assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            factories.load(Ordering::SeqCst),
            if observer_inside { 2 } else { 1 }
        );
    }
}
#[tokio::test]
async fn observer_factory_failures_and_pre_cancelled_calls_do_not_invoke_model() {
    use zhir_models::decorators::ObservedModel;
    for cancelled in [false, true] {
        let inner = Emitting::new();
        let factories = Arc::new(AtomicUsize::new(0));
        let captured = factories.clone();
        let model = ObservedModel::new(inner.clone(), move |_, _| {
            captured.fetch_add(1, Ordering::SeqCst);
            Err(Error::Invalid("factory unavailable".into()))
        });
        let ctx = context();
        if cancelled {
            ctx.cancellation.cancel();
        }
        let error = model.turn(request(), ctx).await.unwrap_err();
        assert!(if cancelled {
            matches!(error, Error::Cancelled)
        } else {
            matches!(error, Error::Invalid(_))
        });
        assert_eq!(factories.load(Ordering::SeqCst), usize::from(!cancelled));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn downstream_sink_errors_stop_before_observer_side_effects() {
    use zhir_models::decorators::ObservedModel;
    let inner = Emitting::new();
    let observer = Arc::new(Journal::default());
    let downstream = Arc::new(Journal {
        fail: true,
        ..Default::default()
    });
    let captured = observer.clone();
    let model = ObservedModel::new(inner.clone(), move |_, _| Ok(captured.clone()));
    let mut ctx = context();
    ctx.deltas = Some(downstream.clone());
    assert!(model.turn(request(), ctx).await.is_err());
    assert_eq!(downstream.values.lock().unwrap().len(), 1);
    assert!(observer.values.lock().unwrap().is_empty());
    assert_eq!(inner.completed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn fallback_recovery_uses_stable_identity_after_candidate_reordering() {
    use zhir_core::{
        model::{SessionEventBody, SessionOpen},
        operation::RecoveryRef,
    };
    let opens = Arc::new(AtomicUsize::new(0));
    let selected: Arc<dyn Model> = Arc::new(zhir_testing::SessionModel::new(
        zhir_testing::model_capabilities(),
        {
            let opens = opens.clone();
            move |open, mut peer| {
                let opens = opens.clone();
                async move {
                    let count = opens.fetch_add(1, Ordering::SeqCst);
                    if count == 1 {
                        assert_eq!(
                            open.recovery,
                            Some(RecoveryRef {
                                adapter: "native".into(),
                                data: serde_json::json!(17)
                            })
                        );
                    }
                    peer.event(SessionEventBody::Recovery {
                        reference: RecoveryRef {
                            adapter: "native".into(),
                            data: serde_json::json!(17),
                        },
                    })
                    .await
                }
            }
        },
    ));
    let failed = Flaky::new(100, true, false);
    let candidates = || {
        vec![
            FallbackCandidate::new("unavailable", failed.clone()),
            FallbackCandidate::new("selected", selected.clone()),
        ]
    };
    let model = FallbackModel::new(candidates()).unwrap();
    let mut open = SessionOpen {
        session_id: "session".into(),
        after_sequence: None,
        epoch: 0,
        limits: zhir_kernel::defaults::limits(),
        request: request(),
        recovery: None,
        context: context(),
    };
    let mut session = model.open_session(open.clone()).await.unwrap();
    let event = session.output.receive().await.unwrap().unwrap();
    let SessionEventBody::Recovery { reference } = event.body else {
        panic!("expected recovery");
    };
    assert_eq!(reference.adapter, "zhir.fallback");
    open.recovery = Some(reference);
    open.after_sequence = Some(event.sequence);
    drop(session);
    let mut reordered = candidates();
    reordered.reverse();
    let recovered = FallbackModel::new(reordered).unwrap();
    let mut session = recovered.open_session(open.clone()).await.unwrap();
    assert!(session.output.receive().await.unwrap().is_some());
    assert_eq!(opens.load(Ordering::SeqCst), 2);
    assert_eq!(failed.calls.load(Ordering::SeqCst), 1);
    let missing =
        FallbackModel::new(vec![FallbackCandidate::new("unavailable", failed.clone())]).unwrap();
    assert!(missing.open_session(open).await.is_err());
    assert_eq!(failed.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn session_concurrency_lease_survives_cloned_command_port() {
    use zhir_core::model::SessionOpen;
    let inner = Arc::new(zhir_testing::SessionModel::new(
        zhir_testing::model_capabilities(),
        |_, mut peer| async move {
            while peer.commands.recv().await.is_some() {}
            Ok(())
        },
    ));
    let model = zhir_models::ConcurrencyLimitedModel::new(inner, 1).unwrap();
    let open = SessionOpen {
        session_id: "session".into(),
        after_sequence: None,
        epoch: 0,
        limits: zhir_kernel::defaults::limits(),
        request: request(),
        recovery: None,
        context: context(),
    };
    let session = model.open_session(open.clone()).await.unwrap();
    let retained = session.input.clone();
    drop(session);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), model.open_session(open.clone()))
            .await
            .is_err()
    );
    drop(retained);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), model.open_session(open))
            .await
            .unwrap()
            .is_ok()
    );
}
