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
    model::{
        Capabilities, Model, ModelContext, ModelDelta, ModelOptions, ModelRequest, ModelResponse,
        ToolChoice,
    },
    run::RunContext,
};
use zhir_models::decorators::{FallbackModel, RetryingModel};

struct Flaky {
    calls: AtomicUsize,
    failures: usize,
    retryable: bool,
    emit: bool,
    capabilities: Capabilities,
}
impl Flaky {
    fn new(failures: usize, retryable: bool, emit: bool) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            failures,
            retryable,
            emit,
            capabilities: Capabilities::default(),
        })
    }
}
impl Model for Flaky {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
    fn invoke(
        &self,
        _: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            if attempt < self.failures {
                if self.emit
                    && let Some(sink) = context.deltas
                {
                    sink.emit(ModelDelta::Text {
                        output_index: 0,
                        text: "visible".into(),
                    })
                    .await?;
                }
                return Err(Error::Model(Failure {
                    code: "fixture".into(),
                    message: "failed".into(),
                    retryable: self.retryable,
                }));
            }
            Ok(ModelResponse::text("done"))
        })
    }
}
fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![Message::user("hello")],
        runtime_tools: vec![],
        provider_tools: vec![],
        options: ModelOptions::default(),
        tool_choice: ToolChoice::Auto,
        response_format: None,
        stream: true,
    }
}
fn context() -> ModelContext {
    ModelContext {
        run: RunContext::default(),
        cancellation: Cancellation::default(),
        deltas: None,
    }
}
#[tokio::test]
async fn retries_only_retryable_failures_before_stream_output() {
    for (retryable, emit, expected) in [(true, false, 2), (false, false, 1), (true, true, 1)] {
        let inner = Flaky::new(1, retryable, emit);
        let model = RetryingModel::new(inner.clone(), 3, Duration::ZERO).unwrap();
        let result = model.invoke(request(), context()).await;
        assert_eq!(result.is_ok(), expected == 2);
        assert_eq!(inner.calls.load(Ordering::SeqCst), expected);
    }
}
#[tokio::test]
async fn fallback_stops_at_permanent_errors_and_visible_output() {
    for (retryable, emit, expected) in [(true, false, 1), (false, false, 0), (true, true, 0)] {
        let first = Flaky::new(1, retryable, emit);
        let second = Flaky::new(0, true, false);
        let model = FallbackModel::new(vec![first, second.clone()]).unwrap();
        let result = model.invoke(request(), context()).await;
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
    calls: AtomicUsize,
    completed: AtomicUsize,
    capabilities: Capabilities,
}
impl Emitting {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            capabilities: Capabilities::default(),
        })
    }
}
impl Model for Emitting {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            for ordinal in 0..2 {
                tokio::task::yield_now().await;
                if let Some(sink) = &context.deltas {
                    sink.emit(ModelDelta::ProtocolEvent {
                        output_index: 0,
                        data: serde_json::json!({"ordinal":ordinal,"run_id":context.run.run_id,"tag":request.options.extra.get("tag")}),
                    }).await?;
                }
            }
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(ModelResponse::text("done"))
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
        let tag = request.options.extra["tag"].clone();
        assert_eq!(run.metadata["tag"], tag);
        let journal = Arc::new(Journal::default());
        captured
            .lock()
            .unwrap()
            .push((run.run_id.clone(), tag, journal.clone()));
        Ok(journal)
    });
    assert_eq!(
        model.capabilities().streaming,
        inner.capabilities().streaming
    );
    let outputs = futures::future::join_all((0..32).map(|index| {
        let model = &model;
        async move {
            let mut request = request();
            request.options.extra.insert("tag".into(), index.into());
            let mut context = context();
            context.run.metadata.insert("tag".into(), index.into());
            model.invoke(request, context).await.unwrap()
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
        let job = tokio::spawn(async move { model.invoke(request(), context()).await });
        tokio::time::timeout(Duration::from_secs(2), sink.entered.notified())
            .await
            .unwrap();
        assert!(!job.is_finished());
        assert_eq!(inner.completed.load(Ordering::SeqCst), 0);
        assert_eq!(sink.writes.load(Ordering::SeqCst), 0);
        if cancel {
            job.abort();
            assert!(job.await.unwrap_err().is_cancelled());
            sink.release.add_permits(2);
            tokio::task::yield_now().await;
            assert_eq!(sink.writes.load(Ordering::SeqCst), 0);
            assert_eq!(inner.completed.load(Ordering::SeqCst), 0);
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
                    3,
                    Duration::ZERO,
                )
                .unwrap(),
            )
        } else {
            Arc::new(ObservedModel::new(
                Arc::new(RetryingModel::new(inner.clone(), 3, Duration::ZERO).unwrap()),
                move |_, _| Ok(captured.clone()),
            ))
        };
        assert!(
            matches!(model.invoke(request(), context()).await, Err(Error::Model(f)) if f.code=="observer_write")
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner.completed.load(Ordering::SeqCst), 0);
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
                    3,
                    Duration::ZERO,
                )
                .unwrap(),
            )
        } else {
            Arc::new(ObservedModel::new(
                Arc::new(RetryingModel::new(inner.clone(), 3, Duration::ZERO).unwrap()),
                factory,
            ))
        };
        model.invoke(request(), context()).await.unwrap();
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
        let error = model.invoke(request(), ctx).await.unwrap_err();
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
    assert!(model.invoke(request(), ctx).await.is_err());
    assert_eq!(downstream.values.lock().unwrap().len(), 1);
    assert!(observer.values.lock().unwrap().is_empty());
    assert_eq!(inner.completed.load(Ordering::SeqCst), 0);
}
