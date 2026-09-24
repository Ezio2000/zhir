use std::{sync::Arc, time::Duration};
use zhir_core::{
    BoxFuture, Result,
    model::*,
    profile::NegotiatedProfile,
    run::{Checkpoint, LimitReason, State},
    storage::{Commit, RunStore},
    tool::*,
};
use zhir_kernel::{RunRequest, Runtime};
struct OpeningModel {
    caps: CapabilitySet,
}
impl Model for OpeningModel {
    fn capabilities(&self) -> &CapabilitySet {
        &self.caps
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        zhir_policies::negotiation::negotiate(request, &self.caps)
    }
    fn open_session(&self, _: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(std::future::pending())
    }
}
struct OpeningCatalog;
impl RuntimeToolCatalogProvider for OpeningCatalog {
    fn open_catalog(
        &self,
        _: CatalogContext,
    ) -> BoxFuture<'_, Result<Arc<dyn RuntimeToolCatalog>>> {
        Box::pin(std::future::pending())
    }
}
struct HangingStore;
impl RunStore for HangingStore {
    fn commit(&self, _: Commit) -> BoxFuture<'_, Result<()>> {
        Box::pin(std::future::pending())
    }
    fn load_head(&self, _: &str) -> BoxFuture<'_, Result<Option<Arc<Checkpoint>>>> {
        Box::pin(async { Ok(None) })
    }
    fn delete(&self, _: &str) -> BoxFuture<'_, Result<()>> {
        Box::pin(std::future::pending())
    }
}
#[tokio::test]
async fn execution_deadline_covers_catalog_and_model_session_establishment() {
    for catalog in [false, true] {
        let mut builder = Runtime::builder(Arc::new(OpeningModel {
            caps: zhir_testing::model_capabilities(),
        }));
        if catalog {
            builder = builder.runtime_tools(Arc::new(OpeningCatalog));
        }
        let runtime = builder.build().unwrap();
        let request = RunRequest::new(vec![zhir_core::message::Message::user("start")]).limits(
            zhir_core::run::Limits {
                elapsed_ms: Some(30),
                ..zhir_kernel::defaults::limits()
            },
        );
        let completion = tokio::time::timeout(
            Duration::from_secs(2),
            runtime.start(request).unwrap().result(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            completion.checkpoint().state,
            State::Limited {
                reason: LimitReason::Deadline
            }
        ));
    }
}
#[tokio::test]
async fn commit_timeout_reports_uncertainty_without_fabricating_a_checkpoint() {
    let runtime = Runtime::builder(Arc::new(OpeningModel {
        caps: zhir_testing::model_capabilities(),
    }))
    .store(Arc::new(HangingStore))
    .build()
    .unwrap();
    let request = RunRequest::new(vec![zhir_core::message::Message::user("start")]).limits(
        zhir_core::run::Limits {
            commit_timeout_ms: 30,
            ..zhir_kernel::defaults::limits()
        },
    );
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        runtime.start(request).unwrap().result(),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(error.error, zhir_core::error::Error::Storage(_)));
    assert!(error.last_checkpoint.is_none());
}

/// Counts engine loop iterations: the kernel reads session capabilities once per pass.
struct CountingModel {
    inner: Arc<dyn Model>,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}
struct CountingControl {
    inner: Arc<dyn SessionControl>,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}
impl SessionControl for CountingControl {
    fn binding(&self) -> Option<ModelBinding> {
        self.inner.binding()
    }
    fn capabilities(&self) -> &CapabilitySet {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn submit(&self, command: SessionCommand) -> BoxFuture<'_, Result<()>> {
        self.inner.submit(command)
    }
}
impl Model for CountingModel {
    fn capabilities(&self) -> &CapabilitySet {
        self.inner.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn open_session(&self, open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            let mut session = self.inner.open_session(open).await?;
            session.control = Arc::new(CountingControl {
                inner: session.control,
                reads: self.reads.clone(),
            });
            Ok(session)
        })
    }
}
fn idle_runtime() -> (Runtime, Arc<std::sync::atomic::AtomicUsize>) {
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let model = Arc::new(CountingModel {
        inner: Arc::new(zhir_models::FunctionModel::new(
            zhir_testing::model_capabilities(),
            |_, _| async { Ok(GenerationOutput::text("ready")) },
        )),
        reads: reads.clone(),
    });
    (Runtime::builder(model).build().unwrap(), reads)
}
fn interactive(elapsed_ms: Option<u64>) -> RunRequest {
    RunRequest::new(vec![zhir_core::message::Message::user("start")])
        .mode(zhir_core::run::RunMode::Interactive)
        .limits(zhir_core::run::Limits {
            elapsed_ms,
            ..zhir_kernel::defaults::limits()
        })
}
#[tokio::test(start_paused = true)]
async fn idle_runs_wait_without_polling_and_settle_cancellation_immediately() {
    let (runtime, reads) = idle_runtime();
    let mut invocation = runtime.start(interactive(None)).unwrap();
    invocation.start();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle = reads.load(std::sync::atomic::Ordering::Relaxed);
    assert!(idle > 0, "the run never reached its session");
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(reads.load(std::sync::atomic::Ordering::Relaxed), idle);
    let before = tokio::time::Instant::now();
    invocation.control().cancel();
    let settled = match invocation.result().await {
        Ok(completion) => completion.into_checkpoint(),
        Err(error) => error.last_checkpoint.expect("cancelled checkpoint"),
    };
    assert_eq!(tokio::time::Instant::now(), before);
    assert!(
        matches!(settled.state, State::Cancelled),
        "{:?}",
        settled.state
    );
}
#[tokio::test(start_paused = true)]
async fn idle_run_deadline_is_driven_by_a_timer() {
    let (runtime, reads) = idle_runtime();
    let mut invocation = runtime.start(interactive(Some(5_000))).unwrap();
    let started = tokio::time::Instant::now();
    let settled = match invocation.result().await {
        Ok(completion) => completion.into_checkpoint(),
        Err(error) => error.last_checkpoint.expect("limited checkpoint"),
    };
    assert!(started.elapsed() >= Duration::from_millis(4_900));
    assert!(
        matches!(
            settled.state,
            State::Limited {
                reason: LimitReason::Deadline
            }
        ),
        "{:?}",
        settled.state
    );
    assert!(reads.load(std::sync::atomic::Ordering::Relaxed) < 20);
}
