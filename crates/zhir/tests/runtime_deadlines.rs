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
