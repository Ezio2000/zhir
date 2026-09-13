#![cfg(feature = "agent-runtime")]
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use zhir_builtins::agent::{AgentBackend, AgentStatus, runtime_backend::InMemoryAgentBackend};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    model::{Capabilities, Model, ModelContext, ModelRequest, ModelResponse},
    run::RunContext,
};

struct BlockingModel {
    capabilities: Capabilities,
    started: Semaphore,
    finish: Semaphore,
}
impl Model for BlockingModel {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
    fn invoke(&self, _: ModelRequest, _: ModelContext) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            self.started.add_permits(1);
            self.finish.acquire().await.unwrap().forget();
            Ok(ModelResponse::text("done"))
        })
    }
}

#[tokio::test]
async fn child_admission_is_bounded_and_slots_return_after_cancellation_and_completion() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let model = Arc::new(BlockingModel {
            capabilities: Capabilities {
                input_modalities: vec!["text".into()],
                output_modalities: vec!["text".into()],
                structured_runtime_tools: false,
                freeform_runtime_tools: false,
                provider_tools: false,
                parallel_runtime_tools: false,
                parallel_control: false,
                streaming: false,
                usage: false,
                structured_output: false,
                json_mode: false,
                seed: false,
                tool_choices: vec!["auto".into()],
            },
            started: Semaphore::new(0),
            finish: Semaphore::new(0),
        });
        let runtime = zhir_kernel::Runtime::builder(model.clone())
            .build()
            .unwrap();
        assert!(InMemoryAgentBackend::new(runtime.clone(), "", 0).is_err());
        let backend = Arc::new(InMemoryAgentBackend::new(runtime, "", 2).unwrap());
        let owner = RunContext::new("parent", 0);
        let mut starts = tokio::task::JoinSet::new();
        for index in 0..32 {
            let backend = backend.clone();
            let owner = owner.clone();
            starts.spawn(async move {
                let key = format!("key-{index}");
                (
                    key.clone(),
                    backend.start_or_get(key, "work".into(), owner).await,
                )
            });
        }
        let mut admitted = Vec::new();
        let mut rejected = 0;
        while let Some(start) = starts.join_next().await {
            let (key, result) = start.unwrap();
            match result {
                Ok(snapshot) => admitted.push((key, snapshot)),
                Err(Error::RuntimeTool(error)) if error.code == "agent_capacity" => rejected += 1,
                other => panic!("unexpected admission: {other:?}"),
            }
        }
        assert_eq!(admitted.len(), 2);
        assert_eq!(rejected, 30);
        model.started.acquire_many(2).await.unwrap().forget();
        let duplicate = backend
            .start_or_get(admitted[0].0.clone(), "work".into(), owner.clone())
            .await
            .unwrap();
        assert_eq!(duplicate.id, admitted[0].1.id);
        assert_eq!(
            backend
                .cancel(duplicate.id, owner.clone())
                .await
                .unwrap()
                .status,
            AgentStatus::Cancelled
        );
        let next = backend
            .start_or_get("next".into(), "work".into(), owner.clone())
            .await
            .unwrap();
        model.started.acquire().await.unwrap().forget();
        model.finish.add_permits(2);
        assert_eq!(
            backend
                .wait(admitted[1].1.id.clone(), owner.clone())
                .await
                .unwrap()
                .status,
            AgentStatus::Completed
        );
        assert_eq!(
            backend.wait(next.id, owner.clone()).await.unwrap().status,
            AgentStatus::Completed
        );
        let last = backend
            .start_or_get("last".into(), "work".into(), owner.clone())
            .await
            .unwrap();
        assert_eq!(
            backend.cancel(last.id, owner).await.unwrap().status,
            AgentStatus::Cancelled
        );
    })
    .await
    .expect("child tasks must settle");
}
