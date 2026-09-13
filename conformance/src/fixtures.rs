use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use zhir_core::{
    BoxFuture, Result,
    error::{Error, Failure},
    model::*,
    operation::*,
    profile::NegotiatedProfile,
    tool::*,
};
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    #[serde(default)]
    delay_ms: u64,
    #[serde(default)]
    deltas: Vec<ModelDelta>,
    #[serde(default)]
    output: Option<TurnOutput>,
    #[serde(default)]
    error: Option<Failure>,
}
pub struct CaseModel {
    steps: Arc<Mutex<VecDeque<Step>>>,
    capabilities: CapabilitySet,
}
impl CaseModel {
    pub fn new(steps: Vec<Step>) -> Self {
        let mut capabilities = zhir_testing::model_capabilities();
        capabilities
            .features
            .extend([Capability::FreeformTools, Capability::ProviderTools]);
        Self {
            steps: Arc::new(Mutex::new(steps.into())),
            capabilities,
        }
    }
    pub fn remaining(&self) -> usize {
        self.steps.lock().unwrap().len()
    }
}
impl Model for CaseModel {
    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        zhir_policies::negotiation::negotiate(request, &self.capabilities)
    }
    fn open_session(&self, open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            let steps = self.steps.clone();
            let model =
                zhir_models::FunctionModel::new(self.capabilities.clone(), move |_, context| {
                    let step = steps.lock().unwrap().pop_front();
                    async move {
                        let step =
                            step.ok_or_else(|| Error::Protocol("script exhausted".into()))?;
                        tokio::time::sleep(Duration::from_millis(step.delay_ms)).await;
                        for delta in step.deltas {
                            if let Some(sink) = &context.deltas {
                                sink.emit(delta).await?;
                            }
                        }
                        if let Some(error) = step.error {
                            return Err(Error::Model(error));
                        }
                        step.output
                            .ok_or_else(|| Error::Protocol("missing step output".into()))
                    }
                });
            model.open_session(open).await
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tool {
    spec: RuntimeToolSpec,
    #[serde(default)]
    outcome: Option<RuntimeToolOutcome>,
    #[serde(default)]
    delay_ms: u64,
    #[serde(default)]
    waiting: Option<Value>,
}
impl RuntimeTool for Tool {
    fn spec(&self) -> &RuntimeToolSpec {
        &self.spec
    }
    fn start(
        &self,
        _: RuntimeToolCall,
        _: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<ToolExecution>> {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
            if let Some(prompt) = &self.waiting {
                return Ok(ToolExecution::Active(OperationHandle {
                    recovery: Some(RecoveryRef {
                        adapter: "fixture".into(),
                        data: prompt.clone(),
                    }),
                    control: Arc::new(Control),
                    events: Box::new(Events(Some(prompt.clone()))),
                }));
            }
            self.outcome
                .clone()
                .map(ToolExecution::Finished)
                .ok_or_else(|| Error::Protocol("missing tool outcome".into()))
        })
    }
}
struct Control;
impl OperationControl for Control {
    fn cancel(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn reply(&self, _: Value) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Err(Error::Invalid("fixture uses explicit resolution".into())) })
    }
}
struct Events(Option<Value>);
impl OperationEvents for Events {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<OperationEvent>>> {
        Box::pin(async move {
            match self.0.take() {
                Some(prompt) => Ok(Some(OperationEvent {
                    sequence: 0,
                    update: OperationUpdate::Waiting {
                        prompt,
                        recovery: None,
                    },
                })),
                None => std::future::pending().await,
            }
        })
    }
}
