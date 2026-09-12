use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    model::{Capabilities, Model, ModelContext, ModelDelta, ModelRequest, ModelResponse},
    run::RunContext,
};
#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub request: ModelRequest,
    pub run: RunContext,
}
#[derive(Clone, Debug)]
pub struct ModelRecord {
    pub input: RecordedRequest,
    /// None means the invocation has not returned, including a dropped future.
    pub outcome: Option<Result<ModelResponse>>,
}
#[derive(Clone, Debug)]
pub struct ScriptStep {
    pub deltas: Vec<ModelDelta>,
    pub outcome: Result<ModelResponse>,
}
impl ScriptStep {
    pub fn response(response: ModelResponse) -> Self {
        Self {
            deltas: vec![],
            outcome: Ok(response),
        }
    }
    pub fn failure(error: Error) -> Self {
        Self {
            deltas: vec![],
            outcome: Err(error),
        }
    }
    pub fn with_deltas(mut self, deltas: impl IntoIterator<Item = ModelDelta>) -> Self {
        self.deltas = deltas.into_iter().collect();
        self
    }
}
struct Script {
    steps: VecDeque<ScriptStep>,
    requests: Vec<RecordedRequest>,
}
pub struct ScriptedModel {
    capabilities: Capabilities,
    script: Mutex<Script>,
}
impl ScriptedModel {
    pub fn new(steps: impl IntoIterator<Item = ScriptStep>) -> Self {
        Self {
            capabilities: Capabilities::default(),
            script: Mutex::new(Script {
                steps: steps.into_iter().collect(),
                requests: vec![],
            }),
        }
    }
    pub fn responses(responses: impl IntoIterator<Item = ModelResponse>) -> Self {
        Self::new(responses.into_iter().map(ScriptStep::response))
    }
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self
    }
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.script.lock().expect("script lock").requests.clone()
    }
    pub fn remaining(&self) -> usize {
        self.script.lock().expect("script lock").steps.len()
    }
}
impl Model for ScriptedModel {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            context.cancellation.check()?;
            request.validate(&self.capabilities)?;
            let step = {
                let mut script = self.script.lock().expect("script lock");
                script.requests.push(RecordedRequest {
                    request,
                    run: context.run.clone(),
                });
                script
                    .steps
                    .pop_front()
                    .ok_or_else(|| Error::Protocol("model script exhausted".into()))?
            };
            for delta in step.deltas {
                context.cancellation.check()?;
                if let Some(sink) = &context.deltas {
                    sink.emit(delta).await?;
                }
            }
            context.cancellation.check()?;
            let response = step.outcome?;
            response.validate()?;
            Ok(response)
        })
    }
}

pub struct RecordingModel {
    inner: Arc<dyn Model>,
    records: Mutex<Vec<ModelRecord>>,
}
impl RecordingModel {
    pub fn new(inner: Arc<dyn Model>) -> Self {
        Self {
            inner,
            records: Mutex::new(vec![]),
        }
    }
    pub fn records(&self) -> Vec<ModelRecord> {
        self.records.lock().expect("model records lock").clone()
    }
}
impl Model for RecordingModel {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            let index = {
                let mut records = self.records.lock().expect("model records lock");
                let index = records.len();
                records.push(ModelRecord {
                    input: RecordedRequest {
                        request: request.clone(),
                        run: context.run.clone(),
                    },
                    outcome: None,
                });
                index
            };
            let result = self.inner.invoke(request, context).await;
            self.records.lock().expect("model records lock")[index].outcome = Some(result.clone());
            result
        })
    }
}
