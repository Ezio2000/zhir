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
type Matcher = dyn Fn(&RecordedRequest) -> bool + Send + Sync;
pub struct ModelCase {
    name: String,
    matcher: Option<Arc<Matcher>>,
    steps: VecDeque<ScriptStep>,
}
impl ModelCase {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            matcher: None,
            steps: VecDeque::new(),
        }
    }
    pub fn when(
        mut self,
        matcher: impl Fn(&RecordedRequest) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.matcher = Some(Arc::new(matcher));
        self
    }
    pub fn steps(mut self, steps: impl IntoIterator<Item = ScriptStep>) -> Self {
        self.steps = steps.into_iter().collect();
        self
    }
}
enum Steps {
    Ordered(VecDeque<ScriptStep>),
    Matching(Vec<ModelCase>),
}
struct Script {
    steps: Steps,
    violations: Vec<String>,
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
                steps: Steps::Ordered(steps.into_iter().collect()),
                violations: vec![],
                requests: vec![],
            }),
        }
    }
    pub fn matching(cases: impl IntoIterator<Item = ModelCase>) -> Result<Self> {
        let cases: Vec<_> = cases.into_iter().collect();
        let mut names = std::collections::BTreeSet::new();
        for case in &cases {
            if case.name.is_empty() || case.matcher.is_none() || !names.insert(&case.name) {
                return Err(Error::Invalid(
                    "model cases require unique nonempty names and matchers".into(),
                ));
            }
        }
        Ok(Self {
            capabilities: Capabilities::default(),
            script: Mutex::new(Script {
                steps: Steps::Matching(cases),
                requests: vec![],
                violations: vec![],
            }),
        })
    }
    pub fn verify(&self) -> Result<()> {
        let script = self.script.lock().expect("script lock");
        let mut issues = script.violations.clone();
        match &script.steps {
            Steps::Ordered(steps) if !steps.is_empty() => {
                issues.push(format!("{} unconsumed ordered steps", steps.len()))
            }
            Steps::Matching(cases) => {
                for case in cases {
                    if !case.steps.is_empty() {
                        issues.push(format!(
                            "case {}: {} unconsumed steps",
                            case.name,
                            case.steps.len()
                        ));
                    }
                }
            }
            _ => {}
        }
        if issues.is_empty() {
            Ok(())
        } else {
            Err(Error::Protocol(issues.join("; ")))
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
        match &self.script.lock().expect("script lock").steps {
            Steps::Ordered(s) => s.len(),
            Steps::Matching(cases) => cases.iter().map(|c| c.steps.len()).sum(),
        }
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
                let input = RecordedRequest {
                    request,
                    run: context.run.clone(),
                };
                script.requests.push(input.clone());
                let step = match &mut script.steps {
                    Steps::Ordered(steps) => steps
                        .pop_front()
                        .ok_or_else(|| "model script exhausted".to_string()),
                    Steps::Matching(cases) => {
                        let matched: Vec<_> = cases
                            .iter()
                            .enumerate()
                            .filter_map(|(i, c)| {
                                (c.matcher.as_ref().expect("validated matcher"))(&input)
                                    .then_some(i)
                            })
                            .collect();
                        match matched.as_slice() {
                            [index] => {
                                let case = &mut cases[*index];
                                case.steps
                                    .pop_front()
                                    .ok_or_else(|| format!("case {} exhausted", case.name))
                            }
                            [] => Err(format!("no model case matched run {}", input.run.run_id)),
                            _ => Err(format!(
                                "multiple model cases matched: {:?}",
                                matched.iter().map(|i| &cases[*i].name).collect::<Vec<_>>()
                            )),
                        }
                    }
                };
                match step {
                    Ok(step) => step,
                    Err(message) => {
                        script.violations.push(message.clone());
                        return Err(Error::Protocol(message));
                    }
                }
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
