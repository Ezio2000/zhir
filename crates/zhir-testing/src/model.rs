use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    model::{CapabilitySet, GenerationOutput, Model, ModelContext, ModelDelta, ModelRequest},
    run::RunContext,
};
#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub request: ModelRequest,
    pub run: RunContext,
}
#[derive(Clone, Debug)]
pub struct ScriptStep {
    pub deltas: Vec<ModelDelta>,
    pub outcome: Result<GenerationOutput>,
}
impl ScriptStep {
    pub fn response(response: GenerationOutput) -> Self {
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
/// A scripted model declares the capabilities of the exchange sessions it opens
/// through `FunctionModel`.
fn exchange_capabilities(capabilities: CapabilitySet) -> CapabilitySet {
    zhir_models::function::FunctionModel::new(capabilities, |_, _| async {
        Err(Error::Invalid("capability probe".into()))
    })
    .capabilities()
    .clone()
}
pub struct ScriptedModel {
    capabilities: CapabilitySet,
    script: Arc<Mutex<Script>>,
}
impl ScriptedModel {
    pub fn new(steps: impl IntoIterator<Item = ScriptStep>) -> Self {
        Self {
            capabilities: exchange_capabilities(crate::model_capabilities()),
            script: Arc::new(Mutex::new(Script {
                steps: Steps::Ordered(steps.into_iter().collect()),
                violations: vec![],
                requests: vec![],
            })),
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
            capabilities: exchange_capabilities(crate::model_capabilities()),
            script: Arc::new(Mutex::new(Script {
                steps: Steps::Matching(cases),
                requests: vec![],
                violations: vec![],
            })),
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
    pub fn responses(responses: impl IntoIterator<Item = GenerationOutput>) -> Self {
        Self::new(responses.into_iter().map(ScriptStep::response))
    }
    pub fn with_capabilities(mut self, capabilities: CapabilitySet) -> Self {
        self.capabilities = exchange_capabilities(capabilities);
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
            let script = self.script.clone();
            let capabilities = self.capabilities.clone();
            let model = zhir_models::function::FunctionModel::new(
                self.capabilities.clone(),
                move |request: ModelRequest, context: ModelContext| {
                    let script = script.clone();
                    let capabilities = capabilities.clone();
                    async move {
                        context.cancellation.check()?;
                        request.validate(&capabilities)?;
                        let step = {
                            let mut script = script.lock().expect("script lock");
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
                                            case.steps.pop_front().ok_or_else(|| {
                                                format!("case {} exhausted", case.name)
                                            })
                                        }
                                        [] => Err(format!(
                                            "no model case matched run {}",
                                            input.run.run_id
                                        )),
                                        _ => Err(format!(
                                            "multiple model cases matched: {:?}",
                                            matched
                                                .iter()
                                                .map(|i| &cases[*i].name)
                                                .collect::<Vec<_>>()
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
                    }
                },
            );
            model.open_session(open).await
        })
    }
}

pub struct RecordingModel {
    inner: Arc<dyn Model>,
    records: Arc<Mutex<Vec<SessionRecord>>>,
}
#[derive(Clone)]
pub struct SessionRecord {
    pub opening: RecordedRequest,
    pub session_id: String,
    pub commands: Vec<zhir_core::model::SessionCommand>,
    pub events: Vec<zhir_core::model::SessionEvent>,
    pub failure: Option<Error>,
}
impl RecordingModel {
    pub fn new(inner: Arc<dyn Model>) -> Self {
        Self {
            inner,
            records: Arc::new(Mutex::new(vec![])),
        }
    }
    pub fn records(&self) -> Vec<SessionRecord> {
        self.records.lock().expect("records").clone()
    }
}
struct RecordedControl {
    inner: Arc<dyn zhir_core::model::SessionControl>,
    records: Arc<Mutex<Vec<SessionRecord>>>,
    index: usize,
}
impl zhir_core::model::SessionControl for RecordedControl {
    fn binding(&self) -> Option<zhir_core::model::ModelBinding> {
        self.inner.binding()
    }
    fn capabilities(&self) -> &CapabilitySet {
        self.inner.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<zhir_core::profile::NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn submit(&self, command: zhir_core::model::SessionCommand) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.records.lock().expect("records")[self.index]
                .commands
                .push(command.clone());
            self.inner.submit(command).await
        })
    }
}
struct RecordedEvents {
    inner: Box<dyn zhir_core::model::SessionEvents>,
    records: Arc<Mutex<Vec<SessionRecord>>>,
    index: usize,
}
impl zhir_core::model::SessionEvents for RecordedEvents {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<zhir_core::model::SessionEvent>>> {
        Box::pin(async move {
            let event = match self.inner.receive().await {
                Ok(event) => event,
                Err(error) => {
                    self.records.lock().expect("records")[self.index].failure = Some(error.clone());
                    return Err(error);
                }
            };
            if let Some(event) = &event {
                self.records.lock().expect("records")[self.index]
                    .events
                    .push(event.clone());
            }
            Ok(event)
        })
    }
}
impl Model for RecordingModel {
    fn capabilities(&self) -> &CapabilitySet {
        self.inner.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<zhir_core::profile::NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn open_session(
        &self,
        open: zhir_core::model::SessionOpen,
    ) -> BoxFuture<'_, Result<zhir_core::model::ModelSession>> {
        Box::pin(async move {
            let index = {
                let mut records = self.records.lock().expect("records");
                let index = records.len();
                records.push(SessionRecord {
                    opening: RecordedRequest {
                        request: open.request.clone(),
                        run: open.context.run.clone(),
                    },
                    session_id: open.session_id.clone(),
                    commands: vec![],
                    events: vec![],
                    failure: None,
                });
                index
            };
            let mut session = self.inner.open_session(open).await?;
            session.control = Arc::new(RecordedControl {
                inner: session.control,
                records: self.records.clone(),
                index,
            });
            session.events = Box::new(RecordedEvents {
                inner: session.events,
                records: self.records.clone(),
                index,
            });
            Ok(session)
        })
    }
}

/// Drives one native turn in adapter tests, asserting its session boundary.
pub trait ModelTestExt: Model {
    fn generate(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<GenerationOutput>> {
        Box::pin(async move {
            use zhir_core::model::*;
            let mut session = self
                .open_session(SessionOpen {
                    binding: None,
                    session_id: "test-session".into(),
                    after_sequence: None,
                    output_epoch: 0,
                    context_revision: 0,
                    input_position: 0,
                    profile_revision: 0,
                    mode: zhir_core::run::RunMode::Task,
                    limits: zhir_kernel::defaults::limits(),
                    request: request.clone(),
                    recovery: None,
                    context,
                })
                .await?;
            session
                .control
                .submit(SessionCommand {
                    id: "test-command".into(),
                    body: SessionCommandBody::Generate {
                        generation_id: "test-turn".into(),
                        context_revision: 0,
                        input_position: 0,
                        profile_revision: 0,
                    },
                })
                .await?;
            let mut result = GenerationOutput::text("");
            result.output.clear();
            while let Some(event) = session.events.receive().await? {
                match event.body {
                    SessionEventBody::Output { output, .. } => result.output.push(output),
                    SessionEventBody::ResponseFinished {
                        response_status,
                        usage,
                        model_id,
                        response_id,
                        finish_reason,
                        provider_data,
                        ..
                    } => {
                        result.usage = usage;
                        result.status = response_status;
                        result.model_id = model_id;
                        result.response_id = response_id;
                        result.finish_reason = finish_reason;
                        result.provider_data = provider_data;
                        result.validate()?;
                        return Ok(result);
                    }
                    SessionEventBody::Acknowledged { .. }
                    | SessionEventBody::Ready { .. }
                    | SessionEventBody::ResponseStarted { .. }
                    | SessionEventBody::Delta { .. }
                    | SessionEventBody::Recovery { .. } => (),
                    _ => {
                        return Err(Error::Protocol(
                            "unexpected event in isolated turn test".into(),
                        ));
                    }
                }
            }
            Err(Error::Protocol(
                "session ended before turn completion".into(),
            ))
        })
    }
}
impl<T: Model + ?Sized> ModelTestExt for T {}

impl SessionRecord {
    /// Completed turns reconstructed from the session event log; unfinished turns
    /// remain visible in `events` and are deliberately absent here.
    pub fn completed_responses(&self) -> Vec<GenerationOutput> {
        use zhir_core::model::SessionEventBody;
        let mut outputs =
            std::collections::BTreeMap::<String, Vec<zhir_core::message::Output>>::new();
        let mut turns = Vec::new();
        for event in &self.events {
            match &event.body {
                SessionEventBody::Output {
                    generation_id: Some(generation_id),
                    output,
                    ..
                } => outputs
                    .entry(generation_id.clone())
                    .or_default()
                    .push(output.clone()),
                SessionEventBody::ResponseFinished {
                    generation_id,
                    response_status,
                    usage,
                    model_id,
                    response_id,
                    finish_reason,
                    provider_data,
                    ..
                } => turns.push(GenerationOutput {
                    output: outputs.remove(generation_id).unwrap_or_default(),
                    status: response_status.clone(),
                    usage: usage.clone(),
                    model_id: model_id.clone(),
                    response_id: response_id.clone(),
                    finish_reason: finish_reason.clone(),
                    provider_data: provider_data.clone(),
                }),
                _ => (),
            }
        }
        turns
    }
}
