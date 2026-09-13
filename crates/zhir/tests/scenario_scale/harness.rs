use futures::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use zhir::{
    BoxFuture, Invocation, Result,
    error::Error,
    message::{Content, Message},
    model::{
        Capabilities, Model, ModelContext, ModelDelta, ModelOptions, ModelRequest, ModelResponse,
    },
    models::{
        HttpModel, ModelConfig, Protocol, ProtocolExtension, anthropic, openai, transport::SseEvent,
    },
    run::{Checkpoint, EventData},
    storage::{Commit, RunStore},
    stores::{memory::MemoryRunStore, sqlite::SqliteRunStore},
};

pub fn require(condition: bool, message: impl Into<String>) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::Protocol(message.into()))
    }
}
pub fn name(protocol: Protocol) -> &'static str {
    match protocol {
        Protocol::Chat => "chat",
        Protocol::Responses => "responses",
        Protocol::Messages => "messages",
    }
}
pub fn http(protocol: Protocol, base: &str, key: &str, model: &str) -> Result<HttpModel> {
    let mut config = ModelConfig::new(base, key, model);
    config.timeout = Duration::from_secs(60);
    match protocol {
        Protocol::Chat => openai::chat::model(config),
        Protocol::Responses => openai::responses::model(config),
        Protocol::Messages => anthropic::messages::model(config),
    }
}
pub fn options(protocol: Protocol, tag: &str, thinking: bool) -> ModelOptions {
    let mut options = ModelOptions {
        max_output_tokens: (protocol != Protocol::Chat).then_some(1024),
        ..Default::default()
    };
    if protocol == Protocol::Chat {
        options.extra.insert("max_tokens".into(), json!(1024));
    }
    options.extra.insert("consumer_tag".into(), json!(tag));
    match protocol {
        Protocol::Responses => {
            options.extra.insert(
                "reasoning".into(),
                json!({"effort":if thinking {"low"} else {"none"}}),
            );
        }
        _ => {
            options.extra.insert(
                "thinking".into(),
                json!({"type":if thinking {"enabled"} else {"disabled"}}),
            );
            if thinking {
                options.extra.insert(
                    if protocol == Protocol::Chat {
                        "reasoning_effort"
                    } else {
                        "output_config"
                    }
                    .into(),
                    if protocol == Protocol::Chat {
                        json!("low")
                    } else {
                        json!({"effort":"low"})
                    },
                );
            }
        }
    }
    options
}
/// Test consumer owns invocation-local observations.
#[derive(Default)]
pub struct ConsumerSession {
    tag: String,
    frames: usize,
}
impl ProtocolExtension for ConsumerSession {
    fn encode_request(&mut self, _: Protocol, _: &ModelRequest, body: &mut Value) -> Result<()> {
        let map = body.as_object_mut().unwrap();
        self.tag = map
            .remove("consumer_tag")
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default();
        Ok(())
    }
    fn decode_event(&mut self, _: Protocol, _: &mut SseEvent) -> Result<Vec<ModelDelta>> {
        self.frames += 1;
        Ok(vec![])
    }
    fn decode_response(
        &mut self,
        _: Protocol,
        _: &Value,
        decoded: Result<ModelResponse>,
    ) -> Result<ModelResponse> {
        let mut response = decoded?;
        response.provider_data["consumer_session"] = json!({"tag":self.tag,"frames":self.frames});
        Ok(response)
    }
}
pub struct RecordedModel {
    pub inner: Arc<dyn Model>,
    pub records: Mutex<Vec<Value>>,
}
impl RecordedModel {
    pub fn new(inner: Arc<dyn Model>) -> Self {
        Self {
            inner,
            records: Mutex::new(vec![]),
        }
    }
}
impl Model for RecordedModel {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            let requested_tag = request
                .options
                .extra
                .get("consumer_tag")
                .cloned()
                .unwrap_or(Value::Null);
            let run_id = context.run.run_id.clone();
            let request_bytes=serde_json::to_vec(&json!({"messages":request.messages,"tools":request.runtime_tools,"options":request.options})).unwrap().len();
            let result = self.inner.invoke(request, context).await;
            let record = match &result {
                Ok(response) => {
                    json!({"run_id":run_id,"request_bytes":request_bytes,"requested_tag":requested_tag,"response_id":response.response_id,"model_id":response.model_id,"usage":response.usage,"session":response.provider_data["consumer_session"],"output":response.output,"finish_reason":response.finish_reason})
                }
                Err(error) => {
                    json!({"run_id":run_id,"request_bytes":request_bytes,"error":error.to_string()})
                }
            };
            self.records.lock().unwrap().push(record);
            result
        })
    }
}
#[derive(Default)]
pub struct Events {
    pub counts: BTreeMap<String, usize>,
    pub last_sequence: u64,
    pub ordered: bool,
}
impl Events {
    pub fn new() -> Self {
        Self {
            ordered: true,
            ..Default::default()
        }
    }
    pub fn record(&mut self, event: &zhir::run::Event) {
        self.ordered &= event.sequence > self.last_sequence;
        self.last_sequence = event.sequence;
        let kind = match &event.data {
            EventData::ModelDelta { delta } => format!(
                "delta_{}",
                match delta {
                    ModelDelta::Text { .. } => "text",
                    ModelDelta::Reasoning { .. } => "reasoning",
                    ModelDelta::RuntimeTool { .. } => "runtime_tool",
                    ModelDelta::ProtocolEvent { .. } => "protocol_event",
                    ModelDelta::ProviderToolProgress { .. } => "provider_tool_progress",
                    ModelDelta::Usage { .. } => "usage",
                }
            ),
            data => serde_json::to_value(data).unwrap()["kind"]
                .as_str()
                .unwrap()
                .into(),
        };
        *self.counts.entry(kind).or_default() += 1;
    }
}
pub async fn drain(
    mut invocation: Invocation,
) -> (
    std::result::Result<Arc<Checkpoint>, zhir::kernel::RunError>,
    Events,
) {
    let mut summary = Events::new();
    let mut events = invocation.events().unwrap();
    while let Some(event) = events.next().await {
        summary.record(&event);
    }
    (
        invocation.result().await.map(|r| r.into_checkpoint()),
        summary,
    )
}
pub fn text(content: &[Content]) -> String {
    content.iter().filter_map(Content::as_text).collect()
}
pub fn roundtrip(checkpoint: &Checkpoint) -> Result<Arc<Checkpoint>> {
    let bytes = zhir::wire::encode_checkpoint(checkpoint)?;
    let restored = zhir::wire::decode_checkpoint(&bytes)?;
    require(
        restored.history.digest() == checkpoint.history.digest(),
        "wire history digest mismatch",
    )?;
    Ok(Arc::new(restored))
}
struct ObservedStore {
    inner: Arc<dyn RunStore>,
    commits: Arc<Mutex<Vec<Arc<Checkpoint>>>>,
}
impl RunStore for ObservedStore {
    fn commit(&self, commit: Commit) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let checkpoint = commit.checkpoint.clone();
            self.inner.commit(commit).await?;
            self.commits.lock().unwrap().push(checkpoint);
            Ok(())
        })
    }
    fn load_head(&self, id: &str) -> BoxFuture<'_, Result<Option<Arc<Checkpoint>>>> {
        let id = id.to_owned();
        Box::pin(async move { self.inner.load_head(&id).await })
    }
}
pub struct StoreFixture {
    pub api: Arc<dyn RunStore>,
    commits: Arc<Mutex<Vec<Arc<Checkpoint>>>>,
    sqlite: Option<SqliteRunStore>,
    path: Option<std::path::PathBuf>,
}
impl StoreFixture {
    pub async fn new(sqlite: bool) -> Result<Self> {
        let commits = Arc::new(Mutex::new(vec![]));
        if sqlite {
            let path = std::env::temp_dir().join(format!("zhir-scale-{}.db", uuid::Uuid::new_v4()));
            let store =
                SqliteRunStore::connect(&format!("sqlite://{}?mode=rwc", path.display())).await?;
            Ok(Self {
                api: Arc::new(ObservedStore {
                    inner: Arc::new(store.clone()),
                    commits: commits.clone(),
                }),
                commits,
                sqlite: Some(store),
                path: Some(path),
            })
        } else {
            Ok(Self {
                api: Arc::new(ObservedStore {
                    inner: Arc::new(MemoryRunStore::new()),
                    commits: commits.clone(),
                }),
                commits,
                sqlite: None,
                path: None,
            })
        }
    }
    pub fn verify(&self) -> Result<usize> {
        let mut runs: BTreeMap<String, Vec<Arc<Checkpoint>>> = BTreeMap::new();
        for checkpoint in self.commits.lock().unwrap().iter() {
            runs.entry(checkpoint.context.run_id.clone())
                .or_default()
                .push(checkpoint.clone());
        }
        require(!runs.is_empty(), "missing committed trace")?;
        for trace in runs.values() {
            zhir::kernel::diagnostics::verify_trace(trace)?;
        }
        Ok(runs.values().map(Vec::len).sum())
    }
    pub async fn close(self) {
        drop(self.api);
        if let Some(sqlite) = self.sqlite {
            sqlite.close().await;
        }
        if let Some(path) = self.path {
            let _ = std::fs::remove_file(path);
        }
    }
}
pub fn save(path: &str, rows: &[Value]) {
    std::fs::write(path, serde_json::to_vec_pretty(rows).unwrap()).unwrap();
}
pub fn empty_request(stream: bool) -> ModelRequest {
    ModelRequest {
        messages: vec![Message::user("fixture")],
        runtime_tools: vec![],
        provider_tools: vec![],
        options: ModelOptions::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream,
    }
}
