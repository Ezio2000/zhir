use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use zhir::{
    Result,
    error::Error,
    message::{Output, ProviderToolStatus},
    model::{ModelDelta, ProviderToolSpec},
    models::{
        Protocol,
        provider_tools::{ProviderOutput, ProviderToolAdapter, ProviderTools},
        transport::SseEvent,
    },
};

pub fn spec() -> ProviderToolSpec {
    ProviderToolSpec {
        provider: "consumer.example".into(),
        name: "render".into(),
        options: json!({"quality":"test"}),
    }
}
pub fn extension() -> Result<ProviderTools> {
    let mut tools = ProviderTools::new();
    tools.register(Render::default())?;
    Ok(tools)
}
#[derive(Default)]
pub struct Render {
    events: usize,
}
impl ProviderToolAdapter for Render {
    fn identity(&self) -> (&str, &str) {
        ("consumer.example", "render")
    }
    fn encode(&mut self, _: Protocol, spec: &ProviderToolSpec) -> Result<Value> {
        Ok(json!({"type":"consumer_render","config":spec.options}))
    }
    fn decode(&mut self, _: Protocol, item: &Value, _: &Value) -> Result<Option<Vec<Output>>> {
        if item["type"] != "consumer_render_call" {
            return Ok(None);
        }
        let required = |key: &str| {
            item[key]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::Protocol(format!("missing fixture field {key}")))
        };
        let status = match required("stage")?.as_str() {
            "queued" => ProviderToolStatus::Pending,
            "working" => ProviderToolStatus::Running,
            "done" => ProviderToolStatus::Completed,
            "partial" => ProviderToolStatus::Incomplete,
            "failed" => ProviderToolStatus::Failed,
            _ => return Err(Error::Protocol("unknown fixture stage".into())),
        };
        Ok(Some(vec![
            ProviderOutput::new("consumer.example", "render", required("id")?, status)
                .native(item.clone())
                .image("/result", "image/png")?
                .finish()?,
        ]))
    }
    fn choice(&mut self, _: Protocol, _: &ProviderToolSpec) -> Result<Value> {
        Ok(json!({"type":"consumer_render"}))
    }
    fn event(&mut self, _: Protocol, event: &SseEvent) -> Result<Vec<ModelDelta>> {
        let value: Value = serde_json::from_str(&event.data).unwrap_or(Value::Null);
        if value["type"] != "consumer.progress" {
            return Ok(vec![]);
        }
        self.events += 1;
        Ok(vec![ModelDelta::ProviderToolProgress {
            output_index: value["output_index"].as_u64().unwrap() as usize,
            provider: "consumer.example".into(),
            name: "render".into(),
            id: value["call_id"].as_str().map(str::to_owned),
            status: Some(ProviderToolStatus::Running),
            data: json!({"session_event":self.events,"partial":value["partial"]}),
        }])
    }
}
pub fn frame(output: Vec<Value>) -> Value {
    json!({"id":"response-fixture","model":"fixture","status":"completed","output":output,"usage":{"input_tokens":3,"output_tokens":4}})
}
pub fn item(id: usize, stage: &str) -> Value {
    json!({"type":"consumer_render_call","id":format!("render-{id}"),"stage":stage,"result":"aGVsbG8=","custom":{"retained":true}})
}
pub fn stream(response: &Value, count: usize) -> String {
    let mut frames = String::new();
    for index in 0..count {
        frames.push_str(&format!("data: {}\n\n",json!({"type":"consumer.progress","output_index":index,"call_id":format!("render-{index}"),"partial":"cGFydGlhbA=="})));
    }
    frames.push_str(&format!(
        "data: {}\n\n",
        json!({"type":"response.completed","response":response})
    ));
    frames
}
pub async fn server(
    replies: Vec<(String, bool)>,
) -> (String, impl Future<Output = Result<Vec<Value>>>) {
    use zhir_testing::http::{HttpFixture, HttpReply};
    let fixture = HttpFixture::start(replies.into_iter().map(|(body, sse)| {
        HttpReply::bytes(
            if sse {
                "text/event-stream"
            } else {
                "application/json"
            },
            body.into_bytes(),
        )
        .fragment_bytes(13)
    }))
    .await
    .unwrap();
    let url = format!("{}/v1", fixture.url());
    (url, async move {
        fixture.finish().await?.iter().map(|r| r.json()).collect()
    })
}
#[derive(Default)]
pub struct Deltas(pub Mutex<Vec<ModelDelta>>);
impl zhir::model::DeltaSink for Deltas {
    fn emit(&self, value: ModelDelta) -> zhir::BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(value);
            Ok(())
        })
    }
}
pub fn context(deltas: Arc<Deltas>) -> zhir::model::ModelContext {
    zhir::model::ModelContext {
        run: zhir::kernel::defaults::context(),
        cancellation: Default::default(),
        deltas: Some(deltas),
    }
}
pub fn request(stream: bool) -> zhir::model::ModelRequest {
    zhir::model::ModelRequest {
        messages: vec![zhir::message::Message::user("fixture")],
        runtime_tools: vec![],
        provider_tools: vec![spec()],
        options: Default::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream,
    }
}
