#![cfg(all(
    feature = "openai-responses",
    feature = "typed-tools",
    feature = "memory"
))]
//! Added after freezing production sources: a new consumer protocol capability,
//! typed waiting tool and ticket workflow using only public SDK components.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use zhir::{
    Result, ResumeRequest, RunRequest, Runtime, SuspensionTicket,
    error::Error,
    message::{Content, Message, Output, ProviderToolStatus},
    model::ProviderToolSpec,
    models::{
        ModelConfig, Protocol, openai,
        provider_tools::{ProviderOutput, ProviderToolAdapter, ProviderTools},
    },
    run::{RunContext, State},
    runtime_tools::{RuntimeToolRegistry, ToolReply, TypedTool},
    stores::memory::MemoryRunStore,
    tool::{Execution, RuntimeTool},
};
use zhir_testing::http::{HttpFixture, HttpReply};
struct Transcript {
    provider: String,
}
impl ProviderToolAdapter for Transcript {
    fn identity(&self) -> (&str, &str) {
        (&self.provider, "transcript")
    }
    fn encode(&mut self, _: Protocol, spec: &ProviderToolSpec) -> Result<Value> {
        Ok(json!({"type":"consumer_transcriber","tenant":self.provider,"config":spec.options}))
    }
    fn decode(&mut self, _: Protocol, item: &Value, _: &Value) -> Result<Option<Vec<Output>>> {
        if item["type"] != "consumer_transcript" {
            return Ok(None);
        }
        let text = item["segments"]
            .as_array()
            .ok_or_else(|| Error::Protocol("segments required".into()))?
            .iter()
            .map(|s| {
                s.as_str()
                    .ok_or_else(|| Error::Protocol("segment text required".into()))
            })
            .collect::<Result<Vec<_>>>()?
            .join(" ");
        Ok(Some(vec![
            ProviderOutput::new(
                &self.provider,
                "transcript",
                item["job_token"].as_str().unwrap(),
                ProviderToolStatus::Completed,
            )
            .native(item.clone())
            .content(Content::text(text))
            .finish()?,
        ]))
    }
}
#[derive(Deserialize, JsonSchema)]
struct Review {
    document: String,
}
#[derive(Serialize, JsonSchema)]
struct Receipt {
    document: String,
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_transcripts_and_typed_review_compose_without_production_changes() {
    let native = json!({"type":"consumer_transcript","job_token":"native-ticket","segments":["hello","world"],"receipt":{"opaque":7}});
    let first = json!({"output":[native,{"type":"function_call","call_id":"review-1","name":"review","arguments":"{\"document\":\"draft\"}"}],"status":"completed"});
    let done = json!({"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"approved"}]}],"status":"completed"});
    let fixture = HttpFixture::start(
        (0..16)
            .map(|_| HttpReply::json(&first))
            .chain((0..16).map(|_| HttpReply::json(&done))),
    )
    .await
    .unwrap();
    let sessions = Arc::new(AtomicUsize::new(0));
    let counted = sessions.clone();
    let model = openai::responses::model(ModelConfig::new(
        format!("{}/v1", fixture.url()),
        "fixture",
        "fixture",
    ))
    .unwrap()
    .with_extension(move |ctx| {
        assert_eq!(ctx.protocol, Protocol::Responses);
        assert!(!ctx.request.stream);
        let provider = ctx.run.metadata["tenant"]
            .as_str()
            .ok_or_else(|| Error::Invalid("tenant required".into()))?
            .to_owned();
        assert_eq!(ctx.request.provider_tools[0].provider, provider);
        let mut adapters = ProviderTools::new();
        adapters.register(Transcript { provider })?;
        counted.fetch_add(1, Ordering::SeqCst);
        Ok(adapters)
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let tool = TypedTool::<Review, Receipt>::new(
        "review",
        "Confirm transcript",
        Execution::default(),
        move |args, _| {
            invoked.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(ToolReply::waiting(
                    "review-ticket",
                    Receipt {
                        document: args.document,
                    },
                    "consumer-review",
                )
                .content([Content::text("Review required")]))
            }
        },
    )
    .unwrap();
    let registry = Arc::new(
        RuntimeToolRegistry::from_tools([Arc::new(tool) as Arc<dyn RuntimeTool>]).unwrap(),
    );
    let runtime = Runtime::builder(Arc::new(model))
        .runtime_tools(registry)
        .store(Arc::new(MemoryRunStore::new()))
        .defaults(|run| run.stream(true))
        .build()
        .unwrap();
    let checkpoints = futures::future::join_all((0..16).map(|index| {
        let runtime = &runtime;
        async move {
            let tenant = format!("consumer-tenant-{index}");
            let mut context = RunContext::default();
            context.metadata.insert("tenant".into(), json!(tenant));
            let checkpoint = runtime
                .start(
                    RunRequest::new([Message::user("transcribe and review")])
                        .context(context)
                        .stream(false)
                        .provider_tools(vec![ProviderToolSpec {
                            provider: tenant,
                            name: "transcript".into(),
                            options: json!({"mode":"consumer"}),
                        }]),
                )
                .unwrap()
                .result()
                .await
                .unwrap();
            assert!(matches!(checkpoint.state, State::Suspended { .. }));
            assert_eq!(checkpoint.metrics.runtime_tool_calls, 1);
            assert_eq!(
                zhir::output::provider_calls(&checkpoint)[0].call.output,
                vec![Content::text("hello world")]
            );
            checkpoint
        }
    }))
    .await;
    let completed = futures::future::join_all(checkpoints.into_iter().map(|checkpoint| {
        let runtime = &runtime;
        async move {
            let ticket = SuspensionTicket::from_checkpoint(&checkpoint).unwrap();
            let ticket = serde_json::from_slice(&serde_json::to_vec(&ticket).unwrap()).unwrap();
            runtime
                .resume(
                    ResumeRequest::from_ticket(ticket)
                        .message(Message::external("approved by reviewer")),
                )
                .await
                .unwrap()
                .result()
                .await
                .unwrap()
        }
    }))
    .await;
    assert!(completed.iter().all(|c|matches!(&c.state,State::Completed{content} if content==&vec![Content::text("approved")])));
    assert_eq!(sessions.load(Ordering::SeqCst), 32);
    assert_eq!(calls.load(Ordering::SeqCst), 16);
    let sent = fixture.finish().await.unwrap();
    assert_eq!(sent.len(), 32);
    for request in &sent[16..] {
        let body = request.json().unwrap();
        let input = body["input"].as_array().unwrap();
        assert_eq!(
            input
                .iter()
                .filter(|v| v["type"] == "consumer_transcript")
                .count(),
            1
        );
        assert_eq!(
            input
                .iter()
                .find(|v| v["type"] == "consumer_transcript")
                .unwrap(),
            &native
        );
        assert!(
            input
                .iter()
                .any(|v| v["type"] == "function_call_output" && v["call_id"] == "review-1")
        );
        assert_eq!(
            input.last().unwrap()["content"][0]["text"],
            "approved by reviewer"
        );
    }
}
