//! Paid consumer acceptance of the ergonomic APIs; all endpoint policy is test-owned.
use super::harness::*;
use futures::{StreamExt, stream};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
use zhir::{
    Result, Runtime,
    message::Message,
    model::{Model, ModelDelta, ResponseFormat},
    models::{FunctionModel, Protocol, TransformModel, decorators::ObservedModel},
    run::{Limits, State},
    runtime_tools::{RuntimeToolRegistry, TypedTool},
    storage::RunStore,
    tool::{Execution, RuntimeTool},
};
use zhir_testing::{RecordingModel, RecordingSink, RecordingStore};
const CASE: zhir::run::ContextKey<String> = zhir::run::ContextKey::new("consumer.case");
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Product {
    sku: String,
    quantity: u32,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct QuoteInput {
    product: Product,
    request_id: String,
}
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Quote {
    total: u32,
    receipt: String,
}
async fn workflow(
    protocol: Protocol,
    stream: bool,
    repeat: usize,
    shared: Arc<dyn Model>,
    fixture: &StoreFixture,
) -> Result<Value> {
    let tag = format!("typed-{}-{stream}-{repeat}", name(protocol));
    let receipt = format!("receipt-{}", uuid::Uuid::new_v4());
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let expected_tag = tag.clone();
    let returned_receipt = receipt.clone();
    let tool = TypedTool::<QuoteInput, Quote>::new(
        "quote",
        "Return the private total and receipt for a product",
        Execution {
            parallel: false,
            read_only: true,
            idempotent: true,
        },
        move |args, ctx| {
            let receipt = returned_receipt.clone();
            let expected = expected_tag.clone();
            let invoked = invoked.clone();
            async move {
                invoked.fetch_add(1, Ordering::SeqCst);
                require(
                    ctx.run.require(CASE)? == expected
                        && args.product.sku == "widget"
                        && args.product.quantity == 3
                        && args.request_id == expected,
                    "typed consumer arguments mismatch",
                )?;
                Ok(zhir::runtime_tools::ToolReply::success(Quote {
                    total: 67,
                    receipt,
                }))
            }
        },
    )?;
    let tools = Arc::new(RuntimeToolRegistry::from_tools([
        Arc::new(tool) as Arc<dyn RuntimeTool>
    ])?);
    let function = FunctionModel::new(shared.capabilities().clone(), move |request, context| {
        let shared = shared.clone();
        async move { shared.invoke(request, context).await }
    });
    let recorded = Arc::new(RecordingModel::new(Arc::new(function)));
    let directory = tempfile::tempdir().map_err(|e| zhir::error::Error::Invalid(e.to_string()))?;
    let path = directory.path().join("instructions.txt");
    let instructions = "Use the requested tool exactly once. Do not invent the private receipt. After the tool returns, output only one JSON object containing its total and receipt, with no Markdown or extra fields.";
    tokio::fs::write(&path, instructions)
        .await
        .map_err(|e| zhir::error::Error::Invalid(e.to_string()))?;
    let mapped = Arc::new(AtomicUsize::new(0));
    let map_count = mapped.clone();
    let transform = TransformModel::new(recorded.clone(), move |mut request, context| {
        let path = path.clone();
        async move {
            context.cancellation.check()?;
            let instructions = tokio::fs::read_to_string(path)
                .await
                .map_err(|e| zhir::error::Error::Invalid(e.to_string()))?;
            request.messages.insert(0, Message::system(instructions));
            Ok(request)
        }
    });
    let transform = transform.map_response(move |mut response, ctx| {
        map_count.fetch_add(1, Ordering::SeqCst);
        async move {
            response.provider_data["consumer_transform"] = json!(ctx.run.require(CASE)?);
            Ok(response)
        }
    });
    let observer = Arc::new(RecordingSink::default());
    let sink = observer.clone();
    let model = ObservedModel::new(Arc::new(transform), move |_, _| Ok(sink.clone()));
    let store = Arc::new(RecordingStore::new(fixture.api.clone()));
    let format = if protocol == Protocol::Chat {
        ResponseFormat::Json
    } else {
        ResponseFormat::Schema {
            name: "quote_result".into(),
            schema: schemars::schema_for!(Quote).into(),
        }
    };
    let model = zhir::models::decorators::RetryingModel::new(
        Arc::new(model),
        zhir::policies::RetryPolicy::new(2)?.backoff(zhir::policies::Backoff::fixed(
            std::time::Duration::from_millis(50),
        )),
    )?;
    let approvals = Arc::new(AtomicUsize::new(0));
    let approval_count = approvals.clone();
    let policy = zhir::runtime_tools::FunctionApprovalPolicy::per_call(move |_, ctx| {
        approval_count.fetch_add(1, Ordering::SeqCst);
        async move {
            ctx.require(CASE)?;
            Ok(zhir::tool::ApprovalDecision::Allow)
        }
    });
    let sources: Vec<Arc<dyn zhir::tool::RuntimeToolCatalogProvider>> =
        vec![tools, Arc::new(RuntimeToolRegistry::new())];
    let runtime = Runtime::builder(Arc::new(model))
        .runtime_tools(Arc::new(zhir::runtime_tools::CompositeRuntimeTools::new(
            sources,
        )))
        .approval(Arc::new(policy))
        .store(store.clone())
        .defaults(|run| run.options(options(protocol, &tag, false)))
        .defaults(|run| run.response_format(format))
        .defaults(|run| run.stream(stream))
        .defaults(|run| {
            run.limits(Limits {
                max_planning_steps: 3,
                max_runtime_tool_calls: 1,
                elapsed_ms: Some(60_000),
                ..zhir::kernel::defaults::limits()
            })
        })
        .build()?;
    let mut events = Events::new();
    let checkpoint=zhir::runs::drive(runtime.start(zhir::RunRequest::new(vec![Message::user(format!("Call quote with product sku widget, quantity 3, request_id {tag}. Then return its total and receipt as JSON."))]).context_value(CASE,tag.clone())?.runtime_tools(zhir::tool::RuntimeToolSelection::only(["quote"])))?,|event| {
        events.record(&event);async {Ok(())}
    }).await.map_err(|e|zhir::error::Error::Invalid(e.to_string()))?.into_checkpoint();
    let output = zhir::output::decode::<Quote>(&checkpoint)?;
    let records = recorded.records();
    let responses: Vec<_> = records
        .iter()
        .filter_map(|r| r.outcome.as_ref().and_then(|o| o.as_ref().ok()))
        .collect();
    let raw_frames = observer
        .deltas()
        .iter()
        .filter(|d| matches!(d, ModelDelta::ProtocolEvent { .. }))
        .count();
    let sessions_frames: usize = responses
        .iter()
        .map(|r| {
            r.provider_data["consumer_session"]["frames"]
                .as_u64()
                .unwrap_or(0) as usize
        })
        .sum();
    let mut checks = std::collections::BTreeMap::new();
    checks.insert("response_maps", mapped.load(Ordering::SeqCst) == 2);
    checks.insert("function_approval", approvals.load(Ordering::SeqCst) == 1);
    checks.insert(
        "per_run_selection",
        records.iter().all(|r| {
            r.input.request.runtime_tools.len() == 1
                && r.input.request.runtime_tools[0].name == "quote"
        }),
    );
    checks.insert("typed_context", checkpoint.context.require(CASE)? == tag);
    checks.insert(
        "completed",
        matches!(checkpoint.state, State::Completed { .. }),
    );
    checks.insert(
        "typed_result",
        output.total == 67 && output.receipt == receipt,
    );
    checks.insert(
        "one_tool_execution",
        calls.load(Ordering::SeqCst) == 1 && checkpoint.metrics.runtime_tool_calls == 1,
    );
    checks.insert(
        "two_model_requests",
        records.len() == 2 && responses.len() == 2,
    );
    checks.insert(
        "async_preparation",
        records
            .iter()
            .all(|r| r.input.request.messages.first() == Some(&Message::system(instructions))),
    );
    checks.insert(
        "unchanged_run_context",
        records
            .iter()
            .all(|r| r.input.run.run_id == checkpoint.context.run_id),
    );
    checks.insert(
        "extension_session_isolation",
        responses
            .iter()
            .all(|r| r.provider_data["consumer_session"]["tag"] == tag),
    );
    checks.insert(
        "observer_raw_frames",
        raw_frames == sessions_frames && (!stream || raw_frames > 0),
    );
    checks.insert("event_order", events.ordered);
    checks.insert("wire_roundtrip", roundtrip(&checkpoint).is_ok());
    checks.insert(
        "stored_head",
        store
            .load_head(&checkpoint.context.run_id)
            .await?
            .is_some_and(|c| c.id == checkpoint.id),
    );
    checks.insert("commit_trace", store.verify_traces()? == 4);
    checks.insert(
        "usage_and_identity",
        responses.iter().all(|r| {
            r.usage.input_tokens.is_some() && r.response_id.is_some() && r.model_id.is_some()
        }),
    );
    Ok(
        json!({"passed":checks.values().all(|v|*v),"checks":checks,"model_requests":records.len(),"tool_calls":calls.load(Ordering::SeqCst),"verified_commits":store.verify_traces()?,"raw_frames":raw_frames,"events":events.counts,"output":output,"usage":responses.iter().map(|r|&r.usage).collect::<Vec<_>>()}),
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "paid typed SDK acceptance; requires DEEPSEEK_API_KEY"]
async fn live_developer_api() {
    let key = std::env::var("DEEPSEEK_API_KEY").expect("DEEPSEEK_API_KEY");
    let mut jobs = vec![];
    for protocol in [Protocol::Chat, Protocol::Responses, Protocol::Messages] {
        let model: Arc<dyn Model> = Arc::new(
            http(
                protocol,
                if protocol == Protocol::Messages {
                    "https://api.deepseek.com/anthropic/v1"
                } else {
                    "https://api.deepseek.com"
                },
                &key,
                "deepseek-flash",
            )
            .unwrap()
            .with_extension(|_| Ok(ConsumerSession::default())),
        );
        for stream in [false, true] {
            for repeat in 0..4 {
                let shared = model.clone();
                jobs.push(async move {
                    let started = Instant::now();
                    let fixture = StoreFixture::new(repeat % 2 == 1).await.unwrap();
                    let result = workflow(protocol, stream, repeat, shared, &fixture).await;
                    fixture.close().await;
                    let mut row =
                        result.unwrap_or_else(|e| json!({"passed":false,"error":e.to_string()}));
                    row["case_id"] = json!(format!("typed-{}-{stream}-{repeat}", name(protocol)));
                    row["protocol"] = json!(name(protocol));
                    row["stream"] = json!(stream);
                    row["repeat"] = json!(repeat);
                    row["store"] = json!(if repeat % 2 == 1 { "sqlite" } else { "memory" });
                    row["elapsed_ms"] = json!(started.elapsed().as_millis());
                    row
                });
            }
        }
    }
    let path = std::env::var("ZHIR_DEVELOPER_REPORT")
        .unwrap_or_else(|_| "/tmp/zhir-developer-live.json".into());
    let mut pending = stream::iter(jobs).buffer_unordered(12);
    let mut rows = vec![];
    while let Some(row) = pending.next().await {
        eprintln!(
            "{} {}",
            if row["passed"] == true {
                "PASS"
            } else {
                "FAIL"
            },
            row["case_id"]
        );
        if row["passed"] != true {
            eprintln!("{row}");
        }
        rows.push(row);
        save(&path, &rows);
    }
    assert_eq!(rows.len(), 24);
    assert!(rows.iter().all(|r| r["passed"] == true));
}
