use super::harness::*;
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use zhir::{
    BoxFuture, Result, ResumeRequest, Runtime,
    builtins::agent::{AgentBackend, runtime_backend::RuntimeAgentBackend},
    error::{Error, Failure},
    message::{Content, Message},
    model::{Model, ResponseFormat},
    models::Protocol,
    run::{Limits, RunContext, State, Suspension},
    runtime_tools::{FunctionTool, RuntimeToolRegistry},
    tool::{
        ApprovalDecision, ApprovalPolicy, ApprovalRequest, Execution, InputSpec, RuntimeTool,
        RuntimeToolCall, RuntimeToolContext, RuntimeToolInput, RuntimeToolSpec,
    },
};
use zhir_core::operation::OperationOutcome;

#[derive(Clone)]
struct Case {
    family: &'static str,
    protocol: Protocol,
    repeat: usize,
    stream: bool,
    size: usize,
}
impl Case {
    fn tag(&self) -> String {
        format!(
            "{}-{}-{}-{}-{}",
            self.family,
            name(self.protocol),
            self.repeat,
            self.stream,
            self.size
        )
    }
}
struct Gate;
impl ApprovalPolicy for Gate {
    fn decide(
        &self,
        requests: Vec<ApprovalRequest>,
        context: RunContext,
    ) -> BoxFuture<'_, Result<Vec<ApprovalDecision>>> {
        Box::pin(async move {
            Ok(requests
                .iter()
                .map(|_| {
                    if context.metadata.get("approved") == Some(&json!(true)) {
                        ApprovalDecision::Allow
                    } else {
                        ApprovalDecision::Suspend(Suspension {
                            reason: "review".into(),
                            source: "consumer_gate".into(),
                            wait_id: None,
                            metadata: BTreeMap::new(),
                        })
                    }
                })
                .collect())
        })
    }
}
struct ToolEnv {
    family: &'static str,
    receipt: String,
    ticket: String,
    total: i64,
    price: i64,
    calls: Mutex<Vec<Value>>,
    active: AtomicUsize,
    peak: AtomicUsize,
    child: Option<Arc<RuntimeAgentBackend>>,
}
impl ToolEnv {
    async fn execute(
        &self,
        call: RuntimeToolCall,
        context: RuntimeToolContext,
    ) -> Result<zhir_core::operation::ToolExecution> {
        let attempt = {
            let mut calls = self.calls.lock().unwrap();
            calls.push(json!({"name":call.name,"input":call.input,"run_id":context.run.run_id}));
            calls.len()
        };
        context
            .emit_progress(json!({"stage":"entered","tool":call.name}))
            .await?;
        let args = match &call.input {
            RuntimeToolInput::Structured(value) => value.clone(),
            RuntimeToolInput::Freeform(patch) => json!({"patch":patch}),
        };
        match self.family {
            "retrieve_compute" if call.name=="retrieve"=>Ok(zhir::runtime_tools::reply::json(json!({"ticket":self.ticket,"unit_price":self.price,"quantity":3,"fee":5}))),
            "retrieve_compute"=>{
                require(args["ticket"]==self.ticket && args["total"]==self.total,"consumer backend rejected computed total or ticket")?;
                Ok(zhir::runtime_tools::reply::json(json!({"receipt":self.receipt})))
            }
            "parallel_quotes"=>{
                let active=self.active.fetch_add(1,Ordering::SeqCst)+1;self.peak.fetch_max(active,Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(60)).await;
                self.active.fetch_sub(1,Ordering::SeqCst);
                let value=match args["region"].as_str() {Some("A")=>17,Some("B")=>23,Some("C")=>31,_=>return Err(Error::Invalid("unknown region".into()))};
                Ok(zhir::runtime_tools::reply::json(json!({"region":args["region"],"value":value})))
            }
            "tool_recovery" if attempt==1=>Ok(zhir_core::operation::ToolExecution::Finished(OperationOutcome::Failure {error:Failure {code:"temporary_lookup".into(),message:"Temporary fixture lookup failure. Call fetch again with the same arguments; the next attempt will succeed.".into(),retryable:true}})),
            "wait_resume"=>Ok(zhir_core::operation::ToolExecution::Active(zhir_testing::waiting_operation(json!({"status":"awaiting_reviewer"}),0))),
            "delegated_agent"=>{
                let backend=self.child.as_ref().unwrap();
                let prompt=format!("Reply with exactly {} and nothing else.",self.receipt);
                Ok(zhir_core::operation::ToolExecution::Active(backend.start(prompt,context).await?))
            }
            "multimodal_report" if call.name=="read_image"=>Ok(zhir_core::operation::ToolExecution::Finished(OperationOutcome::Success {content:vec![Content::text("Read the alphanumeric code, then call submit_report with that code."),Content::resource(zhir_core::resource::ResourceRef {id:"vision".into(),media_type:"image/png".into(),name:None,source:zhir_core::resource::ResourceSource::Inline {bytes:include_bytes!("../fixtures/vision.png").to_vec()},metadata:Default::default()})],structured:Value::Null})),
            "multimodal_report"=>{require(args["code"]=="K7X42","consumer report contains wrong OCR code")?;Ok(zhir::runtime_tools::reply::json(json!({"receipt":self.receipt})))},
            "freeform_pipeline" if call.name=="apply_patch"=>{
                let patch=args["patch"].as_str().unwrap_or_default();
                require(patch.contains("*** Begin Patch") && patch.contains("+你好"),"consumer patch format mismatch")?;
                Ok(zhir::runtime_tools::reply::json(json!({"ticket":self.ticket})))
            }
            "freeform_pipeline"=>{require(args["ticket"]==self.ticket,"consumer patch ticket mismatch")?;Ok(zhir::runtime_tools::reply::json(json!({"receipt":self.receipt})))},
            _=>Ok(zhir::runtime_tools::reply::json(json!({"receipt":self.receipt}))),
        }
    }
}
fn tool(
    name: &str,
    description: &str,
    schema: Value,
    env: Arc<ToolEnv>,
    parallel: bool,
    freeform: bool,
) -> Arc<dyn RuntimeTool> {
    Arc::new(FunctionTool::new(
        RuntimeToolSpec {
            name: name.into(),
            description: description.into(),
            input: if freeform {
                InputSpec::Freeform { format: None }
            } else {
                InputSpec::Structured { schema }
            },
            output_schema: None,
            execution: Execution {
                parallel,
                read_only: parallel,
                idempotent: parallel,
            },
        },
        move |call, ctx| {
            let env = env.clone();
            async move { env.execute(call, ctx).await }
        },
    ))
}
fn object(properties: Value, required: Value) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}

async fn workflow(
    case: &Case,
    recorded: Arc<RecordingModel>,
    store: &StoreFixture,
) -> Result<Value> {
    let tag = case.tag();
    let receipt = format!("R{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
    let ticket = format!("T{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
    let thinking = case.repeat % 2 == 1 && case.stream;
    let child = if case.family == "delegated_agent" {
        let child_runtime = Runtime::builder(recorded.clone())
            .defaults(|run| run.profile(options(case.protocol, &format!("{tag}-child"), thinking)))
            .store(store.api.clone())
            .defaults(|run| run.stream(case.stream))
            .defaults(|run| {
                run.limits(Limits {
                    max_generation_requests: 2,
                    elapsed_ms: Some(60_000),
                    ..zhir::kernel::defaults::limits()
                })
            })
            .build()?;
        Some(Arc::new(RuntimeAgentBackend::new(
            child_runtime,
            "Complete the user's exact reply task.",
            4,
        )?))
    } else {
        None
    };
    let env = Arc::new(ToolEnv {
        family: case.family,
        receipt: receipt.clone(),
        ticket,
        price: 17 + case.repeat as i64,
        total: (17 + case.repeat as i64) * 3 + 5,
        calls: Mutex::new(vec![]),
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        child,
    });
    let empty = object(json!({}), json!([]));
    let mut tools = vec![];
    let mut expected = receipt.clone();
    let mut expected_calls = 1;
    let mut format = None;
    let prompt = match case.family {
        "retrieve_compute" => {
            tools.push(tool(
                "retrieve",
                "Read the private order record, including a settlement ticket.",
                empty.clone(),
                env.clone(),
                false,
                false,
            ));
            tools.push(tool(
                "settle",
                "Submit unit_price * quantity + fee and the exact ticket returned by retrieve.",
                object(
                    json!({"ticket":{"type":"string"},"total":{"type":"integer"}}),
                    json!(["ticket", "total"]),
                ),
                env.clone(),
                false,
                false,
            ));
            expected_calls = 2;
            "First call retrieve once. Use its private values to compute unit_price * quantity + fee. Then call settle once with the total and ticket. Finally reply with only the returned receipt.".into()
        }
        "parallel_quotes" => {
            tools.push(tool(
                "quote",
                "Return a region quote. Regions are independent and may be queried in parallel.",
                object(
                    json!({"region":{"type":"string","enum":["A","B","C"]}}),
                    json!(["region"]),
                ),
                env.clone(),
                true,
                false,
            ));
            expected = "71".into();
            expected_calls = 3;
            "Call quote for A, B and C exactly once each. Issue these three independent calls together in one batch. Add their returned values. Your final response must contain only the decimal digits of the integer sum, with no equation, calculation steps, Markdown or explanation.".into()
        }
        "tool_recovery" => {
            tools.push(tool(
                "fetch",
                "Fetch the private receipt. A temporary failure may require a retry.",
                empty.clone(),
                env.clone(),
                false,
                false,
            ));
            expected_calls = 2;
            "Call fetch. If it reports a temporary failure, retry fetch once. Reply with only the returned receipt.".into()
        }
        "wait_resume" => {
            tools.push(tool(
                "request_review",
                "Request human review and wait for the reviewer response.",
                empty.clone(),
                env.clone(),
                false,
                false,
            ));
            "Call request_review exactly once, then wait. When a reviewer message supplies a receipt, reply with only that receipt. Do not call request_review again.".into()
        }
        "approval_resume" => {
            tools.push(tool(
                "approve_draft",
                "Create a test draft after host approval. Returns a receipt.",
                empty.clone(),
                env.clone(),
                false,
                false,
            ));
            "Call approve_draft exactly once. After its result arrives, reply with only the returned receipt.".into()
        }
        "delegated_agent" => {
            tools.push(tool(
                "delegate",
                "Delegate a private receipt lookup to a child agent and return its result.",
                empty.clone(),
                env.clone(),
                false,
                false,
            ));
            "Call delegate exactly once and reply with only the returned receipt.".into()
        }
        "catalog_selection" => {
            for i in 0..case.size {
                tools.push(tool(
                    &format!("catalog_{i:03}"),
                    &format!("Read catalog record {i:03}; returns its receipt."),
                    empty.clone(),
                    env.clone(),
                    false,
                    false,
                ));
            }
            format!(
                "Call catalog_{:03} exactly once. Do not call any other tool. Then reply with only its returned receipt.",
                case.size / 2
            )
        }
        "multimodal_report" => {
            tools.push(tool(
                "read_image",
                "Read the image attachment used for this report.",
                empty.clone(),
                env.clone(),
                false,
                false,
            ));
            tools.push(tool(
                "submit_report",
                "Submit the alphanumeric code read from the image.",
                object(json!({"code":{"type":"string"}}), json!(["code"])),
                env.clone(),
                false,
                false,
            ));
            expected_calls = 2;
            "First call read_image. Read the printed alphanumeric code in its image and call submit_report with that code. Finally reply with only the returned receipt.".into()
        }
        "freeform_pipeline" => {
            tools.push(tool(
                "apply_patch",
                "Accept a freeform patch and return a ticket. This is an in-memory test.",
                Value::Null,
                env.clone(),
                false,
                true,
            ));
            tools.push(tool(
                "confirm_patch",
                "Confirm the ticket returned by apply_patch.",
                object(json!({"ticket":{"type":"string"}}), json!(["ticket"])),
                env.clone(),
                false,
                false,
            ));
            expected_calls = 2;
            "First call apply_patch with a patch adding greeting.txt containing the line 你好; use the *** Begin Patch format. Then call confirm_patch with its ticket. Finally reply with only the returned receipt.".into()
        }
        "structured_report" => {
            expected_calls = 0;
            let value = json!({"receipt":receipt,"items":[{"name":"红茶","quantity":3},{"name":"咖啡","quantity":2}],"total":71});
            expected = value.to_string();
            let schema = object(
                json!({"receipt":{"type":"string"},"items":{"type":"array","items":object(json!({"name":{"type":"string"},"quantity":{"type":"integer"}}),json!(["name","quantity"]))},"total":{"type":"integer"}}),
                json!(["receipt", "items", "total"]),
            );
            format = Some(if case.protocol == Protocol::Chat {
                ResponseFormat::Json
            } else {
                ResponseFormat::Schema {
                    name: "report".into(),
                    schema,
                }
            });
            format!(
                "Convert this order into JSON with exactly receipt, items (name and quantity), total. Receipt: {receipt}. Items: 红茶 quantity 3; 咖啡 quantity 2. Total: 71. Return only the JSON object."
            )
        }
        "long_context" => {
            expected_calls = 0;
            let mut corpus = String::new();
            let mut i = 0;
            while corpus.len() < case.size {
                corpus.push_str(&format!("record-{i:05}: ordinary inventory item with category supplies and code NOISE{i:05}\n"));
                i += 1;
            }
            let middle = corpus[..corpus.len() / 2].rfind('\n').unwrap() + 1;
            corpus.insert_str(middle, &format!("target-record: code {receipt}\n"));
            format!(
                "Read the following data. Reply with only the exact code belonging to target-record.\n{corpus}\nReturn only the code for target-record."
            )
        }
        "session_labels" | "shared_burst" => {
            expected_calls = 0;
            format!("Reply with exactly {receipt} and nothing else.")
        }
        _ => unreachable!(),
    };
    let tool_registry = Arc::new(RuntimeToolRegistry::from_tools(tools)?);
    let make_runtime = || {
        let mut builder = Runtime::builder(recorded.clone())
            .runtime_tools(tool_registry.clone())
            .store(store.api.clone())
            .defaults(|run| run.profile(options(case.protocol, &tag, thinking)))
            .defaults(|run| run.stream(case.stream))
            .defaults(|run| {
                run.limits(Limits {
                    max_generation_requests: 8,
                    max_runtime_tool_calls: 12,
                    max_operation_concurrency: 3,
                    max_observer_events: 4096,
                    elapsed_ms: Some(120_000),
                    ..zhir::kernel::defaults::limits()
                })
            });
        if case.family == "approval_resume" {
            builder = builder.approval(Arc::new(Gate));
        }
        if let Some(format) = &format {
            builder = builder.defaults(|run| run.response_format(format.clone()));
        }
        builder.build()
    };
    // Reconstructing the runtime after suspension emulates application restart.
    let runtime = make_runtime()?;
    let (first,events)=drain(runtime.start(zhir::RunRequest::new(vec![Message::system("Follow the workflow and exact output instructions. Tools use private fixture data. Do not invent tool results. When asked for only a receipt, output the receipt field string value itself, without JSON, quotation marks or explanation."),Message::user(prompt)]))?).await;
    let mut all_events = vec![json!({"ordered":events.ordered,"counts":events.counts})];
    let mut checkpoint = first.map_err(|e| e.error)?;
    let mut suspended_revision = None;
    if matches!(case.family, "wait_resume" | "approval_resume") {
        require(
            matches!(checkpoint.state, State::Suspended { .. }),
            format!("expected suspension, got {}", checkpoint.state.kind()),
        )?;
        if case.family == "approval_resume" {
            require(
                env.calls.lock().unwrap().is_empty(),
                "tool executed before approval",
            )?;
        }
        suspended_revision = Some(checkpoint.revision);
        checkpoint = roundtrip(&checkpoint)?;
        let mut resume = ResumeRequest::from_checkpoint(checkpoint.clone());
        if case.family == "wait_resume" {
            resume = resume.resolve(zhir_core::operation::RecoveryResolution::Complete {
                operation_id: checkpoint.active.operations.keys().next().unwrap().clone(),
                outcome: OperationOutcome::Success {
                    content: vec![],
                    structured: json!({"reviewer":"accepted","receipt":receipt}),
                },
            });
            resume.messages.push(Message::external(
                json!({"reviewer":"accepted","receipt":receipt}).to_string(),
            ));
        } else {
            resume.metadata.insert("approved".into(), json!(true));
        }
        let (next, events) = drain(make_runtime()?.resume(resume).await?).await;
        all_events.push(json!({"ordered":events.ordered,"counts":events.counts}));
        checkpoint = next.map_err(|e| e.error)?;
    }
    let calls = env.calls.lock().unwrap().clone();
    let records = recorded_turns(&recorded);
    let settled_failure = if let State::Failed { error } = &checkpoint.state {
        Some(error.clone())
    } else {
        None
    };
    let state = checkpoint.state.kind();
    let output = if let State::Completed { content } = &checkpoint.state {
        text(content)
    } else {
        String::new()
    };
    let mut checks = BTreeMap::new();
    checks.insert("completed", state == zhir_core::run::StateKind::Completed);
    checks.insert(
        "expected_output",
        if case.family == "structured_report" {
            serde_json::from_str::<Value>(output.trim()).ok()
                == serde_json::from_str::<Value>(&expected).ok()
        } else {
            output.trim() == expected
        },
    );
    checks.insert(
        "exact_tool_count",
        calls.len() == expected_calls
            && checkpoint.metrics.runtime_tool_calls as usize == expected_calls,
    );
    checks.insert(
        "ordered_events",
        all_events.iter().all(|e| e["ordered"] == true),
    );
    checks.insert(
        "session_isolation",
        records
            .iter()
            .all(|r| r["session"]["tag"] == r["requested_tag"]),
    );
    checks.insert("wire_roundtrip", roundtrip(&checkpoint).is_ok());
    checks.insert(
        "stored_head",
        store
            .api
            .load_head(&checkpoint.context.run_id)
            .await?
            .is_some_and(|head| {
                head.id == checkpoint.id && head.history.digest() == checkpoint.history.digest()
            }),
    );
    checks.insert(
        "usage_and_identity",
        records.iter().all(|r| {
            r["response_id"].is_string()
                && r["model_id"] == "deepseek-flash"
                && r["usage"]["input_tokens"].is_number()
        }),
    );
    if case.stream {
        checks.insert(
            "extension_observed_frames",
            records
                .iter()
                .all(|r| r["session"]["frames"].as_u64().unwrap_or(0) > 0),
        );
    }
    if case.family == "parallel_quotes" {
        let mut regions: Vec<_> = calls
            .iter()
            .map(|c| c["input"]["value"]["region"].as_str().unwrap_or_default())
            .collect();
        regions.sort();
        checks.insert("unique_regions", regions == vec!["A", "B", "C"]);
        checks.insert("concurrency_bound", env.peak.load(Ordering::SeqCst) <= 3);
        checks.insert("parallel_execution", env.peak.load(Ordering::SeqCst) == 3);
    }
    if case.family == "catalog_selection" {
        checks.insert(
            "selected_tool",
            calls
                .first()
                .is_some_and(|c| c["name"] == format!("catalog_{:03}", case.size / 2)),
        );
    }
    let trace = store.verify();
    checks.insert("valid_commit_traces", trace.is_ok());
    let passed = checks.values().all(|v| *v);
    Ok(
        json!({"passed":passed,"verified_commits":trace.ok(),"checks":checks,"output":output,"expected":expected,"state":state,"settled_failure":settled_failure,"metrics":checkpoint.metrics,"run_id":checkpoint.context.run_id,"suspended_revision":suspended_revision,"tool_peak":env.peak.load(Ordering::SeqCst),"tool_calls":calls,"events":all_events,"model_requests":records,"store":if case.family=="wait_resume" {"sqlite"} else {"memory"}}),
    )
}
async fn run_case(case: Case, shared: Arc<dyn Model>) -> Value {
    let start = Instant::now();
    let tag = case.tag();
    let store = StoreFixture::new(case.family == "wait_resume")
        .await
        .unwrap();
    let recorded = Arc::new(RecordingModel::new(shared));
    let mut row = match workflow(&case, recorded.clone(), &store).await {
        Ok(row) => row,
        Err(error) => {
            json!({"passed":false,"error":error.to_string(),"error_kind":format!("{error:?}")})
        }
    };
    if row.get("model_requests").is_none() {
        row["model_requests"] = json!(recorded_turns(&recorded));
    }
    store.close().await;
    row["case_id"] = json!(tag);
    row["family"] = json!(case.family);
    row["protocol"] = json!(name(case.protocol));
    row["stream"] = json!(case.stream);
    row["repeat"] = json!(case.repeat);
    row["size"] = json!(case.size);
    row["elapsed_ms"] = json!(start.elapsed().as_millis());
    row
}
fn cases() -> Vec<Case> {
    let repeats = std::env::var("ZHIR_SCALE_REPEATS")
        .ok()
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(2);
    let mut cases = vec![];
    for protocol in [Protocol::Chat, Protocol::Responses, Protocol::Messages] {
        for family in [
            "retrieve_compute",
            "parallel_quotes",
            "tool_recovery",
            "wait_resume",
            "approval_resume",
            "delegated_agent",
            "structured_report",
            "session_labels",
        ] {
            for repeat in 0..repeats {
                for stream in [false, true] {
                    cases.push(Case {
                        family,
                        protocol,
                        repeat,
                        stream,
                        size: 0,
                    });
                }
            }
        }
        for size in [8, 64, 128] {
            for stream in [false, true] {
                cases.push(Case {
                    family: "catalog_selection",
                    protocol,
                    repeat: 0,
                    stream,
                    size,
                });
            }
        }
        for size in [4096, 32768, 131072] {
            cases.push(Case {
                family: "long_context",
                protocol,
                repeat: 0,
                stream: true,
                size,
            });
        }
        if protocol != Protocol::Chat {
            for repeat in 0..repeats {
                for stream in [false, true] {
                    cases.push(Case {
                        family: "multimodal_report",
                        protocol,
                        repeat,
                        stream,
                        size: 0,
                    });
                }
            }
        }
        if protocol == Protocol::Responses {
            for repeat in 0..repeats {
                for stream in [false, true] {
                    cases.push(Case {
                        family: "freeform_pipeline",
                        protocol,
                        repeat,
                        stream,
                        size: 0,
                    });
                }
            }
        }
        for repeat in 0..16 {
            cases.push(Case {
                family: "shared_burst",
                protocol,
                repeat,
                stream: true,
                size: 16,
            });
        }
    }
    let filter = std::env::var("ZHIR_SCALE_FILTER").unwrap_or_default();
    cases.retain(|c| c.tag().contains(&filter));
    if std::env::var("ZHIR_SCALE_PILOT").is_ok() {
        cases.retain(|c| c.repeat == 0 && c.stream && c.size <= 4096);
    }
    cases
}
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "paid live scenario-scale acceptance; requires DEEPSEEK_API_KEY"]
async fn live_scenario_scale() {
    let key = std::env::var("DEEPSEEK_API_KEY").expect("DEEPSEEK_API_KEY");
    let output = std::env::var("ZHIR_SCALE_REPORT")
        .unwrap_or_else(|_| "test-results/zhir-scenario-live.json".into());
    let concurrency = std::env::var("ZHIR_SCALE_CONCURRENCY")
        .ok()
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(12);
    assert!(concurrency > 0);
    let shared: Vec<Arc<dyn Model>> = [Protocol::Chat, Protocol::Responses, Protocol::Messages]
        .into_iter()
        .map(|p| {
            Arc::new(
                http(
                    p,
                    if p == Protocol::Messages {
                        "https://api.deepseek.com/anthropic/v1"
                    } else {
                        "https://api.deepseek.com"
                    },
                    &key,
                    "deepseek-flash",
                )
                .unwrap()
                .with_extension(|_| Ok(ConsumerSession::default())),
            ) as Arc<dyn Model>
        })
        .collect();
    let cases = cases();
    assert!(!cases.is_empty());
    eprintln!(
        "RUN {} live workflows with concurrency {concurrency}",
        cases.len()
    );
    let started = Instant::now();
    let mut pending = stream::iter(cases.into_iter().map(|case| {
        let index = match case.protocol {
            Protocol::Chat => 0,
            Protocol::Responses => 1,
            Protocol::Messages => 2,
        };
        run_case(case, shared[index].clone())
    }))
    .buffer_unordered(concurrency);
    let mut rows = vec![];
    while let Some(row) = pending.next().await {
        eprintln!(
            "{} {} ({} ms)",
            if row["passed"] == true {
                "PASS"
            } else {
                "FAIL"
            },
            row["case_id"],
            row["elapsed_ms"]
        );
        rows.push(row);
        save(&output, &rows);
    }
    let failed = rows.iter().filter(|row| row["passed"] != true).count();
    eprintln!(
        "{} workflows, {} failed, {} ms; report {output}",
        rows.len(),
        failed,
        started.elapsed().as_millis()
    );
    assert_eq!(failed, 0, "scenario failures retained in {output}");
}
