//! Runtime tool admission order, settlement and observation through the kernel.
use futures::StreamExt;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    message::{Message, Output},
    model::GenerationOutput,
    operation::{OperationOutcome, OperationState},
    run::{Checkpoint, EventData, Fact, RunContext, State},
    storage::RunStore,
    tool::{
        ApprovalDecision, ApprovalPolicy, ApprovalRequest, Execution, InputSpec, RuntimeTool,
        RuntimeToolCall, RuntimeToolInput, RuntimeToolSpec,
    },
};
use zhir_kernel::{RunRequest, Runtime};
use zhir_testing::{RecordingStore, ScriptedModel};
use zhir_tools::{RuntimeToolRegistry, ToolReply, TypedTool};

const SERIAL: Execution = Execution {
    parallel: false,
    read_only: false,
    idempotent: false,
};
const PARALLEL: Execution = Execution {
    parallel: true,
    read_only: true,
    idempotent: true,
};

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct Step {
    n: usize,
}
/// Execution intervals by emission index.
type Log = Arc<Mutex<Vec<(usize, Instant, Instant)>>>;
fn probe(name: &str, execution: Execution, log: Log) -> Arc<dyn RuntimeTool> {
    let spec = RuntimeToolSpec {
        name: name.into(),
        description: "record an execution interval".into(),
        input: InputSpec::Structured {
            schema: json!({"type":"object","properties":{"n":{"type":"integer"}},"required":["n"],"additionalProperties":false}),
        },
        output_schema: None,
        execution,
    };
    Arc::new(
        zhir_tools::function::structured(spec, move |step: Step, _| {
            let log = log.clone();
            async move {
                let started = Instant::now();
                tokio::time::sleep(Duration::from_millis(20)).await;
                log.lock().unwrap().push((step.n, started, Instant::now()));
                Ok(zhir_tools::reply::json(json!(step.n)))
            }
        })
        .unwrap(),
    )
}
fn calls(names: &[&str]) -> GenerationOutput {
    GenerationOutput {
        output: names
            .iter()
            .enumerate()
            .map(|(n, name)| Output::RuntimeToolCall {
                call: RuntimeToolCall {
                    id: format!("call-{n}"),
                    name: (*name).into(),
                    input: RuntimeToolInput::Structured(json!({"n": n})),
                },
            })
            .collect(),
        ..GenerationOutput::text("")
    }
}
struct Harness {
    tools: Vec<Arc<dyn RuntimeTool>>,
    approval: Option<Arc<dyn ApprovalPolicy>>,
    concurrency: usize,
}
impl Harness {
    fn new(tools: Vec<Arc<dyn RuntimeTool>>) -> Self {
        Self {
            tools,
            approval: None,
            concurrency: 8,
        }
    }
    fn runtime(self, output: GenerationOutput, store: Arc<dyn RunStore>) -> Runtime {
        let model = Arc::new(ScriptedModel::responses([
            output,
            GenerationOutput::text("done"),
        ]));
        let mut builder = Runtime::builder(model)
            .runtime_tools(Arc::new(
                RuntimeToolRegistry::from_tools(self.tools).unwrap(),
            ))
            .store(store)
            .defaults(move |mut options| {
                options.limits.max_operation_concurrency = self.concurrency;
                options
            });
        if let Some(approval) = self.approval {
            builder = builder.approval(approval);
        }
        builder.build().unwrap()
    }
    async fn run(self, output: GenerationOutput) -> (Arc<Checkpoint>, Arc<RecordingStore>) {
        let store = Arc::new(RecordingStore::new(Arc::new(
            zhir_storage::MemoryRunStore::new(),
        )));
        let runtime = self.runtime(output, store.clone());
        let mut invocation = runtime
            .start(RunRequest::new(vec![Message::user("go")]))
            .unwrap();
        let checkpoint = tokio::time::timeout(Duration::from_secs(10), invocation.result())
            .await
            .expect("run stalled")
            .unwrap()
            .into_checkpoint();
        (checkpoint, store)
    }
}
fn intervals(log: &Log) -> Vec<(usize, Instant, Instant)> {
    let mut log = log.lock().unwrap().clone();
    log.sort_by_key(|(_, started, _)| *started);
    log
}
fn assert_isolated(log: &[(usize, Instant, Instant)], serial: &[usize]) {
    for (n, start, end) in log {
        if !serial.contains(n) {
            continue;
        }
        for (other, other_start, other_end) in log {
            assert!(
                other == n || other_end <= start || other_start >= end,
                "serial call {n} overlapped call {other}"
            );
        }
    }
}
/// Emission indexes in the order their operations became Running.
fn running_order(store: &RecordingStore) -> Vec<usize> {
    store
        .commits()
        .iter()
        .filter_map(|commit| match &commit.checkpoint.fact {
            Fact::Operation {
                operation_id,
                state: OperationState::Running,
            } => {
                let call = &commit.checkpoint.active.operations[operation_id]
                    .origin
                    .call_id;
                Some(call.trim_start_matches("call-").parse().unwrap())
            }
            _ => None,
        })
        .collect()
}
fn outcomes(checkpoint: &Checkpoint) -> BTreeMap<String, OperationOutcome> {
    checkpoint
        .history
        .messages()
        .into_iter()
        .filter_map(|message| match message {
            Message::RuntimeTool {
                call_id, outcome, ..
            } => Some((call_id, outcome)),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn serial_calls_run_alone_in_emission_order() {
    let log = Log::default();
    let (checkpoint, store) = Harness::new(vec![probe("serial", SERIAL, log.clone())])
        .run(calls(&["serial"; 4]))
        .await;
    assert!(matches!(checkpoint.state, State::Completed { .. }));
    let log = intervals(&log);
    assert_eq!(
        log.iter().map(|(n, ..)| *n).collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );
    assert_isolated(&log, &[0, 1, 2, 3]);
    assert_eq!(running_order(&store), [0, 1, 2, 3]);
    store.verify_traces().unwrap();
}

#[tokio::test]
async fn queued_calls_start_in_emission_order_under_the_concurrency_limit() {
    let log = Log::default();
    let harness = Harness {
        concurrency: 3,
        ..Harness::new(vec![probe("parallel", PARALLEL, log.clone())])
    };
    let (checkpoint, store) = harness.run(calls(&["parallel"; 12])).await;
    assert!(matches!(checkpoint.state, State::Completed { .. }));
    assert_eq!(running_order(&store), (0..12).collect::<Vec<_>>());
    assert_eq!(log.lock().unwrap().len(), 12);
}

#[tokio::test]
async fn serial_calls_are_barriers_between_parallel_groups() {
    let log = Log::default();
    let (checkpoint, store) = Harness::new(vec![
        probe("serial", SERIAL, log.clone()),
        probe("parallel", PARALLEL, log.clone()),
    ])
    .run(calls(&[
        "serial", "parallel", "parallel", "serial", "parallel",
    ]))
    .await;
    assert!(matches!(checkpoint.state, State::Completed { .. }));
    let log = intervals(&log);
    assert_isolated(&log, &[0, 3]);
    assert_eq!(running_order(&store), [0, 1, 2, 3, 4]);
    let span = |n: usize| log.iter().find(|(m, ..)| *m == n).unwrap();
    // The two parallel calls between barriers overlap.
    assert!(span(1).1 < span(2).2 && span(2).1 < span(1).2);
    assert!(span(4).1 >= span(3).2);
}

struct SlowApproval {
    batches: Mutex<Vec<usize>>,
}
impl ApprovalPolicy for SlowApproval {
    fn decide(
        &self,
        requests: Vec<ApprovalRequest>,
        _: RunContext,
    ) -> BoxFuture<'_, Result<Vec<ApprovalDecision>>> {
        Box::pin(async move {
            self.batches.lock().unwrap().push(requests.len());
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok(vec![ApprovalDecision::Allow; requests.len()])
        })
    }
}
#[tokio::test]
async fn pending_approval_holds_the_next_serial_call() {
    let log = Log::default();
    let approval = Arc::new(SlowApproval {
        batches: Mutex::default(),
    });
    let harness = Harness {
        approval: Some(approval.clone()),
        ..Harness::new(vec![probe("serial", SERIAL, log.clone())])
    };
    let (checkpoint, _) = harness.run(calls(&["serial"; 3])).await;
    assert!(matches!(checkpoint.state, State::Completed { .. }));
    assert_isolated(&intervals(&log), &[0, 1, 2]);
    assert_eq!(*approval.batches.lock().unwrap(), [1, 1, 1]);
}

fn typed(name: &str, error: fn() -> Error) -> Arc<dyn RuntimeTool> {
    Arc::new(
        TypedTool::new(name, "fails", SERIAL, move |_: Step, _| async move {
            Err::<ToolReply<()>, _>(error())
        })
        .unwrap(),
    )
}
#[tokio::test]
async fn deterministic_tool_errors_settle_as_failures() {
    let (checkpoint, _) = Harness::new(vec![typed("invalid", || {
        Error::Invalid("customer is unknown".into())
    })])
    .run(calls(&["invalid"]))
    .await;
    assert!(
        matches!(checkpoint.state, State::Completed { .. }),
        "{:?}",
        checkpoint.state
    );
    let outcome = &outcomes(&checkpoint)["call-0"];
    assert!(
        matches!(outcome, OperationOutcome::Failure { error } if error.code == "invalid_arguments"),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn uncertain_tool_errors_require_recovery() {
    let (checkpoint, _) = Harness::new(vec![typed("uncertain", || {
        Error::Uncertain("request may have been applied".into())
    })])
    .run(calls(&["uncertain"]))
    .await;
    assert!(
        matches!(&checkpoint.state, State::Suspended { suspension } if suspension.reason == "RecoveryRequired"),
        "{:?}",
        checkpoint.state
    );
    let operation = checkpoint.active.operations.values().next().unwrap();
    assert_eq!(operation.state, OperationState::Unknown);
}

/// Cancels each operation once it is observed Running, then returns the run result.
async fn cancel_running(runtime: Runtime) -> (Arc<Checkpoint>, Vec<(String, OperationState)>) {
    let mut invocation = runtime
        .start(RunRequest::new(vec![Message::user("go")]))
        .unwrap();
    let control = invocation.control();
    let mut events = invocation.events().unwrap();
    let mut changes = vec![];
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = events.next().await {
            if let EventData::OperationChanged {
                operation_id,
                state,
            } = event.data
            {
                if state == OperationState::Running {
                    control
                        .cancel_operation(operation_id.clone())
                        .await
                        .unwrap();
                }
                changes.push((operation_id, state));
            }
        }
    })
    .await
    .expect("run stalled");
    (
        invocation.result().await.unwrap().into_checkpoint(),
        changes,
    )
}
#[tokio::test]
async fn cancelling_running_tools_settles_them_as_cancelled() {
    let directory = tempfile::tempdir().unwrap();
    let waiting: Arc<dyn RuntimeTool> = Arc::new(
        TypedTool::new(
            "waiting",
            "waits for cancellation",
            SERIAL,
            |_: Step, context| async move {
                while !context.cancellation.is_cancelled() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err::<ToolReply<()>, _>(Error::Cancelled)
            },
        )
        .unwrap(),
    );
    let bash =
        zhir_builtins::shell::bash(zhir_builtins::shell::ShellOptions::new(directory.path()))
            .unwrap();
    let model = Arc::new(ScriptedModel::responses([
        GenerationOutput {
            output: vec![
                Output::RuntimeToolCall {
                    call: RuntimeToolCall {
                        id: "typed".into(),
                        name: "waiting".into(),
                        input: RuntimeToolInput::Structured(json!({"n": 0})),
                    },
                },
                Output::RuntimeToolCall {
                    call: RuntimeToolCall {
                        id: "shell".into(),
                        name: "bash".into(),
                        input: RuntimeToolInput::Structured(json!({"command": "sleep 30"})),
                    },
                },
            ],
            ..GenerationOutput::text("")
        },
        GenerationOutput::text("done"),
    ]));
    let runtime = Runtime::builder(model.clone())
        .runtime_tools(Arc::new(
            RuntimeToolRegistry::from_tools([waiting, bash]).unwrap(),
        ))
        .build()
        .unwrap();
    let started = Instant::now();
    let (checkpoint, _) = cancel_running(runtime).await;
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(
        matches!(checkpoint.state, State::Completed { .. }),
        "{:?}",
        checkpoint.state
    );
    let outcomes = outcomes(&checkpoint);
    for call in ["typed", "shell"] {
        assert!(
            matches!(outcomes[call], OperationOutcome::Cancelled { .. }),
            "{call}: {:?}",
            outcomes[call]
        );
    }
    model.verify().unwrap();
}

#[tokio::test]
async fn every_operation_transition_is_observed_once() {
    let log = Log::default();
    let runtime = Harness::new(vec![probe("serial", SERIAL, log)]).runtime(
        calls(&["serial", "serial"]),
        Arc::new(zhir_storage::MemoryRunStore::new()),
    );
    let mut invocation = runtime
        .start(RunRequest::new(vec![Message::user("go")]))
        .unwrap();
    let mut events = invocation.events().unwrap();
    let mut changes: BTreeMap<String, Vec<OperationState>> = BTreeMap::new();
    while let Some(event) = events.next().await {
        if let EventData::OperationChanged {
            operation_id,
            state,
        } = event.data
        {
            changes.entry(operation_id).or_default().push(state);
        }
    }
    assert!(matches!(
        invocation.result().await.unwrap().into_checkpoint().state,
        State::Completed { .. }
    ));
    assert_eq!(changes.len(), 2);
    for states in changes.values() {
        assert_eq!(
            states,
            &[
                OperationState::Queued,
                OperationState::Running,
                OperationState::Succeeded
            ]
        );
    }
}

#[tokio::test]
async fn binding_failures_found_during_admission_are_delivered() {
    let log = Log::default();
    let (checkpoint, _) = Harness::new(vec![probe("serial", SERIAL, log)])
        .run(calls(&["missing"]))
        .await;
    assert!(
        matches!(checkpoint.state, State::Completed { .. }),
        "{:?}",
        checkpoint.state
    );
    assert!(matches!(
        outcomes(&checkpoint)["call-0"],
        OperationOutcome::Failure { .. }
    ));
}
