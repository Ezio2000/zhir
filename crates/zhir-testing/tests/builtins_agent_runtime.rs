#![cfg(feature = "agent-runtime")]
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use zhir_builtins::agent::{AgentBackend, runtime_backend::RuntimeAgentBackend};
use zhir_core::operation::OperationOutcome;
use zhir_core::{
    error::Error, model::GenerationOutput, operation::*, run::RunContext, tool::RuntimeToolContext,
};
fn context(id: &str) -> RuntimeToolContext {
    RuntimeToolContext {
        operation_id: id.into(),
        run: RunContext::new("parent", 0),
        cancellation: Default::default(),
        progress: None,
    }
}
async fn outcome(handle: &mut OperationHandle) -> OperationOutcome {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let event = handle
                .events
                .receive()
                .await
                .unwrap()
                .expect("child closed before completion");
            match event.update {
                OperationUpdate::Finished { outcome } => return outcome,
                OperationUpdate::Unknown { reason } => panic!("{reason}"),
                _ => (),
            }
        }
    })
    .await
    .expect("child stalled")
}
#[tokio::test]
async fn child_admission_is_bounded_and_slots_return_after_cancel_and_completion() {
    let started = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let model = Arc::new(zhir_models::FunctionModel::new(
        zhir_testing::model_capabilities(),
        {
            let started = started.clone();
            let finish = finish.clone();
            move |_, _| {
                let started = started.clone();
                let finish = finish.clone();
                async move {
                    started.add_permits(1);
                    finish.acquire().await.unwrap().forget();
                    Ok(GenerationOutput::text("done"))
                }
            }
        },
    ));
    let runtime = zhir_kernel::Runtime::builder(model).build().unwrap();
    assert!(RuntimeAgentBackend::new(runtime.clone(), "", 0).is_err());
    let backend = Arc::new(RuntimeAgentBackend::new(runtime, "", 2).unwrap());
    let mut starts = tokio::task::JoinSet::new();
    for index in 0..32 {
        let backend = backend.clone();
        starts.spawn(async move {
            backend
                .start("work".into(), context(&format!("key-{index}")))
                .await
        });
    }
    let mut admitted = vec![];
    let mut rejected = 0;
    while let Some(start) = starts.join_next().await {
        match start.unwrap() {
            Ok(handle) => admitted.push(handle),
            Err(Error::RuntimeTool(error)) if error.code == "agent_capacity" => rejected += 1,
            Err(error) => panic!("{error}"),
        }
    }
    assert_eq!(admitted.len(), 2);
    assert_eq!(rejected, 30);
    tokio::time::timeout(Duration::from_secs(3), started.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    admitted[0].control.cancel().await.unwrap();
    assert!(matches!(
        outcome(&mut admitted[0]).await,
        OperationOutcome::Cancelled { .. }
    ));
    let mut next = backend.start("work".into(), context("next")).await.unwrap();
    started.acquire().await.unwrap().forget();
    finish.add_permits(2);
    assert!(matches!(
        outcome(&mut admitted[1]).await,
        OperationOutcome::Success { .. }
    ));
    assert!(matches!(
        outcome(&mut next).await,
        OperationOutcome::Success { .. }
    ));
}
#[tokio::test]
async fn child_recovery_reads_kernel_checkpoint_without_reexecuting_the_model() {
    let calls = Arc::new(AtomicUsize::new(0));
    let model = Arc::new(zhir_models::FunctionModel::new(
        zhir_testing::model_capabilities(),
        {
            let calls = calls.clone();
            move |_, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(GenerationOutput::text("child done")) }
            }
        },
    ));
    let store = Arc::new(zhir_storage::MemoryRunStore::new());
    let runtime = zhir_kernel::Runtime::builder(model)
        .store(store)
        .build()
        .unwrap();
    let backend = RuntimeAgentBackend::new(runtime.clone(), "instructions", 2).unwrap();
    let mut handle = backend
        .start("work".into(), context("child"))
        .await
        .unwrap();
    assert!(matches!(
        outcome(&mut handle).await,
        OperationOutcome::Success { .. }
    ));
    let record = OperationRecord {
        id: "child".into(),
        origin: CallRef {
            session_id: "session".into(),
            item_id: "call".into(),
            generation_id: Some("turn".into()),
            caller_id: "parent".into(),
            call_id: "call".into(),
        },
        owner: OperationOwner::RuntimeTool {
            name: "agent_run".into(),
        },
        state: OperationState::Running,
        call_entry: 0,
        result_entry: None,
        recovery: handle.recovery.clone(),
        last_sequence: None,
        last_update: None,
        wait: None,
    };
    drop(handle);
    drop(backend);
    let backend = RuntimeAgentBackend::new(runtime, "instructions", 2).unwrap();
    let mut recovered = backend.recover(record, context("child")).await.unwrap();
    assert!(matches!(
        outcome(&mut recovered).await,
        OperationOutcome::Success { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn parent_detach_suspends_child_and_cancelling_recovered_wait_is_durable() {
    use zhir_core::run::State;
    let started = Arc::new(Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let model = Arc::new(zhir_models::FunctionModel::new(
        zhir_testing::model_capabilities(),
        {
            let started = started.clone();
            let calls = calls.clone();
            move |_, _| {
                started.add_permits(1);
                calls.fetch_add(1, Ordering::SeqCst);
                async { std::future::pending().await }
            }
        },
    ));
    let runtime = zhir_kernel::Runtime::builder(model)
        .store(Arc::new(zhir_storage::MemoryRunStore::new()))
        .build()
        .unwrap();
    let backend = RuntimeAgentBackend::new(runtime.clone(), "", 1).unwrap();
    let handle = backend
        .start("work".into(), context("detach"))
        .await
        .unwrap();
    let record = OperationRecord {
        id: "detach".into(),
        origin: CallRef {
            session_id: "s".into(),
            item_id: "call".into(),
            generation_id: Some("t".into()),
            caller_id: "model".into(),
            call_id: "call".into(),
        },
        owner: OperationOwner::RuntimeTool {
            name: "agent_run".into(),
        },
        state: OperationState::Running,
        call_entry: 0,
        result_entry: None,
        recovery: handle.recovery.clone(),
        last_sequence: None,
        last_update: None,
        wait: None,
    };
    started.acquire().await.unwrap().forget();
    drop(handle);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(checkpoint) = runtime
                .load_checkpoint("parent:child:detach")
                .await
                .unwrap()
                && matches!(checkpoint.state, State::Suspended { .. })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("detached child was not suspended");
    let mut recovered = backend.recover(record, context("detach")).await.unwrap();
    let event = tokio::time::timeout(Duration::from_secs(3), recovered.events.receive())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(event.update, OperationUpdate::Waiting { .. }));
    recovered.control.cancel().await.unwrap();
    assert!(matches!(
        outcome(&mut recovered).await,
        OperationOutcome::Cancelled { .. }
    ));
    assert!(matches!(
        runtime
            .load_checkpoint("parent:child:detach")
            .await
            .unwrap()
            .unwrap()
            .state,
        State::Cancelled
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
