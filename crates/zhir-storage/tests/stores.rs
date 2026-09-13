use std::sync::Arc;
use zhir_core::{
    message::Message,
    run::{Checkpoint, Fact, History, Metrics, State},
    storage::{Commit, HistoryDelta, RunStore},
    wire,
};
fn initial() -> Commit {
    let messages = vec![Message::user("initial")];
    let checkpoint = Arc::new(Checkpoint {
        options: zhir_kernel::defaults::run_options().stream(true).options(
            zhir_core::model::ModelOptions {
                seed: Some(23),
                ..Default::default()
            },
        ),
        id: uuid::Uuid::new_v4().to_string(),
        parent_id: None,
        revision: 0,
        context: zhir_kernel::defaults::context(),
        history: History::new(messages.clone()).unwrap(),
        state: State::Planning {
            provider_turn_pending: false,
        },
        metrics: Metrics::default(),
        fact: Fact::Started,
    });
    Commit::new(checkpoint, HistoryDelta::Initial(messages))
}
fn append(previous: &Commit, text: &str) -> Commit {
    let messages = vec![Message::external(text)];
    let mut next = previous.checkpoint.as_ref().clone();
    next.parent_id = Some(next.id.clone());
    next.id = uuid::Uuid::new_v4().to_string();
    next.revision += 1;
    next.history = next.history.append(messages.clone()).unwrap();
    next.fact = Fact::ConversationInsert {
        source: "test".into(),
    };
    Commit::new(Arc::new(next), HistoryDelta::Append(messages))
}
async fn exercise(store: Arc<dyn RunStore>) {
    let first = initial();
    let run = &first.checkpoint.context.run_id;
    assert!(store.load_head(run).await.unwrap().is_none());
    store.commit(first.clone()).await.unwrap();
    store.commit(first.clone()).await.unwrap();
    let mut changed = append(&first, "changed options");
    Arc::make_mut(&mut changed.checkpoint).options.stream = false;
    assert!(
        matches!(store.commit(changed).await, Err(zhir_core::error::Error::Storage(e)) if e.contains("options changed"))
    );
    assert_eq!(
        store.load_head(run).await.unwrap().unwrap().id,
        first.checkpoint.id
    );
    for fault in ["parent", "delta", "empty_append", "initial_again"] {
        let mut invalid = append(&first, "invalid");
        match fault {
            "parent" => Arc::make_mut(&mut invalid.checkpoint).parent_id = Some("wrong".into()),
            "delta" => invalid.history = HistoryDelta::Append(vec![Message::external("different")]),
            "empty_append" => invalid.history = HistoryDelta::Append(vec![]),
            "initial_again" => {
                invalid.history = HistoryDelta::Initial(invalid.checkpoint.history.messages())
            }
            _ => unreachable!(),
        }
        assert!(store.commit(invalid).await.is_err(), "accepted {fault}");
        assert_eq!(
            store.load_head(run).await.unwrap().unwrap().id,
            first.checkpoint.id
        );
    }
    let second = append(&first, "second");
    store.commit(second.clone()).await.unwrap();
    store.commit(first.clone()).await.unwrap();
    let conflict = append(&first, "stale");
    assert!(store.commit(conflict).await.is_err());
    let mut reused = second.clone();
    let mut altered = reused.checkpoint.as_ref().clone();
    altered.state = State::Failed {
        error: zhir_core::error::Failure::new("changed", "changed"),
    };
    reused.checkpoint = Arc::new(altered);
    assert!(store.commit(reused).await.is_err());
    let recovered = store.load_head(run).await.unwrap().unwrap();
    assert_eq!(recovered.id, second.checkpoint.id);
    assert_eq!(recovered.options, first.checkpoint.options);
    assert_eq!(
        recovered.history.messages(),
        second.checkpoint.history.messages()
    );
    let json = wire::encode_checkpoint(&recovered).unwrap();
    assert_eq!(
        wire::decode_checkpoint(&json).unwrap().history.digest(),
        recovered.history.digest()
    );
    let messages = vec![Message::system("summary")];
    let mut rewritten = recovered.as_ref().clone();
    rewritten.parent_id = Some(rewritten.id.clone());
    rewritten.id = uuid::Uuid::new_v4().to_string();
    rewritten.revision += 1;
    rewritten.history = History::new(messages.clone()).unwrap();
    rewritten.fact = Fact::HistoryRewrite {
        reason: "compact".into(),
    };
    let replace = Commit::new(Arc::new(rewritten), HistoryDelta::Replace(messages));
    store.commit(replace.clone()).await.unwrap();
    let fourth = append(&replace, "after summary");
    store.commit(fourth.clone()).await.unwrap();
    let recovered = store.load_head(run).await.unwrap().unwrap();
    assert_eq!(
        recovered.history.messages(),
        vec![
            Message::system("summary"),
            Message::external("after summary")
        ]
    );
    let mut expired = append(&fourth, "too late");
    expired.deadline = Some(std::time::Instant::now());
    assert!(store.commit(expired).await.is_err());
    assert_eq!(store.load_head(run).await.unwrap().unwrap().revision, 3);
    pending_recovery(store).await;
}
async fn pending_recovery(store: Arc<dyn RunStore>) {
    use zhir_core::{
        message::Output,
        run::{ActiveState, ControlAction, StateKind},
        tool::{RuntimeToolCall, RuntimeToolInput, RuntimeToolOutcome, RuntimeToolOutcomeKind},
    };
    let first = initial();
    let run = first.checkpoint.context.run_id.clone();
    store.commit(first.clone()).await.unwrap();
    let calls: Vec<_> = (0..96)
        .map(|i| RuntimeToolCall {
            id: format!("call-{i}"),
            name: "echo".into(),
            input: RuntimeToolInput::Structured(serde_json::json!({"i":i})),
        })
        .collect();
    let message = Message::Assistant {
        output: calls
            .iter()
            .cloned()
            .map(|call| Output::RuntimeToolCall { call })
            .collect(),
        provider_data: serde_json::Value::Null,
    };
    let mut head = first.checkpoint.as_ref().clone();
    head.history = head.history.append(vec![message.clone()]).unwrap();
    head.state = State::RuntimeToolsPending {
        calls: head.history.pending().unwrap().unwrap(),
        provider_turn_pending: false,
    };
    head.fact = Fact::ModelTurn {
        runtime_tool_call_ids: calls.iter().map(|c| c.id.clone()).collect(),
        result: StateKind::RuntimeToolsPending,
    };
    head.parent_id = Some(head.id.clone());
    head.id = uuid::Uuid::new_v4().to_string();
    head.revision += 1;
    let mut invalid = head.clone();
    if let State::RuntimeToolsPending { calls, .. } = &mut invalid.state {
        calls.next += 1;
    }
    assert!(
        store
            .commit(Commit::new(
                Arc::new(invalid),
                HistoryDelta::Append(vec![message.clone()])
            ))
            .await
            .is_err()
    );
    store
        .commit(Commit::new(
            Arc::new(head),
            HistoryDelta::Append(vec![message]),
        ))
        .await
        .unwrap();
    for batch in calls.chunks(17) {
        let loaded = store.load_head(&run).await.unwrap().unwrap();
        let cursor = loaded.history.pending().unwrap().unwrap();
        assert_eq!(
            loaded.history.resolve_pending(cursor).unwrap()[0].id,
            batch[0].id
        );
        let messages: Vec<_> = batch
            .iter()
            .map(|call| Message::RuntimeTool {
                call_id: call.id.clone(),
                name: call.name.clone(),
                outcome: RuntimeToolOutcome::Success {
                    content: vec![],
                    structured: serde_json::Value::Null,
                },
            })
            .collect();
        let mut next = loaded.as_ref().clone();
        next.parent_id = Some(next.id.clone());
        next.id = uuid::Uuid::new_v4().to_string();
        next.revision += 1;
        next.history = next.history.append(messages.clone()).unwrap();
        next.state = match cursor.advance(batch.len()).unwrap() {
            Some(calls) => State::RuntimeToolsPending {
                calls,
                provider_turn_pending: false,
            },
            None => State::Planning {
                provider_turn_pending: false,
            },
        };
        next.fact = Fact::RuntimeToolBatch {
            call_ids: batch.iter().map(|c| c.id.clone()).collect(),
            outcomes: vec![RuntimeToolOutcomeKind::Success; batch.len()],
            parallel: true,
        };
        store
            .commit(Commit::new(Arc::new(next), HistoryDelta::Append(messages)))
            .await
            .unwrap();
        if cursor.next == 0 {
            let mut pause = store
                .load_head(&run)
                .await
                .unwrap()
                .unwrap()
                .as_ref()
                .clone();
            let resume_to = pause.state.active().unwrap();
            pause.parent_id = Some(pause.id.clone());
            pause.id = uuid::Uuid::new_v4().to_string();
            pause.revision += 1;
            pause.state = State::Suspended {
                resume_to,
                suspension: zhir_kernel::defaults::pause(),
            };
            pause.fact = Fact::Control {
                action: ControlAction::Suspended,
            };
            store
                .commit(Commit::new(Arc::new(pause), HistoryDelta::Unchanged))
                .await
                .unwrap();
            let loaded = store.load_head(&run).await.unwrap().unwrap();
            let State::Suspended {
                resume_to: ActiveState::RuntimeToolsPending { calls, .. },
                ..
            } = loaded.state
            else {
                panic!("recovered suspension");
            };
            assert_eq!(calls.next, 17);
            assert_eq!(loaded.history.resolve_pending(calls).unwrap().len(), 79);
            let mut resumed = loaded.as_ref().clone();
            resumed.parent_id = Some(resumed.id.clone());
            resumed.id = uuid::Uuid::new_v4().to_string();
            resumed.revision += 1;
            resumed.state = State::RuntimeToolsPending {
                calls,
                provider_turn_pending: false,
            };
            resumed.fact = Fact::Resumed;
            store
                .commit(Commit::new(Arc::new(resumed), HistoryDelta::Unchanged))
                .await
                .unwrap();
        }
    }
    let loaded = store.load_head(&run).await.unwrap().unwrap();
    assert_eq!(loaded.history.pending().unwrap(), None);
    assert_eq!(loaded.history.len(), 98);
}
#[tokio::test]
async fn memory() {
    exercise(Arc::new(zhir_storage::MemoryRunStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("runs.db").display()
    );
    let store = zhir_storage::sqlite::SqliteRunStore::connect(&url)
        .await
        .unwrap();
    exercise(Arc::new(store.clone())).await;
    store.close().await;
}
#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires ZHIR_TEST_MYSQL_URL"]
async fn mysql() {
    let url = std::env::var("ZHIR_TEST_MYSQL_URL").expect("ZHIR_TEST_MYSQL_URL");
    let store = zhir_storage::mysql::MysqlRunStore::connect(&url)
        .await
        .unwrap();
    exercise(Arc::new(store.clone())).await;
    store.close().await;
}
#[cfg(feature = "redis")]
#[tokio::test]
#[ignore = "requires ZHIR_TEST_REDIS_URL"]
async fn redis() {
    let url = std::env::var("ZHIR_TEST_REDIS_URL").expect("ZHIR_TEST_REDIS_URL");
    exercise(Arc::new(
        zhir_storage::redis::RedisRunStore::connect(&url)
            .await
            .unwrap(),
    ))
    .await;
}
