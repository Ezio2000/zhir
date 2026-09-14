use std::sync::Arc;
use zhir_core::operation::OperationOutcome;
use zhir_core::{
    message::Message,
    run::{Fact, History, HistoryEntry, State},
    storage::{Commit, HistoryDelta, RunStore},
    wire,
};
fn initial() -> Commit {
    let mut checkpoint = zhir_testing::checkpoint(vec![Message::user("initial")]);
    checkpoint.context = zhir_kernel::defaults::context();
    checkpoint.id = uuid::Uuid::new_v4().to_string();
    checkpoint.options.stream = true;
    checkpoint.options.profile.generation.seed = Some(23);
    let entries = checkpoint.history.entries();
    Commit::new(Arc::new(checkpoint), HistoryDelta::Initial(entries))
}
fn append(previous: &Commit, text: &str) -> Commit {
    let messages = vec![HistoryEntry {
        id: uuid::Uuid::new_v4().to_string(),
        origin: None,
        message: Message::external(text),
    }];
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
            "delta" => {
                invalid.history = HistoryDelta::Append(vec![HistoryEntry {
                    id: "different".into(),
                    origin: None,
                    message: Message::external("different"),
                }])
            }
            "empty_append" => invalid.history = HistoryDelta::Append(vec![]),
            "initial_again" => {
                invalid.history = HistoryDelta::Initial(invalid.checkpoint.history.entries())
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
    let replace = Commit::new(
        Arc::new(rewritten),
        HistoryDelta::Replace(History::new(messages).unwrap().entries()),
    );
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
        model::TurnOutput,
        operation::RecoveryResolution,
        tool::{RuntimeToolCall, RuntimeToolInput},
    };
    let calls: Vec<_> = (0..96)
        .map(|index| Output::RuntimeToolCall {
            call: RuntimeToolCall {
                id: format!("call-{index}"),
                name: "ask_question".into(),
                input: RuntimeToolInput::Structured(
                    serde_json::json!({"questions":[{"id":"answer","title":"Choose"}]}),
                ),
            },
        })
        .collect();
    let model = Arc::new(zhir_testing::ScriptedModel::responses([
        TurnOutput {
            output: calls,
            ..TurnOutput::text("")
        },
        TurnOutput::text("done"),
    ]));
    let tools = zhir_tools::RuntimeToolRegistry::from_tools([
        zhir_builtins::interaction::ask_question().unwrap(),
    ])
    .unwrap();
    let runtime = zhir_kernel::Runtime::builder(model.clone())
        .runtime_tools(Arc::new(tools))
        .store(store.clone())
        .defaults(|mut options| {
            options.limits.max_inflight_operations = 128;
            options
        })
        .build()
        .unwrap();
    let mut invocation = runtime
        .start(zhir_kernel::RunRequest::new(vec![Message::user(
            "questions",
        )]))
        .unwrap();
    let paused = tokio::time::timeout(std::time::Duration::from_secs(30), invocation.result())
        .await
        .unwrap()
        .unwrap()
        .into_checkpoint();
    assert!(
        matches!(paused.state, State::Suspended { .. }),
        "{:?}",
        paused.state
    );
    let loaded = store
        .load_head(&paused.context.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.active.operations.len(), 96);
    assert_eq!(loaded.history.pending_calls().count(), 96);
    let mut corrupt = loaded.as_ref().clone();
    corrupt.parent_id = Some(corrupt.id.clone());
    corrupt.id = uuid::Uuid::new_v4().to_string();
    corrupt.revision += 1;
    corrupt.state = State::Running;
    corrupt.fact = Fact::Attached;
    corrupt
        .active
        .operations
        .values_mut()
        .next()
        .unwrap()
        .call_entry = 0;
    assert!(
        store
            .commit(Commit::new(Arc::new(corrupt), HistoryDelta::Unchanged))
            .await
            .is_err()
    );
    let mut request = zhir_kernel::ResumeRequest::from_checkpoint(loaded.clone());
    for id in loaded.active.operations.keys().rev() {
        request = request.resolve(RecoveryResolution::Complete {
            operation_id: id.clone(),
            outcome: OperationOutcome::Success {
                content: vec![],
                structured: serde_json::json!({"answer":"yes"}),
            },
        });
    }
    let mut invocation = runtime.resume(request).await.unwrap();
    let completed = tokio::time::timeout(std::time::Duration::from_secs(30), invocation.result())
        .await
        .unwrap()
        .unwrap()
        .into_checkpoint();
    assert!(
        matches!(completed.state, State::Completed { .. }),
        "{:?}",
        completed.state
    );
    assert!(completed.active.operations.is_empty());
    assert_eq!(completed.metrics.runtime_tool_calls, 96);
    assert_eq!(completed.history.pending_calls().count(), 0);
    model.verify().unwrap();
    let loaded = store
        .load_head(&completed.context.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.history.digest(), completed.history.digest());
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
        zhir_storage::redis::RedisRunStore::connect(
            &url,
            &format!("zhir-test-{}", uuid::Uuid::new_v4()),
        )
        .await
        .unwrap(),
    ))
    .await;
}

#[tokio::test]
async fn sqlite_refuses_unversioned_and_other_version_layouts() {
    for version in [None, Some(1), Some(2), Some(4)] {
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory.path().join("format.db").display()
        );
        let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
        if let Some(version) = version {
            sqlx::query(
                "CREATE TABLE zhir_format (id INTEGER PRIMARY KEY, version INTEGER NOT NULL)",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO zhir_format VALUES (1, ?)")
                .bind(version)
                .execute(&pool)
                .await
                .unwrap();
        } else {
            sqlx::query("CREATE TABLE zhir_run_heads (legacy INTEGER)")
                .execute(&pool)
                .await
                .unwrap();
        }
        pool.close().await;
        assert!(
            zhir_storage::sqlite::SqliteRunStore::connect(&url)
                .await
                .is_err()
        );
    }
}
