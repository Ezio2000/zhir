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
    let mut next = previous.checkpoint().as_ref().clone();
    next.parent_id = Some(next.id.clone());
    next.id = uuid::Uuid::new_v4().to_string();
    next.revision += 1;
    next.history = next.history.append(messages.clone()).unwrap();
    next.fact = Fact::ConversationInsert {
        source: "test".into(),
    };
    Commit::new(Arc::new(next), HistoryDelta::Append(messages))
}
/// The same write with a changed checkpoint.
fn altered(commit: &Commit, change: impl FnOnce(&mut zhir_core::run::Checkpoint)) -> Commit {
    let mut checkpoint = commit.checkpoint().as_ref().clone();
    change(&mut checkpoint);
    Commit::new(Arc::new(checkpoint), commit.history().clone())
}
/// Returns the exercised run, whose history was rewritten once.
async fn exercise(store: Arc<dyn RunStore>) -> String {
    let first = initial();
    let run = &first.checkpoint().context.run_id.clone();
    assert!(store.load_head(run).await.unwrap().is_none());
    store.commit(first.clone()).await.unwrap();
    store.commit(first.clone()).await.unwrap();
    let changed = altered(&append(&first, "changed options"), |c| {
        c.options.stream = false
    });
    assert!(
        matches!(store.commit(changed).await, Err(zhir_core::error::Error::Storage(e)) if e.contains("options changed"))
    );
    assert_eq!(
        store.load_head(run).await.unwrap().unwrap().id,
        first.checkpoint().id
    );
    for fault in ["parent", "delta", "empty_append", "initial_again"] {
        let valid = append(&first, "invalid");
        let invalid = match fault {
            "parent" => altered(&valid, |c| c.parent_id = Some("wrong".into())),
            "delta" => Commit::new(
                valid.checkpoint().clone(),
                HistoryDelta::Append(vec![HistoryEntry {
                    id: "different".into(),
                    origin: None,
                    message: Message::external("different"),
                }]),
            ),
            "empty_append" => Commit::new(valid.checkpoint().clone(), HistoryDelta::Append(vec![])),
            "initial_again" => Commit::new(
                valid.checkpoint().clone(),
                HistoryDelta::Initial(valid.checkpoint().history.entries()),
            ),
            _ => unreachable!(),
        };
        assert!(store.commit(invalid).await.is_err(), "accepted {fault}");
        assert_eq!(
            store.load_head(run).await.unwrap().unwrap().id,
            first.checkpoint().id
        );
    }
    let second = append(&first, "second");
    store.commit(second.clone()).await.unwrap();
    store.commit(first.clone()).await.unwrap();
    let conflict = append(&first, "stale");
    assert!(store.commit(conflict).await.is_err());
    let reused = altered(&second, |c| {
        c.state = State::Failed {
            error: zhir_core::error::Failure::new("changed", "changed"),
        }
    });
    assert!(store.commit(reused).await.is_err());
    let recovered = store.load_head(run).await.unwrap().unwrap();
    assert_eq!(recovered.id, second.checkpoint().id);
    assert_eq!(recovered.options, first.checkpoint().options);
    assert_eq!(
        recovered.history.messages(),
        second.checkpoint().history.messages()
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
    run.clone()
}
/// Deleting a run removes it entirely; deleting it again succeeds.
async fn delete(store: &dyn RunStore, run: &str) {
    assert!(store.load_head(run).await.unwrap().is_some());
    store.delete(run).await.unwrap();
    assert!(store.load_head(run).await.unwrap().is_none());
    store.delete(run).await.unwrap();
    let fresh = initial();
    store.commit(fresh.clone()).await.unwrap();
    store
        .delete(&fresh.checkpoint().context.run_id)
        .await
        .unwrap();
}
async fn pending_recovery(store: Arc<dyn RunStore>) {
    use zhir_core::{
        message::Output,
        model::GenerationOutput,
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
        GenerationOutput {
            output: calls,
            ..GenerationOutput::text("")
        },
        GenerationOutput::text("done"),
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
    let store = Arc::new(zhir_storage::MemoryRunStore::new());
    let run = exercise(store.clone()).await;
    delete(store.as_ref(), &run).await;
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
    let run = exercise(Arc::new(store.clone())).await;
    let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
    let rows = |table: &'static str| {
        let pool = pool.clone();
        let run = run.clone();
        async move {
            sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table} WHERE run_id=?"))
                .bind(run)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    // The rewrite removed the rows of the earlier generation.
    let generations: Vec<i64> =
        sqlx::query_scalar("SELECT DISTINCT generation FROM zhir_history WHERE run_id=?")
            .bind(&run)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(generations.len(), 1);
    assert_eq!(rows("zhir_history").await, 2);
    delete(&store, &run).await;
    for table in ["zhir_history", "zhir_commits", "zhir_run_heads"] {
        assert_eq!(rows(table).await, 0, "{table}");
    }
    pool.close().await;
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
    let run = exercise(Arc::new(store.clone())).await;
    delete(&store, &run).await;
    store.close().await;
}
#[cfg(feature = "redis")]
#[tokio::test]
#[ignore = "requires ZHIR_TEST_REDIS_URL"]
async fn redis() {
    let url = std::env::var("ZHIR_TEST_REDIS_URL").expect("ZHIR_TEST_REDIS_URL");
    let namespace = format!("zhir-test-{}", uuid::Uuid::new_v4());
    let store = zhir_storage::redis::RedisRunStore::connect(&url, &namespace)
        .await
        .unwrap();
    let run = exercise(Arc::new(store.clone())).await;
    let tag: String = run.bytes().map(|b| format!("{b:02x}")).collect();
    let history = format!("{namespace}:{{{tag}}}:history");
    let mut conn = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    // The rewrite removed the fields of the earlier generation.
    let fields: usize = redis::AsyncCommands::hlen(&mut conn, &history)
        .await
        .unwrap();
    assert_eq!(fields, 2);
    delete(&store, &run).await;
    let exists: bool = redis::AsyncCommands::exists(&mut conn, &history)
        .await
        .unwrap();
    assert!(!exists);
}

#[tokio::test]
async fn sqlite_refuses_unversioned_and_other_version_layouts() {
    for version in [None, Some(1), Some(2), Some(3), Some(4)] {
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
#[tokio::test]
async fn reachable_lists_the_stored_resources_a_checkpoint_references() {
    use zhir_core::{
        message::{Content, Output},
        resource::{ArchivedMedia, ResourceRef, ResourceSource, ResourceStore, SealedMedia},
        run::StreamCursor,
    };
    use zhir_storage::MemoryResourceStore;
    async fn put(
        store: &MemoryResourceStore,
        key: &str,
        media_type: &str,
        bytes: Vec<u8>,
    ) -> ResourceRef {
        let mut writer = store.create(key.into(), media_type.into()).await.unwrap();
        writer.append(0, bytes).await.unwrap();
        writer.finish().await.unwrap()
    }
    async fn sealed(
        store: &MemoryResourceStore,
        key: &str,
        previous: Option<ResourceRef>,
    ) -> ResourceRef {
        let resource = put(store, &format!("{key}-data"), "audio/pcm", vec![0; 4]).await;
        let node = SealedMedia {
            stream_id: key.into(),
            session_id: "session".into(),
            epoch: 0,
            chunks: vec![zhir_core::resource::SealedChunk {
                sequence: 0,
                timestamp_us: 0,
                offset: 0,
                length: 4,
                end: false,
            }],
            resource,
            previous,
        };
        put(
            store,
            key,
            "application/vnd.zhir.sealed-media+json",
            serde_json::to_vec(&node).unwrap(),
        )
        .await
    }
    let store = MemoryResourceStore::new();
    let user = put(&store, "user", "image/png", b"user".to_vec()).await;
    let output = put(&store, "output", "text/plain", b"output".to_vec()).await;
    let delegated = put(&store, "delegated", "text/plain", b"delegated".to_vec()).await;
    let completed = put(&store, "completed", "text/plain", b"completed".to_vec()).await;
    put(
        &store,
        "unreferenced",
        "text/plain",
        b"unreferenced".to_vec(),
    )
    .await;
    let inline = ResourceRef {
        id: "inline".into(),
        media_type: "text/plain".into(),
        name: None,
        source: ResourceSource::Inline {
            bytes: b"inline".to_vec(),
        },
        metadata: Default::default(),
    };
    let active = sealed(&store, "active-0", None).await;
    let active = sealed(&store, "active-1", Some(active)).await;
    let mut archive = None;
    for index in 0..2 {
        let node = ArchivedMedia {
            stream_key: format!("archived-{index}"),
            sealed: sealed(&store, &format!("archived-{index}"), None).await,
            complete: true,
            previous: archive,
        };
        archive = Some(
            put(
                &store,
                &format!("archive-{index}"),
                "application/vnd.zhir.archived-media+json",
                serde_json::to_vec(&node).unwrap(),
            )
            .await,
        );
    }
    let mut checkpoint = zhir_testing::checkpoint(vec![
        Message::User {
            content: vec![Content::resource(user.clone()), Content::resource(inline)],
        },
        Message::Assistant {
            output: vec![Output::Content {
                content: Content::resource(output),
            }],
            provider_data: serde_json::Value::Null,
        },
        Message::DelegationResult {
            id: "delegation".into(),
            outcome: OperationOutcome::Success {
                content: vec![Content::resource(delegated), Content::resource(user)],
                structured: serde_json::Value::Null,
            },
        },
    ]);
    checkpoint.state = State::Completed {
        content: vec![Content::resource(completed)],
    };
    checkpoint.active.media.insert(
        "active".into(),
        StreamCursor {
            sequence: 1,
            epoch: 0,
            sealed: active,
        },
    );
    checkpoint.active.session.media_archive = archive;
    let keys = |resources: Vec<ResourceRef>| {
        let mut keys: Vec<_> = resources
            .into_iter()
            .map(|r| match r.source {
                ResourceSource::Stored { key } => key,
                _ => unreachable!(),
            })
            .collect();
        keys.sort();
        keys
    };
    let found = zhir_storage::resources::reachable(&checkpoint, &store)
        .await
        .unwrap();
    assert_eq!(
        keys(found),
        [
            "active-0",
            "active-0-data",
            "active-1",
            "active-1-data",
            "archive-0",
            "archive-1",
            "archived-0",
            "archived-0-data",
            "archived-1",
            "archived-1-data",
            "completed",
            "delegated",
            "output",
            "user",
        ]
    );
    // A missing media node cannot be walked past, so the listing fails instead of
    // omitting the resources behind it.
    let missing = ResourceRef {
        id: "active-0".into(),
        media_type: "application/vnd.zhir.sealed-media+json".into(),
        name: None,
        source: ResourceSource::Stored {
            key: "active-0".into(),
        },
        metadata: Default::default(),
    };
    store.delete(missing).await.unwrap();
    assert!(
        zhir_storage::resources::reachable(&checkpoint, &store)
            .await
            .is_err()
    );
}
/// Two stores on one file stand in for two processes: concurrent writes to different
/// runs all commit, and racing first commits of one run yield one head and a conflict.
#[tokio::test]
async fn sqlite_writers_sharing_a_file_serialize() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("shared.db").display()
    );
    let first = zhir_storage::sqlite::SqliteRunStore::connect(&url)
        .await
        .unwrap();
    let second = zhir_storage::sqlite::SqliteRunStore::connect(&url)
        .await
        .unwrap();
    let writers = [first.clone(), second.clone()].map(|store| {
        tokio::spawn(async move {
            for _ in 0..20 {
                let commit = initial();
                store.commit(commit.clone()).await.unwrap();
                store.commit(append(&commit, "next")).await.unwrap();
            }
        })
    });
    for writer in writers {
        writer.await.unwrap();
    }
    for _ in 0..10 {
        let commit = initial();
        let (a, b) = tokio::join!(
            first.commit(commit.clone()),
            second.commit(altered(&commit, |c| {
                c.id = uuid::Uuid::new_v4().to_string();
            }))
        );
        assert!(
            matches!(
                (&a, &b),
                (Ok(()), Err(zhir_core::error::Error::Conflict { .. }))
                    | (Err(zhir_core::error::Error::Conflict { .. }), Ok(()))
            ),
            "{a:?} {b:?}"
        );
    }
    first.close().await;
    second.close().await;
}
