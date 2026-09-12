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
