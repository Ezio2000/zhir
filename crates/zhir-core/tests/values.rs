use zhir_core::{
    message::Message,
    run::{Checkpoint, Fact, History, Metrics, RunContext, State},
    wire,
};
#[test]
fn history_snapshots_remain_immutable_and_large_drops_do_not_recurse() {
    let mut history = History::new(vec![Message::user("begin")]).unwrap();
    let snapshot = history.clone();
    let digest = snapshot.digest();
    for _ in 0..10000 {
        history = history.append(vec![Message::external("next")]).unwrap();
    }
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot.digest(), digest);
    assert_eq!(history.len(), 10001);
    assert_eq!(history.last(), Some(&Message::external("next")));
}
#[test]
fn wire_rejects_version_drift_unknown_fields_and_corrupt_history() {
    let c = Checkpoint {
        options: options(),
        id: "checkpoint-test".into(),
        parent_id: None,
        revision: 0,
        context: RunContext::new("run-test", 0),
        history: History::new(vec![Message::user("hello")]).unwrap(),
        state: State::Planning {
            provider_turn_pending: false,
        },
        metrics: Metrics::default(),
        fact: Fact::Started,
    };
    let bytes = wire::encode_checkpoint(&c).unwrap();
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let mut changed = original.clone();
    changed["version"] = serde_json::json!(2);
    assert!(wire::decode_checkpoint(&serde_json::to_vec(&changed).unwrap()).is_err());
    let mut changed = original.clone();
    changed["extra"] = serde_json::json!(true);
    assert!(wire::decode_checkpoint(&serde_json::to_vec(&changed).unwrap()).is_err());
    let mut changed = original;
    changed["history"][0]["content"][0]["text"] = serde_json::json!("changed");
    assert!(wire::decode_checkpoint(&serde_json::to_vec(&changed).unwrap()).is_err());
}

#[test]
fn limits_require_explicit_budgets_and_preserve_caller_values() {
    let mut limits = options().limits;
    limits.max_planning_steps = 3;
    limits.max_runtime_tool_concurrency = 2;
    limits.commit_timeout_ms = 17;
    let encoded = serde_json::to_value(&limits).unwrap();
    assert_eq!(
        serde_json::from_value::<zhir_core::run::Limits>(encoded.clone()).unwrap(),
        limits
    );
    for field in [
        "max_planning_steps",
        "max_runtime_tool_calls",
        "max_runtime_tool_batch_size",
        "max_runtime_tool_concurrency",
        "max_progress_events",
        "max_buffered_progress",
        "commit_timeout_ms",
    ] {
        let mut incomplete = encoded.clone();
        incomplete.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<zhir_core::run::Limits>(incomplete).is_err(),
            "missing {field}"
        );
    }
    assert!(serde_json::from_str::<zhir_core::run::Limits>("{}").is_err());
}

fn options() -> zhir_core::run::RunOptions {
    zhir_core::run::RunOptions {
        runtime_tools: Default::default(),
        limits: zhir_core::run::Limits {
            max_planning_steps: 100,
            max_runtime_tool_calls: 1000,
            max_runtime_tool_batch_size: 32,
            max_runtime_tool_concurrency: 8,
            max_progress_events: 256,
            max_buffered_progress: 256,
            max_total_tokens: None,
            elapsed_ms: None,
            commit_timeout_ms: 5000,
        },
        model: Default::default(),
        provider_tools: vec![],
        tool_choice: Default::default(),
        response_format: None,
        stream: false,
    }
}
