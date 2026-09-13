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

fn tool_turn(count: usize) -> (History, Vec<Message>) {
    use zhir_core::{
        message::Output,
        tool::{RuntimeToolCall, RuntimeToolInput, RuntimeToolOutcome},
    };
    let calls: Vec<_> = (0..count)
        .map(|index| RuntimeToolCall {
            id: format!("call-{index}"),
            name: "echo".into(),
            input: RuntimeToolInput::Structured(serde_json::json!({"index":index})),
        })
        .collect();
    let history = History::new(vec![
        Message::user("run"),
        Message::Assistant {
            output: calls
                .iter()
                .cloned()
                .map(|call| Output::RuntimeToolCall { call })
                .collect(),
            provider_data: serde_json::Value::Null,
        },
    ])
    .unwrap();
    let results = calls
        .into_iter()
        .map(|call| Message::RuntimeTool {
            call_id: call.id,
            name: call.name,
            outcome: RuntimeToolOutcome::Success {
                content: vec![],
                structured: serde_json::Value::Null,
            },
        })
        .collect();
    (history, results)
}

#[test]
fn pending_cursor_tracks_order_across_chunks_and_wire_recovery() {
    let (original, results) = tool_turn(130);
    let first = original.pending().unwrap().unwrap();
    let history = original.append(results[..70].to_vec()).unwrap();
    let cursor = first.advance(70).unwrap().unwrap();
    assert_eq!(history.pending().unwrap(), Some(cursor));
    assert_eq!(history.resolve_pending(cursor).unwrap()[0].id, "call-70");
    assert_eq!(original.resolve_pending(first).unwrap().len(), 130);
    assert!(history.resolve_pending(first).is_err());
    let checkpoint = Checkpoint {
        options: options(),
        id: "cursor".into(),
        parent_id: Some("previous".into()),
        revision: 1,
        context: RunContext::new("cursor-run", 0),
        history: history.clone(),
        metrics: Metrics::default(),
        state: State::RuntimeToolsPending {
            calls: cursor,
            provider_turn_pending: false,
        },
        fact: Fact::Resumed,
    };
    let bytes = wire::encode_checkpoint(&checkpoint).unwrap();
    let recovered = wire::decode_checkpoint(&bytes).unwrap();
    assert_eq!(
        recovered.history.resolve_pending(cursor).unwrap()[0].id,
        "call-70"
    );
    let mut encoded: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        encoded["checkpoint"]["state"]["calls"],
        serde_json::json!({"message_index":1,"next":70,"end":130})
    );
    encoded["checkpoint"]["state"]["calls"]["next"] = serde_json::json!(69);
    assert!(wire::decode_checkpoint(&serde_json::to_vec(&encoded).unwrap()).is_err());
    let complete = history.append(results[70..].to_vec()).unwrap();
    assert_eq!(complete.pending().unwrap(), None);
    zhir_core::run::validate_history(
        &complete,
        Some(&zhir_core::run::ActiveState::Planning {
            provider_turn_pending: false,
        }),
    )
    .unwrap();
    assert!(
        original
            .append(vec![results[1].clone()])
            .unwrap()
            .pending()
            .is_err()
    );
    assert!(
        original
            .append(vec![Message::external("interrupt")])
            .unwrap()
            .pending()
            .is_err()
    );
}

#[test]
fn history_suffix_checks_reconstructed_prefixes_at_chunk_boundaries() {
    let messages: Vec<_> = (0..200)
        .map(|i| Message::user(format!("message-{i}")))
        .collect();
    let complete = History::new(messages.clone()).unwrap();
    for length in [0, 1, 63, 64, 65, 127, 128, 199, 200] {
        let prefix = History::new(messages[..length].to_vec()).unwrap();
        assert_eq!(
            complete.appended_since(&prefix).unwrap(),
            messages[length..]
        );
        if length > 0 {
            let mut altered = messages[..length].to_vec();
            altered[0] = Message::user("changed prefix");
            assert!(
                complete
                    .appended_since(&History::new(altered).unwrap())
                    .is_err()
            );
        }
    }
    assert!(
        History::new(messages[..100].to_vec())
            .unwrap()
            .appended_since(&complete)
            .is_err()
    );
}

#[test]
fn pending_state_size_is_independent_of_tool_payloads_and_unknown_kinds_are_rejected() {
    let sizes: Vec<_> = [1, 2000]
        .into_iter()
        .map(|n| {
            let (history, _) = tool_turn(n);
            let state = State::RuntimeToolsPending {
                calls: history.pending().unwrap().unwrap(),
                provider_turn_pending: false,
            };
            let encoded = serde_json::to_vec(&state).unwrap();
            assert!(encoded.len() < 160);
            encoded.len()
        })
        .collect();
    assert!(sizes[1] - sizes[0] < 8);
    assert!(serde_json::from_str::<Fact>(r#"{"kind":"control","action":"misspelled"}"#).is_err());
    assert!(
        serde_json::from_str::<zhir_core::tool::RuntimeToolOutcomeKind>(r#""sucess""#).is_err()
    );
}

#[test]
fn commit_consistency_is_pure_and_deadline_checks_use_explicit_time() {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };
    use zhir_core::{
        error::Error,
        storage::{Commit, HistoryDelta},
    };
    let messages = vec![Message::user("initial")];
    let checkpoint = Arc::new(Checkpoint {
        options: options(),
        id: "initial".into(),
        parent_id: None,
        revision: 0,
        context: RunContext::new("run", 0),
        history: History::new(messages.clone()).unwrap(),
        state: State::Planning {
            provider_turn_pending: false,
        },
        metrics: Metrics::default(),
        fact: Fact::Started,
    });
    let at = Instant::now();
    let mut commit = Commit::new(checkpoint, HistoryDelta::Initial(messages));
    commit.deadline = Some(at);
    commit.validate_against(None).unwrap();
    assert!(commit.check_deadline(at - Duration::from_millis(1)).is_ok());
    assert!(matches!(commit.check_deadline(at), Err(Error::Deadline)));
    assert!(matches!(
        commit.check_deadline(at + Duration::from_millis(1)),
        Err(Error::Deadline)
    ));
    commit.history = HistoryDelta::Initial(vec![Message::user("wrong")]);
    assert!(matches!(
        commit.validate_against(None),
        Err(Error::Storage(_))
    ));
}
