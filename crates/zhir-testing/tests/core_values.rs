use serde_json::json;
use zhir_core::operation::OperationOutcome;
use zhir_core::{
    message::{Message, Output},
    operation::CallRef,
    run::{Fact, History, HistoryEntry},
    tool::{RuntimeToolCall, RuntimeToolInput},
    wire,
};
fn entry(id: impl Into<String>, message: Message) -> HistoryEntry {
    HistoryEntry {
        id: id.into(),
        origin: None,
        message,
    }
}
fn origin(index: usize) -> CallRef {
    CallRef {
        session_id: "session".into(),
        item_id: format!("call-{index}"),
        generation_id: Some("turn".into()),
        caller_id: "caller".into(),
        call_id: format!("call-{index}"),
    }
}
fn tool_turn(count: usize, payload_size: usize) -> (History, Vec<HistoryEntry>) {
    let mut entries = vec![entry("user", Message::user("run"))];
    let mut results = vec![];
    for index in 0..count {
        let origin = origin(index);
        entries.push(HistoryEntry {
            id: format!("request-{index}"),
            origin: Some(origin.clone()),
            message: Message::Assistant {
                output: vec![Output::RuntimeToolCall {
                    call: RuntimeToolCall {
                        id: origin.call_id.clone(),
                        name: "echo".into(),
                        input: RuntimeToolInput::Structured(
                            json!({"data":"x".repeat(payload_size)}),
                        ),
                    },
                }],
                provider_data: Default::default(),
            },
        });
        results.push(HistoryEntry {
            id: format!("result-{index}"),
            origin: Some(origin.clone()),
            message: Message::RuntimeTool {
                call_id: origin.call_id,
                name: "echo".into(),
                outcome: OperationOutcome::Success {
                    content: vec![],
                    structured: Default::default(),
                },
            },
        });
    }
    (History::from_entries(entries).unwrap(), results)
}
#[test]
fn history_snapshots_remain_immutable_and_large_drops_do_not_recurse() {
    let mut history = History::new(vec![Message::user("begin")]).unwrap();
    let snapshot = history.clone();
    let digest = snapshot.digest();
    for index in 0..10000 {
        history = history
            .append(vec![entry(
                format!("added-{index}"),
                Message::external("next"),
            )])
            .unwrap();
    }
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot.digest(), digest);
    assert_eq!(history.len(), 10001);
    assert_eq!(&history.last().unwrap().message, &Message::external("next"));
}
#[test]
fn wire_rejects_version_drift_unknown_fields_and_corrupt_history() {
    let checkpoint = zhir_testing::checkpoint(vec![Message::user("hello")]);
    let bytes = wire::encode_checkpoint(&checkpoint).unwrap();
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    for version in [0, 1, 2, 3, 4, 6] {
        let mut changed = original.clone();
        changed["version"] = json!(version);
        assert!(wire::decode_checkpoint(&serde_json::to_vec(&changed).unwrap()).is_err());
    }
    let mut changed = original.clone();
    changed["extra"] = json!(true);
    assert!(wire::decode_checkpoint(&serde_json::to_vec(&changed).unwrap()).is_err());
    let mut changed = original;
    changed["history"][0]["message"]["content"][0]["text"] = json!("changed");
    assert!(wire::decode_checkpoint(&serde_json::to_vec(&changed).unwrap()).is_err());
}
#[test]
fn limits_require_explicit_budgets_and_preserve_caller_values() {
    let mut limits = zhir_kernel::defaults::limits();
    limits.max_generation_requests = 3;
    limits.max_operation_concurrency = 2;
    limits.commit_timeout_ms = 17;
    let encoded = serde_json::to_value(&limits).unwrap();
    assert_eq!(
        serde_json::from_value::<zhir_core::run::Limits>(encoded.clone()).unwrap(),
        limits
    );
    for field in [
        "max_generation_requests",
        "max_runtime_tool_calls",
        "max_inflight_operations",
        "max_operation_concurrency",
        "max_control_commands",
        "max_session_events",
        "max_media_chunk_bytes",
        "max_buffered_media_bytes",
        "commit_timeout_ms",
    ] {
        let mut incomplete = encoded.clone();
        incomplete.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<zhir_core::run::Limits>(incomplete).is_err(),
            "{field}"
        );
    }
    limits.max_buffered_media_bytes = 1;
    assert!(limits.validate().is_err());
}
#[test]
fn causal_history_accepts_interleaved_input_and_out_of_order_results() {
    let (original, results) = tool_turn(130, 0);
    let history = original
        .append(vec![entry("interrupt", Message::external("steer"))])
        .unwrap()
        .append(results.iter().rev().take(70).cloned().collect())
        .unwrap();
    history.validate().unwrap();
    assert_eq!(history.pending_calls().count(), 60);
    assert_eq!(original.pending_calls().count(), 130);
    let checkpoint = zhir_testing::checkpoint_with_history(history.clone());
    checkpoint.validate().unwrap();
    let recovered =
        wire::decode_checkpoint(&wire::encode_checkpoint(&checkpoint).unwrap()).unwrap();
    assert_eq!(recovered.history.pending_calls().count(), 60);
    let complete = history.append(results[..60].to_vec()).unwrap();
    complete.validate().unwrap();
    assert_eq!(complete.pending_calls().count(), 0);
    assert!(
        complete
            .append(vec![results[0].clone()])
            .unwrap()
            .validate()
            .is_err()
    );
    let mut corrupt = checkpoint;
    corrupt
        .active
        .operations
        .values_mut()
        .next()
        .unwrap()
        .call_entry = 0;
    assert!(corrupt.validate().is_err());
}
#[test]
fn history_suffix_checks_reconstructed_prefixes_at_chunk_boundaries() {
    let entries: Vec<_> = (0..200)
        .map(|i| entry(format!("entry-{i}"), Message::user(format!("message-{i}"))))
        .collect();
    let complete = History::from_entries(entries.clone()).unwrap();
    for length in [0, 1, 63, 64, 65, 127, 128, 199, 200] {
        let prefix = History::from_entries(entries[..length].to_vec()).unwrap();
        assert_eq!(complete.appended_since(&prefix).unwrap(), entries[length..]);
        if length > 0 {
            let mut altered = entries[..length].to_vec();
            altered[0].message = Message::user("changed");
            assert!(
                complete
                    .appended_since(&History::from_entries(altered).unwrap())
                    .is_err()
            );
        }
    }
    assert!(
        History::from_entries(entries[..100].to_vec())
            .unwrap()
            .appended_since(&complete)
            .is_err()
    );
}
#[test]
fn active_records_reference_payloads_and_reject_unknown_kinds() {
    let sizes: Vec<_> = [1, 100000]
        .into_iter()
        .map(|size| {
            let (history, _) = tool_turn(2, size);
            let checkpoint = zhir_testing::checkpoint_with_history(history);
            serde_json::to_vec(&checkpoint.active).unwrap().len()
        })
        .collect();
    assert_eq!(sizes[0], sizes[1]);
    assert!(serde_json::from_str::<Fact>(r#"{"kind":"control","action":"misspelled"}"#).is_err());
    assert!(
        serde_json::from_str::<zhir_core::operation::OperationOutcomeKind>(r#""sucess""#).is_err()
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
    let checkpoint = zhir_testing::checkpoint(vec![Message::user("initial")]);
    let entries = checkpoint.history.entries();
    let at = Instant::now();
    let mut commit = Commit::new(Arc::new(checkpoint), HistoryDelta::Initial(entries));
    commit.deadline = Some(at);
    commit.validate_against(None).unwrap();
    assert!(commit.check_deadline(at - Duration::from_millis(1)).is_ok());
    assert!(matches!(commit.check_deadline(at), Err(Error::Deadline)));
    assert!(matches!(
        commit.check_deadline(at + Duration::from_millis(1)),
        Err(Error::Deadline)
    ));
    commit.history = HistoryDelta::Initial(vec![entry("wrong", Message::user("wrong"))]);
    assert!(matches!(
        commit.validate_against(None),
        Err(Error::Storage(_))
    ));
}
