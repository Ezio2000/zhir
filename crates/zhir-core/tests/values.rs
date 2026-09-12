use zhir_core::{
    message::Message,
    run::{Checkpoint, Fact, History, Metrics, RunContext, State, new_id},
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
        options: Default::default(),
        id: new_id(),
        parent_id: None,
        revision: 0,
        context: RunContext::default(),
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
