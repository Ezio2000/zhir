use super::*;
struct Replay;
impl ProtocolExtension for Replay {
    fn encode_provider_history(
        &mut self,
        _: Protocol,
        call: &ProviderToolCall,
    ) -> Result<Option<Vec<Value>>> {
        Ok(Some(vec![json!({"id":call.id})]))
    }
}
#[test]
fn replay_indexes_calls_without_changing_native_order_or_ownership_checks() {
    let mut calls: Vec<_> = (0..512)
        .map(|i| Output::ProviderToolCall {
            call: ProviderToolCall {
                id: format!("c{i}"),
                provider: "test".into(),
                name: "remote".into(),
                status: ProviderToolStatus::Completed,
                output: vec![],
                data: Value::Null,
            },
        })
        .collect();
    calls.reverse();
    let ids: Vec<_> = (0..512).map(|i| format!("c{i}")).collect();
    let mut extension: Option<Box<dyn ProtocolExtension>> = Some(Box::new(Replay));
    let items = vec![
        json!({"type":"reasoning","text":"prefix"}),
        json!({POSITION:ids}),
    ];
    let replay = replay_items(Protocol::Responses, &items, &calls, &mut extension).unwrap();
    assert_eq!(replay[0], items[0]);
    for i in 0..512 {
        assert_eq!(replay[i + 1]["id"], format!("c{i}"));
    }
    for items in [
        vec![json!({POSITION:["absent"]})],
        vec![json!({POSITION:["c0","c0"]})],
        vec![json!({POSITION:["c0"]})],
        vec![json!({POSITION:ids,"other":true})],
    ] {
        assert!(replay_items(Protocol::Responses, &items, &calls, &mut extension).is_err());
    }
}
