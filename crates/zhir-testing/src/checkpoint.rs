//! Native checkpoint seeds for value, storage and recovery tests.
use std::collections::BTreeMap;
use zhir_core::{
    message::{Message, Output},
    operation::*,
    run::*,
};
pub fn checkpoint(messages: Vec<Message>) -> Checkpoint {
    checkpoint_with_history(History::new(messages).expect("fixture history"))
}
pub fn checkpoint_with_history(history: History) -> Checkpoint {
    let mut operations = BTreeMap::new();
    for (origin, call) in history.pending_calls() {
        let call_entry = history.entries().iter().position(|entry| entry.origin.as_ref().is_some_and(|candidate| candidate.session_id == origin.session_id && candidate.turn_id == origin.turn_id && candidate.caller_id == origin.caller_id) && matches!(&entry.message, Message::Assistant { output, .. } if output.iter().any(|item| matches!(item, Output::RuntimeToolCall { call: actual } if actual == call)))).expect("fixture call");
        let id = format!("fixture-operation-{call_entry}-{}", call.id);
        operations.insert(
            id.clone(),
            OperationRecord {
                id,
                origin: origin.clone(),
                owner: OperationOwner::RuntimeTool {
                    name: call.name.clone(),
                },
                state: OperationState::Queued,
                call_entry,
                result_entry: None,
                recovery: None,
                last_sequence: None,
                last_update: None,
                wait: None,
            },
        );
    }
    let mut options = zhir_kernel::defaults::run_options();
    options.limits.max_inflight_operations = operations.len().max(64);
    Checkpoint {
        id: "fixture-checkpoint".into(),
        parent_id: None,
        revision: 0,
        context: RunContext::new("fixture-run", 0),
        history,
        state: State::Running,
        active: ActiveState {
            session: SessionSnapshot {
                capabilities: None,
                id: "fixture-session".into(),
                turn_id: None,
                turn_start: 0,
                last_sequence: None,
                disposition: None,
                recovery: None,
                epoch: 0,
                input_closed: true,
                closing: false,
                closed: false,
                profile_revision: 0,
                profile: Default::default(),
                negotiated: Default::default(),
                effective: Default::default(),
            },
            operations,
            commands: vec![],
            media: BTreeMap::new(),
        },
        options,
        metrics: Default::default(),
        fact: Fact::Started,
    }
}
