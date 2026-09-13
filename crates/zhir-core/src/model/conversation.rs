use crate::{
    message::{Message, Output},
    operation::CallRef,
    run::HistoryEntry,
};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Default)]
struct Turn {
    message_index: usize,
    provider_positions: BTreeMap<(String, String), usize>,
}

/// Project causal history into logical turns, retaining each provider call's
/// original output position when a later item updates it. History remains in
/// arrival order; only the request projection groups assistant output by turn.
pub fn conversation(entries: impl IntoIterator<Item = HistoryEntry>) -> Vec<Message> {
    let mut messages = Vec::new();
    let mut turns = BTreeMap::new();
    for entry in entries {
        match (entry.origin, entry.message) {
            (
                Some(origin),
                Message::Assistant {
                    output,
                    provider_data,
                },
            ) => {
                let turn = turn(&mut turns, &mut messages, origin);
                let Message::Assistant {
                    output: accumulated,
                    provider_data: data,
                } = &mut messages[turn.message_index]
                else {
                    unreachable!("turns index assistant messages")
                };
                for item in output {
                    turn.merge(accumulated, item);
                }
                if !provider_data.is_null() {
                    *data = provider_data;
                }
            }
            (_, message) => messages.push(message),
        }
    }
    messages
}

fn turn<'a>(
    turns: &'a mut BTreeMap<(String, String), Turn>,
    messages: &mut Vec<Message>,
    origin: CallRef,
) -> &'a mut Turn {
    turns
        .entry((origin.session_id, origin.turn_id))
        .or_insert_with(|| {
            let message_index = messages.len();
            messages.push(Message::Assistant {
                output: Vec::new(),
                provider_data: Value::Null,
            });
            Turn {
                message_index,
                ..Turn::default()
            }
        })
}

impl Turn {
    fn merge(&mut self, output: &mut Vec<Output>, item: Output) {
        if let Output::ProviderToolCall { call } = &item {
            let position = self
                .provider_positions
                .entry((call.provider.clone(), call.id.clone()))
                .or_insert(output.len());
            if *position < output.len() {
                output[*position] = item;
                return;
            }
        }
        output.push(item);
    }
}
