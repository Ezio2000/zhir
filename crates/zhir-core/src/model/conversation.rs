use crate::{
    message::{Message, Output},
    operation::CallRef,
    run::HistoryEntry,
};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Default)]
struct Response {
    message_index: usize,
    provider_positions: BTreeMap<(String, String), usize>,
}

/// Project causal history into real responses or independent items, retaining each provider call's
/// original output position when a later item updates it. History remains in
/// arrival order; only the context projection groups causally related assistant output.
pub fn conversation<'a>(entries: impl IntoIterator<Item = &'a HistoryEntry>) -> Vec<Message> {
    let mut messages = Vec::new();
    let mut responses = BTreeMap::new();
    for entry in entries {
        match (&entry.origin, &entry.message) {
            (
                Some(origin),
                Message::Assistant {
                    output,
                    provider_data,
                },
            ) => {
                let response = response(&mut responses, &mut messages, origin);
                let Message::Assistant {
                    output: accumulated,
                    provider_data: data,
                } = &mut messages[response.message_index]
                else {
                    unreachable!("responses index assistant messages")
                };
                for item in output {
                    response.merge(accumulated, item.clone());
                }
                if !provider_data.is_null() {
                    *data = provider_data.clone();
                }
            }
            (_, message) => messages.push(message.clone()),
        }
    }
    messages
}

fn response<'a>(
    responses: &'a mut BTreeMap<(String, Option<String>, Option<String>), Response>,
    messages: &mut Vec<Message>,
    origin: &CallRef,
) -> &'a mut Response {
    let item = origin
        .generation_id
        .is_none()
        .then(|| origin.item_id.clone());
    responses
        .entry((
            origin.session_id.clone(),
            origin.generation_id.clone(),
            item,
        ))
        .or_insert_with(|| {
            let message_index = messages.len();
            messages.push(Message::Assistant {
                output: Vec::new(),
                provider_data: Value::Null,
            });
            Response {
                message_index,
                ..Response::default()
            }
        })
}

impl Response {
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
