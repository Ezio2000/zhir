use crate::{
    Result,
    error::Error,
    message::{Message, Output},
    operation::CallRef,
    tool::RuntimeToolCall,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryEntry {
    pub id: String,
    pub origin: Option<CallRef>,
    pub message: Message,
}
impl HistoryEntry {
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty() {
            return Err(Error::Invalid("empty history identity".into()));
        }
        self.message.validate()
    }
}
const CHUNK_SIZE: usize = 64;
#[derive(Debug)]
struct Node {
    previous: Option<Arc<Node>>,
    messages: Vec<HistoryEntry>,
    prefix_digests: Vec<[u8; 32]>,
    len: usize,
    digest: [u8; 32],
}
impl Drop for Node {
    fn drop(&mut self) {
        let mut tail = self.previous.take();
        while let Some(node) = tail {
            match Arc::try_unwrap(node) {
                Ok(mut owned) => tail = owned.previous.take(),
                Err(_) => break,
            }
        }
    }
}
/// Persistent append-only chunks. Cloning a history does not clone its messages.
#[derive(Debug, Clone, Default)]
pub struct History {
    head: Option<Arc<Node>>,
    len: usize,
    digest: [u8; 32],
    order: Order,
}
#[derive(Debug, Clone, Default)]
struct Order {
    ids: im::OrdMap<String, usize>,
    calls: im::OrdMap<CallRef, RuntimeToolCall>,
    completed: im::OrdSet<CallRef>,
    error: Option<&'static str>,
}
impl Order {
    fn append(&mut self, entry: &HistoryEntry, index: usize) {
        if self.error.is_none() {
            self.error = self.update(entry, index).err();
        }
    }
    fn update(
        &mut self,
        entry: &HistoryEntry,
        index: usize,
    ) -> std::result::Result<(), &'static str> {
        if self.ids.contains_key(&entry.id) {
            return Err("duplicate history identity");
        }
        self.ids.insert(entry.id.clone(), index);
        match &entry.message {
            Message::Assistant { output, .. } => {
                for item in output {
                    if let Output::RuntimeToolCall { call } = item {
                        let mut origin = entry
                            .origin
                            .clone()
                            .ok_or("tool request requires an origin")?;
                        origin.call_id = call.id.clone();
                        if self.calls.contains_key(&origin) || self.completed.contains(&origin) {
                            return Err("duplicate tool call identity");
                        }
                        self.calls.insert(origin, call.clone());
                    }
                }
            }
            Message::RuntimeTool { call_id, name, .. } => {
                let origin = entry
                    .origin
                    .as_ref()
                    .ok_or("tool result requires an origin")?;
                let call = self
                    .calls
                    .get(origin)
                    .ok_or("tool result has no pending call")?;
                if &call.id != call_id || &call.name != name {
                    return Err("tool result identity mismatch");
                }
                self.calls.remove(origin);
                self.completed.insert(origin.clone());
            }
            _ => (),
        }
        Ok(())
    }
}
impl History {
    pub fn new(messages: Vec<Message>) -> Result<Self> {
        Self::default().append(
            messages
                .into_iter()
                .enumerate()
                .map(|(index, message)| HistoryEntry {
                    id: format!("input:{index}"),
                    origin: None,
                    message,
                })
                .collect(),
        )
    }
    pub fn from_entries(entries: Vec<HistoryEntry>) -> Result<Self> {
        Self::default().append(entries)
    }
    pub fn validate(&self) -> Result<()> {
        if self.is_empty() {
            return Err(Error::Invalid("empty history".into()));
        }
        if let Some(error) = self.order.error {
            return Err(Error::Invalid(error.into()));
        }
        Ok(())
    }
    pub fn pending_calls(&self) -> impl Iterator<Item = (&CallRef, &RuntimeToolCall)> {
        self.order.calls.iter()
    }
    pub fn by_id(&self, id: &str) -> Option<&HistoryEntry> {
        self.order.ids.get(id).and_then(|index| self.get(*index))
    }
    pub fn get(&self, index: usize) -> Option<&HistoryEntry> {
        let mut node = self.head.as_ref();
        while let Some(n) = node {
            let start = n.len - n.messages.len();
            if index >= start {
                return n.messages.get(index - start);
            }
            node = n.previous.as_ref();
        }
        None
    }
    pub fn messages(&self) -> Vec<Message> {
        self.entries()
            .into_iter()
            .map(|entry| entry.message)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub fn append(&self, messages: Vec<HistoryEntry>) -> Result<Self> {
        let mut result = self.clone();
        for message in messages {
            message.validate()?;
            result.order.append(&message, result.len);
            result.digest = append_digest(result.digest, &message)?;
            let (previous, mut chunk, mut prefix_digests) = match &result.head {
                Some(head) if head.messages.len() < CHUNK_SIZE => (
                    head.previous.clone(),
                    head.messages.clone(),
                    head.prefix_digests.clone(),
                ),
                _ => (result.head.clone(), Vec::new(), Vec::new()),
            };
            chunk.push(message);
            prefix_digests.push(result.digest);
            result.head = Some(Arc::new(Node {
                previous,
                messages: chunk,
                prefix_digests,
                len: result.len + 1,
                digest: result.digest,
            }));
            result.len += 1;
        }
        Ok(result)
    }
    pub fn entries(&self) -> Vec<HistoryEntry> {
        let mut chunks = Vec::new();
        let mut head = self.head.as_ref();
        while let Some(node) = head {
            chunks.push(&node.messages);
            head = node.previous.as_ref();
        }
        let mut result = Vec::with_capacity(self.len);
        for chunk in chunks.into_iter().rev() {
            result.extend(chunk.iter().cloned());
        }
        result
    }
    pub fn last(&self) -> Option<&HistoryEntry> {
        self.head.as_ref().and_then(|n| n.messages.last())
    }
    /// Return just the added messages, verifying the immutable prefix by its
    /// cached incremental digests. Work is proportional to the suffix.
    pub fn appended_since(&self, previous: &Self) -> Result<Vec<HistoryEntry>> {
        let mismatch = || Error::Invalid("history prefix changed without rewrite".into());
        if previous.len > self.len {
            return Err(mismatch());
        }
        if previous.len == self.len {
            return if previous.digest == self.digest {
                Ok(Vec::new())
            } else {
                Err(mismatch())
            };
        }
        let mut chunks = Vec::new();
        let mut head = self.head.as_ref();
        while let Some(node) = head {
            let start = node.len - node.messages.len();
            if previous.len >= start {
                let offset = previous.len - start;
                let digest = match offset.checked_sub(1) {
                    Some(index) => node.prefix_digests[index],
                    None => node.previous.as_ref().map_or([0; 32], |p| p.digest),
                };
                if digest != previous.digest {
                    return Err(mismatch());
                }
                chunks.push(&node.messages[offset..]);
                break;
            }
            chunks.push(node.messages.as_slice());
            head = node.previous.as_ref();
        }
        Ok(chunks.into_iter().rev().flatten().cloned().collect())
    }
}
pub fn append_digest(previous: [u8; 32], message: &HistoryEntry) -> Result<[u8; 32]> {
    let encoded =
        serde_json::to_vec(message).map_err(|e| crate::error::Error::Invalid(e.to_string()))?;
    let mut digest = Sha256::new();
    digest.update(previous);
    digest.update((encoded.len() as u64).to_be_bytes());
    digest.update(encoded);
    Ok(digest.finalize().into())
}
