use super::PendingCalls;
use crate::{
    Result,
    error::Error,
    message::{Message, Output},
    tool::RuntimeToolCall,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const CHUNK_SIZE: usize = 64;
#[derive(Debug)]
struct Node {
    previous: Option<Arc<Node>>,
    messages: Vec<Message>,
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
#[derive(Debug, Clone)]
struct Pending {
    range: PendingCalls,
    calls: Arc<[RuntimeToolCall]>,
}
#[derive(Debug, Clone, Default)]
struct Order {
    pending: Option<Pending>,
    error: Option<&'static str>,
}
impl Order {
    fn append(&mut self, message: &Message, index: usize) {
        if self.error.is_some() {
            return;
        }
        self.error = self.update(message, index).err();
    }
    fn update(&mut self, message: &Message, index: usize) -> std::result::Result<(), &'static str> {
        match message {
            Message::Assistant { output, .. } => {
                if self.pending.is_some() {
                    return Err("assistant interrupts pending tools");
                }
                let calls: Arc<[RuntimeToolCall]> = output
                    .iter()
                    .filter_map(|o| match o {
                        Output::RuntimeToolCall { call } => Some(call.clone()),
                        _ => None,
                    })
                    .collect();
                if !calls.is_empty() {
                    self.pending = Some(Pending {
                        range: PendingCalls {
                            message_index: index,
                            next: 0,
                            end: calls.len(),
                        },
                        calls,
                    });
                }
            }
            Message::RuntimeTool { call_id, name, .. } => {
                let pending = self
                    .pending
                    .as_mut()
                    .ok_or("tool message does not match pending order")?;
                let call = &pending.calls[pending.range.next];
                if &call.id != call_id || &call.name != name {
                    return Err("tool message does not match pending order");
                }
                pending.range.next += 1;
                if pending.range.is_empty() {
                    self.pending = None;
                }
            }
            _ if self.pending.is_some() => return Err("message interrupts pending tools"),
            _ => {}
        }
        Ok(())
    }
}
impl History {
    pub fn new(messages: Vec<Message>) -> Result<Self> {
        Self::default().append(messages)
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
    pub fn append(&self, messages: Vec<Message>) -> Result<Self> {
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
    pub fn messages(&self) -> Vec<Message> {
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
    pub fn last(&self) -> Option<&Message> {
        self.head.as_ref().and_then(|n| n.messages.last())
    }
    /// Validated unresolved work, maintained incrementally during append.
    pub fn pending(&self) -> Result<Option<PendingCalls>> {
        if self.is_empty() {
            return Err(Error::Invalid("empty history".into()));
        }
        if let Some(error) = self.order.error {
            return Err(Error::Invalid(error.into()));
        }
        Ok(self.order.pending.as_ref().map(|p| p.range))
    }
    /// Borrow the remaining payloads only when the cursor matches this history.
    pub fn resolve_pending(&self, calls: PendingCalls) -> Result<&[RuntimeToolCall]> {
        if self.pending()? != Some(calls) {
            return Err(Error::Invalid("pending state differs from history".into()));
        }
        let pending = self.order.pending.as_ref().expect("matched pending range");
        Ok(&pending.calls[calls.next..calls.end])
    }
    /// Return just the added messages, verifying the immutable prefix by its
    /// cached incremental digests. Work is proportional to the suffix.
    pub fn appended_since(&self, previous: &Self) -> Result<Vec<Message>> {
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
pub fn append_digest(previous: [u8; 32], message: &Message) -> Result<[u8; 32]> {
    let encoded =
        serde_json::to_vec(message).map_err(|e| crate::error::Error::Invalid(e.to_string()))?;
    let mut digest = Sha256::new();
    digest.update(previous);
    digest.update((encoded.len() as u64).to_be_bytes());
    digest.update(encoded);
    Ok(digest.finalize().into())
}
