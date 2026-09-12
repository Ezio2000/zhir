use crate::{Result, message::Message};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const CHUNK_SIZE: usize = 64;
#[derive(Debug)]
struct Node {
    previous: Option<Arc<Node>>,
    messages: Vec<Message>,
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
            result.digest = append_digest(result.digest, &message)?;
            let (previous, mut chunk) = match &result.head {
                Some(head) if head.messages.len() < CHUNK_SIZE => {
                    (head.previous.clone(), head.messages.clone())
                }
                _ => (result.head.clone(), Vec::new()),
            };
            chunk.push(message);
            result.head = Some(Arc::new(Node {
                previous,
                messages: chunk,
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
