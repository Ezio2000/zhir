mod commit;
use crate::{BoxFuture, Result, run::Checkpoint, run::HistoryEntry};
pub use commit::Commit;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "messages",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum HistoryDelta {
    Initial(Vec<HistoryEntry>),
    Append(Vec<HistoryEntry>),
    Replace(Vec<HistoryEntry>),
    Unchanged,
}
/// Implementations settle atomic writes before returning. A dropped caller does not
/// cancel an ambiguous database commit; close waits for owned write operations.
pub trait RunStore: Send + Sync {
    fn commit(&self, commit: Commit) -> BoxFuture<'_, Result<()>>;
    fn load_head(&self, run_id: &str) -> BoxFuture<'_, Result<Option<Arc<Checkpoint>>>>;
    /// Removes every checkpoint, commit identity and history record of a run. Deleting an
    /// unknown run succeeds. Resources a run references are deleted separately.
    fn delete(&self, run_id: &str) -> BoxFuture<'_, Result<()>>;
}
