use serde::{Deserialize, Serialize};
use zhir_core::{
    BoxFuture, Result,
    message::Content,
    run::{RunContext, StateKind},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Running,
    Completed,
    Failed,
    Limited,
    Cancelled,
    Suspended,
}
impl From<StateKind> for AgentStatus {
    fn from(state: StateKind) -> Self {
        match state {
            StateKind::Planning | StateKind::RuntimeToolsPending => Self::Running,
            StateKind::Completed => Self::Completed,
            StateKind::Failed => Self::Failed,
            StateKind::Limited => Self::Limited,
            StateKind::Suspended => Self::Suspended,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSnapshot {
    pub id: String,
    pub status: AgentStatus,
    pub content: Vec<Content>,
    pub error: Option<String>,
}
impl AgentSnapshot {
    pub fn terminal(&self) -> bool {
        self.status != AgentStatus::Running
    }
}
pub trait AgentBackend: Send + Sync {
    fn start_or_get(
        &self,
        key: String,
        prompt: String,
        requester: RunContext,
    ) -> BoxFuture<'_, Result<AgentSnapshot>>;
    fn get(&self, id: String, requester: RunContext) -> BoxFuture<'_, Result<AgentSnapshot>>;
    fn wait(&self, id: String, requester: RunContext) -> BoxFuture<'_, Result<AgentSnapshot>>;
    fn cancel(&self, id: String, requester: RunContext) -> BoxFuture<'_, Result<AgentSnapshot>>;
}
