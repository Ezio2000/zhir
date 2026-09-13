//! Complete user-turn windows with caller-owned cross-turn dependencies.
use std::sync::Arc;
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    message::Message,
    run::{Checkpoint, HistoryReducer, HistoryRewrite, validate_history},
};

type Dependencies = dyn Fn(&Checkpoint, usize) -> Result<usize> + Send + Sync;
pub struct HistoryWindow {
    turns: usize,
    dependencies: Option<Arc<Dependencies>>,
}
impl HistoryWindow {
    /// Keep the last N user turns, including the current turn, and all system messages.
    pub fn last_turns(turns: usize) -> Result<Self> {
        if turns == 0 {
            return Err(Error::Invalid(
                "history window must retain at least one turn".into(),
            ));
        }
        Ok(Self {
            turns,
            dependencies: None,
        })
    }
    /// Return the earliest message needed by protocol-specific dependencies.
    /// The window expands to a complete user turn; a callback cannot remove more history.
    pub fn with_dependencies(
        mut self,
        dependencies: impl Fn(&Checkpoint, usize) -> Result<usize> + Send + Sync + 'static,
    ) -> Self {
        self.dependencies = Some(Arc::new(dependencies));
        self
    }
}
impl HistoryReducer for HistoryWindow {
    fn reduce(&self, checkpoint: Arc<Checkpoint>) -> BoxFuture<'_, Result<Option<HistoryRewrite>>> {
        Box::pin(async move {
            checkpoint.validate()?;
            if !checkpoint.state.active()
                || checkpoint.active.session.disposition.is_none()
                || checkpoint
                    .active
                    .operations
                    .values()
                    .any(|op| !op.state.terminal())
                || !checkpoint.active.commands.is_empty()
            {
                return Ok(None);
            }
            let messages = checkpoint.history.entries();
            let starts: Vec<usize> = messages
                .iter()
                .enumerate()
                .filter_map(|(index, message)| {
                    matches!(message.message, Message::User { .. }).then_some(index)
                })
                .collect();
            if starts.len() <= self.turns {
                return Ok(None);
            }
            let mut first = starts[starts.len() - self.turns];
            if let Some(dependencies) = &self.dependencies {
                let required = dependencies(&checkpoint, first)?;
                if required > first {
                    return Err(Error::Invalid(
                        "history dependency must expand the retained window".into(),
                    ));
                }
                first = starts
                    .iter()
                    .rev()
                    .copied()
                    .find(|index| *index <= required)
                    .unwrap_or(0);
            }
            let retained: Vec<_> = messages
                .iter()
                .enumerate()
                .filter(|(index, message)| {
                    *index >= first || matches!(message.message, Message::System { .. })
                })
                .map(|(_, message)| message.clone())
                .collect();
            if retained.len() == messages.len() {
                return Ok(None);
            }
            let history = zhir_core::run::History::from_entries(retained.clone())?;
            validate_history(&history)?;
            Ok(Some(HistoryRewrite {
                entries: retained,
                reason: format!("retain last {} complete user turns", self.turns),
            }))
        })
    }
}
