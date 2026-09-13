use std::collections::BTreeMap;
use zhir_core::{
    Result,
    error::Error,
    tool::{BatchPolicy, RuntimeToolBatch, RuntimeToolCall, RuntimeToolSpec},
};
pub(crate) struct DefaultBatch;
impl BatchPolicy for DefaultBatch {
    fn select(
        &self,
        candidates: &[RuntimeToolCall],
        specs: &BTreeMap<String, RuntimeToolSpec>,
    ) -> Result<RuntimeToolBatch> {
        let first = candidates
            .first()
            .ok_or_else(|| Error::Invalid("empty batch candidates".into()))?;
        let safe = |c: &RuntimeToolCall| {
            specs
                .get(&c.name)
                .is_some_and(|s| s.execution.parallel_safe())
        };
        let calls = if safe(first) {
            candidates
                .iter()
                .take_while(|c| safe(c))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            vec![first.clone()]
        };
        Ok(RuntimeToolBatch {
            parallel: calls.len() > 1,
            calls,
        })
    }
}
