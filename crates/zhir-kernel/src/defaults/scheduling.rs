use std::collections::BTreeMap;
use zhir_core::{
    Result,
    tool::{Admission, RuntimeToolCall, RuntimeToolSpec, SchedulingPolicy},
};
/// Admits the leading serial call alone, or the leading run of parallel calls.
/// A serial call is a barrier: calls emitted after it wait until it settles.
pub(crate) struct DefaultScheduling;
impl SchedulingPolicy for DefaultScheduling {
    fn select(
        &self,
        candidates: &[RuntimeToolCall],
        specs: &BTreeMap<String, RuntimeToolSpec>,
    ) -> Result<Admission> {
        let parallel = |call: &RuntimeToolCall| {
            specs
                .get(&call.name)
                .is_some_and(|spec| spec.execution.parallel)
        };
        let Some(first) = candidates.first() else {
            return Ok(Admission {
                calls: vec![],
                parallel: false,
            });
        };
        if !parallel(first) {
            return Ok(Admission {
                calls: vec![first.clone()],
                parallel: false,
            });
        }
        Ok(Admission {
            calls: candidates
                .iter()
                .take_while(|call| parallel(call))
                .cloned()
                .collect(),
            parallel: true,
        })
    }
}
