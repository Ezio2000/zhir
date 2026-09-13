use std::collections::BTreeMap;
use zhir_core::{
    Result,
    tool::{Admission, RuntimeToolCall, RuntimeToolSpec, SchedulingPolicy},
};
pub(crate) struct DefaultScheduling;
impl SchedulingPolicy for DefaultScheduling {
    fn select(
        &self,
        candidates: &[RuntimeToolCall],
        specs: &BTreeMap<String, RuntimeToolSpec>,
    ) -> Result<Admission> {
        let Some(first) = candidates.first() else {
            return Ok(Admission {
                calls: vec![],
                parallel: false,
            });
        };
        let parallel = specs
            .get(&first.name)
            .is_some_and(|spec| spec.execution.parallel);
        let calls = if parallel {
            candidates
                .iter()
                .filter(|call| {
                    specs
                        .get(&call.name)
                        .is_some_and(|spec| spec.execution.parallel)
                })
                .cloned()
                .collect()
        } else {
            vec![first.clone()]
        };
        Ok(Admission { calls, parallel })
    }
}
