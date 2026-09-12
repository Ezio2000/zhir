use super::*;
use futures::{StreamExt, stream};
use zhir_core::tool::{ApprovalDecision, ApprovalRequest};

enum WorkState {
    Ready(Arc<dyn RuntimeToolBinding>),
    Settled(RuntimeToolResult),
}
struct Work {
    call: RuntimeToolCall,
    state: WorkState,
}
impl Work {
    fn ready(&self) -> bool {
        matches!(self.state, WorkState::Ready(_))
    }
    fn approval(&self) -> Option<ApprovalRequest> {
        match &self.state {
            WorkState::Ready(binding) => Some(ApprovalRequest {
                call: self.call.clone(),
                spec: binding.spec().clone(),
            }),
            WorkState::Settled(_) => None,
        }
    }
    fn result(self) -> (RuntimeToolCall, RuntimeToolResult) {
        let WorkState::Settled(result) = self.state else {
            unreachable!("every selected call settled");
        };
        (self.call, result)
    }
}
pub(super) struct PreparedBatch {
    work: Vec<Work>,
    parallel: bool,
}
impl Engine {
    pub(super) fn prepare_batch(
        &self,
        candidates: &[RuntimeToolCall],
        cap: usize,
    ) -> Result<PreparedBatch> {
        let catalog = self.catalog.as_ref().expect("opened catalog");
        let batch = self
            .config
            .batch
            .select(&candidates[..cap], &catalog.specs)?;
        if batch.calls.is_empty()
            || batch.calls.len() > cap
            || candidates[..batch.calls.len()] != batch.calls
            || (!batch.parallel && batch.calls.len() != 1)
        {
            return Err(Error::Protocol(
                "batch policy must select a nonempty bounded prefix".into(),
            ));
        }
        if batch.parallel
            && batch.calls.iter().any(|c| {
                !catalog
                    .specs
                    .get(&c.name)
                    .is_some_and(|s| s.execution.parallel_safe())
            })
        {
            return Err(Error::Protocol(
                "parallel batch contains an ineligible tool".into(),
            ));
        }
        let work = batch
            .calls
            .into_iter()
            .map(|call| {
                let state = match catalog.bind(&call) {
                    Ok(binding) => WorkState::Ready(binding),
                    Err(error) => WorkState::Settled(RuntimeToolResult::failure(
                        crate::failure::failure(&error),
                    )),
                };
                Work { call, state }
            })
            .collect();
        Ok(PreparedBatch {
            work,
            parallel: batch.parallel,
        })
    }
    pub(super) async fn approve_batch(
        &mut self,
        batch: &mut PreparedBatch,
        current: &Checkpoint,
    ) -> Result<bool> {
        let Some(policy) = self.config.approval.clone() else {
            return Ok(true);
        };
        let requests: Vec<_> = batch.work.iter().filter_map(Work::approval).collect();
        if requests.is_empty() {
            return Ok(true);
        }
        let count = requests.len();
        for request in &requests {
            self.emitter.emit(EventData::ApprovalRequested {
                call_id: request.call.id.clone(),
            });
        }
        let context = current.context.clone();
        let effect = self
            .effect(
                Box::pin(async move { policy.decide(requests, context).await }),
                Cancellation::default(),
                true,
                true,
            )
            .await;
        let Some(decisions) = self.interruption(effect).await? else {
            return Ok(false);
        };
        if decisions.len() != count {
            return Err(Error::Protocol("approval count mismatch".into()));
        }
        for (work, decision) in batch.work.iter().filter(|w| w.ready()).zip(&decisions) {
            self.emitter.emit(EventData::ApprovalDecided {
                call_id: work.call.id.clone(),
                decision: decision.kind(),
            });
        }
        if let Some(suspension) = decisions.iter().find_map(|d| match d {
            ApprovalDecision::Suspend(s) => Some(s),
            _ => None,
        }) {
            self.suspend(suspension.clone()).await?;
            return Ok(false);
        }
        for (work, decision) in batch.work.iter_mut().filter(|w| w.ready()).zip(decisions) {
            if let ApprovalDecision::Deny(message) = decision {
                work.state =
                    WorkState::Settled(RuntimeToolResult::failure(Failure::new("denied", message)));
            }
        }
        Ok(true)
    }
    pub(super) async fn execute_batch(
        &mut self,
        batch: &mut PreparedBatch,
        current: &Checkpoint,
    ) -> Result<bool> {
        let concurrency = if batch.parallel {
            self.options.limits.max_runtime_tool_concurrency
        } else {
            1
        };
        let futures: Vec<_> = batch
            .work
            .iter()
            .enumerate()
            .filter_map(|(index, work)| {
                let WorkState::Ready(binding) = &work.state else {
                    return None;
                };
                Some(run_tool(
                    index,
                    work.call.clone(),
                    binding.clone(),
                    current.context.clone(),
                    self.emitter.clone(),
                    self.active.clone(),
                    self.options.limits.max_buffered_progress,
                ))
            })
            .collect();
        let future = Box::pin(async move {
            Ok(stream::iter(futures)
                .buffer_unordered(concurrency)
                .collect::<Vec<_>>()
                .await)
        });
        let effect = self
            .effect(future, Cancellation::default(), false, true)
            .await;
        let Some(completed) = self.interruption(effect).await? else {
            return Ok(false);
        };
        if self.expired() {
            self.limit(LimitReason::Deadline).await?;
            return Ok(false);
        }
        for (index, result) in completed {
            batch.work[index].state = WorkState::Settled(result);
        }
        Ok(true)
    }
    pub(super) async fn commit_batch(
        &mut self,
        batch: PreparedBatch,
        pending: PendingCalls,
        provider_pending: bool,
        current: &Checkpoint,
    ) -> Result<()> {
        let results: Vec<_> = batch.work.into_iter().map(Work::result).collect();
        let active = match pending.advance(results.len())? {
            None => ActiveState::Planning {
                provider_turn_pending: provider_pending,
            },
            Some(calls) => ActiveState::RuntimeToolsPending {
                calls,
                provider_turn_pending: provider_pending,
            },
        };
        while let Ok(control) = self.controls.try_recv() {
            self.queue(control)?;
        }
        let suspension = results
            .iter()
            .find_map(|(_, r)| r.suspension.clone())
            .or_else(|| self.pause.take());
        let state = match suspension {
            Some(suspension) => State::Suspended {
                resume_to: active,
                suspension,
            },
            None => active.into_state(),
        };
        let fact = Fact::RuntimeToolBatch {
            call_ids: results.iter().map(|(c, _)| c.id.clone()).collect(),
            outcomes: results.iter().map(|(_, r)| r.outcome.kind()).collect(),
            parallel: batch.parallel,
        };
        let mut metrics = current.metrics.clone();
        metrics.runtime_tool_calls += results.len() as u64;
        let messages = results
            .into_iter()
            .map(|(call, result)| Message::RuntimeTool {
                call_id: call.id,
                name: call.name,
                outcome: result.outcome,
            })
            .collect();
        self.advance(state, fact, HistoryDelta::Append(messages), Some(metrics))
            .await
    }
}
