use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use zhir_core::operation::ToolExecution;
use zhir_core::{
    BoxFuture, Result,
    error::{Error, Failure},
    tool::{RuntimeTool, RuntimeToolCall, RuntimeToolContext, RuntimeToolSpec},
};

pub struct RetryingTool {
    inner: Arc<dyn RuntimeTool>,
    policy: zhir_policies::RetryPolicy,
}
impl RetryingTool {
    pub fn new(inner: Arc<dyn RuntimeTool>, policy: zhir_policies::RetryPolicy) -> Result<Self> {
        if policy.max_attempts() > 1 && !inner.spec().execution.idempotent {
            return Err(Error::Invalid("retries require an idempotent tool".into()));
        }
        Ok(Self { inner, policy })
    }
}
impl RuntimeTool for RetryingTool {
    fn recover(
        &self,
        record: zhir_core::operation::OperationRecord,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<ToolExecution>> {
        self.inner.recover(record, context)
    }
    fn spec(&self) -> &RuntimeToolSpec {
        self.inner.spec()
    }
    fn start(
        &self,
        call: RuntimeToolCall,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<ToolExecution>> {
        Box::pin(async move {
            let deadline = crate::retry_wait::deadline(&context.run)?;
            for attempt in 0..self.policy.max_attempts() {
                crate::retry_wait::check(&context.cancellation, deadline)?;
                match self.inner.start(call.clone(), context.clone()).await {
                    Err(Error::RuntimeTool(error))
                        if error.retryable && attempt + 1 < self.policy.max_attempts() =>
                    {
                        crate::retry_wait::wait(
                            self.policy
                                .delay_after(attempt + 1)
                                .expect("remaining attempt"),
                            &context.cancellation,
                            deadline,
                        )
                        .await?
                    }
                    result => {
                        crate::retry_wait::check(&context.cancellation, deadline)?;
                        return result;
                    }
                }
            }
            unreachable!("positive retry count")
        })
    }
}
struct Circuit {
    failures: usize,
    opened: Option<Instant>,
    probe: bool,
}
pub struct CircuitBreakingTool {
    inner: Arc<dyn RuntimeTool>,
    threshold: usize,
    cooldown: Duration,
    state: Arc<Mutex<Circuit>>,
}
impl CircuitBreakingTool {
    pub fn new(inner: Arc<dyn RuntimeTool>, threshold: usize, cooldown: Duration) -> Result<Self> {
        if threshold == 0 {
            return Err(Error::Invalid("circuit threshold must be positive".into()));
        }
        Ok(Self {
            inner,
            threshold,
            cooldown,
            state: Arc::new(Mutex::new(Circuit {
                failures: 0,
                opened: None,
                probe: false,
            })),
        })
    }
}
struct Probe {
    state: Arc<Mutex<Circuit>>,
    threshold: usize,
    armed: bool,
    settled: bool,
}
impl Probe {
    fn settle(&mut self, failure: Option<bool>) {
        if self.settled {
            return;
        }
        self.settled = true;
        let mut state = self.state.lock().expect("circuit lock");
        match failure {
            Some(false) => {
                state.failures = 0;
                state.opened = None;
            }
            Some(true) => {
                state.failures += 1;
                if state.failures >= self.threshold {
                    state.opened = Some(Instant::now());
                }
            }
            None => (),
        }
        if self.armed {
            state.probe = false;
            self.armed = false;
        }
    }
    fn outcome(&mut self, outcome: &zhir_core::tool::RuntimeToolOutcome) {
        self.settle(match outcome {
            zhir_core::tool::RuntimeToolOutcome::Success { .. } => Some(false),
            zhir_core::tool::RuntimeToolOutcome::Failure { .. } => Some(true),
            zhir_core::tool::RuntimeToolOutcome::Cancelled { .. } => None,
        });
    }
    fn execution(mut self, result: Result<ToolExecution>) -> Result<ToolExecution> {
        match result {
            Ok(ToolExecution::Finished(outcome)) => {
                self.outcome(&outcome);
                Ok(ToolExecution::Finished(outcome))
            }
            Ok(ToolExecution::Active(mut handle)) => {
                handle.events = Box::new(CircuitEvents {
                    inner: handle.events,
                    probe: self,
                });
                Ok(ToolExecution::Active(handle))
            }
            Err(error) => {
                self.settle((!matches!(error, Error::Cancelled)).then_some(true));
                Err(error)
            }
        }
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        if self.armed {
            self.state.lock().expect("circuit lock").probe = false;
        }
    }
}
struct CircuitEvents {
    inner: Box<dyn zhir_core::operation::OperationEvents>,
    probe: Probe,
}
impl zhir_core::operation::OperationEvents for CircuitEvents {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<zhir_core::operation::OperationEvent>>> {
        Box::pin(async move {
            let event = self.inner.receive().await;
            match &event {
                Ok(Some(zhir_core::operation::OperationEvent {
                    update: zhir_core::operation::OperationUpdate::Finished { outcome },
                    ..
                })) => self.probe.outcome(outcome),
                Err(_) | Ok(None) => self.probe.settle(Some(true)),
                _ => (),
            }
            event
        })
    }
}
impl RuntimeTool for CircuitBreakingTool {
    fn spec(&self) -> &RuntimeToolSpec {
        self.inner.spec()
    }
    fn start(
        &self,
        call: RuntimeToolCall,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<ToolExecution>> {
        Box::pin(async move {
            let probing = {
                let mut state = self.state.lock().expect("circuit lock");
                if let Some(at) = state.opened {
                    if at.elapsed() < self.cooldown || state.probe {
                        return Err(Error::RuntimeTool(Failure::new(
                            "circuit_open",
                            "tool circuit is open",
                        )));
                    }
                    state.probe = true;
                    true
                } else {
                    false
                }
            };
            let probe = Probe {
                state: self.state.clone(),
                threshold: self.threshold,
                armed: probing,
                settled: false,
            };
            probe.execution(self.inner.start(call, context).await)
        })
    }
    fn recover(
        &self,
        record: zhir_core::operation::OperationRecord,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<ToolExecution>> {
        Box::pin(async move {
            Probe {
                state: self.state.clone(),
                threshold: self.threshold,
                armed: false,
                settled: false,
            }
            .execution(self.inner.recover(record, context).await)
        })
    }
}
