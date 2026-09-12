use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use zhir_core::{
    BoxFuture, Result,
    error::{Error, Failure},
    tool::{RuntimeTool, RuntimeToolCall, RuntimeToolContext, RuntimeToolResult, RuntimeToolSpec},
};

pub struct RetryingTool {
    inner: Arc<dyn RuntimeTool>,
    attempts: usize,
    delay: Duration,
}
impl RetryingTool {
    pub fn new(inner: Arc<dyn RuntimeTool>, attempts: usize, delay: Duration) -> Result<Self> {
        if attempts == 0 || (attempts > 1 && !inner.spec().execution.idempotent) {
            return Err(Error::Invalid(
                "retries require a positive count and an idempotent tool".into(),
            ));
        }
        Ok(Self {
            inner,
            attempts,
            delay,
        })
    }
}
impl RuntimeTool for RetryingTool {
    fn spec(&self) -> &RuntimeToolSpec {
        self.inner.spec()
    }
    fn invoke(
        &self,
        call: RuntimeToolCall,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<RuntimeToolResult>> {
        Box::pin(async move {
            for attempt in 0..self.attempts {
                context.cancellation.check()?;
                match self.inner.invoke(call.clone(), context.clone()).await {
                    Err(Error::RuntimeTool(error))
                        if error.retryable && attempt + 1 < self.attempts =>
                    {
                        tokio::time::sleep(self.delay).await
                    }
                    result => return result,
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
    state: Mutex<Circuit>,
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
            state: Mutex::new(Circuit {
                failures: 0,
                opened: None,
                probe: false,
            }),
        })
    }
}
struct Probe<'a> {
    state: &'a Mutex<Circuit>,
    armed: bool,
}
impl Drop for Probe<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.state.lock().expect("circuit lock").probe = false;
        }
    }
}
impl RuntimeTool for CircuitBreakingTool {
    fn spec(&self) -> &RuntimeToolSpec {
        self.inner.spec()
    }
    fn invoke(
        &self,
        call: RuntimeToolCall,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<RuntimeToolResult>> {
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
            let _probe = Probe {
                state: &self.state,
                armed: probing,
            };
            let result = self.inner.invoke(call, context).await;
            let mut state = self.state.lock().expect("circuit lock");
            match &result {
                Ok(_) => {
                    state.failures = 0;
                    state.opened = None;
                }
                Err(Error::Cancelled) => {}
                Err(_) => {
                    state.failures += 1;
                    if state.failures >= self.threshold {
                        state.opened = Some(Instant::now());
                    }
                }
            }
            result
        })
    }
}
