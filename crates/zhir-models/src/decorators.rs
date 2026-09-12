use std::{sync::Arc, time::Duration};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    model::{
        Capabilities, DeltaSink, Model, ModelContext, ModelDelta, ModelRequest, ModelResponse,
    },
    run::RunContext,
};
type ObserverFactory =
    dyn Fn(&ModelRequest, &RunContext) -> Result<Arc<dyn DeltaSink>> + Send + Sync;

/// Observe emitted model deltas independently of a runtime's lossy progress queue.
///
/// The factory runs once per invocation of this wrapper. Each successful emission
/// awaits the observer; this wrapper has no queue or background task. The observer
/// owns retention. Its errors abort the model call, and it may retain partial
/// output from failed calls: observation is not a checkpoint transaction.
///
/// Wrap this inside RetryingModel for a separate observer per attempt, or outside
/// it to observe the logical call. Existing downstream sinks run first so retry
/// trackers see a delta before an observer can perform an external side effect.
pub struct ObservedModel {
    inner: Arc<dyn Model>,
    factory: Arc<ObserverFactory>,
}
impl ObservedModel {
    pub fn new(
        inner: Arc<dyn Model>,
        factory: impl Fn(&ModelRequest, &RunContext) -> Result<Arc<dyn DeltaSink>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            inner,
            factory: Arc::new(factory),
        }
    }
}
struct ObservedSink {
    downstream: Option<Arc<dyn DeltaSink>>,
    observer: Arc<dyn DeltaSink>,
}
impl DeltaSink for ObservedSink {
    fn emit(&self, delta: ModelDelta) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if let Some(sink) = &self.downstream {
                sink.emit(delta.clone()).await?;
            }
            self.observer.emit(delta).await
        })
    }
}
impl Model for ObservedModel {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }
    fn invoke(
        &self,
        request: ModelRequest,
        mut context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            context.cancellation.check()?;
            let observer = (self.factory)(&request, &context.run)?;
            context.cancellation.check()?;
            context.deltas = Some(Arc::new(ObservedSink {
                downstream: context.deltas,
                observer,
            }));
            self.inner.invoke(request, context).await
        })
    }
}

struct Tracker {
    inner: Option<Arc<dyn DeltaSink>>,
    seen: std::sync::atomic::AtomicBool,
}
impl DeltaSink for Tracker {
    fn emit(&self, delta: ModelDelta) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.seen.store(true, std::sync::atomic::Ordering::Release);
            if let Some(inner) = &self.inner {
                inner.emit(delta).await?;
            }
            Ok(())
        })
    }
}
pub struct RetryingModel {
    inner: Arc<dyn Model>,
    attempts: usize,
    delay: Duration,
}
impl RetryingModel {
    pub fn new(inner: Arc<dyn Model>, attempts: usize, delay: Duration) -> Result<Self> {
        if attempts == 0 {
            return Err(Error::Invalid("retry count must be positive".into()));
        }
        Ok(Self {
            inner,
            attempts,
            delay,
        })
    }
}
impl Model for RetryingModel {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            for attempt in 0..self.attempts {
                context.cancellation.check()?;
                let tracker = Arc::new(Tracker {
                    inner: context.deltas.clone(),
                    seen: false.into(),
                });
                let mut ctx = context.clone();
                ctx.deltas = Some(tracker.clone());
                match self.inner.invoke(request.clone(), ctx).await {
                    Err(Error::Model(error))
                        if error.retryable
                            && attempt + 1 < self.attempts
                            && !tracker.seen.load(std::sync::atomic::Ordering::Acquire) =>
                    {
                        tokio::time::sleep(self.delay).await
                    }
                    result => return result,
                }
            }
            unreachable!("positive attempts")
        })
    }
}
pub struct FallbackModel {
    models: Vec<Arc<dyn Model>>,
    capabilities: Capabilities,
}
impl FallbackModel {
    pub fn new(models: Vec<Arc<dyn Model>>) -> Result<Self> {
        let first = models
            .first()
            .ok_or_else(|| Error::Invalid("fallback requires models".into()))?;
        let mut capabilities = first.capabilities().clone();
        for m in models.iter().skip(1) {
            let c = m.capabilities();
            capabilities
                .input_modalities
                .retain(|v| c.input_modalities.contains(v));
            capabilities
                .output_modalities
                .retain(|v| c.output_modalities.contains(v));
            capabilities
                .tool_choices
                .retain(|v| c.tool_choices.contains(v));
            capabilities.structured_runtime_tools &= c.structured_runtime_tools;
            capabilities.freeform_runtime_tools &= c.freeform_runtime_tools;
            capabilities.provider_tools &= c.provider_tools;
            capabilities.parallel_runtime_tools &= c.parallel_runtime_tools;
            capabilities.parallel_control &= c.parallel_control;
            capabilities.streaming &= c.streaming;
            capabilities.usage &= c.usage;
            capabilities.structured_output &= c.structured_output;
            capabilities.json_mode &= c.json_mode;
            capabilities.seed &= c.seed;
        }
        Ok(Self {
            models,
            capabilities,
        })
    }
}
impl Model for FallbackModel {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelContext,
    ) -> BoxFuture<'_, Result<ModelResponse>> {
        Box::pin(async move {
            request.validate(&self.capabilities)?;
            for (index, model) in self.models.iter().enumerate() {
                context.cancellation.check()?;
                let tracker = Arc::new(Tracker {
                    inner: context.deltas.clone(),
                    seen: false.into(),
                });
                let mut ctx = context.clone();
                ctx.deltas = Some(tracker.clone());
                match model.invoke(request.clone(), ctx).await {
                    Err(Error::Model(error))
                        if error.retryable
                            && index + 1 < self.models.len()
                            && !tracker.seen.load(std::sync::atomic::Ordering::Acquire) =>
                    {
                        continue;
                    }
                    result => return result,
                }
            }
            unreachable!("nonempty models")
        })
    }
}
