use zhir_core::{operation::ToolExecution, tool::RuntimeToolOutcome};
/// Assert that a test call settled synchronously; active operations must be driven explicitly.
pub trait FinalExecution {
    fn final_outcome(&self) -> &RuntimeToolOutcome;
}
impl FinalExecution for ToolExecution {
    fn final_outcome(&self) -> &RuntimeToolOutcome {
        match self {
            Self::Finished(outcome) => outcome,
            Self::Active(_) => panic!("expected a final outcome, received active work"),
        }
    }
}

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::mpsc;
use zhir_core::{BoxFuture, Result, operation::*, tool::*};
/// An external waiting operation for recovery/control acceptance tests.
pub struct WaitingTool {
    spec: RuntimeToolSpec,
    starts: Arc<AtomicUsize>,
}
impl WaitingTool {
    pub fn new(spec: RuntimeToolSpec) -> Self {
        Self {
            spec,
            starts: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn starts(&self) -> usize {
        self.starts.load(Ordering::SeqCst)
    }
}
impl RuntimeTool for WaitingTool {
    fn spec(&self) -> &RuntimeToolSpec {
        &self.spec
    }
    fn start(
        &self,
        call: RuntimeToolCall,
        _: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<ToolExecution>> {
        Box::pin(async move {
            self.starts.fetch_add(1, Ordering::SeqCst);
            let RuntimeToolInput::Structured(prompt) = call.input else {
                return Err(zhir_core::error::Error::Invalid(
                    "fixture requires structured input".into(),
                ));
            };
            Ok(ToolExecution::Active(waiting_operation(prompt, 0)))
        })
    }
    fn recover(
        &self,
        operation: OperationRecord,
        _: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<ToolExecution>> {
        Box::pin(async move {
            Ok(ToolExecution::Active(waiting_operation(
                operation.wait.unwrap_or_default(),
                operation.last_sequence.map_or(0, |s| s + 1),
            )))
        })
    }
}
pub fn waiting_operation(prompt: serde_json::Value, sequence: u64) -> OperationHandle {
    let (sender, receiver) = mpsc::channel(4);
    sender
        .try_send(OperationEvent {
            sequence,
            update: OperationUpdate::Waiting {
                prompt,
                recovery: None,
            },
        })
        .expect("empty fixture queue");
    OperationHandle {
        recovery: None,
        events: Box::new(WaitingEvents(receiver)),
        control: Arc::new(WaitingControl {
            sender,
            sequence: std::sync::atomic::AtomicU64::new(sequence + 1),
        }),
    }
}
struct WaitingEvents(mpsc::Receiver<OperationEvent>);
impl OperationEvents for WaitingEvents {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<OperationEvent>>> {
        Box::pin(async move { Ok(self.0.recv().await) })
    }
}
struct WaitingControl {
    sender: mpsc::Sender<OperationEvent>,
    sequence: std::sync::atomic::AtomicU64,
}
impl WaitingControl {
    async fn finish(&self, outcome: RuntimeToolOutcome) -> Result<()> {
        self.sender
            .send(OperationEvent {
                sequence: self.sequence.fetch_add(1, Ordering::SeqCst),
                update: OperationUpdate::Finished { outcome },
            })
            .await
            .map_err(|_| zhir_core::error::Error::Cancelled)
    }
}
impl OperationControl for WaitingControl {
    fn cancel(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.finish(RuntimeToolOutcome::Cancelled {
            reason: "fixture cancellation".into(),
        }))
    }
    fn reply(&self, structured: serde_json::Value) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.finish(RuntimeToolOutcome::Success {
            content: vec![],
            structured,
        }))
    }
}
