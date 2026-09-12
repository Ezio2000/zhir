//! Event consumption helpers that delegate execution and settlement to kernel.
use futures::StreamExt;
use std::{future::Future, sync::Arc};
use zhir_core::{
    Result,
    error::Error,
    run::{Checkpoint, Event},
};
use zhir_kernel::{Invocation, RunError, invocation::RunResult};

#[derive(Debug, thiserror::Error)]
pub enum DriveError {
    #[error(transparent)]
    Run(#[from] RunError),
    #[error("event observer failed: {error}")]
    Observer {
        error: Error,
        /// The actual settled run result; it may already have completed before
        /// cancellation arrived. Neither that result nor an observer error is lost.
        settled: RunResult,
    },
}
/// Consume events in order and return the run result. Handler errors request
/// cancellation and await settlement before returning both outcomes. A slow
/// handler still observes the runtime's lossy progress stream. Dropping this
/// future requests cancellation via EventStream's Drop; it cannot await cleanup.
pub async fn drive<F, Fut>(
    mut invocation: Invocation,
    mut handler: F,
) -> std::result::Result<Arc<Checkpoint>, DriveError>
where
    F: FnMut(Event) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut events = match invocation.events() {
        Ok(events) => events,
        Err(error) => {
            invocation.control().cancel();
            return Err(DriveError::Observer {
                error,
                settled: invocation.result().await,
            });
        }
    };
    while let Some(event) = events.next().await {
        if let Err(error) = handler(event).await {
            invocation.control().cancel();
            drop(events);
            return Err(DriveError::Observer {
                error,
                settled: invocation.result().await,
            });
        }
    }
    invocation.result().await.map_err(DriveError::Run)
}
