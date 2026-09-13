use zhir_core::{
    BoxFuture, Result,
    operation::{OperationHandle, OperationRecord},
    tool::RuntimeToolContext,
};

/// Child execution is an operation driven by the same runtime as its parent.
pub trait AgentBackend: Send + Sync {
    fn start(
        &self,
        prompt: String,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<OperationHandle>>;
    fn recover(
        &self,
        operation: OperationRecord,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<OperationHandle>>;
}
