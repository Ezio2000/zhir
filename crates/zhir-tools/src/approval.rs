//! Function adaptation of the existing approval port. Policies belong to callers.
use std::{future::Future, sync::Arc};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    run::RunContext,
    tool::{ApprovalDecision, ApprovalPolicy, ApprovalRequest},
};
type Batch = dyn Fn(Vec<ApprovalRequest>, RunContext) -> BoxFuture<'static, Result<Vec<ApprovalDecision>>>
    + Send
    + Sync;
pub struct FunctionApprovalPolicy {
    callback: Arc<Batch>,
}
impl FunctionApprovalPolicy {
    pub fn batch<F, Fut>(callback: F) -> Self
    where
        F: Fn(Vec<ApprovalRequest>, RunContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<ApprovalDecision>>> + Send + 'static,
    {
        Self {
            callback: Arc::new(move |requests, context| Box::pin(callback(requests, context))),
        }
    }
    pub fn per_call<F, Fut>(callback: F) -> Self
    where
        F: Fn(ApprovalRequest, RunContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ApprovalDecision>> + Send + 'static,
    {
        let callback = Arc::new(callback);
        Self::batch(move |requests, context| {
            let callback = callback.clone();
            async move {
                let mut decisions = Vec::with_capacity(requests.len());
                for request in requests {
                    decisions.push(callback(request, context.clone()).await?);
                }
                Ok(decisions)
            }
        })
    }
}
impl ApprovalPolicy for FunctionApprovalPolicy {
    fn decide(
        &self,
        requests: Vec<ApprovalRequest>,
        context: RunContext,
    ) -> BoxFuture<'_, Result<Vec<ApprovalDecision>>> {
        Box::pin(async move {
            let count = requests.len();
            let decisions = (self.callback)(requests, context).await?;
            if decisions.len() != count {
                return Err(Error::Protocol("approval count mismatch".into()));
            }
            for decision in &decisions {
                if let ApprovalDecision::Suspend(s) = decision {
                    s.validate()?;
                }
            }
            Ok(decisions)
        })
    }
}
