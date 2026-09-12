use serde::de::DeserializeOwned;
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    tool::{
        InputSpec, RuntimeTool, RuntimeToolCall, RuntimeToolContext, RuntimeToolInput,
        RuntimeToolResult, RuntimeToolSpec,
    },
};

/// Owns a specification and a typed asynchronous callback, with no reflection.
pub struct FunctionTool<F> {
    spec: RuntimeToolSpec,
    callback: F,
}
impl<F> FunctionTool<F> {
    pub fn new(spec: RuntimeToolSpec, callback: F) -> Self {
        Self { spec, callback }
    }
}
impl<F, Fut> RuntimeTool for FunctionTool<F>
where
    F: Fn(RuntimeToolCall, RuntimeToolContext) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<RuntimeToolResult>> + Send + 'static,
{
    fn spec(&self) -> &RuntimeToolSpec {
        &self.spec
    }
    fn invoke(
        &self,
        call: RuntimeToolCall,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<RuntimeToolResult>> {
        Box::pin((self.callback)(call, context))
    }
}
/// Adapt structured JSON to an explicitly chosen Rust input type. Schema
/// validation belongs to the registry; deserialization never inserts schema defaults.
pub fn structured<A, F, Fut>(spec: RuntimeToolSpec, callback: F) -> Result<impl RuntimeTool>
where
    A: DeserializeOwned + Send + 'static,
    F: Fn(A, RuntimeToolContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<RuntimeToolResult>> + Send + 'static,
{
    if !matches!(spec.input, InputSpec::Structured { .. }) {
        return Err(Error::Invalid(
            "structured callback requires structured specification".into(),
        ));
    }
    let callback = std::sync::Arc::new(callback);
    Ok(FunctionTool::new(
        spec,
        move |call: RuntimeToolCall, context: RuntimeToolContext| {
            let callback = callback.clone();
            async move {
                let RuntimeToolInput::Structured(value) = call.input else {
                    return Err(Error::Invalid("expected structured input".into()));
                };
                let args =
                    serde_json::from_value(value).map_err(|e| Error::Invalid(e.to_string()))?;
                callback(args, context).await
            }
        },
    ))
}
pub fn freeform<F, Fut>(spec: RuntimeToolSpec, callback: F) -> Result<impl RuntimeTool>
where
    F: Fn(String, RuntimeToolContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<RuntimeToolResult>> + Send + 'static,
{
    if !matches!(spec.input, InputSpec::Freeform { .. }) {
        return Err(Error::Invalid(
            "freeform callback requires freeform specification".into(),
        ));
    }
    let callback = std::sync::Arc::new(callback);
    Ok(FunctionTool::new(
        spec,
        move |call: RuntimeToolCall, context: RuntimeToolContext| {
            let callback = callback.clone();
            async move {
                let RuntimeToolInput::Freeform(input) = call.input else {
                    return Err(Error::Invalid("expected freeform input".into()));
                };
                callback(input, context).await
            }
        },
    ))
}
