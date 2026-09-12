//! Type-derived schemas and explicit typed outcomes for structured tools.
use crate::ToolReply;
use schemars::{JsonSchema, generate::SchemaSettings};
use serde::{Serialize, de::DeserializeOwned};
use std::{future::Future, sync::Arc};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    tool::{
        Execution, InputSpec, RuntimeTool, RuntimeToolCall, RuntimeToolContext, RuntimeToolInput,
        RuntimeToolResult, RuntimeToolSpec,
    },
};
type Callback<A, O> =
    dyn Fn(A, RuntimeToolContext) -> BoxFuture<'static, Result<ToolReply<O>>> + Send + Sync;

/// A structured RuntimeTool whose schemas follow Serde's input/output contracts.
///
/// Registration still owns schema validation. This adapter does not infer
/// execution facts, insert JSON Schema defaults, or rewrite schemas for endpoints.
/// ToolReply preserves media and explicit waiting/accepted lifecycle semantics.
pub struct TypedTool<A, O> {
    spec: RuntimeToolSpec,
    callback: Arc<Callback<A, O>>,
}
impl<A, O> TypedTool<A, O>
where
    A: DeserializeOwned + JsonSchema + Send + 'static,
    O: Serialize + JsonSchema + Send + 'static,
{
    pub fn new<F, Fut>(
        name: impl Into<String>,
        description: impl Into<String>,
        execution: Execution,
        callback: F,
    ) -> Result<Self>
    where
        F: Fn(A, RuntimeToolContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolReply<O>>> + Send + 'static,
    {
        execution.validate()?;
        let name = name.into();
        if name.is_empty() {
            return Err(Error::Invalid("empty tool name".into()));
        }
        let input = SchemaSettings::draft2020_12()
            .for_deserialize()
            .into_generator()
            .into_root_schema_for::<A>();
        let output = SchemaSettings::draft2020_12()
            .for_serialize()
            .into_generator()
            .into_root_schema_for::<O>();
        Ok(Self {
            spec: RuntimeToolSpec {
                name,
                description: description.into(),
                execution,
                input: InputSpec::Structured {
                    schema: input.into(),
                },
                output_schema: Some(output.into()),
            },
            callback: Arc::new(move |args, context| Box::pin(callback(args, context))),
        })
    }
}
impl<A, O> RuntimeTool for TypedTool<A, O>
where
    A: DeserializeOwned + Send + 'static,
    O: Serialize + Send + 'static,
{
    fn spec(&self) -> &RuntimeToolSpec {
        &self.spec
    }
    fn invoke(
        &self,
        call: RuntimeToolCall,
        context: RuntimeToolContext,
    ) -> BoxFuture<'_, Result<RuntimeToolResult>> {
        Box::pin(async move {
            context.cancellation.check()?;
            call.validate()?;
            let RuntimeToolInput::Structured(value) = call.input else {
                return Err(Error::Invalid("expected structured input".into()));
            };
            let args = serde_json::from_value(value)
                .map_err(|e| Error::Invalid(format!("tool input: {e}")))?;
            let cancellation = context.cancellation.clone();
            let output = (self.callback)(args, context).await?;
            cancellation.check()?;
            output.into_result()
        })
    }
}
