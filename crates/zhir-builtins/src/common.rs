use serde_json::Value;
#[cfg(any(feature = "filesystem", feature = "shell"))]
use zhir_core::error::{Error, Failure};
use zhir_core::tool::{Execution, InputSpec, RuntimeToolSpec};
pub(crate) fn spec(
    name: &str,
    description: &str,
    schema: Value,
    read_only: bool,
) -> RuntimeToolSpec {
    RuntimeToolSpec {
        name: name.into(),
        description: description.into(),
        input: InputSpec::Structured { schema },
        output_schema: None,
        execution: Execution {
            parallel: read_only,
            read_only,
            idempotent: read_only,
        },
    }
}
#[cfg(any(feature = "filesystem", feature = "shell"))]
pub(crate) fn failure(code: &str, error: impl std::fmt::Display) -> Error {
    Error::RuntimeTool(Failure::new(code, error.to_string()))
}
