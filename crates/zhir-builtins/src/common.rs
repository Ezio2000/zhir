use schemars::{JsonSchema, generate::SchemaSettings};
#[cfg(any(feature = "filesystem", feature = "shell"))]
use zhir_core::error::{Error, Failure};
use zhir_core::tool::{Execution, InputSpec, RuntimeToolSpec};
pub(crate) fn spec<A: JsonSchema>(
    name: &str,
    description: &str,
    read_only: bool,
) -> RuntimeToolSpec {
    RuntimeToolSpec {
        name: name.into(),
        description: description.into(),
        input: InputSpec::Structured {
            schema: SchemaSettings::draft2020_12()
                .for_deserialize()
                .into_generator()
                .into_root_schema_for::<A>()
                .into(),
        },
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
