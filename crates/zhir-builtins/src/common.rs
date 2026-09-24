use schemars::{JsonSchema, generate::SchemaSettings};
#[cfg(any(feature = "filesystem", feature = "shell"))]
use zhir_core::error::{Error, Failure};
use zhir_core::tool::{Execution, InputSpec, RuntimeToolSpec};
#[cfg(feature = "filesystem")]
pub(crate) const READ_ONLY: Execution = Execution {
    parallel: true,
    read_only: true,
    idempotent: true,
};
#[cfg(any(feature = "filesystem", feature = "shell", feature = "interaction"))]
pub(crate) const MUTATING: Execution = Execution {
    parallel: false,
    read_only: false,
    idempotent: false,
};
pub(crate) fn spec<A: JsonSchema>(
    name: &str,
    description: &str,
    execution: Execution,
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
        execution,
    }
}
#[cfg(any(feature = "filesystem", feature = "shell"))]
pub(crate) fn failure(code: &str, error: impl std::fmt::Display) -> Error {
    Error::RuntimeTool(Failure::new(code, error.to_string()))
}
