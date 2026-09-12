use serde_json::Value;
use zhir_core::{Result, error::Error};
pub(crate) fn compile(schema: &Value) -> Result<jsonschema::Validator> {
    jsonschema::validator_for(schema).map_err(|e| Error::Invalid(format!("invalid schema: {e}")))
}
pub(crate) fn validate(validator: &jsonschema::Validator, value: &Value) -> Result<()> {
    validator
        .validate(value)
        .map_err(|e| Error::Invalid(format!("schema validation: {e}")))
}
