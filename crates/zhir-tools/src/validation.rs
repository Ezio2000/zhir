use serde_json::Value;
use zhir_core::{Result, error::ValidationError};
pub(crate) fn compile(schema: &Value) -> Result<jsonschema::Validator> {
    jsonschema::validator_for(schema).map_err(|e| {
        ValidationError::Schema {
            path: e.instance_path().to_string(),
            message: e.to_string(),
        }
        .into()
    })
}
pub(crate) fn validate(validator: &jsonschema::Validator, value: &Value) -> Result<()> {
    validator.validate(value).map_err(|e| {
        ValidationError::Value {
            path: e.instance_path().to_string(),
            schema_path: e.schema_path().to_string(),
            message: e.to_string(),
        }
        .into()
    })
}
