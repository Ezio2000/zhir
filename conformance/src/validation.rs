use serde_json::Value;
use zhir_core::{Result, error::Error};
pub fn run(case: &Value) -> Result<()> {
    let kind = case["schema"]
        .as_str()
        .ok_or_else(|| Error::Invalid("missing schema".into()))?;
    let schemas = zhir_core::wire::schemas();
    let schema = schemas
        .get(kind)
        .ok_or_else(|| Error::Invalid("unknown schema".into()))?;
    let valid = jsonschema::validator_for(schema)
        .map_err(|e| Error::Invalid(e.to_string()))?
        .is_valid(&case["value"]);
    let expected = case["valid"]
        .as_bool()
        .ok_or_else(|| Error::Invalid("missing expected validity".into()))?;
    if valid != expected {
        return Err(Error::Protocol(format!(
            "{kind}: schema validity {valid}, expected {expected}"
        )));
    }
    Ok(())
}
