//! Provider-independent request intent and explicit negotiation results.
use crate::{Result, error::Error};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub type Extensions = BTreeMap<String, BTreeMap<String, Value>>;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "strength",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Requirement<T> {
    Required(T),
    Preferred(T),
}
impl<T> Requirement<T> {
    pub fn value(&self) -> &T {
        match self {
            Self::Required(v) | Self::Preferred(v) => v,
        }
    }
    pub fn required(&self) -> bool {
        matches!(self, Self::Required(_))
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceUsage {
    pub fidelity: Option<Requirement<Fidelity>>,
    pub transforms: Vec<String>,
    pub extensions: Extensions,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fidelity {
    Economy,
    High,
    Original,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Serving {
    Balanced,
    LowLatency,
    LowCost,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reasoning {
    Low,
    Balanced,
    High,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Interaction {
    TurnBased,
    Duplex,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestProfile {
    pub generation: crate::model::GenerationProfile,
    pub serving: Option<Requirement<Serving>>,
    pub reasoning: Option<Requirement<Reasoning>>,
    pub language: Option<Requirement<String>>,
    pub interaction: Option<Requirement<Interaction>>,
    /// Exact, ordered alternatives. An empty list prohibits semantic degradation.
    pub alternatives: BTreeMap<String, Vec<Value>>,
    pub extensions: Extensions,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NegotiatedProfile {
    pub selected: BTreeMap<String, Value>,
    pub unmet_preferences: BTreeMap<String, String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "source",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Confirmation {
    Provider(Value),
    Verified(Value),
    Unknown,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectiveProfile {
    pub values: BTreeMap<String, Confirmation>,
}

pub fn validate_extensions(extensions: &Extensions) -> Result<()> {
    if extensions
        .iter()
        .any(|(namespace, values)| namespace.is_empty() || values.keys().any(String::is_empty))
    {
        return Err(Error::Invalid(
            "extension namespace and key must be nonempty".into(),
        ));
    }
    Ok(())
}

/// Shared names for built-in negotiation dimensions. Custom dimensions stay caller-owned.
pub mod keys {
    pub const SERVING: &str = "serving";
    pub const REASONING: &str = "reasoning";
    pub const LANGUAGE: &str = "language";
    pub const INTERACTION: &str = "interaction";

    /// A modality names a capability; a resource identity names a selected value.
    pub fn resource_fidelity(subject: &str) -> String {
        format!("resource.{subject}.fidelity")
    }
    pub fn is_resource_fidelity(key: &str) -> bool {
        key.strip_prefix("resource.")
            .and_then(|subject| subject.strip_suffix(".fidelity"))
            .is_some_and(|subject| !subject.is_empty())
    }
}
