use super::RunContext;
use crate::{Result, error::ContextError};
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, marker::PhantomData};

/// A shared key definition for a serializable value in run metadata.
#[derive(Debug)]
pub struct ContextKey<T> {
    name: &'static str,
    marker: PhantomData<fn() -> T>,
}
impl<T> Copy for ContextKey<T> {}
impl<T> Clone for ContextKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> ContextKey<T> {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            marker: PhantomData,
        }
    }
    pub fn name(self) -> &'static str {
        self.name
    }
    /// Insert a typed value into serialized metadata without constructing a run.
    pub fn insert(self, metadata: &mut BTreeMap<String, serde_json::Value>, value: T) -> Result<()>
    where
        T: Serialize,
    {
        self.validate()?;
        let value = serde_json::to_value(value).map_err(|e| ContextError::Encode {
            key: self.name.into(),
            message: e.to_string(),
        })?;
        metadata.insert(self.name.into(), value);
        Ok(())
    }
    fn validate(self) -> Result<()> {
        if self.name.is_empty() {
            return Err(ContextError::EmptyKey.into());
        }
        Ok(())
    }
}
impl RunContext {
    pub fn insert<T: Serialize>(&mut self, key: ContextKey<T>, value: T) -> Result<()> {
        key.insert(&mut self.metadata, value)
    }
    pub fn get<T: DeserializeOwned>(&self, key: ContextKey<T>) -> Result<Option<T>> {
        key.validate()?;
        self.metadata
            .get(key.name)
            .cloned()
            .map(|v| {
                serde_json::from_value(v).map_err(|e| {
                    ContextError::Decode {
                        key: key.name.into(),
                        message: e.to_string(),
                    }
                    .into()
                })
            })
            .transpose()
    }
    pub fn require<T: DeserializeOwned>(&self, key: ContextKey<T>) -> Result<T> {
        self.get(key)?.ok_or_else(|| {
            ContextError::Missing {
                key: key.name.into(),
            }
            .into()
        })
    }
}
