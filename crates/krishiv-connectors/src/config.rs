//! config.

use std::collections::BTreeMap;

use crate::error::{ConnectorError, ConnectorResult};

// ---------------------------------------------------------------------------
// ConnectorConfig
// ---------------------------------------------------------------------------

/// Key/value configuration bag for connector instantiation.
///
/// Properties are stored in a sorted map to make serialisation deterministic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorConfig {
    /// Logical name for this connector instance.
    pub name: String,
    /// Connector kind identifier (e.g., `"parquet"`, `"kafka"`, `"s3"`).
    pub kind: String,
    properties: BTreeMap<String, String>,
}

impl ConnectorConfig {
    /// Create a new config with the given name and kind, and no properties.
    pub fn new(name: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: kind.into(),
            properties: BTreeMap::new(),
        }
    }

    /// Add a property and return the updated config (builder style).
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Look up a property by key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(String::as_str)
    }

    /// Look up a required property, returning a [`ConnectorError::Config`] if
    /// it is absent.
    pub fn required(&self, key: &str) -> ConnectorResult<&str> {
        self.get(key).ok_or_else(|| ConnectorError::Config {
            message: format!(
                "required property '{key}' is missing from connector '{}'",
                self.name
            ),
        })
    }
}
