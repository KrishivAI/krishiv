//! Key-value configuration bag for connector instantiation with sensitive
//! field redaction.

use std::collections::BTreeMap;

use crate::error::{ConnectorError, ConnectorResult};

// ---------------------------------------------------------------------------
// ConnectorConfig
// ---------------------------------------------------------------------------

/// Key/value configuration bag for connector instantiation.
///
/// Properties are stored in a sorted map to make serialisation deterministic.
#[derive(Clone, PartialEq, Eq)]
pub struct ConnectorConfig {
    /// Logical name for this connector instance.
    pub name: String,
    /// Connector kind identifier (e.g., `"parquet"`, `"kafka"`, `"s3"`).
    pub kind: String,
    properties: BTreeMap<String, String>,
}

impl std::fmt::Debug for ConnectorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted_properties: BTreeMap<String, String> = self
            .properties
            .iter()
            .map(|(k, v)| {
                let lower_k = k.to_lowercase();
                if lower_k.contains("password")
                    || lower_k.contains("secret")
                    || lower_k.contains("token")
                    || lower_k.contains("key")
                    || lower_k.contains("credential")
                {
                    (k.clone(), "[REDACTED]".to_string())
                } else {
                    (k.clone(), v.clone())
                }
            })
            .collect();

        f.debug_struct("ConnectorConfig")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("properties", &redacted_properties)
            .finish()
    }
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

    /// Iterate over connector properties as borrowed key/value pairs.
    pub fn properties(&self) -> impl Iterator<Item = (&str, &str)> {
        self.properties
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
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
