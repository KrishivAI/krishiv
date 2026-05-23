//! Iceberg partition spec resolution across metadata versions (R18 S6).

use std::collections::HashMap;

/// A partition field within a spec version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionField {
    pub name: String,
    pub source_column: String,
    pub transform: String,
}

/// One partition spec version bound to a `spec_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSpecVersion {
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

/// Resolves `spec_id` from data files to the correct partition spec.
#[derive(Debug, Default, Clone)]
pub struct PartitionSpecResolver {
    specs: HashMap<i32, PartitionSpecVersion>,
    default_spec_id: i32,
}

impl PartitionSpecResolver {
    /// Create with the current default spec id.
    pub fn new(default_spec_id: i32) -> Self {
        Self {
            specs: HashMap::new(),
            default_spec_id,
        }
    }

    /// Register a spec version.
    pub fn register(&mut self, spec: PartitionSpecVersion) {
        self.specs.insert(spec.spec_id, spec);
    }

    /// Lookup spec for a data file.
    pub fn resolve(&self, spec_id: Option<i32>) -> Option<&PartitionSpecVersion> {
        let id = spec_id.unwrap_or(self.default_spec_id);
        self.specs.get(&id)
    }

    /// Add a partition field to the current default spec (evolution).
    pub fn add_field(&mut self, field: PartitionField) {
        let entry = self
            .specs
            .entry(self.default_spec_id)
            .or_insert_with(|| PartitionSpecVersion {
                spec_id: self.default_spec_id,
                fields: Vec::new(),
            });
        entry.fields.push(field);
    }

    /// Drop a partition field by name from the default spec.
    pub fn drop_field(&mut self, name: &str) {
        if let Some(spec) = self.specs.get_mut(&self.default_spec_id) {
            spec.fields.retain(|f| f.name != name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolver_returns_registered_spec() {
        let mut r = PartitionSpecResolver::new(0);
        r.register(PartitionSpecVersion {
            spec_id: 0,
            fields: vec![PartitionField {
                name: "dt".into(),
                source_column: "event_time".into(),
                transform: "day".into(),
            }],
        });
        assert!(r.resolve(Some(0)).is_some());
    }
}
