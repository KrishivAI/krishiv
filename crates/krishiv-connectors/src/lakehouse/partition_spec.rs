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
        let entry =
            self.specs
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

    /// Return the active partition fields for the current default spec.
    ///
    /// Returns an empty slice when no spec has been registered yet.
    pub fn active_fields(&self) -> &[PartitionField] {
        self.specs
            .get(&self.default_spec_id)
            .map(|s| s.fields.as_slice())
            .unwrap_or(&[])
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

    #[test]
    fn resolve_returns_none_for_unregistered_id() {
        let r = PartitionSpecResolver::new(0);
        assert!(r.resolve(Some(99)).is_none());
    }

    #[test]
    fn resolve_uses_default_when_none() {
        let mut r = PartitionSpecResolver::new(0);
        r.register(PartitionSpecVersion {
            spec_id: 0,
            fields: vec![PartitionField {
                name: "dt".into(),
                source_column: "ts".into(),
                transform: "day".into(),
            }],
        });
        let spec = r.resolve(None).unwrap();
        assert_eq!(spec.spec_id, 0);
        assert_eq!(spec.fields.len(), 1);
    }

    #[test]
    fn add_field_creates_default_spec_if_absent() {
        let mut r = PartitionSpecResolver::new(5);
        assert!(r.resolve(None).is_none());
        r.add_field(PartitionField {
            name: "region".into(),
            source_column: "region_id".into(),
            transform: "identity".into(),
        });
        let spec = r.resolve(None).unwrap();
        assert_eq!(spec.spec_id, 5);
        assert_eq!(spec.fields.len(), 1);
        assert_eq!(spec.fields[0].name, "region");
    }

    #[test]
    fn add_field_appends_to_existing_default_spec() {
        let mut r = PartitionSpecResolver::new(0);
        r.register(PartitionSpecVersion {
            spec_id: 0,
            fields: vec![PartitionField {
                name: "year".into(),
                source_column: "event_time".into(),
                transform: "year".into(),
            }],
        });
        r.add_field(PartitionField {
            name: "month".into(),
            source_column: "event_time".into(),
            transform: "month".into(),
        });
        let spec = r.resolve(None).unwrap();
        assert_eq!(spec.fields.len(), 2);
        assert_eq!(spec.fields[0].name, "year");
        assert_eq!(spec.fields[1].name, "month");
    }

    #[test]
    fn drop_field_removes_by_name() {
        let mut r = PartitionSpecResolver::new(0);
        r.register(PartitionSpecVersion {
            spec_id: 0,
            fields: vec![
                PartitionField {
                    name: "year".into(),
                    source_column: "ts".into(),
                    transform: "year".into(),
                },
                PartitionField {
                    name: "month".into(),
                    source_column: "ts".into(),
                    transform: "month".into(),
                },
                PartitionField {
                    name: "day".into(),
                    source_column: "ts".into(),
                    transform: "day".into(),
                },
            ],
        });
        r.drop_field("month");
        let spec = r.resolve(None).unwrap();
        assert_eq!(spec.fields.len(), 2);
        assert_eq!(spec.fields[0].name, "year");
        assert_eq!(spec.fields[1].name, "day");
    }

    #[test]
    fn drop_field_noop_when_name_not_found() {
        let mut r = PartitionSpecResolver::new(0);
        r.register(PartitionSpecVersion {
            spec_id: 0,
            fields: vec![PartitionField {
                name: "dt".into(),
                source_column: "ts".into(),
                transform: "day".into(),
            }],
        });
        r.drop_field("nonexistent");
        let spec = r.resolve(None).unwrap();
        assert_eq!(spec.fields.len(), 1);
    }

    #[test]
    fn drop_field_noop_when_no_default_spec() {
        let mut r = PartitionSpecResolver::new(7);
        r.drop_field("anything");
        assert!(r.resolve(None).is_none());
    }

    #[test]
    fn active_fields_empty_when_no_spec_registered() {
        let r = PartitionSpecResolver::new(0);
        assert!(r.active_fields().is_empty());
    }

    #[test]
    fn active_fields_returns_default_spec_fields() {
        let mut r = PartitionSpecResolver::new(3);
        r.add_field(PartitionField {
            name: "a".into(),
            source_column: "col_a".into(),
            transform: "identity".into(),
        });
        r.add_field(PartitionField {
            name: "b".into(),
            source_column: "col_b".into(),
            transform: "bucket[16]".into(),
        });
        let fields = r.active_fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].transform, "identity");
        assert_eq!(fields[1].transform, "bucket[16]");
    }

    #[test]
    fn resolve_does_not_mutate_spec() {
        let mut r = PartitionSpecResolver::new(0);
        r.register(PartitionSpecVersion {
            spec_id: 0,
            fields: vec![PartitionField {
                name: "dt".into(),
                source_column: "ts".into(),
                transform: "day".into(),
            }],
        });
        let _ = r.resolve(None);
        let _ = r.resolve(None);
        let spec = r.resolve(None).unwrap();
        assert_eq!(spec.fields.len(), 1);
    }

    #[test]
    fn register_overwrites_same_spec_id() {
        let mut r = PartitionSpecResolver::new(0);
        r.register(PartitionSpecVersion {
            spec_id: 0,
            fields: vec![PartitionField {
                name: "old".into(),
                source_column: "x".into(),
                transform: "old".into(),
            }],
        });
        r.register(PartitionSpecVersion {
            spec_id: 0,
            fields: vec![PartitionField {
                name: "new".into(),
                source_column: "y".into(),
                transform: "new".into(),
            }],
        });
        let spec = r.resolve(None).unwrap();
        assert_eq!(spec.fields.len(), 1);
        assert_eq!(spec.fields[0].name, "new");
    }
}
