//! Savepoint rename mapping: load, apply, and persist a JSON map of
//! `old_operator_id → new_operator_id` so restored state from a prior
//! schema can be migrated to the current operator graph.
//!
//! The wire format matches what `migrate_snapshot_with_keys` consumes
//! (a `Vec<(String, String)>`) but is serialised as a JSON object
//! `{"old_id": "new_id", ...}` for human readability.
//!
//! CLI subcommand `krishiv savepoint restore-with-mapping` consumes a
//! file produced by [`SavepointRenameMap::save_json`].

use std::collections::BTreeMap;
use std::path::Path;

use crate::error::StateError;
use crate::error::StateResult;

/// A mapping from old operator ids to new operator ids used at
/// restore time.
///
/// Stored as a sorted `BTreeMap` so the JSON output is deterministic
/// (stable test snapshots, easier diffs).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SavepointRenameMap {
    by_old: BTreeMap<String, String>,
}

impl SavepointRenameMap {
    /// Create an empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or replace a rename entry. Returns the previous `new_id`
    /// for the `old_id`, if any.
    pub fn insert(
        &mut self,
        old_id: impl Into<String>,
        new_id: impl Into<String>,
    ) -> Option<String> {
        self.by_old.insert(old_id.into(), new_id.into())
    }

    /// Remove a rename entry. Returns the previous `new_id` for the
    /// `old_id`, if any.
    pub fn remove(&mut self, old_id: &str) -> Option<String> {
        self.by_old.remove(old_id)
    }

    /// Look up the new id for an old id. Returns `None` if the old id
    /// is not in the map (i.e. the operator was not renamed).
    pub fn get(&self, old_id: &str) -> Option<&str> {
        self.by_old.get(old_id).map(String::as_str)
    }

    /// Number of rename entries.
    pub fn len(&self) -> usize {
        self.by_old.len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.by_old.is_empty()
    }

    /// Iterate `(old_id, new_id)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.by_old.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Load a rename map from a JSON file.
    ///
    /// Accepts two JSON shapes:
    /// - `{"old_id": "new_id", ...}` (object)
    /// - `[{"old": "old_id", "new": "new_id"}, ...]` (array of pairs)
    pub fn load_json(path: impl AsRef<Path>) -> StateResult<Self> {
        let bytes = std::fs::read(path.as_ref()).map_err(|e| StateError::BackendUnavailable {
            message: format!(
                "failed to read savepoint rename map '{}': {e}",
                path.as_ref().display()
            ),
            source: None,
        })?;
        Self::parse_json(&bytes)
    }

    /// Parse a rename map from JSON bytes.
    pub fn parse_json(bytes: &[u8]) -> StateResult<Self> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|e| StateError::SnapshotCorrupt {
                message: format!("savepoint rename map is not valid JSON: {e}"),
            })?;
        let mut map = SavepointRenameMap::new();
        match value {
            serde_json::Value::Object(obj) => {
                for (k, v) in obj {
                    let new_id = match v {
                        serde_json::Value::String(s) => s,
                        other => {
                            return Err(StateError::SnapshotCorrupt {
                                message: format!(
                                    "rename map value for '{k}' must be a string, got {other:?}"
                                ),
                            });
                        }
                    };
                    map.insert(k, new_id);
                }
            }
            serde_json::Value::Array(arr) => {
                for (i, entry) in arr.into_iter().enumerate() {
                    let obj = entry
                        .as_object()
                        .ok_or_else(|| StateError::SnapshotCorrupt {
                            message: format!("rename map array entry {i} must be an object"),
                        })?;
                    let old = obj
                        .get("old")
                        .or_else(|| obj.get("from"))
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| StateError::SnapshotCorrupt {
                            message: format!(
                                "rename map array entry {i} missing 'old'/'from' string"
                            ),
                        })?
                        .to_owned();
                    let new = obj
                        .get("new")
                        .or_else(|| obj.get("to"))
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| StateError::SnapshotCorrupt {
                            message: format!(
                                "rename map array entry {i} missing 'new'/'to' string"
                            ),
                        })?
                        .to_owned();
                    map.insert(old, new);
                }
            }
            other => {
                return Err(StateError::SnapshotCorrupt {
                    message: format!("rename map must be a JSON object or array, got {other:?}"),
                });
            }
        }
        Ok(map)
    }

    /// Serialise the map to a JSON object string.
    pub fn to_json(&self) -> StateResult<String> {
        serde_json::to_string_pretty(&self.by_old).map_err(|e| StateError::SnapshotCorrupt {
            message: format!("savepoint rename map serialisation failed: {e}"),
        })
    }

    /// Save the map to a JSON file.
    pub fn save_json(&self, path: impl AsRef<Path>) -> StateResult<()> {
        let json = self.to_json()?;
        std::fs::write(path.as_ref(), json).map_err(|e| StateError::BackendUnavailable {
            message: format!(
                "failed to write savepoint rename map '{}': {e}",
                path.as_ref().display()
            ),
            source: None,
        })?;
        Ok(())
    }

    /// Validate the map: every `new_id` must be non-empty and must
    /// not equal its own `old_id` (an identity rename is meaningless
    /// and a common typo).
    pub fn validate(&self) -> StateResult<()> {
        for (old, new) in self.iter() {
            if new.is_empty() {
                return Err(StateError::SnapshotCorrupt {
                    message: format!("rename map: new_id for '{old}' is empty"),
                });
            }
            if old == new {
                return Err(StateError::SnapshotCorrupt {
                    message: format!(
                        "rename map: identity rename for '{old}' is a no-op — likely typo"
                    ),
                });
            }
        }
        Ok(())
    }

    /// Apply this map to a list of records, returning a new list with
    /// each record's id field rewritten. The `id_field` closure
    /// mutates the id field in place; the rest of the record is passed
    /// through unchanged.
    ///
    /// Use this to rewrite the operator ids in a checkpoint metadata
    /// list, a `PersistedTaskRecord` batch, or any other `(id, ...)`
    /// collection.
    pub fn apply_to<T, F>(&self, records: Vec<T>, mut id_field: F) -> Vec<T>
    where
        F: FnMut(&mut T),
    {
        let mut out = records;
        for r in &mut out {
            id_field(r);
        }
        out
    }

    /// Convenience wrapper for the common case of "each record is a
    /// `String` id; rewrite in place".
    pub fn rewrite_ids(&self, mut ids: Vec<String>) -> Vec<String> {
        for id in &mut ids {
            if let Some(new_id) = self.get(id.as_str()) {
                *id = new_id.to_owned();
            }
        }
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir();
        dir.join(format!("krishiv_savepoint_rename_{name}.json"))
    }

    #[test]
    fn empty_map_round_trips() {
        let m = SavepointRenameMap::new();
        assert!(m.is_empty());
        assert_eq!(m.len(), 0);
        assert!(m.validate().is_ok());
        let json = m.to_json().unwrap();
        assert_eq!(json, "{}");
    }

    #[test]
    fn insert_and_lookup() {
        let mut m = SavepointRenameMap::new();
        m.insert("old.agg", "new.agg_v2");
        m.insert("old.filter", "new.filter");
        assert_eq!(m.len(), 2);
        assert_eq!(m.get("old.agg"), Some("new.agg_v2"));
        assert_eq!(m.get("missing"), None);
    }

    #[test]
    fn parse_object_json() {
        let json = r#"{"old.agg": "new.agg_v2", "old.filter": "new.filter"}"#;
        let m = SavepointRenameMap::parse_json(json.as_bytes()).unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m.get("old.agg"), Some("new.agg_v2"));
    }

    #[test]
    fn parse_array_json() {
        let json = r#"[{"old": "a", "new": "b"}, {"from": "c", "to": "d"}]"#;
        let m = SavepointRenameMap::parse_json(json.as_bytes()).unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m.get("a"), Some("b"));
        assert_eq!(m.get("c"), Some("d"));
    }

    #[test]
    fn parse_invalid_json_errors() {
        let bad = "not json";
        let err = SavepointRenameMap::parse_json(bad.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("not valid JSON"));
    }

    #[test]
    fn parse_array_entry_missing_old_errors() {
        let bad = r#"[{"new": "b"}]"#;
        let err = SavepointRenameMap::parse_json(bad.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("missing 'old'"));
    }

    #[test]
    fn parse_non_object_value_errors() {
        let bad = r#"{"k": 42}"#;
        let err = SavepointRenameMap::parse_json(bad.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("must be a string"));
    }

    #[test]
    fn save_and_load_round_trip() {
        let mut m = SavepointRenameMap::new();
        m.insert("old.agg", "new.agg_v2");
        m.insert("old.window", "new.window_v3");
        let path = temp_path("round_trip");
        m.save_json(&path).unwrap();
        let loaded = SavepointRenameMap::load_json(&path).unwrap();
        assert_eq!(loaded, m);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_rejects_empty_new_id() {
        let mut m = SavepointRenameMap::new();
        m.insert("old", "");
        assert!(m.validate().is_err());
    }

    #[test]
    fn validate_rejects_identity_rename() {
        let mut m = SavepointRenameMap::new();
        m.insert("same", "same");
        let err = m.validate().unwrap_err();
        assert!(err.to_string().contains("identity rename"));
    }

    #[test]
    fn apply_to_rewrites_ids() {
        let mut m = SavepointRenameMap::new();
        m.insert("a", "A");
        m.insert("c", "C");
        let records = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let out = m.apply_to(records, |s: &mut String| {
            if let Some(new_id) = m.get(s.as_str()) {
                *s = new_id.to_owned();
            }
        });
        assert_eq!(out, vec!["A".to_string(), "b".to_string(), "C".to_string()]);
    }

    #[test]
    fn rewrite_ids_helper_rewrites_in_place() {
        let mut m = SavepointRenameMap::new();
        m.insert("old.agg", "new.agg");
        m.insert("old.filter", "new.filter");
        let ids = vec![
            "old.agg".to_string(),
            "untouched".to_string(),
            "old.filter".to_string(),
        ];
        let out = m.rewrite_ids(ids);
        assert_eq!(out, vec!["new.agg", "untouched", "new.filter"]);
    }

    #[test]
    fn iter_visits_in_sorted_order() {
        let mut m = SavepointRenameMap::new();
        m.insert("c", "C");
        m.insert("a", "A");
        m.insert("b", "B");
        let keys: Vec<&str> = m.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn remove_returns_previous_value() {
        let mut m = SavepointRenameMap::new();
        m.insert("a", "A");
        assert_eq!(m.remove("a"), Some("A".to_string()));
        assert_eq!(m.remove("a"), None);
        assert!(m.is_empty());
    }
}
