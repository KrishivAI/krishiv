//! State schema migration registry (R16 S4.3).

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// Migrates serialized state bytes from one schema version to another.
pub type StateMigrationFn =
    Arc<dyn Fn(&[u8]) -> Result<Vec<u8>, StateMigrationError> + Send + Sync>;

/// Migration failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("state migration error: {message}")]
pub struct StateMigrationError {
    pub message: String,
}

/// Registry of `(from_version, to_version) -> migration fn`.
#[derive(Default)]
pub struct StateMigrationRegistry {
    migrations: BTreeMap<(u32, u32), StateMigrationFn>,
}

impl StateMigrationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, from: u32, to: u32, f: StateMigrationFn) {
        self.migrations.insert((from, to), f);
    }

    /// Apply chained migrations from `from` to `to` inclusive.
    pub fn migrate(
        &self,
        from: u32,
        to: u32,
        bytes: &[u8],
    ) -> Result<Vec<u8>, StateMigrationError> {
        if from == to {
            return Ok(bytes.to_vec());
        }
        if from > to {
            return Err(StateMigrationError {
                message: format!("cannot downgrade schema {from} -> {to}"),
            });
        }
        let mut current = bytes.to_vec();
        let mut version = from;
        while version < to {
            let next = version + 1;
            let key = (version, next);
            let migrator = self
                .migrations
                .get(&key)
                .ok_or_else(|| StateMigrationError {
                    message: format!("missing migration {version} -> {next}"),
                })?;
            current = migrator(&current).map_err(|e| StateMigrationError { message: e.message })?;
            version = next;
        }
        Ok(current)
    }
}

/// Current operator state schema version stamped into checkpoint acks.
///
/// Bump this when an operator's serialized state value layout changes in a
/// way that requires a registered [`StateMigrationFn`] to read old snapshots.
pub const CURRENT_STATE_SCHEMA_VERSION: u32 = 1;

/// Migrate every entry *value* of a portable state snapshot from schema
/// version `from` to `to` using `registry`.
///
/// Decodes the snapshot produced by `StateBackend::snapshot()`, applies the
/// chained migrations to each entry's value bytes, and re-encodes.  Entry
/// keys, namespaces, and ordering are preserved.  Empty snapshots (stateless
/// operators) pass through unchanged.
///
/// Returns a typed error when a migration step is missing — restoring a
/// snapshot written by incompatible operator code must fail loudly, never
/// load garbage state.
pub fn migrate_snapshot(
    snapshot_bytes: &[u8],
    registry: &StateMigrationRegistry,
    from: u32,
    to: u32,
) -> Result<Vec<u8>, StateMigrationError> {
    if from == to || snapshot_bytes.is_empty() {
        return Ok(snapshot_bytes.to_vec());
    }
    let mut entries =
        crate::snapshot::decode_snapshot_entries(snapshot_bytes).map_err(|error| {
            StateMigrationError {
                message: format!("snapshot decode before migration: {error}"),
            }
        })?;
    for entry in &mut entries {
        entry.3 = registry.migrate(from, to, &entry.3)?;
    }
    Ok(crate::snapshot::encode_snapshot_entries(&entries))
}

/// SH19: migrate every entry *value* AND *key* of a portable state
/// snapshot from `from` to `to`. Use when a schema bump changes both
/// the value layout *and* the key encoding (e.g. a key prefix swap or
/// a hash algorithm change).
///
/// `key_migrator` is applied to the raw key bytes of every entry;
/// passing `None` leaves the keys unchanged (equivalent to
/// [`migrate_snapshot`]).
///
/// Empty snapshots pass through unchanged. Entry order is preserved
/// — if `key_migrator` produces duplicate keys, the encoded
/// snapshot will contain duplicates and downstream reads will see
/// only one (the last one wins on the RocksDB side).  Callers that
/// introduce a key collision must deduplicate themselves.
/// SH19: type alias for a key-encoding migration closure.
pub type KeyMigrationFn<'a> = &'a dyn Fn(&[u8]) -> Result<Vec<u8>, StateMigrationError>;

/// SH19: migrate every entry *value* AND *key* of a portable state
/// snapshot from `from` to `to`. Use when a schema bump changes both
/// the value layout *and* the key encoding (e.g. a key prefix swap or
/// a hash algorithm change).
///
/// `key_migrator` is applied to the raw key bytes of every entry;
/// passing `None` leaves the keys unchanged (equivalent to
/// [`migrate_snapshot`]).
///
/// Empty snapshots pass through unchanged. Entry order is preserved
/// — if `key_migrator` produces duplicate keys, the encoded
/// snapshot will contain duplicates and downstream reads will see
/// only one (the last one wins on the RocksDB side).  Callers that
/// introduce a key collision must deduplicate themselves.
#[allow(clippy::type_complexity)]
pub fn migrate_snapshot_with_keys(
    snapshot_bytes: &[u8],
    registry: &StateMigrationRegistry,
    from: u32,
    to: u32,
    key_migrator: Option<KeyMigrationFn<'_>>,
) -> Result<Vec<u8>, StateMigrationError> {
    if from == to || snapshot_bytes.is_empty() {
        return Ok(snapshot_bytes.to_vec());
    }
    let mut entries =
        crate::snapshot::decode_snapshot_entries(snapshot_bytes).map_err(|error| {
            StateMigrationError {
                message: format!("snapshot decode before migration: {error}"),
            }
        })?;
    for entry in &mut entries {
        if let Some(km) = key_migrator {
            entry.2 = km(&entry.2)?;
        }
        entry.3 = registry.migrate(from, to, &entry.3)?;
    }
    Ok(crate::snapshot::encode_snapshot_entries(&entries))
}

/// Thread-safe global registry for session-scoped migrations.
#[derive(Default, Clone)]
pub struct SharedStateMigrationRegistry {
    inner: Arc<RwLock<StateMigrationRegistry>>,
}

impl SharedStateMigrationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &self,
        from: u32,
        to: u32,
        f: StateMigrationFn,
    ) -> Result<(), crate::StateError> {
        self.inner
            .write()
            .map_err(|e| crate::StateError::LockPoisoned {
                message: e.to_string(),
            })?
            .register(from, to, f);
        Ok(())
    }

    pub fn migrate(
        &self,
        from: u32,
        to: u32,
        bytes: &[u8],
    ) -> Result<Vec<u8>, StateMigrationError> {
        self.inner
            .read()
            .map_err(|e| StateMigrationError {
                message: format!("state migration registry lock poisoned: {e}"),
            })?
            .migrate(from, to, bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chained_migration_applies_in_order() {
        let mut reg = StateMigrationRegistry::new();
        reg.register(1, 2, Arc::new(|b| Ok([b, b"->2"].concat())));
        reg.register(2, 3, Arc::new(|b| Ok([b, b"->3"].concat())));
        let out = reg.migrate(1, 3, b"v1").unwrap();
        assert_eq!(out, b"v1->2->3");
    }

    #[test]
    fn missing_migration_returns_error() {
        let reg = StateMigrationRegistry::new();
        assert!(reg.migrate(1, 2, b"x").is_err());
    }

    #[test]
    fn migrate_snapshot_transforms_values_and_preserves_keys() {
        let entries = vec![
            (
                "op".to_owned(),
                "window".to_owned(),
                b"k1".to_vec(),
                b"old-1".to_vec(),
            ),
            (
                "op".to_owned(),
                "window".to_owned(),
                b"k2".to_vec(),
                b"old-2".to_vec(),
            ),
        ];
        let snapshot = crate::snapshot::encode_snapshot_entries(&entries);

        let mut reg = StateMigrationRegistry::new();
        reg.register(
            1,
            2,
            Arc::new(|b| {
                let mut out = b"new:".to_vec();
                out.extend_from_slice(b);
                Ok(out)
            }),
        );

        let migrated = migrate_snapshot(&snapshot, &reg, 1, 2).unwrap();
        let decoded = crate::snapshot::decode_snapshot_entries(&migrated).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].2, b"k1");
        assert_eq!(decoded[0].3, b"new:old-1");
        assert_eq!(decoded[1].3, b"new:old-2");
    }

    #[test]
    fn migrate_snapshot_same_version_is_identity() {
        let entries = vec![(
            "op".to_owned(),
            "s".to_owned(),
            b"k".to_vec(),
            b"v".to_vec(),
        )];
        let snapshot = crate::snapshot::encode_snapshot_entries(&entries);
        let reg = StateMigrationRegistry::new();
        assert_eq!(migrate_snapshot(&snapshot, &reg, 1, 1).unwrap(), snapshot);
    }

    #[test]
    fn migrate_snapshot_missing_step_fails_loudly() {
        let entries = vec![(
            "op".to_owned(),
            "s".to_owned(),
            b"k".to_vec(),
            b"v".to_vec(),
        )];
        let snapshot = crate::snapshot::encode_snapshot_entries(&entries);
        let reg = StateMigrationRegistry::new();
        let err = migrate_snapshot(&snapshot, &reg, 1, 3).expect_err("missing migration");
        assert!(err.message.contains("missing migration"));
    }

    /// SH19: when a `key_migrator` is supplied, every entry's key is
    /// transformed as well as its value.
    #[test]
    fn migrate_snapshot_with_keys_transforms_keys() {
        let entries = vec![(
            "op".to_owned(),
            "s".to_owned(),
            b"k_old".to_vec(),
            b"v".to_vec(),
        )];
        let snapshot = crate::snapshot::encode_snapshot_entries(&entries);
        let mut reg = StateMigrationRegistry::new();
        reg.register(1, 2, Arc::new(|bytes| Ok(bytes.to_vec())));
        let migrated = migrate_snapshot_with_keys(
            &snapshot,
            &reg,
            1,
            2,
            Some(&|key: &[u8]| {
                let mut out = b"k_new_".to_vec();
                out.extend_from_slice(&key[2..]);
                Ok(out)
            }),
        )
        .expect("migrate");
        let decoded = crate::snapshot::decode_snapshot_entries(&migrated).expect("decode");
        assert_eq!(decoded[0].2, b"k_new_old");
    }

    /// SH19: when no `key_migrator` is supplied, the keys are
    /// preserved unchanged (equivalent to `migrate_snapshot`).
    #[test]
    fn migrate_snapshot_with_keys_passthrough_when_none() {
        let entries = vec![(
            "op".to_owned(),
            "s".to_owned(),
            b"k".to_vec(),
            b"v".to_vec(),
        )];
        let snapshot = crate::snapshot::encode_snapshot_entries(&entries);
        let mut reg = StateMigrationRegistry::new();
        reg.register(1, 2, Arc::new(|bytes| Ok(bytes.to_vec())));
        let migrated = migrate_snapshot_with_keys(&snapshot, &reg, 1, 2, None).expect("migrate");
        let decoded = crate::snapshot::decode_snapshot_entries(&migrated).expect("decode");
        assert_eq!(decoded[0].2, b"k");
    }
}
