//! State schema migration registry (R16 S4.3).

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// Migrates serialized state bytes from one schema version to another.
pub type StateMigrationFn =
    Arc<dyn Fn(&[u8]) -> Result<Vec<u8>, StateMigrationError> + Send + Sync>;

/// Migration failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateMigrationError {
    pub message: String,
}

impl std::fmt::Display for StateMigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "state migration error: {}", self.message)
    }
}

impl std::error::Error for StateMigrationError {}

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
}
