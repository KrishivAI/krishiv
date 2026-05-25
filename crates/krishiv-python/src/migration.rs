//! `@ks.state_migration` — register keyed-state schema migrations (R16).

use std::sync::Arc;

use krishiv_state::{SharedStateMigrationRegistry, StateMigrationError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::errors::CheckpointError;
use crate::session::PySession;

static GLOBAL_MIGRATIONS: std::sync::LazyLock<SharedStateMigrationRegistry> =
    std::sync::LazyLock::new(SharedStateMigrationRegistry::new);

/// Register a Python callable as a state migration between schema versions.
#[pyfunction]
#[pyo3(signature = (from_version, to_version, migration_fn, session=None))]
pub fn register_state_migration(
    py: Python<'_>,
    from_version: u32,
    to_version: u32,
    migration_fn: Py<PyAny>,
    session: Option<&PySession>,
) -> PyResult<()> {
    let registry = session
        .map(|s| s.state_migrations.clone())
        .unwrap_or_else(|| GLOBAL_MIGRATIONS.clone());
    let callable = migration_fn.clone_ref(py);
    registry.register(
        from_version,
        to_version,
        Arc::new(move |old: &[u8]| {
            Python::attach(|py| {
                let arg = PyBytes::new(py, old);
                let result = callable
                    .call1(py, (arg,))
                    .map_err(|e| StateMigrationError {
                        message: e.to_string(),
                    })?;
                let bytes: &Bound<'_, PyBytes> =
                    result.cast_bound(py).map_err(|e| StateMigrationError {
                        message: format!("migration must return bytes: {e}"),
                    })?;
                Ok(bytes.as_bytes().to_vec())
            })
        }),
    ).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(())
}

/// Decorator factory: `@state_migration(from_version=1, to_version=2)`.
#[pyfunction]
#[pyo3(signature = (from_version, to_version, *, session=None))]
pub fn state_migration(
    py: Python<'_>,
    from_version: u32,
    to_version: u32,
    session: Option<&PySession>,
) -> PyResult<Py<PyAny>> {
    let registry_target = session.map(|s| s.state_migrations.clone());
    let decorator = Py::new(
        py,
        StateMigrationDecorator {
            from_version,
            to_version,
            registry_target,
        },
    )?;
    Ok(decorator.into_any())
}

#[pyclass]
struct StateMigrationDecorator {
    from_version: u32,
    to_version: u32,
    registry_target: Option<SharedStateMigrationRegistry>,
}

#[pymethods]
impl StateMigrationDecorator {
    #[pyo3(signature = (migration_fn))]
    fn __call__(&self, py: Python<'_>, migration_fn: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let session = self.registry_target.as_ref().map(|_| {
            // When decorator was created with session=, registry is already captured.
            None::<&PySession>
        });
        let _ = session;
        let registry = self
            .registry_target
            .clone()
            .unwrap_or_else(|| GLOBAL_MIGRATIONS.clone());
        let from = self.from_version;
        let to = self.to_version;
        let callable = migration_fn.clone_ref(py);
        registry.register(
            from,
            to,
            Arc::new(move |old: &[u8]| {
                Python::attach(|py| {
                    let arg = PyBytes::new(py, old);
                    let result = callable
                        .call1(py, (arg,))
                        .map_err(|e| StateMigrationError {
                            message: e.to_string(),
                        })?;
                    let bytes: &Bound<'_, PyBytes> =
                        result.cast_bound(py).map_err(|e| StateMigrationError {
                            message: format!("migration must return bytes: {e}"),
                        })?;
                    Ok(bytes.as_bytes().to_vec())
                })
            }),
        ).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(migration_fn)
    }
}

/// Run migrations using the global registry (for tests).
#[pyfunction]
pub fn apply_state_migration(from_version: u32, to_version: u32, data: &[u8]) -> PyResult<Vec<u8>> {
    migrate_global(from_version, to_version, data)
}

pub fn migrate_global(from: u32, to: u32, bytes: &[u8]) -> PyResult<Vec<u8>> {
    GLOBAL_MIGRATIONS
        .migrate(from, to, bytes)
        .map_err(|e| CheckpointError::new_err(e.to_string()))
}
