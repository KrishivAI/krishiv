//! Operator identity and state compatibility contracts.

use crate::error::{StateError, StateResult};

/// Stable identity and serializer version for one named operator state.
///
/// `operator_id` must remain stable across compatible job upgrades. Renaming an
/// operator creates new state unless the deployment explicitly supplies a
/// migration. `state_name` distinguishes independent state values owned by the
/// same operator. `serializer_version` changes whenever persisted bytes change.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperatorStateDescriptor {
    pub operator_id: String,
    pub state_name: String,
    pub serializer_version: u32,
}

impl OperatorStateDescriptor {
    pub fn new(
        operator_id: impl Into<String>,
        state_name: impl Into<String>,
        serializer_version: u32,
    ) -> StateResult<Self> {
        let descriptor = Self {
            operator_id: operator_id.into(),
            state_name: state_name.into(),
            serializer_version,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub fn validate(&self) -> StateResult<()> {
        if self.operator_id.trim().is_empty() || self.state_name.trim().is_empty() {
            return Err(StateError::SnapshotCorrupt {
                message: "operator_id and state_name must be non-empty".into(),
            });
        }
        if self.serializer_version == 0 {
            return Err(StateError::SnapshotCorrupt {
                message: "serializer_version must be greater than zero".into(),
            });
        }
        Ok(())
    }

    /// Determine whether `restored` can be opened by `current` without a migration.
    pub fn is_directly_compatible_with(&self, restored: &Self) -> bool {
        self.operator_id == restored.operator_id
            && self.state_name == restored.state_name
            && self.serializer_version == restored.serializer_version
    }

    /// Validate direct restore compatibility. Version changes require a registered
    /// [`crate::StateMigrationRegistry`] migration before this check is retried.
    pub fn validate_direct_restore(&self, restored: &Self) -> StateResult<()> {
        self.validate()?;
        restored.validate()?;
        if !self.is_directly_compatible_with(restored) {
            return Err(StateError::SnapshotCorrupt {
                message: format!(
                    "incompatible operator state: current={}/{}@v{}, restored={}/{}@v{}",
                    self.operator_id,
                    self.state_name,
                    self.serializer_version,
                    restored.operator_id,
                    restored.state_name,
                    restored.serializer_version
                ),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_identity_and_version_restore_directly() {
        let current = OperatorStateDescriptor::new("orders-by-user", "windows", 1).unwrap();
        assert!(current.validate_direct_restore(&current).is_ok());
    }

    #[test]
    fn rename_or_serializer_change_requires_migration() {
        let restored = OperatorStateDescriptor::new("orders-by-user", "windows", 1).unwrap();
        let renamed = OperatorStateDescriptor::new("orders-v2", "windows", 1).unwrap();
        let upgraded = OperatorStateDescriptor::new("orders-by-user", "windows", 2).unwrap();
        assert!(renamed.validate_direct_restore(&restored).is_err());
        assert!(upgraded.validate_direct_restore(&restored).is_err());
    }
}
