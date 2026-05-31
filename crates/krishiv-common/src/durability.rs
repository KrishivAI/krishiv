//! Shared durability profiles for scheduler, executor, state, shuffle, and checkpoint setup.

use std::fmt;
use std::str::FromStr;

/// Named durability profile for a Krishiv deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurabilityProfile {
    /// Developer/local mode. Fast startup; process-local state may be lost.
    DevLocal,
    /// Single-host durable mode. Uses local durable files/databases.
    SingleNodeDurable,
    /// Distributed durable mode. Requires shared durable storage and fencing.
    DistributedDurable,
}

impl DurabilityProfile {
    /// Stable kebab-case profile name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DevLocal => "dev-local",
            Self::SingleNodeDurable => "single-node-durable",
            Self::DistributedDurable => "distributed-durable",
        }
    }

    /// Component-level expectations implied by this profile.
    pub const fn spec(self) -> DurabilityProfileSpec {
        match self {
            Self::DevLocal => DurabilityProfileSpec {
                profile: self,
                metadata: MetadataDurability::Memory,
                shuffle: ShuffleDurability::Memory,
                state: StateDurability::Memory,
                checkpoint: CheckpointDurability::EphemeralLocal,
                restart_durable: false,
                multi_node_safe: false,
                requires_fencing: false,
            },
            Self::SingleNodeDurable => DurabilityProfileSpec {
                profile: self,
                metadata: MetadataDurability::LocalFile,
                shuffle: ShuffleDurability::LocalDisk,
                state: StateDurability::LocalRedb,
                checkpoint: CheckpointDurability::LocalFilesystem,
                restart_durable: true,
                multi_node_safe: false,
                requires_fencing: false,
            },
            Self::DistributedDurable => DurabilityProfileSpec {
                profile: self,
                metadata: MetadataDurability::DistributedConsensus,
                shuffle: ShuffleDurability::ObjectStore,
                state: StateDurability::LocalRedbWithCheckpointRestore,
                checkpoint: CheckpointDurability::ObjectStore,
                restart_durable: true,
                multi_node_safe: true,
                requires_fencing: true,
            },
        }
    }

    /// All supported profile names.
    pub const fn supported_names() -> &'static [&'static str] {
        &["dev-local", "single-node-durable", "distributed-durable"]
    }
}

impl Default for DurabilityProfile {
    fn default() -> Self {
        Self::DevLocal
    }
}

impl fmt::Display for DurabilityProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DurabilityProfile {
    type Err = DurabilityProfileParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "dev-local" | "dev" | "local" => Ok(Self::DevLocal),
            "single-node-durable" | "single-node" | "single" => Ok(Self::SingleNodeDurable),
            "distributed-durable" | "distributed" => Ok(Self::DistributedDurable),
            other => Err(DurabilityProfileParseError {
                value: other.to_owned(),
            }),
        }
    }
}

/// Error returned for an unknown durability profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurabilityProfileParseError {
    /// Supplied profile value.
    pub value: String,
}

impl fmt::Display for DurabilityProfileParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown durability profile '{}'", self.value)
    }
}

impl std::error::Error for DurabilityProfileParseError {}

/// Concrete durability expectations per component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DurabilityProfileSpec {
    /// Source profile.
    pub profile: DurabilityProfile,
    /// Coordinator metadata durability.
    pub metadata: MetadataDurability,
    /// Shuffle durability.
    pub shuffle: ShuffleDurability,
    /// Operator state durability.
    pub state: StateDurability,
    /// Checkpoint durability.
    pub checkpoint: CheckpointDurability,
    /// Whether the profile is expected to survive process restart.
    pub restart_durable: bool,
    /// Whether the profile is safe for multiple worker hosts.
    pub multi_node_safe: bool,
    /// Whether the profile requires coordinator fencing/leader leases.
    pub requires_fencing: bool,
}

/// Coordinator metadata durability class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetadataDurability {
    /// Process memory only.
    Memory,
    /// Local durable file/database.
    LocalFile,
    /// Shared consensus-backed metadata.
    DistributedConsensus,
}

/// Shuffle durability class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShuffleDurability {
    /// Process memory only.
    Memory,
    /// Local disk on one host.
    LocalDisk,
    /// Shared object store for multi-host workers.
    ObjectStore,
}

/// Operator state durability class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StateDurability {
    /// In-memory operator state.
    Memory,
    /// File-backed redb state on one host.
    LocalRedb,
    /// Local redb state restored from distributed checkpoints.
    LocalRedbWithCheckpointRestore,
}

/// Checkpoint durability class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CheckpointDurability {
    /// Ephemeral local/test storage.
    EphemeralLocal,
    /// Durable local filesystem checkpoint storage.
    LocalFilesystem,
    /// Shared object-store checkpoint storage.
    ObjectStore,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_profiles() {
        assert_eq!(
            "dev-local".parse::<DurabilityProfile>().unwrap(),
            DurabilityProfile::DevLocal
        );
        assert_eq!(
            "single-node-durable".parse::<DurabilityProfile>().unwrap(),
            DurabilityProfile::SingleNodeDurable
        );
        assert_eq!(
            "distributed-durable".parse::<DurabilityProfile>().unwrap(),
            DurabilityProfile::DistributedDurable
        );
    }

    #[test]
    fn profile_specs_capture_component_expectations() {
        let dev = DurabilityProfile::DevLocal.spec();
        assert_eq!(dev.metadata, MetadataDurability::Memory);
        assert!(!dev.restart_durable);
        assert!(!dev.multi_node_safe);

        let single = DurabilityProfile::SingleNodeDurable.spec();
        assert_eq!(single.shuffle, ShuffleDurability::LocalDisk);
        assert_eq!(single.checkpoint, CheckpointDurability::LocalFilesystem);
        assert!(single.restart_durable);
        assert!(!single.multi_node_safe);

        let distributed = DurabilityProfile::DistributedDurable.spec();
        assert_eq!(
            distributed.metadata,
            MetadataDurability::DistributedConsensus
        );
        assert_eq!(distributed.shuffle, ShuffleDurability::ObjectStore);
        assert_eq!(distributed.checkpoint, CheckpointDurability::ObjectStore);
        assert!(distributed.restart_durable);
        assert!(distributed.multi_node_safe);
        assert!(distributed.requires_fencing);
    }
}
