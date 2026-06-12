//! Shared durability profiles for scheduler, executor, state, shuffle, and checkpoint setup.

use std::fmt;
use std::str::FromStr;

/// Named durability profile for a Krishiv deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum DurabilityProfile {
    /// Developer/local mode. Fast startup; process-local state may be lost.
    #[default]
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
                state: StateDurability::LocalRocksDb,
                checkpoint: CheckpointDurability::LocalFilesystem,
                restart_durable: true,
                multi_node_safe: false,
                requires_fencing: false,
            },
            Self::DistributedDurable => DurabilityProfileSpec {
                profile: self,
                metadata: MetadataDurability::DistributedConsensus,
                // Tiered: local-disk first for fast P2P fetches, async-backed by
                // object store for durability across executor restarts and node loss.
                shuffle: ShuffleDurability::Tiered,
                state: StateDurability::LocalRocksDbWithCheckpointRestore,
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
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown durability profile '{value}'")]
pub struct DurabilityProfileParseError {
    /// Supplied profile value.
    pub value: String,
}

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
    /// Remote object store (S3, GCS) only.
    ObjectStore,
    /// Tiered hybrid mode: writes locally first for P2P fetch, asynchronously backs up to Object Store.
    Tiered,
}

/// Operator state durability class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StateDurability {
    /// In-memory operator state.
    Memory,
    /// File-backed embedded state on one host (RocksDB LSM).
    LocalRocksDb,
    /// Local RocksDB state restored from distributed checkpoints.
    LocalRocksDbWithCheckpointRestore,
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
        assert_eq!(distributed.shuffle, ShuffleDurability::Tiered);
        assert_eq!(distributed.checkpoint, CheckpointDurability::ObjectStore);
        assert!(distributed.restart_durable);
        assert!(distributed.multi_node_safe);
        assert!(distributed.requires_fencing);
    }

    // ── Shuffle durability wiring ────────────────────────────────────────────
    // Regression guard: changes to profile→shuffle mapping must be explicit.

    #[test]
    fn dev_local_maps_to_memory_shuffle() {
        assert_eq!(
            DurabilityProfile::DevLocal.spec().shuffle,
            ShuffleDurability::Memory,
            "DevLocal must use in-memory shuffle (no disk I/O)"
        );
    }

    #[test]
    fn single_node_durable_maps_to_local_disk_shuffle() {
        assert_eq!(
            DurabilityProfile::SingleNodeDurable.spec().shuffle,
            ShuffleDurability::LocalDisk,
            "SingleNodeDurable must use LocalDisk shuffle (restart-safe)"
        );
    }

    #[test]
    fn distributed_durable_maps_to_tiered_shuffle() {
        assert_eq!(
            DurabilityProfile::DistributedDurable.spec().shuffle,
            ShuffleDurability::Tiered,
            "DistributedDurable must use Tiered shuffle (local P2P + object-store durability)"
        );
    }

    #[test]
    fn single_node_durable_is_restart_safe_but_not_multi_node() {
        let spec = DurabilityProfile::SingleNodeDurable.spec();
        assert!(
            spec.restart_durable,
            "SingleNodeDurable must survive restarts"
        );
        assert!(
            !spec.multi_node_safe,
            "SingleNodeDurable must not claim multi-node safety"
        );
    }

    #[test]
    fn default_profile_is_dev_local() {
        assert_eq!(
            DurabilityProfile::default(),
            DurabilityProfile::DevLocal,
            "default profile must be DevLocal for safe embedded use"
        );
    }
}
