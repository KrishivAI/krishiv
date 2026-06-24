//! IO and task specs.

use crate::ids::*;

/// Connector capability flags surfaced in task metadata.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnectorCapabilityFlags {
    pub bounded: bool,
    pub unbounded: bool,
    pub rewindable: bool,
    pub transactional: bool,
    pub idempotent: bool,
}

// ── R4a Shuffle configs ────────────────────────────────────────────────────────

/// Configuration for a task that writes its output to the shuffle store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShuffleWriteConfig {
    /// Stage whose output is being written.
    pub stage_id: StageId,
    /// Total number of output partitions.
    pub num_partitions: usize,
    /// Column names used as hash partitioning keys. Empty = round-robin.
    pub key_columns: Vec<String>,
    /// Lease token for fencing.
    pub lease_token: u64,
}

/// Configuration for a task that reads its input from the shuffle store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShuffleReadConfig {
    /// Stage whose shuffle output to read from.
    pub stage_id: StageId,
    /// Partition index this task should read.
    pub partition_id: usize,
    /// Lease token for fencing.
    pub lease_token: u64,
}

/// Task contract inside a stage.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskSpec {
    task_id: TaskId,
    description: String,
    task_timeout_secs: Option<u64>,
    /// Capability flags declared by the source connector for this task, if known.
    pub source_capabilities: Option<ConnectorCapabilityFlags>,
    /// Capability flags declared by the sink connector for this task, if known.
    pub sink_capabilities: Option<ConnectorCapabilityFlags>,
    shuffle_write: Option<ShuffleWriteConfig>,
    shuffle_read: Option<ShuffleReadConfig>,
    /// SC10: per-task resource profile. When set, the executor reserves
    /// `task_cpus` CPU shares and `task_memory_bytes` of memory before
    /// running the task; the placement layer also uses this hint to
    /// avoid oversubscribing a slot. Defaults to `None` (executor
    /// uses its own slot capacity).
    resource_profile: Option<ResourceProfile>,
    /// Sink output contract description for terminal write tasks (e.g.
    /// `object-parquet-sink:<base_dir>:<dest>:mode=overwrite`). When set, the
    /// coordinator launches this task with `OutputContractKind::Sink` instead
    /// of the default inline-record-batches contract.
    sink_contract: Option<String>,
}

/// SC10: per-task resource profile.
///
/// Mirrors Spark 3.x's `ResourceProfile` (`task.cpus`, `task.memory`,
/// `executor.cpus`, `executor.memory`). For Phase 1 we model the
/// per-task fields only; executor-level fields are out of scope and
/// are reserved for a follow-up that adds dynamic-allocation
/// integration.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceProfile {
    /// CPU shares reserved for the task (1.0 = one full core).
    pub task_cpus: f64,
    /// Memory bytes reserved for the task.
    pub task_memory_bytes: u64,
}

impl ResourceProfile {
    /// Default profile: one full core, 1 GiB of memory.
    pub fn default_task() -> Self {
        Self {
            task_cpus: 1.0,
            task_memory_bytes: 1 << 30,
        }
    }
}

impl TaskSpec {
    /// Create a task spec.
    pub fn new(task_id: TaskId, description: impl Into<String>) -> Self {
        Self {
            task_id,
            description: description.into(),
            task_timeout_secs: None,
            source_capabilities: None,
            sink_capabilities: None,
            shuffle_write: None,
            shuffle_read: None,
            resource_profile: None,
            sink_contract: None,
        }
    }

    /// Attach a per-task execution timeout.
    #[must_use]
    pub fn with_task_timeout_secs(mut self, secs: u64) -> Self {
        self.task_timeout_secs = Some(secs);
        self
    }

    /// Attach source connector capability flags.
    #[must_use]
    pub fn with_source_capabilities(mut self, caps: ConnectorCapabilityFlags) -> Self {
        self.source_capabilities = Some(caps);
        self
    }

    /// Attach sink connector capability flags.
    #[must_use]
    pub fn with_sink_capabilities(mut self, caps: ConnectorCapabilityFlags) -> Self {
        self.sink_capabilities = Some(caps);
        self
    }

    /// Task id.
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Human-readable task description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Per-task execution timeout in seconds, if set.
    pub fn task_timeout_secs(&self) -> Option<u64> {
        self.task_timeout_secs
    }

    /// Attach a shuffle write configuration.
    #[must_use]
    pub fn with_shuffle_write(mut self, config: ShuffleWriteConfig) -> Self {
        self.shuffle_write = Some(config);
        self
    }

    /// Attach a shuffle read configuration.
    #[must_use]
    pub fn with_shuffle_read(mut self, config: ShuffleReadConfig) -> Self {
        self.shuffle_read = Some(config);
        self
    }

    /// Shuffle write configuration, if this task writes to the shuffle store.
    pub fn shuffle_write(&self) -> Option<&ShuffleWriteConfig> {
        self.shuffle_write.as_ref()
    }

    /// Shuffle read configuration, if this task reads from the shuffle store.
    pub fn shuffle_read(&self) -> Option<&ShuffleReadConfig> {
        self.shuffle_read.as_ref()
    }

    /// Attach a sink output contract description for a terminal write task.
    #[must_use]
    pub fn with_sink_contract(mut self, contract: impl Into<String>) -> Self {
        self.sink_contract = Some(contract.into());
        self
    }

    /// Sink output contract description, if this task writes to a sink.
    pub fn sink_contract(&self) -> Option<&str> {
        self.sink_contract.as_deref()
    }

    /// SC10: set the per-task resource profile.
    pub fn with_resource_profile(mut self, profile: ResourceProfile) -> Self {
        self.resource_profile = Some(profile);
        self
    }

    /// SC10: per-task resource profile, if set.
    pub fn resource_profile(&self) -> Option<&ResourceProfile> {
        self.resource_profile.as_ref()
    }
}

#[cfg(test)]
mod resource_profile_tests {
    use super::*;

    /// SC10: `ResourceProfile::default_task` is one full core, 1 GiB.
    #[test]
    fn default_task_is_one_core_one_gib() {
        let p = ResourceProfile::default_task();
        assert_eq!(p.task_cpus, 1.0);
        assert_eq!(p.task_memory_bytes, 1 << 30);
    }

    /// SC10: builder round-trips.
    #[test]
    fn task_spec_with_resource_profile_round_trips() {
        let spec = TaskSpec::new(TaskId::try_new("t1").unwrap(), "shuffle").with_resource_profile(
            ResourceProfile {
                task_cpus: 2.0,
                task_memory_bytes: 4 << 30,
            },
        );
        let p = spec.resource_profile().unwrap();
        assert_eq!(p.task_cpus, 2.0);
        assert_eq!(p.task_memory_bytes, 4 << 30);
    }
}
