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
#[derive(Debug, Clone, PartialEq, Eq)]
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
}
