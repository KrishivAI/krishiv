use std::collections::HashMap;

use krishiv_proto::{
    AttemptId, ConnectorCapabilityFlags, ExecutorDescriptor, ExecutorId, JobId, JobKind, JobSpec,
    JobState, StageId, StageSpec, StageState, TaskId, TaskOutputMetadata, TaskSpec, TaskState,
};
use serde::{Deserialize, Serialize};

use krishiv_shuffle::{ShuffleMetadata, ShufflePath};

use crate::{JobRecord, ResourceUsage, SchedulerError, SchedulerResult, StageRecord, TaskRecord};

/// Rotate the events NDJSON log once it exceeds this size (default 64 MiB).
/// Long-running streaming jobs can accumulate many task-state events between
/// `save_job` calls; rotation takes a full snapshot and clears the log to
/// prevent unbounded disk growth.
pub const MAX_EVENTS_LOG_BYTES: u64 = 64 * 1024 * 1024;

/// Approximate in-memory cost of a single [`EventLogEvent`] in bytes.
///
/// Used by the in-memory ring buffer to bound the events log. The number is a
/// conservative upper bound: the variant discriminant plus the largest field
/// (`TaskFailed::reason: String` heap allocation). The actual `String` content
/// is counted separately via [`EventLogEvent::approx_heap_bytes`].
const EVENT_BASE_BYTES: u64 = 96;

impl EventLogEvent {
    /// Approximate heap-allocated bytes owned by this event.
    ///
    /// `String` fields carry their own heap allocation that is not counted by
    /// [`std::mem::size_of`]. We approximate the byte cost by summing the
    /// UTF-8 length of every owned string plus the per-string allocation
    /// overhead. This is an over-estimate by design so the ring buffer evicts
    /// slightly before reaching the cap, not slightly after.
    pub fn approx_heap_bytes(&self) -> u64 {
        let str_cost = |s: &str| s.len() as u64 + 24;
        match self {
            EventLogEvent::JobSubmitted { job_id } => str_cost(job_id.as_str()),
            EventLogEvent::StagePlanned { job_id, stage_id } => {
                str_cost(job_id.as_str()) + str_cost(stage_id.as_str())
            }
            EventLogEvent::TaskAssigned {
                job_id,
                stage_id,
                task_id,
                executor_id,
            } => {
                str_cost(job_id.as_str())
                    + str_cost(stage_id.as_str())
                    + str_cost(task_id.as_str())
                    + str_cost(executor_id.as_str())
            }
            EventLogEvent::TaskStarted {
                job_id,
                stage_id,
                task_id,
                ..
            } => {
                str_cost(job_id.as_str()) + str_cost(stage_id.as_str()) + str_cost(task_id.as_str())
            }
            EventLogEvent::TaskSucceeded {
                job_id,
                stage_id,
                task_id,
                ..
            } => {
                str_cost(job_id.as_str()) + str_cost(stage_id.as_str()) + str_cost(task_id.as_str())
            }
            EventLogEvent::TaskFailed {
                job_id,
                stage_id,
                task_id,
                reason,
                ..
            } => {
                str_cost(job_id.as_str())
                    + str_cost(stage_id.as_str())
                    + str_cost(task_id.as_str())
                    + str_cost(reason)
            }
            EventLogEvent::ExecutorLost { executor_id } => str_cost(executor_id.as_str()),
            EventLogEvent::JobCancelled { job_id } => str_cost(job_id.as_str()),
        }
    }
}

/// Events written to the durable job event log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventLogEvent {
    /// A new job was accepted by the coordinator.
    JobSubmitted { job_id: JobId },
    /// Stage task graph was determined by the planner.
    StagePlanned { job_id: JobId, stage_id: StageId },
    /// A task was placed on an executor.
    TaskAssigned {
        job_id: JobId,
        stage_id: StageId,
        task_id: TaskId,
        executor_id: ExecutorId,
    },
    /// Executor reported a task as started (Running state).
    TaskStarted {
        job_id: JobId,
        stage_id: StageId,
        task_id: TaskId,
        attempt: AttemptId,
    },
    /// Executor reported a task as completed.
    TaskSucceeded {
        job_id: JobId,
        stage_id: StageId,
        task_id: TaskId,
        attempt: AttemptId,
    },
    /// Executor reported a task as failed or the coordinator timed it out.
    TaskFailed {
        job_id: JobId,
        stage_id: StageId,
        task_id: TaskId,
        attempt: AttemptId,
        reason: String,
    },
    /// Executor missed heartbeats and was marked lost.
    ExecutorLost { executor_id: ExecutorId },
    /// Job was cancelled by an operator or user request.
    JobCancelled { job_id: JobId },
}

pub trait MetadataStore: Send + Sync {
    fn append_event(&mut self, event: EventLogEvent) -> SchedulerResult<()>;
    fn events(&self) -> &[EventLogEvent];
    fn save_job(&mut self, record: &JobRecord) -> SchedulerResult<()>;
    fn jobs(&self) -> &[JobRecord];

    /// Persist an executor descriptor so it survives coordinator restarts (R10).
    ///
    /// On recovery, executors reloaded from the store are re-registered in the
    /// `ExecutorRegistry` so re-attaching executors are recognised without a
    /// fresh registration handshake.
    fn save_executor(&mut self, descriptor: &ExecutorDescriptor) -> SchedulerResult<()>;

    /// Return all persisted executor descriptors.
    fn executors(&self) -> Vec<ExecutorDescriptor>;

    /// Remove a persisted executor descriptor (called on clean deregister).
    fn remove_executor(&mut self, executor_id: &ExecutorId) -> SchedulerResult<()>;
}

// ── InMemoryMetadataStore ─────────────────────────────────────────────────────

/// In-memory metadata store for tests and embedded single-process deployments.
///
/// The event log is bounded by [`MAX_EVENTS_LOG_BYTES`]. When an append would
/// push the total over the cap, the oldest events are evicted (FIFO) until
/// the new event fits. Eviction is the in-memory analogue of the on-disk
/// rotation that `JsonFileMetadataStore` performs: in both cases the events
/// log stops growing unboundedly, and the durability of the snapshot
/// (`save_job`/`save_executor`) is unaffected because those are kept in
/// separate fields and are never evicted.
///
/// Eviction is O(n) per removed event (`Vec::remove(0)` shifts the tail) but
/// is amortized O(1) per appended event because it only fires when the buffer
/// is full (every ~[`MAX_EVENTS_LOG_BYTES`] / avg_event_size appends).
#[derive(Debug)]
pub struct InMemoryMetadataStore {
    events: Vec<EventLogEvent>,
    events_byte_size: u64,
    jobs: Vec<JobRecord>,
    executors: Vec<ExecutorDescriptor>,
    /// Number of events evicted by the ring buffer since the store was created.
    /// Exposed via [`InMemoryMetadataStore::evicted_event_count`] for tests and
    /// metrics.
    evicted_event_count: u64,
}

impl Default for InMemoryMetadataStore {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            events_byte_size: 0,
            jobs: Vec::new(),
            executors: Vec::new(),
            evicted_event_count: 0,
        }
    }
}

impl InMemoryMetadataStore {
    /// Number of oldest events evicted by the ring buffer to keep the events
    /// log under [`MAX_EVENTS_LOG_BYTES`].
    pub fn evicted_event_count(&self) -> u64 {
        self.evicted_event_count
    }

    /// Current approximate byte size of the in-memory events log.
    pub fn events_byte_size(&self) -> u64 {
        self.events_byte_size
    }

    fn evict_until_fits(&mut self, incoming_bytes: u64) {
        while self.events_byte_size + incoming_bytes > MAX_EVENTS_LOG_BYTES
            && !self.events.is_empty()
        {
            let oldest = self.events.remove(0);
            self.events_byte_size = self
                .events_byte_size
                .saturating_sub(EVENT_BASE_BYTES + oldest.approx_heap_bytes());
            self.evicted_event_count = self.evicted_event_count.wrapping_add(1);
        }
    }
}

impl MetadataStore for InMemoryMetadataStore {
    fn append_event(&mut self, event: EventLogEvent) -> SchedulerResult<()> {
        let incoming = EVENT_BASE_BYTES + event.approx_heap_bytes();
        self.evict_until_fits(incoming);
        self.events_byte_size = self.events_byte_size.saturating_add(incoming);
        self.events.push(event);
        Ok(())
    }
    fn events(&self) -> &[EventLogEvent] {
        &self.events
    }
    fn save_job(&mut self, record: &JobRecord) -> SchedulerResult<()> {
        if let Some(e) = self.jobs.iter_mut().find(|j| j.job_id() == record.job_id()) {
            *e = record.clone();
        } else {
            self.jobs.push(record.clone());
        }
        Ok(())
    }
    fn jobs(&self) -> &[JobRecord] {
        &self.jobs
    }
    fn save_executor(&mut self, descriptor: &ExecutorDescriptor) -> SchedulerResult<()> {
        let id = descriptor.executor_id();
        if let Some(e) = self.executors.iter_mut().find(|d| d.executor_id() == id) {
            *e = descriptor.clone();
        } else {
            self.executors.push(descriptor.clone());
        }
        Ok(())
    }
    fn executors(&self) -> Vec<ExecutorDescriptor> {
        self.executors.clone()
    }
    fn remove_executor(&mut self, executor_id: &ExecutorId) -> SchedulerResult<()> {
        self.executors.retain(|d| d.executor_id() != executor_id);
        Ok(())
    }
}

#[cfg(feature = "etcd")]
const JSON_METADATA_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistedExecutorDescriptor {
    executor_id: String,
    host: String,
    slots: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    task_endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    barrier_endpoint: Option<String>,
}

impl From<&ExecutorDescriptor> for PersistedExecutorDescriptor {
    fn from(d: &ExecutorDescriptor) -> Self {
        Self {
            executor_id: d.executor_id().as_str().to_string(),
            host: d.host().to_string(),
            slots: d.slots(),
            task_endpoint: d.task_endpoint().map(str::to_string),
            barrier_endpoint: d.barrier_endpoint().map(str::to_string),
        }
    }
}

impl TryFrom<PersistedExecutorDescriptor> for ExecutorDescriptor {
    type Error = SchedulerError;
    fn try_from(p: PersistedExecutorDescriptor) -> SchedulerResult<Self> {
        let executor_id =
            ExecutorId::try_new(p.executor_id).map_err(|e| SchedulerError::Transport {
                message: format!("invalid executor_id in metadata store: {e}"),
            })?;
        let mut d = ExecutorDescriptor::new(executor_id, p.host, p.slots);
        if let Some(ep) = p.task_endpoint {
            d = d.with_task_endpoint(ep);
        }
        if let Some(ep) = p.barrier_endpoint {
            d = d.with_barrier_endpoint(ep);
        }
        Ok(d)
    }
}

#[cfg(feature = "etcd")]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedMetadata {
    #[serde(default = "default_json_metadata_schema_version")]
    schema_version: u32,
    #[serde(default = "default_json_metadata_store_kind")]
    store_kind: String,
    pub(crate) events: Vec<PersistedEvent>,
    pub(crate) jobs: Vec<PersistedJobRecord>,
    #[serde(default)]
    pub(crate) executor_descriptors: Vec<PersistedExecutorDescriptor>,
}

#[cfg(feature = "etcd")]
impl PersistedMetadata {
    pub(crate) fn validate_schema_version(&self) -> SchedulerResult<()> {
        if self.schema_version > JSON_METADATA_SCHEMA_VERSION {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "metadata store schema version {} is newer than supported version {}",
                    self.schema_version, JSON_METADATA_SCHEMA_VERSION
                ),
            });
        }
        Ok(())
    }
}

#[cfg(feature = "etcd")]
fn default_json_metadata_schema_version() -> u32 {
    JSON_METADATA_SCHEMA_VERSION
}

#[cfg(feature = "etcd")]
fn default_json_metadata_store_kind() -> String {
    String::from("krishiv.scheduler.metadata")
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum PersistedEvent {
    JobSubmitted {
        job_id: String,
    },
    StagePlanned {
        job_id: String,
        stage_id: String,
    },
    TaskAssigned {
        job_id: String,
        stage_id: String,
        task_id: String,
        executor_id: String,
    },
    TaskStarted {
        job_id: String,
        stage_id: String,
        task_id: String,
        attempt: u32,
    },
    TaskSucceeded {
        job_id: String,
        stage_id: String,
        task_id: String,
        attempt: u32,
    },
    TaskFailed {
        job_id: String,
        stage_id: String,
        task_id: String,
        attempt: u32,
        reason: String,
    },
    ExecutorLost {
        executor_id: String,
    },
    JobCancelled {
        job_id: String,
    },
}

impl From<&EventLogEvent> for PersistedEvent {
    fn from(value: &EventLogEvent) -> Self {
        match value {
            EventLogEvent::JobSubmitted { job_id } => Self::JobSubmitted {
                job_id: job_id.to_string(),
            },
            EventLogEvent::StagePlanned { job_id, stage_id } => Self::StagePlanned {
                job_id: job_id.to_string(),
                stage_id: stage_id.to_string(),
            },
            EventLogEvent::TaskAssigned {
                job_id,
                stage_id,
                task_id,
                executor_id,
            } => Self::TaskAssigned {
                job_id: job_id.to_string(),
                stage_id: stage_id.to_string(),
                task_id: task_id.to_string(),
                executor_id: executor_id.to_string(),
            },
            EventLogEvent::TaskStarted {
                job_id,
                stage_id,
                task_id,
                attempt,
            } => Self::TaskStarted {
                job_id: job_id.to_string(),
                stage_id: stage_id.to_string(),
                task_id: task_id.to_string(),
                attempt: attempt.as_u32(),
            },
            EventLogEvent::TaskSucceeded {
                job_id,
                stage_id,
                task_id,
                attempt,
            } => Self::TaskSucceeded {
                job_id: job_id.to_string(),
                stage_id: stage_id.to_string(),
                task_id: task_id.to_string(),
                attempt: attempt.as_u32(),
            },
            EventLogEvent::TaskFailed {
                job_id,
                stage_id,
                task_id,
                attempt,
                reason,
            } => Self::TaskFailed {
                job_id: job_id.to_string(),
                stage_id: stage_id.to_string(),
                task_id: task_id.to_string(),
                attempt: attempt.as_u32(),
                reason: reason.clone(),
            },
            EventLogEvent::ExecutorLost { executor_id } => Self::ExecutorLost {
                executor_id: executor_id.to_string(),
            },
            EventLogEvent::JobCancelled { job_id } => Self::JobCancelled {
                job_id: job_id.to_string(),
            },
        }
    }
}

impl TryFrom<PersistedEvent> for EventLogEvent {
    type Error = SchedulerError;

    fn try_from(value: PersistedEvent) -> SchedulerResult<Self> {
        Ok(match value {
            PersistedEvent::JobSubmitted { job_id } => Self::JobSubmitted {
                job_id: JobId::try_new(job_id).map_err(invalid_metadata_id)?,
            },
            PersistedEvent::StagePlanned { job_id, stage_id } => Self::StagePlanned {
                job_id: JobId::try_new(job_id).map_err(invalid_metadata_id)?,
                stage_id: StageId::try_new(stage_id).map_err(invalid_metadata_id)?,
            },
            PersistedEvent::TaskAssigned {
                job_id,
                stage_id,
                task_id,
                executor_id,
            } => Self::TaskAssigned {
                job_id: JobId::try_new(job_id).map_err(invalid_metadata_id)?,
                stage_id: StageId::try_new(stage_id).map_err(invalid_metadata_id)?,
                task_id: TaskId::try_new(task_id).map_err(invalid_metadata_id)?,
                executor_id: ExecutorId::try_new(executor_id).map_err(invalid_metadata_id)?,
            },
            PersistedEvent::TaskStarted {
                job_id,
                stage_id,
                task_id,
                attempt,
            } => Self::TaskStarted {
                job_id: JobId::try_new(job_id).map_err(invalid_metadata_id)?,
                stage_id: StageId::try_new(stage_id).map_err(invalid_metadata_id)?,
                task_id: TaskId::try_new(task_id).map_err(invalid_metadata_id)?,
                attempt: AttemptId::try_new(attempt).map_err(invalid_metadata_id)?,
            },
            PersistedEvent::TaskSucceeded {
                job_id,
                stage_id,
                task_id,
                attempt,
            } => Self::TaskSucceeded {
                job_id: JobId::try_new(job_id).map_err(invalid_metadata_id)?,
                stage_id: StageId::try_new(stage_id).map_err(invalid_metadata_id)?,
                task_id: TaskId::try_new(task_id).map_err(invalid_metadata_id)?,
                attempt: AttemptId::try_new(attempt).map_err(invalid_metadata_id)?,
            },
            PersistedEvent::TaskFailed {
                job_id,
                stage_id,
                task_id,
                attempt,
                reason,
            } => Self::TaskFailed {
                job_id: JobId::try_new(job_id).map_err(invalid_metadata_id)?,
                stage_id: StageId::try_new(stage_id).map_err(invalid_metadata_id)?,
                task_id: TaskId::try_new(task_id).map_err(invalid_metadata_id)?,
                attempt: AttemptId::try_new(attempt).map_err(invalid_metadata_id)?,
                reason,
            },
            PersistedEvent::ExecutorLost { executor_id } => Self::ExecutorLost {
                executor_id: ExecutorId::try_new(executor_id).map_err(invalid_metadata_id)?,
            },
            PersistedEvent::JobCancelled { job_id } => Self::JobCancelled {
                job_id: JobId::try_new(job_id).map_err(invalid_metadata_id)?,
            },
        })
    }
}

pub(crate) fn invalid_metadata_id(error: krishiv_proto::IdError) -> SchedulerError {
    SchedulerError::InvalidJob {
        message: format!("invalid persisted metadata id: {error}"),
    }
}

/// Persisted shuffle partition availability entry.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedShufflePartition {
    pub(crate) stage_id: String,
    pub(crate) partition_id: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedJobRecord {
    pub(crate) spec: PersistedJobSpec,
    pub(crate) state: String,
    pub(crate) max_stage_retries: u32,
    pub(crate) stages: Vec<PersistedStageRecord>,
    /// Accumulated resource consumption. `None` in records written before R7.1.
    #[serde(default)]
    pub(crate) resource_usage: Option<ResourceUsage>,
    /// Available shuffle partitions by stage.  Absent in records before this field was added.
    #[serde(default)]
    pub(crate) shuffle_output: Vec<PersistedShufflePartition>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedStageRecord {
    pub(crate) spec: PersistedStageSpec,
    pub(crate) state: String,
    pub(crate) retry_count: u32,
    pub(crate) tasks: Vec<PersistedTaskRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedTaskRecord {
    pub(crate) spec: PersistedTaskSpec,
    pub(crate) state: String,
    pub(crate) assigned_executor: Option<String>,
    pub(crate) attempt: u32,
    pub(crate) output_metadata: Option<PersistedTaskOutputMetadata>,
    pub(crate) last_failure_reason: Option<String>,
    /// Track consecutive failures so retry budgets survive coordinator restart.
    /// Defaults to 0 for records written before this field was added (backward compatible).
    #[serde(default)]
    pub(crate) failure_count: u32,
    /// Number of times this task's executor was lost and the task rescheduled.
    /// Defaults to 0 for records written before this field was added.
    #[serde(default)]
    pub(crate) executor_loss_count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedJobSpec {
    pub(crate) job_id: String,
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) stages: Vec<PersistedStageSpec>,
    /// R7.1 fields — absent in records written before R7.1 (backward compatible).
    #[serde(default)]
    pub(crate) priority: Option<u8>,
    #[serde(default)]
    pub(crate) namespace_id: Option<String>,
    #[serde(default)]
    pub(crate) cpu_limit_nanos: Option<u64>,
    #[serde(default)]
    pub(crate) memory_limit_bytes: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedStageSpec {
    pub(crate) stage_id: String,
    pub(crate) name: String,
    pub(crate) tasks: Vec<PersistedTaskSpec>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedTaskSpec {
    pub(crate) task_id: String,
    pub(crate) description: String,
    pub(crate) task_timeout_secs: Option<u64>,
    pub(crate) source_capabilities: Option<PersistedConnectorCapabilities>,
    pub(crate) sink_capabilities: Option<PersistedConnectorCapabilities>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedConnectorCapabilities {
    pub(crate) bounded: bool,
    pub(crate) unbounded: bool,
    pub(crate) rewindable: bool,
    pub(crate) transactional: bool,
    pub(crate) idempotent: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedTaskOutputMetadata {
    pub(crate) output_kind: String,
    pub(crate) row_count: u64,
    pub(crate) batch_count: u64,
    pub(crate) column_count: u64,
}

impl From<&JobRecord> for PersistedJobRecord {
    fn from(value: &JobRecord) -> Self {
        // Serialize only Available partitions (Pending/Failed are transient).
        let shuffle_output: Vec<PersistedShufflePartition> = value
            .shuffle_output
            .iter()
            .flat_map(|(stage_id, meta)| {
                meta.available_paths()
                    .into_iter()
                    .map(|path| PersistedShufflePartition {
                        stage_id: stage_id.to_string(),
                        partition_id: path.partition_id,
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        Self {
            spec: PersistedJobSpec::from(&value.spec),
            state: value.state.to_string(),
            max_stage_retries: value.max_stage_retries,
            stages: value
                .stages
                .iter()
                .map(PersistedStageRecord::from)
                .collect(),
            resource_usage: Some(value.resource_usage.clone()),
            shuffle_output,
        }
    }
}

impl TryFrom<PersistedJobRecord> for JobRecord {
    type Error = SchedulerError;

    fn try_from(value: PersistedJobRecord) -> SchedulerResult<Self> {
        // Rebuild shuffle_output from persisted Available partitions.
        let job_id = value.spec.job_id.clone();
        let mut shuffle_output: HashMap<krishiv_proto::StageId, ShuffleMetadata> = HashMap::new();
        for p in value.shuffle_output {
            let stage_id =
                krishiv_proto::StageId::try_new(p.stage_id.clone()).map_err(invalid_metadata_id)?;
            let meta = shuffle_output.entry(stage_id).or_default();
            let path = ShufflePath {
                job_id: job_id.clone(),
                stage_id: p.stage_id,
                partition_id: p.partition_id,
            };
            meta.mark_available(&path);
        }
        Ok(Self {
            spec: JobSpec::try_from(value.spec)?,
            state: parse_job_state(&value.state)?,
            max_stage_retries: value.max_stage_retries,
            stages: value
                .stages
                .into_iter()
                .map(StageRecord::try_from)
                .collect::<SchedulerResult<Vec<_>>>()?,
            shuffle_output,
            resource_usage: value.resource_usage.unwrap_or_default(),
        })
    }
}

impl From<&StageRecord> for PersistedStageRecord {
    fn from(value: &StageRecord) -> Self {
        Self {
            spec: PersistedStageSpec::from(&value.spec),
            state: value.state.to_string(),
            retry_count: value.retry_count,
            tasks: value.tasks.iter().map(PersistedTaskRecord::from).collect(),
        }
    }
}

impl TryFrom<PersistedStageRecord> for StageRecord {
    type Error = SchedulerError;

    fn try_from(value: PersistedStageRecord) -> SchedulerResult<Self> {
        Ok(Self {
            spec: StageSpec::try_from(value.spec)?,
            state: parse_stage_state(&value.state)?,
            retry_count: value.retry_count,
            tasks: value
                .tasks
                .into_iter()
                .map(TaskRecord::try_from)
                .collect::<SchedulerResult<Vec<_>>>()?,
        })
    }
}

impl From<&TaskRecord> for PersistedTaskRecord {
    fn from(value: &TaskRecord) -> Self {
        Self {
            spec: PersistedTaskSpec::from(&value.spec),
            state: value.state.to_string(),
            assigned_executor: value.assigned_executor.as_ref().map(ToString::to_string),
            attempt: value.attempt,
            output_metadata: value
                .output_metadata
                .as_ref()
                .map(PersistedTaskOutputMetadata::from),
            last_failure_reason: value.last_failure_reason.clone(),
            failure_count: value.failure_count,
            executor_loss_count: value.executor_loss_count,
        }
    }
}

impl TryFrom<PersistedTaskRecord> for TaskRecord {
    type Error = SchedulerError;

    fn try_from(value: PersistedTaskRecord) -> SchedulerResult<Self> {
        Ok(Self {
            spec: TaskSpec::try_from(value.spec)?,
            state: parse_task_state(&value.state)?,
            assigned_executor: value
                .assigned_executor
                .map(ExecutorId::try_new)
                .transpose()
                .map_err(invalid_metadata_id)?,
            attempt: value.attempt,
            launch_in_flight: false,
            output_metadata: value.output_metadata.map(TaskOutputMetadata::from),
            last_failure_reason: value.last_failure_reason,
            failure_count: value.failure_count,
            executor_loss_count: value.executor_loss_count,
            // Streaming state is not persisted in R5.1; executors re-report it on re-attach.
            last_watermark_ms: None,
            last_source_offset: None,
            assigned_at_ms: None,
        })
    }
}

impl From<&JobSpec> for PersistedJobSpec {
    fn from(value: &JobSpec) -> Self {
        Self {
            job_id: value.job_id().to_string(),
            name: value.name().to_owned(),
            kind: value.kind().to_string(),
            stages: value
                .stages()
                .iter()
                .map(PersistedStageSpec::from)
                .collect(),
            priority: Some(value.priority()),
            namespace_id: value.namespace_id().map(str::to_owned),
            cpu_limit_nanos: value.cpu_limit_nanos(),
            memory_limit_bytes: value.memory_limit_bytes(),
        }
    }
}

impl TryFrom<PersistedJobSpec> for JobSpec {
    type Error = SchedulerError;

    fn try_from(value: PersistedJobSpec) -> SchedulerResult<Self> {
        let mut spec = JobSpec::new(
            JobId::try_new(value.job_id).map_err(invalid_metadata_id)?,
            value.name,
            parse_job_kind(&value.kind)?,
        );
        for stage in value.stages {
            spec = spec.with_stage(StageSpec::try_from(stage)?);
        }
        if let Some(p) = value.priority {
            spec = spec.with_priority(p);
        }
        if let Some(ns) = value.namespace_id {
            spec = spec.with_namespace(ns);
        }
        if let Some(cpu) = value.cpu_limit_nanos {
            spec = spec.with_cpu_limit_nanos(cpu);
        }
        if let Some(mem) = value.memory_limit_bytes {
            spec = spec.with_memory_limit_bytes(mem);
        }
        Ok(spec)
    }
}

impl From<&StageSpec> for PersistedStageSpec {
    fn from(value: &StageSpec) -> Self {
        Self {
            stage_id: value.stage_id().to_string(),
            name: value.name().to_owned(),
            tasks: value.tasks().iter().map(PersistedTaskSpec::from).collect(),
        }
    }
}

impl TryFrom<PersistedStageSpec> for StageSpec {
    type Error = SchedulerError;

    fn try_from(value: PersistedStageSpec) -> SchedulerResult<Self> {
        let mut spec = StageSpec::new(
            StageId::try_new(value.stage_id).map_err(invalid_metadata_id)?,
            value.name,
        );
        for task in value.tasks {
            spec = spec.with_task(TaskSpec::try_from(task)?);
        }
        Ok(spec)
    }
}

impl From<&TaskSpec> for PersistedTaskSpec {
    fn from(value: &TaskSpec) -> Self {
        Self {
            task_id: value.task_id().to_string(),
            description: value.description().to_owned(),
            task_timeout_secs: value.task_timeout_secs(),
            source_capabilities: value
                .source_capabilities
                .as_ref()
                .map(PersistedConnectorCapabilities::from),
            sink_capabilities: value
                .sink_capabilities
                .as_ref()
                .map(PersistedConnectorCapabilities::from),
        }
    }
}

impl TryFrom<PersistedTaskSpec> for TaskSpec {
    type Error = SchedulerError;

    fn try_from(value: PersistedTaskSpec) -> SchedulerResult<Self> {
        let mut spec = TaskSpec::new(
            TaskId::try_new(value.task_id).map_err(invalid_metadata_id)?,
            value.description,
        );
        if let Some(secs) = value.task_timeout_secs {
            spec = spec.with_task_timeout_secs(secs);
        }
        if let Some(caps) = value.source_capabilities {
            spec = spec.with_source_capabilities(ConnectorCapabilityFlags::from(caps));
        }
        if let Some(caps) = value.sink_capabilities {
            spec = spec.with_sink_capabilities(ConnectorCapabilityFlags::from(caps));
        }
        Ok(spec)
    }
}

impl From<&ConnectorCapabilityFlags> for PersistedConnectorCapabilities {
    fn from(value: &ConnectorCapabilityFlags) -> Self {
        Self {
            bounded: value.bounded,
            unbounded: value.unbounded,
            rewindable: value.rewindable,
            transactional: value.transactional,
            idempotent: value.idempotent,
        }
    }
}

impl From<PersistedConnectorCapabilities> for ConnectorCapabilityFlags {
    fn from(value: PersistedConnectorCapabilities) -> Self {
        Self {
            bounded: value.bounded,
            unbounded: value.unbounded,
            rewindable: value.rewindable,
            transactional: value.transactional,
            idempotent: value.idempotent,
        }
    }
}

impl From<&TaskOutputMetadata> for PersistedTaskOutputMetadata {
    fn from(value: &TaskOutputMetadata) -> Self {
        Self {
            output_kind: value.output_kind().to_owned(),
            row_count: value.row_count(),
            batch_count: value.batch_count(),
            column_count: value.column_count(),
        }
    }
}

impl From<PersistedTaskOutputMetadata> for TaskOutputMetadata {
    fn from(value: PersistedTaskOutputMetadata) -> Self {
        Self::new(
            value.output_kind,
            value.row_count,
            value.batch_count,
            value.column_count,
        )
    }
}

pub(crate) fn parse_job_kind(value: &str) -> SchedulerResult<JobKind> {
    match value {
        "batch" => Ok(JobKind::Batch),
        "streaming" => Ok(JobKind::Streaming),
        other => Err(SchedulerError::InvalidJob {
            message: format!("unknown persisted job kind: {other}"),
        }),
    }
}

pub(crate) fn parse_job_state(value: &str) -> SchedulerResult<JobState> {
    match value {
        "accepted" => Ok(JobState::Accepted),
        "planning" => Ok(JobState::Planning),
        "running" => Ok(JobState::Running),
        "succeeded" => Ok(JobState::Succeeded),
        "failed" => Ok(JobState::Failed),
        "cancelled" => Ok(JobState::Cancelled),
        other => Err(SchedulerError::InvalidJob {
            message: format!("unknown persisted job state: {other}"),
        }),
    }
}

pub(crate) fn parse_stage_state(value: &str) -> SchedulerResult<StageState> {
    match value {
        "pending" => Ok(StageState::Pending),
        "scheduling" => Ok(StageState::Scheduling),
        "running" => Ok(StageState::Running),
        "succeeded" => Ok(StageState::Succeeded),
        "failed" => Ok(StageState::Failed),
        "retrying" => Ok(StageState::Retrying),
        "cancelled" => Ok(StageState::Cancelled),
        other => Err(SchedulerError::InvalidJob {
            message: format!("unknown persisted stage state: {other}"),
        }),
    }
}

pub(crate) fn parse_task_state(value: &str) -> SchedulerResult<TaskState> {
    match value {
        "pending" => Ok(TaskState::Pending),
        "assigned" => Ok(TaskState::Assigned),
        "running" => Ok(TaskState::Running),
        "succeeded" => Ok(TaskState::Succeeded),
        "failed" => Ok(TaskState::Failed),
        "retrying" => Ok(TaskState::Retrying),
        "cancelled" => Ok(TaskState::Cancelled),
        other => Err(SchedulerError::InvalidJob {
            message: format!("unknown persisted task state: {other}"),
        }),
    }
}

/// Serialize coordinator metadata for etcd or other blob stores.
#[cfg(feature = "etcd")]
pub(crate) fn encode_metadata_snapshot(
    events: &[EventLogEvent],
    jobs: &[JobRecord],
) -> SchedulerResult<Vec<u8>> {
    encode_metadata_snapshot_with_executors(events, jobs, &[])
}

/// Like [`encode_metadata_snapshot`] but also includes executor descriptors.
#[cfg(feature = "etcd")]
pub(crate) fn encode_metadata_snapshot_with_executors(
    events: &[EventLogEvent],
    jobs: &[JobRecord],
    executors: &[krishiv_proto::ExecutorDescriptor],
) -> SchedulerResult<Vec<u8>> {
    let persisted = PersistedMetadata {
        schema_version: JSON_METADATA_SCHEMA_VERSION,
        store_kind: String::from("krishiv.scheduler.metadata"),
        events: events.iter().map(PersistedEvent::from).collect(),
        jobs: jobs.iter().map(PersistedJobRecord::from).collect(),
        executor_descriptors: executors
            .iter()
            .map(PersistedExecutorDescriptor::from)
            .collect(),
    };
    serde_json::to_vec_pretty(&persisted).map_err(|error| SchedulerError::Transport {
        message: format!("failed to encode metadata snapshot: {error}"),
    })
}

/// Commands sent from the coordinator to the background store writer.
#[derive(Debug)]
enum StoreCommand {
    AppendEvent(EventLogEvent),
    SaveJob(Box<JobRecord>),
    Flush(tokio::sync::oneshot::Sender<()>),
}

/// Non-blocking handle to a [`MetadataStore`].
///
/// Writes are sent through a bounded channel to a background task so the
/// coordinator lock is released immediately (backpressured at capacity).
/// In synchronous contexts (unit tests, startup), writes happen inline on the
/// calling thread.
///
/// Use [`NonBlockingStoreHandle::flush`] to wait for all pending async writes
/// (useful on graceful shutdown).
/// Use [`NonBlockingStoreHandle::inner`] for reads (takes the mutex directly).
#[derive(Clone)]
pub struct NonBlockingStoreHandle {
    inner: std::sync::Arc<std::sync::Mutex<dyn MetadataStore + 'static>>,
    /// Bounded sender; `None` when no Tokio runtime was available at construction.
    tx: Option<tokio::sync::mpsc::Sender<StoreCommand>>,
    /// When true, never drop writes on channel backpressure — fall back to sync write.
    fail_closed_writes: bool,
}

impl std::fmt::Debug for NonBlockingStoreHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NonBlockingStoreHandle")
            .field("async_mode", &self.tx.is_some())
            .field("fail_closed_writes", &self.fail_closed_writes)
            .finish_non_exhaustive()
    }
}

/// Default channel capacity: 1024 pending writes before backpressure applies.
const STORE_CHANNEL_CAPACITY: usize = 1024;

impl NonBlockingStoreHandle {
    /// Create a handle backed by `store`.
    ///
    /// If a Tokio runtime is running, a background drain task is spawned and
    /// writes become non-blocking.  Otherwise writes happen synchronously in
    /// the calling thread (safe for unit tests and startup code).
    pub fn new(store: impl MetadataStore + 'static) -> Self {
        let inner: std::sync::Arc<std::sync::Mutex<dyn MetadataStore + 'static>> =
            std::sync::Arc::new(std::sync::Mutex::new(store));

        // Only spawn the background task when a Tokio runtime is available.
        let tx = if tokio::runtime::Handle::try_current().is_ok() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<StoreCommand>(STORE_CHANNEL_CAPACITY);
            let bg_store = std::sync::Arc::clone(&inner);
            let in_flight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let notify = std::sync::Arc::new(tokio::sync::Notify::new());
            tokio::spawn(async move {
                while let Some(cmd) = rx.recv().await {
                    match cmd {
                        StoreCommand::AppendEvent(event) => {
                            in_flight.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            let bg = std::sync::Arc::clone(&bg_store);
                            let in_flight_done = std::sync::Arc::clone(&in_flight);
                            let notify_done = std::sync::Arc::clone(&notify);
                            tokio::task::spawn_blocking(move || {
                                let mut guard = bg.lock().unwrap_or_else(|p| p.into_inner());
                                if let Err(e) = guard.append_event(event) {
                                    tracing::error!(
                                        error = %e,
                                        "NonBlockingStoreHandle: append_event failed"
                                    );
                                }
                                in_flight_done.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                notify_done.notify_one();
                            })
                            .await
                            .ok();
                        }
                        StoreCommand::SaveJob(record) => {
                            in_flight.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            let bg = std::sync::Arc::clone(&bg_store);
                            let in_flight_done = std::sync::Arc::clone(&in_flight);
                            let notify_done = std::sync::Arc::clone(&notify);
                            tokio::task::spawn_blocking(move || {
                                let mut guard = bg.lock().unwrap_or_else(|p| p.into_inner());
                                if let Err(e) = guard.save_job(&record) {
                                    tracing::error!(
                                        error = %e,
                                        "NonBlockingStoreHandle: save_job failed"
                                    );
                                }
                                in_flight_done.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                notify_done.notify_one();
                            })
                            .await
                            .ok();
                        }
                        StoreCommand::Flush(reply) => {
                            // Wait for all in-flight spawn_blocking tasks to complete.
                            // Uses Notify wakeups set by each task on completion so
                            // the loop only wakes when real progress is made.
                            loop {
                                if in_flight.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                                    break;
                                }
                                notify.notified().await;
                            }
                            let _ = reply.send(());
                        }
                    }
                }
            });
            Some(tx)
        } else {
            None
        };

        Self {
            inner,
            tx,
            fail_closed_writes: false,
        }
    }

    /// Require that metadata writes are never dropped under backpressure.
    #[must_use]
    pub fn with_fail_closed_writes(mut self, fail_closed: bool) -> Self {
        self.fail_closed_writes = fail_closed;
        self
    }

    fn append_event_sync(&self, event: EventLogEvent) {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Err(e) = guard.append_event(event) {
            tracing::error!(error = %e, "NonBlockingStoreHandle: append_event failed (sync fallback)");
        }
    }

    fn save_job_sync(&self, record: &JobRecord) {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Err(e) = guard.save_job(record) {
            tracing::error!(error = %e, "NonBlockingStoreHandle: save_job failed (sync fallback)");
        }
    }

    /// Access the underlying store for reads (blocks on mutex).
    pub fn inner(&self) -> std::sync::MutexGuard<'_, dyn MetadataStore + 'static> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Enqueue an event write (sync, uses `try_send`).
    ///
    /// When the bounded channel is full and [`Self::with_fail_closed_writes`] is
    /// enabled, the write is performed synchronously instead of being dropped.
    /// Async callers should prefer [`Self::append_event_async`].
    pub fn append_event(&self, event: EventLogEvent) {
        match &self.tx {
            Some(tx) => match tx.try_send(StoreCommand::AppendEvent(event)) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(StoreCommand::AppendEvent(
                    event,
                ))) => {
                    if self.fail_closed_writes {
                        tracing::warn!(
                            "NonBlockingStoreHandle: channel full; performing synchronous append_event"
                        );
                        self.append_event_sync(event);
                    } else {
                        tracing::warn!(
                            "NonBlockingStoreHandle: append_event dropped (channel full, {} pending)",
                            tx.max_capacity()
                        );
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    tracing::error!("NonBlockingStoreHandle: store background task dropped");
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
            },
            None => self.append_event_sync(event),
        }
    }

    /// Enqueue a job save (sync, uses `try_send`).
    ///
    /// When the bounded channel is full and fail-closed mode is enabled, the
    /// save is performed synchronously instead of being dropped.
    pub fn save_job(&self, record: &JobRecord) {
        match &self.tx {
            Some(tx) => match tx.try_send(StoreCommand::SaveJob(Box::new(record.clone()))) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(StoreCommand::SaveJob(
                    record,
                ))) => {
                    if self.fail_closed_writes {
                        tracing::warn!(
                            "NonBlockingStoreHandle: channel full; performing synchronous save_job"
                        );
                        self.save_job_sync(&record);
                    } else {
                        tracing::warn!(
                            "NonBlockingStoreHandle: save_job dropped (channel full, {} pending)",
                            tx.max_capacity()
                        );
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    tracing::error!("NonBlockingStoreHandle: store background task dropped");
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
            },
            None => self.save_job_sync(record),
        }
    }

    /// Async version of `append_event` with proper backpressure.
    ///
    /// Awaits until capacity is available in the bounded channel.
    pub async fn append_event_async(&self, event: EventLogEvent) {
        if let Some(ref tx) = self.tx {
            if tx.send(StoreCommand::AppendEvent(event)).await.is_err() {
                tracing::error!("NonBlockingStoreHandle: store background task dropped");
            }
        } else {
            self.append_event_sync(event);
        }
    }

    /// Async version of `save_job` with proper backpressure.
    ///
    /// Awaits until capacity is available in the bounded channel.
    pub async fn save_job_async(&self, record: &JobRecord) {
        if let Some(ref tx) = self.tx {
            if tx
                .send(StoreCommand::SaveJob(Box::new(record.clone())))
                .await
                .is_err()
            {
                tracing::error!("NonBlockingStoreHandle: store background task dropped");
            }
        } else {
            self.save_job_sync(record);
        }
    }

    /// Persist an executor descriptor (synchronous — executor registration is
    /// infrequent so inline locking is acceptable; R10).
    pub fn save_executor(&self, descriptor: &ExecutorDescriptor) {
        if let Err(e) = self.inner().save_executor(descriptor) {
            tracing::warn!(
                executor_id = %descriptor.executor_id().as_str(),
                error = %e,
                "failed to persist executor descriptor to metadata store"
            );
        }
    }

    /// Remove a persisted executor descriptor (synchronous; R10).
    pub fn remove_executor(&self, executor_id: &ExecutorId) {
        if let Err(e) = self.inner().remove_executor(executor_id) {
            tracing::warn!(
                executor_id = %executor_id.as_str(),
                error = %e,
                "failed to remove executor descriptor from metadata store"
            );
        }
    }

    /// Wait until all previously enqueued async writes have been processed.
    ///
    /// No-op in synchronous mode (writes already landed synchronously).
    pub async fn flush(&self) {
        if let Some(ref tx) = self.tx {
            let (reply_tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(StoreCommand::Flush(reply_tx)).await;
            let _ = rx.await;
        }
    }
}

/// Restore coordinator metadata from a serialized snapshot blob.
#[cfg(feature = "etcd")]
pub(crate) fn decode_metadata_snapshot(
    bytes: &[u8],
) -> SchedulerResult<(Vec<EventLogEvent>, Vec<JobRecord>)> {
    let (events, jobs, _executors) = decode_metadata_snapshot_with_executors(bytes)?;
    Ok((events, jobs))
}

/// Like [`decode_metadata_snapshot`] but also restores executor descriptors.
#[cfg(feature = "etcd")]
pub(crate) fn decode_metadata_snapshot_with_executors(
    bytes: &[u8],
) -> SchedulerResult<(
    Vec<EventLogEvent>,
    Vec<JobRecord>,
    Vec<krishiv_proto::ExecutorDescriptor>,
)> {
    if bytes.is_empty() {
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    }
    let persisted: PersistedMetadata =
        serde_json::from_slice(bytes).map_err(|error| SchedulerError::InvalidJob {
            message: format!("failed to decode metadata snapshot: {error}"),
        })?;
    persisted.validate_schema_version()?;
    let events = persisted
        .events
        .into_iter()
        .map(EventLogEvent::try_from)
        .collect::<SchedulerResult<Vec<_>>>()?;
    let jobs = persisted
        .jobs
        .into_iter()
        .map(JobRecord::try_from)
        .collect::<SchedulerResult<Vec<_>>>()?;
    let executors = persisted
        .executor_descriptors
        .into_iter()
        .filter_map(|p| krishiv_proto::ExecutorDescriptor::try_from(p).ok())
        .collect();
    Ok((events, jobs, executors))
}
