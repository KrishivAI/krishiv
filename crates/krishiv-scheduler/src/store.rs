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
            EventLogEvent::JobCompleted {
                job_id,
                final_state,
            } => str_cost(job_id.as_str()) + str_cost(final_state.as_str()),
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
    /// SC13: Job reached a terminal state (Succeeded or Failed).
    /// `final_state` is one of `"Succeeded"`, `"Failed"`, `"Killed"`.
    JobCompleted { job_id: JobId, final_state: String },
}

/// Durable snapshot of a continuous streaming job's window operator state.
///
/// Persisted after each drain cycle so a restarted session can resume
/// from where it left off without reprocessing already-aggregated events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuousSnapshot {
    /// Opaque serialized window operator state (from `ContinuousWindowExecutor::snapshot`).
    pub snapshot_bytes: Vec<u8>,
    /// Most recent event-time watermark at snapshot time (ms since Unix epoch).
    /// `i64::MIN` means no watermark has been observed yet.
    pub watermark_ms: i64,
}

impl ContinuousSnapshot {
    /// Encode as `watermark_ms:i64 LE | bytes_len:u32 LE | snapshot_bytes`.
    pub fn encode(&self) -> SchedulerResult<Vec<u8>> {
        let len = u32::try_from(self.snapshot_bytes.len()).map_err(|_| SchedulerError::Store {
            message: format!(
                "ContinuousSnapshot encode: snapshot too large ({} bytes, max {})",
                self.snapshot_bytes.len(),
                u32::MAX,
            ),
        })?;
        let mut out = Vec::with_capacity(12 + self.snapshot_bytes.len());
        out.extend_from_slice(&self.watermark_ms.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&self.snapshot_bytes);
        Ok(out)
    }

    /// Decode from bytes produced by [`encode`].
    pub fn decode(bytes: &[u8]) -> SchedulerResult<Self> {
        if bytes.len() < 12 {
            return Err(SchedulerError::Store {
                message: "ContinuousSnapshot decode: buffer too short".into(),
            });
        }
        let watermark_ms = i64::from_le_bytes(
            bytes
                .get(..8)
                .ok_or_else(|| SchedulerError::Store {
                    message: "ContinuousSnapshot decode: buffer too short for watermark".into(),
                })?
                .try_into()
                .map_err(|_| SchedulerError::Store {
                    message: "ContinuousSnapshot decode: watermark bytes invalid".into(),
                })?,
        );
        let len_bytes: [u8; 4] = bytes
            .get(8..12)
            .ok_or_else(|| SchedulerError::Store {
                message: "ContinuousSnapshot decode: buffer too short for len".into(),
            })?
            .try_into()
            .map_err(|_| SchedulerError::Store {
                message: "ContinuousSnapshot decode: len bytes invalid".into(),
            })?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        if bytes.len() != 12 + len {
            return Err(SchedulerError::Store {
                message: format!(
                    "ContinuousSnapshot decode: expected {} bytes, got {}",
                    12 + len,
                    bytes.len()
                ),
            });
        }
        Ok(Self {
            watermark_ms,
            snapshot_bytes: bytes.get(12..).unwrap_or(&[]).to_vec(),
        })
    }
}

/// Immutable archive record written when a job reaches a terminal state.
///
/// Kept permanently so `/ui/history` can show completed jobs after they are
/// evicted from the live coordinator snapshot. Serialized as JSON for all
/// store backends.
/// Maximum number of terminal-job history records retained per store. Oldest
/// records are evicted past this bound so the archive can't grow without limit.
pub const MAX_JOB_HISTORY: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobHistoryRecord {
    pub job_id: String,
    pub job_kind: String,
    /// Terminal state string: "succeeded", "failed", or "cancelled".
    pub final_state: String,
    /// Approximate wall-clock time the job completed (UNIX ms; 0 = unknown).
    pub completed_at_ms: u64,
    pub stage_count: usize,
    pub task_count: usize,
    pub succeeded_task_count: u32,
    pub failed_task_count: u32,
    pub cpu_nanos: u64,
    pub memory_peak_task_bytes: u64,
    pub namespace_id: Option<String>,
    pub priority: u8,
}

pub trait MetadataStore: Send + Sync {
    /// Refresh any read-through recovery snapshot from the authoritative backend.
    ///
    /// Stores whose reads are already live may keep the default no-op. Shared
    /// stores such as etcd override this so a long-running standby reloads state
    /// immediately before it is promoted to leader.
    fn refresh(&mut self) -> SchedulerResult<()> {
        Ok(())
    }

    fn append_event(&mut self, event: EventLogEvent) -> SchedulerResult<()>;
    fn events(&self) -> &[EventLogEvent];
    fn save_job(&mut self, record: &JobRecord) -> SchedulerResult<()>;
    fn jobs(&self) -> &[JobRecord];

    /// Append an immutable terminal-job record to the history log.
    fn save_job_history(&mut self, record: JobHistoryRecord) -> SchedulerResult<()>;

    /// Return all history records, most-recent first.
    fn list_job_history(&self) -> Vec<JobHistoryRecord>;

    /// Look up a single history record by job_id.
    fn get_job_history(&self, job_id: &str) -> Option<JobHistoryRecord>;

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

    /// Persist the window operator state for a continuous streaming job (C9).
    ///
    /// Called after each drain cycle so a restarted session can restore the
    /// window executor state without reprocessing already-committed data.
    fn save_continuous_snapshot(
        &mut self,
        job_id: &str,
        snapshot: ContinuousSnapshot,
    ) -> SchedulerResult<()>;

    /// Load the most recently persisted continuous job snapshot, if any.
    fn load_continuous_snapshot(&self, job_id: &str) -> Option<ContinuousSnapshot>;

    /// Remove the persisted snapshot for a continuous job (called on deregistration).
    fn remove_continuous_snapshot(&mut self, job_id: &str) -> SchedulerResult<()>;

    /// Persist a complete coordinator-side IVM job snapshot.
    fn save_ivm_snapshot(&mut self, job_id: &str, snapshot: Vec<u8>) -> SchedulerResult<()>;

    /// Load a complete coordinator-side IVM job snapshot.
    fn load_ivm_snapshot(&self, job_id: &str) -> Option<Vec<u8>>;

    /// List persisted IVM job snapshots for standby recovery and discovery.
    fn list_ivm_snapshots(&self) -> Vec<(String, Vec<u8>)>;

    /// Remove the persisted IVM job snapshot when the job is deleted.
    fn remove_ivm_snapshot(&mut self, job_id: &str) -> SchedulerResult<()>;
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
#[derive(Debug, Default)]
pub struct InMemoryMetadataStore {
    events: Vec<EventLogEvent>,
    events_byte_size: u64,
    jobs: Vec<JobRecord>,
    executors: Vec<ExecutorDescriptor>,
    continuous_snapshots: std::collections::HashMap<String, ContinuousSnapshot>,
    ivm_snapshots: std::collections::HashMap<String, Vec<u8>>,
    /// Number of events evicted by the ring buffer since the store was created.
    /// Exposed via [`InMemoryMetadataStore::evicted_event_count`] for tests and
    /// metrics.
    evicted_event_count: u64,
    history: Vec<JobHistoryRecord>,
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
            self.evicted_event_count = self.evicted_event_count.saturating_add(1);
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

    fn save_continuous_snapshot(
        &mut self,
        job_id: &str,
        snapshot: ContinuousSnapshot,
    ) -> SchedulerResult<()> {
        self.continuous_snapshots
            .insert(job_id.to_owned(), snapshot);
        Ok(())
    }

    fn load_continuous_snapshot(&self, job_id: &str) -> Option<ContinuousSnapshot> {
        self.continuous_snapshots.get(job_id).cloned()
    }

    fn remove_continuous_snapshot(&mut self, job_id: &str) -> SchedulerResult<()> {
        self.continuous_snapshots.remove(job_id);
        Ok(())
    }

    fn save_ivm_snapshot(&mut self, job_id: &str, snapshot: Vec<u8>) -> SchedulerResult<()> {
        self.ivm_snapshots.insert(job_id.to_owned(), snapshot);
        Ok(())
    }

    fn load_ivm_snapshot(&self, job_id: &str) -> Option<Vec<u8>> {
        self.ivm_snapshots.get(job_id).cloned()
    }

    fn list_ivm_snapshots(&self) -> Vec<(String, Vec<u8>)> {
        self.ivm_snapshots
            .iter()
            .map(|(job_id, snapshot)| (job_id.clone(), snapshot.clone()))
            .collect()
    }

    fn remove_ivm_snapshot(&mut self, job_id: &str) -> SchedulerResult<()> {
        self.ivm_snapshots.remove(job_id);
        Ok(())
    }

    fn save_job_history(&mut self, record: JobHistoryRecord) -> SchedulerResult<()> {
        self.history.retain(|r| r.job_id != record.job_id);
        self.history.insert(0, record);
        self.history.truncate(MAX_JOB_HISTORY);
        Ok(())
    }

    fn list_job_history(&self) -> Vec<JobHistoryRecord> {
        self.history.clone()
    }

    fn get_job_history(&self, job_id: &str) -> Option<JobHistoryRecord> {
        self.history.iter().find(|r| r.job_id == job_id).cloned()
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
    JobCompleted {
        job_id: String,
        final_state: String,
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
            EventLogEvent::JobCompleted {
                job_id,
                final_state,
            } => Self::JobCompleted {
                job_id: job_id.to_string(),
                final_state: final_state.clone(),
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
            PersistedEvent::JobCompleted {
                job_id,
                final_state,
            } => Self::JobCompleted {
                job_id: JobId::try_new(job_id).map_err(invalid_metadata_id)?,
                final_state,
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
    /// SC3: deterministic coordinator tick at which the task was last
    /// assigned to an executor. Restored on coordinator restart so a
    /// stalled-task timeout window does not reset across a leader
    /// failover. Defaults to `None` for records written before this
    /// field was added (backward compatible).
    #[serde(default)]
    pub(crate) assigned_at_tick: Option<u64>,
    /// SC3: deterministic coordinator tick of the last progress event
    /// reported by the executor. Used by the heartbeat-driven stall
    /// detector. Defaults to `None` for records written before this
    /// field was added.
    #[serde(default)]
    pub(crate) last_progress_tick: Option<u64>,
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
    /// Sink output contract for terminal write tasks (Phase 2.3).
    /// Absent in records written before this field was added.
    #[serde(default)]
    pub(crate) sink_contract: Option<String>,
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
            // Phase 53: retry backoff policy is coordinator config, not
            // persisted state; the restore path re-applies the configured
            // values via `set_retry_backoff` (defaults here are the same).
            retry_backoff_base_ms: 1_000,
            retry_backoff_cap_ms: 30_000,
            // Phase 58: transient regeneration counter, reset on restore.
            shuffle_regen_total: 0,
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
            // SC3: round-trip the stall-tracking timestamps so a coordinator
            // restart does not reset the timeout window. `assigned_at_ms`
            // and `last_progress_ms` on `TaskRecord` are populated by the
            // scheduler's task-assignment / progress paths.
            assigned_at_tick: None,
            last_progress_tick: None,
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
            // SC3: restore the stall-tracking timestamps so a stalled task
            // that survived a coordinator restart keeps its elapsed time
            // budget. The wall-clock variants are derived from the tick
            // values when the coordinator's tick clock is rewound.
            assigned_at_ms: None,
            last_progress_ms: None,
            completed_duration_ms: None,
            // Phase 53: backoff/delay-scheduling anchors are transient — a
            // coordinator restart resets them (backoff restarts, locality
            // wait re-anchors on the next assignment attempt).
            retry_backoff_until_ms: None,
            pending_since_ms: None,
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
            sink_contract: value.sink_contract().map(str::to_owned),
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
        if let Some(contract) = value.sink_contract {
            spec = spec.with_sink_contract(contract);
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
        "queued" => Ok(JobState::Queued),
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
#[cfg(all(feature = "etcd", test))]
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
    SaveContinuousSnapshot {
        job_id: String,
        snapshot: ContinuousSnapshot,
    },
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
    /// Handle to the background store drain task.  When all clones are dropped the
    /// handle is dropped (the task auto-terminates on channel close).
    _bg_task: std::sync::Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Job ids already durably persisted in a terminal state (Cancelled /
    /// Succeeded / Failed), guarding against a resurrection race: a `SaveJob`
    /// enqueued *before* a cancellation (e.g. a routine task-update persist)
    /// can still be sitting in the channel when the cancellation's own
    /// synchronous, must-be-durable write (see `cancel_job`) completes and
    /// returns 200 to the caller. If the background worker then dequeues that
    /// older command, it silently overwrites the just-committed terminal
    /// record with stale non-terminal data — a coordinator restart afterward
    /// reloads that stale record and resurrects an already-cancelled job,
    /// which then cycles forever in the stuck-Assigned reclaim loop (observed
    /// live on the Phase 58 HA chaos gate, 2026-07-20: `r1-i1-streaming`
    /// stayed Running and kept consuming an executor slot 38+ minutes after
    /// its own deregister call had already returned `{"cancelled":true}`).
    /// Every write that would regress a latched id back to non-terminal is
    /// skipped; [`Self::forget_terminal_job`] clears the latch for the
    /// documented, legitimate job-id-reuse path (a fresh `submit_job` after a
    /// full deregister).
    terminal_jobs: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

/// Returns `true` if `record` should be written, `false` if this write must be
/// skipped as a stale attempt to regress an already-terminal job back to a
/// non-terminal state. See the `terminal_jobs` field doc for why this exists.
fn admit_job_write(
    terminal_jobs: &std::sync::Mutex<std::collections::HashSet<String>>,
    record: &JobRecord,
) -> bool {
    let job_id = record.job_id().as_str();
    let mut terminal = terminal_jobs.lock().unwrap_or_else(|p| p.into_inner());
    if record.state().is_terminal() {
        terminal.insert(job_id.to_owned());
        true
    } else if terminal.contains(job_id) {
        tracing::warn!(
            job_id = %job_id,
            "NonBlockingStoreHandle: refusing to persist a non-terminal record over an \
             already-terminal job (stale/delayed write — this is what a resurrection race \
             looks like; see the `terminal_jobs` field doc)"
        );
        false
    } else {
        true
    }
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
        // Seed the terminal-job latch from whatever the store already has on
        // disk (e.g. a warm reconnect) before `store` is moved into the Mutex
        // below, so a write racing in immediately after construction is
        // protected too, not just ones persisted during this process's life.
        let terminal_jobs: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>> = {
            let initial: std::collections::HashSet<String> = store
                .jobs()
                .iter()
                .filter(|record| record.state().is_terminal())
                .map(|record| record.job_id().as_str().to_owned())
                .collect();
            std::sync::Arc::new(std::sync::Mutex::new(initial))
        };
        let inner: std::sync::Arc<std::sync::Mutex<dyn MetadataStore + 'static>> =
            std::sync::Arc::new(std::sync::Mutex::new(store));

        // Only spawn the background task when a Tokio runtime is available.
        let bg_task: std::sync::Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let tx = if tokio::runtime::Handle::try_current().is_ok() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<StoreCommand>(STORE_CHANNEL_CAPACITY);
            let bg_store = std::sync::Arc::clone(&inner);
            let bg_terminal_jobs = std::sync::Arc::clone(&terminal_jobs);
            let in_flight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let notify = std::sync::Arc::new(tokio::sync::Notify::new());
            let handle = tokio::spawn(async move {
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
                            // This command may have been enqueued well before it is
                            // dequeued here; if a synchronous terminal-state write
                            // (cancel_job et al.) has since latched this job_id, this
                            // is exactly the stale write the latch exists to catch.
                            if !admit_job_write(&bg_terminal_jobs, &record) {
                                continue;
                            }
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
                        StoreCommand::SaveContinuousSnapshot { job_id, snapshot } => {
                            in_flight.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            let bg = std::sync::Arc::clone(&bg_store);
                            let in_flight_done = std::sync::Arc::clone(&in_flight);
                            let notify_done = std::sync::Arc::clone(&notify);
                            tokio::task::spawn_blocking(move || {
                                let mut guard = bg.lock().unwrap_or_else(|p| p.into_inner());
                                if let Err(e) = guard.save_continuous_snapshot(&job_id, snapshot) {
                                    tracing::error!(
                                        error = %e,
                                        job_id = %job_id,
                                        "NonBlockingStoreHandle: save_continuous_snapshot failed"
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
            *bg_task.lock().unwrap_or_else(|p| p.into_inner()) = Some(handle);
            Some(tx)
        } else {
            None
        };

        Self {
            inner,
            tx,
            fail_closed_writes: false,
            _bg_task: bg_task,
            terminal_jobs,
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
        if !admit_job_write(&self.terminal_jobs, record) {
            return;
        }
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Err(e) = guard.save_job(record) {
            tracing::error!(error = %e, "NonBlockingStoreHandle: save_job failed (sync fallback)");
        }
    }

    /// Clear the terminal-job latch for `job_id`, allowing a fresh, legitimate
    /// resubmission under the same id to persist normally. Callers must only
    /// invoke this at the point a reused id is deliberately being replaced
    /// (see `Coordinator::submit_job`'s terminal-id-reuse branch) — clearing
    /// it any earlier would reopen the resurrection race this latch closes.
    pub fn forget_terminal_job(&self, job_id: &str) {
        let mut terminal = self
            .terminal_jobs
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        terminal.remove(job_id);
    }

    /// Synchronous, latch-checked, error-propagating job save.
    ///
    /// This is the method every "must be durable before returning" caller
    /// (`cancel_job` et al., via `Coordinator::persist_job_record(_, true)`)
    /// should use instead of reaching into [`Self::inner`] directly. Calling
    /// `.inner().save_job(...)` bypasses this handle's own methods entirely —
    /// which is exactly the gap that shipped in this latch's first version:
    /// `persist_job_record`'s sync branch used `.inner()` directly, so
    /// `cancel_job`'s durable write never passed through [`admit_job_write`]
    /// and never actually latched the job_id, making the whole terminal-jobs
    /// mechanism a no-op for the one call site it exists to protect (found
    /// live: a job cancelled through this exact path still resurrected after
    /// a later coordinator restart, on the very first post-fix gate rerun).
    pub(crate) fn save_job_checked(&self, record: &JobRecord) -> SchedulerResult<()> {
        if !admit_job_write(&self.terminal_jobs, record) {
            return Ok(());
        }
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        guard.save_job(record)
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

    /// Enqueue a continuous job snapshot write (C9).
    ///
    /// Fire-and-forget via the async channel. When the channel is full or no
    /// async runtime is available, the write happens synchronously. A dropped
    /// snapshot is logged as a warning — the old snapshot remains in the store
    /// so crash recovery falls back to a slightly earlier state, not a blank.
    pub fn save_continuous_snapshot(&self, job_id: &str, snapshot: ContinuousSnapshot) {
        let cmd = StoreCommand::SaveContinuousSnapshot {
            job_id: job_id.to_owned(),
            snapshot: snapshot.clone(),
        };
        match &self.tx {
            Some(tx) => match tx.try_send(cmd) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    if self.fail_closed_writes {
                        tracing::warn!(
                            job_id = %job_id,
                            "NonBlockingStoreHandle: channel full; \
                             performing synchronous save_continuous_snapshot"
                        );
                        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                        if let Err(e) = guard.save_continuous_snapshot(job_id, snapshot) {
                            tracing::error!(error = %e, job_id = %job_id,
                                "NonBlockingStoreHandle: save_continuous_snapshot failed (sync fallback)");
                        }
                    } else {
                        tracing::warn!(
                            job_id = %job_id,
                            "NonBlockingStoreHandle: save_continuous_snapshot dropped (channel full)"
                        );
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    tracing::error!("NonBlockingStoreHandle: store background task dropped");
                }
            },
            None => {
                let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                if let Err(e) = guard.save_continuous_snapshot(job_id, snapshot) {
                    tracing::error!(error = %e, job_id = %job_id,
                        "NonBlockingStoreHandle: save_continuous_snapshot failed");
                }
            }
        }
    }

    /// Load the most recently persisted continuous job snapshot (synchronous read).
    pub fn load_continuous_snapshot(&self, job_id: &str) -> Option<ContinuousSnapshot> {
        self.inner().load_continuous_snapshot(job_id)
    }

    /// Remove a persisted continuous job snapshot (synchronous — deregistration
    /// is infrequent, no need for the async write channel). Without this, a
    /// deregistered job's snapshot lingers in the store keyed by its plain
    /// job id string; a later job registered with the *same* id (as this
    /// engine's continuous-streaming API allows and this repo's own fault-
    /// loop tests do, deliberately reusing one id across iterations)
    /// silently inherits a stale watermark/state that has nothing to do
    /// with its own run.
    pub fn remove_continuous_snapshot(&self, job_id: &str) {
        if let Err(e) = self.inner().remove_continuous_snapshot(job_id) {
            tracing::warn!(error = %e, job_id = %job_id,
                "NonBlockingStoreHandle: remove_continuous_snapshot failed");
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
#[cfg(all(feature = "etcd", test))]
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

#[cfg(test)]
mod tests {
    use super::*;

    /// SC3: `PersistedTaskRecord` round-trips the new
    /// `assigned_at_tick` / `last_progress_tick` fields through JSON.
    #[test]
    fn persisted_task_record_round_trips_stall_tracking_ticks() {
        let rec = PersistedTaskRecord {
            spec: PersistedTaskSpec {
                task_id: String::from("task-1"),
                description: String::from("desc"),
                task_timeout_secs: Some(0),
                source_capabilities: None,
                sink_capabilities: None,
                sink_contract: None,
            },
            state: String::from("Running"),
            assigned_executor: Some(String::from("executor-1")),
            attempt: 1,
            output_metadata: None,
            last_failure_reason: None,
            failure_count: 0,
            executor_loss_count: 0,
            assigned_at_tick: Some(42),
            last_progress_tick: Some(47),
        };
        let json = serde_json::to_string(&rec).expect("serialise");
        // Both fields must be present in the JSON output.
        assert!(
            json.contains("\"assigned_at_tick\":42"),
            "missing field in {json}"
        );
        assert!(
            json.contains("\"last_progress_tick\":47"),
            "missing field in {json}"
        );
        let round: PersistedTaskRecord = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(round.assigned_at_tick, Some(42));
        assert_eq!(round.last_progress_tick, Some(47));
    }

    /// SC3: a `PersistedTaskRecord` written before this field existed
    /// (no `assigned_at_tick` / `last_progress_tick` in the JSON) must
    /// still deserialise with `None` for both fields.
    #[test]
    fn persisted_task_record_back_compat_with_legacy_payload() {
        let legacy = r#"{
            "spec": {
                "task_id": "task-1",
                "description": "desc",
                "task_timeout_secs": null,
                "source_capabilities": null,
                "sink_capabilities": null
            },
            "state": "Running",
            "assigned_executor": "executor-1",
            "attempt": 1,
            "output_metadata": null,
            "last_failure_reason": null,
            "failure_count": 0,
            "executor_loss_count": 0
        }"#;
        let round: PersistedTaskRecord =
            serde_json::from_str(legacy).expect("legacy payload must deserialise");
        assert_eq!(round.assigned_at_tick, None);
        assert_eq!(round.last_progress_tick, None);
    }
}

#[cfg(test)]
mod job_completed_event_tests {
    use super::*;

    /// SC13: `EventLogEvent::JobCompleted` round-trips through the
    /// `PersistedEvent` round-trip without losing the `final_state` string.
    #[test]
    fn job_completed_event_round_trips() {
        let event = EventLogEvent::JobCompleted {
            job_id: JobId::try_new("job-1").expect("id"),
            final_state: String::from("Succeeded"),
        };
        let persisted = PersistedEvent::from(&event);
        let round: EventLogEvent = persisted.try_into().expect("round-trip");
        match round {
            EventLogEvent::JobCompleted {
                job_id,
                final_state,
            } => {
                assert_eq!(job_id.as_str(), "job-1");
                assert_eq!(final_state, "Succeeded");
            }
            _ => panic!("expected JobCompleted, got {round:?}"),
        }
    }
}

#[cfg(test)]
mod terminal_job_latch_tests {
    use super::*;
    use crate::job_spec_from_logical_plan;
    use krishiv_plan::{ExecutionKind, LogicalPlan};

    fn job_record(id: &str) -> JobRecord {
        let job_id = JobId::try_new(id).unwrap();
        let spec =
            job_spec_from_logical_plan(job_id, &LogicalPlan::new("test", ExecutionKind::Batch))
                .unwrap();
        JobRecord::from_spec(spec, 1)
    }

    /// Direct unit test of the admission rule `admit_job_write` enforces:
    /// once a job_id is latched terminal, a write attempting to regress it
    /// back to non-terminal is rejected; clearing the latch (what
    /// `submit_job`'s terminal-id-reuse branch does via
    /// `forget_terminal_job`) is what re-opens it for a legitimate
    /// resubmission.
    #[test]
    fn admit_job_write_rejects_a_stale_regression_but_allows_legitimate_reuse() {
        let latch: std::sync::Mutex<std::collections::HashSet<String>> =
            std::sync::Mutex::new(std::collections::HashSet::new());

        // Ordinary non-terminal write before any cancellation: admitted,
        // latch untouched.
        let running = job_record("job-a");
        assert!(admit_job_write(&latch, &running));
        assert!(latch.lock().unwrap().is_empty());

        // The job is cancelled and durably persisted (this is `cancel_job`'s
        // synchronous, must-be-durable write): admitted, and now latched.
        let mut cancelled = job_record("job-a");
        cancelled.state = JobState::Cancelled;
        assert!(admit_job_write(&latch, &cancelled));
        assert!(latch.lock().unwrap().contains("job-a"));

        // A stale write from before the cancellation — e.g. a background
        // command enqueued while the job was still Running that only gets
        // dequeued after the sync cancel write already landed — must never
        // resurrect it. This is the exact 2026-07-20 Phase 58 gate finding:
        // `r1-i1-streaming` stayed Running and kept cycling in the
        // stuck-Assigned reclaim loop for 38+ minutes after its own
        // deregister call had already returned `{"cancelled":true}`.
        let mut stale = job_record("job-a");
        stale.state = JobState::Running;
        assert!(
            !admit_job_write(&latch, &stale),
            "a stale non-terminal write must be rejected once the job is latched terminal"
        );

        // Legitimate id reuse (a fresh `continuous-register-sql`/`submit_job`
        // after a full deregister) clears the latch first, so the new
        // registration's write is admitted normally.
        latch.lock().unwrap().remove("job-a");
        let mut fresh = job_record("job-a");
        fresh.state = JobState::Queued;
        assert!(
            admit_job_write(&latch, &fresh),
            "after the latch is cleared for legitimate reuse, a fresh registration must be admitted"
        );
    }

    /// End-to-end version through the real async plumbing (channel +
    /// background worker, not just the pure decision function): a `SaveJob`
    /// that reaches the store after the job is already durably cancelled must
    /// not resurrect it, and `forget_terminal_job` must still let a
    /// legitimate resubmission through afterward.
    #[tokio::test]
    async fn non_blocking_store_handle_rejects_a_stale_queued_write_after_cancellation() {
        let handle = NonBlockingStoreHandle::new(InMemoryMetadataStore::default());

        let mut cancelled = job_record("job-a");
        cancelled.state = JobState::Cancelled;
        handle.save_job(&cancelled);
        handle.flush().await;
        assert_eq!(handle.inner().jobs()[0].state(), JobState::Cancelled);

        // A stale write for the same id landing after the cancellation is
        // already durable — exactly what a delayed background command looks
        // like from the store's point of view.
        let mut stale = job_record("job-a");
        stale.state = JobState::Running;
        handle.save_job(&stale);
        handle.flush().await;
        assert_eq!(
            handle.inner().jobs()[0].state(),
            JobState::Cancelled,
            "a stale write must never resurrect an already-cancelled job"
        );

        // Legitimate reuse: forget the latch, then a fresh registration
        // persists normally.
        handle.forget_terminal_job("job-a");
        let mut fresh = job_record("job-a");
        fresh.state = JobState::Queued;
        handle.save_job(&fresh);
        handle.flush().await;
        assert_eq!(handle.inner().jobs()[0].state(), JobState::Queued);
    }
}
