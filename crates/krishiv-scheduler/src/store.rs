use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use krishiv_proto::{
    AttemptId, ConnectorCapabilityFlags, ExecutorId, JobId, JobKind, JobSpec, JobState, StageId,
    StageSpec, StageState, TaskId, TaskOutputMetadata, TaskSpec, TaskState,
};
use serde::{Deserialize, Serialize};

use crate::{JobRecord, ResourceUsage, SchedulerError, SchedulerResult, StageRecord, TaskRecord};

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

/// Durable store for coordinator restart-recovery state and the event log.
///
/// `InMemoryMetadataStore` is used for tests and single-process deployments.
/// `JsonFileMetadataStore` is the R3.1 durable local backend for bare-metal / VM
/// recovery tests. `SqliteMetadataStore` and `KubernetesMetadataStore` are
/// deferred to later releases.
pub trait MetadataStore: Send + Sync {
    fn append_event(&mut self, event: EventLogEvent) -> SchedulerResult<()>;
    fn events(&self) -> &[EventLogEvent];
    fn save_job(&mut self, record: &JobRecord) -> SchedulerResult<()>;
    fn jobs(&self) -> &[JobRecord];
}

/// In-memory metadata store for tests and single-process deployments.
#[derive(Debug, Default)]
pub struct InMemoryMetadataStore {
    events: Vec<EventLogEvent>,
    jobs: Vec<JobRecord>,
}

impl MetadataStore for InMemoryMetadataStore {
    fn append_event(&mut self, event: EventLogEvent) -> SchedulerResult<()> {
        self.events.push(event);
        Ok(())
    }

    fn events(&self) -> &[EventLogEvent] {
        &self.events
    }

    fn save_job(&mut self, record: &JobRecord) -> SchedulerResult<()> {
        if let Some(existing) = self.jobs.iter_mut().find(|j| j.job_id() == record.job_id()) {
            *existing = record.clone();
        } else {
            self.jobs.push(record.clone());
        }
        Ok(())
    }

    fn jobs(&self) -> &[JobRecord] {
        &self.jobs
    }
}

const JSON_METADATA_SCHEMA_VERSION: u32 = 1;

/// JSON-file metadata store for durable local coordinator recovery.
#[derive(Debug)]
pub struct JsonFileMetadataStore {
    path: PathBuf,
    events: Vec<EventLogEvent>,
    jobs: Vec<JobRecord>,
}

impl JsonFileMetadataStore {
    /// Open or create a JSON-file metadata store at `path`.
    pub fn open(path: impl AsRef<Path>) -> SchedulerResult<Self> {
        let path = path.as_ref().to_path_buf();
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let store = Self {
                    path,
                    events: Vec::new(),
                    jobs: Vec::new(),
                };
                store.persist()?;
                return Ok(store);
            }
            Err(e) => {
                return Err(SchedulerError::Transport {
                    message: format!("failed to read metadata store '{}': {e}", path.display()),
                });
            }
        };
        if bytes.is_empty() {
            return Err(SchedulerError::Transport {
                message: format!(
                    "metadata store '{}' is empty; refusing to treat a torn write as an empty store",
                    path.display()
                ),
            });
        }
        let persisted: PersistedMetadata =
            serde_json::from_slice(&bytes).map_err(|error| SchedulerError::InvalidJob {
                message: format!(
                    "failed to decode metadata store '{}': {error}",
                    path.display()
                ),
            })?;
        persisted.validate_schema_version()?;
        Ok(Self {
            path,
            events: persisted
                .events
                .into_iter()
                .map(EventLogEvent::try_from)
                .collect::<SchedulerResult<Vec<_>>>()?,
            jobs: persisted
                .jobs
                .into_iter()
                .map(JobRecord::try_from)
                .collect::<SchedulerResult<Vec<_>>>()?,
        })
    }

    fn persist(&self) -> SchedulerResult<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| SchedulerError::Transport {
                message: format!(
                    "failed to create metadata store dir '{}': {error}",
                    parent.display()
                ),
            })?;
        }
        let persisted = PersistedMetadata {
            schema_version: JSON_METADATA_SCHEMA_VERSION,
            store_kind: String::from("krishiv.scheduler.metadata"),
            events: self.events.iter().map(PersistedEvent::from).collect(),
            jobs: self.jobs.iter().map(PersistedJobRecord::from).collect(),
        };
        let bytes =
            serde_json::to_vec_pretty(&persisted).map_err(|error| SchedulerError::Transport {
                message: format!("failed to encode metadata store: {error}"),
            })?;
        let tmp_path = self
            .path
            .with_extension(format!("tmp-{}", std::process::id()));
        let mut file =
            std::fs::File::create(&tmp_path).map_err(|error| SchedulerError::Transport {
                message: format!(
                    "failed to create temporary metadata store '{}': {error}",
                    tmp_path.display()
                ),
            })?;
        file.write_all(&bytes)
            .map_err(|error| SchedulerError::Transport {
                message: format!(
                    "failed to write temporary metadata store '{}': {error}",
                    tmp_path.display()
                ),
            })?;
        file.sync_all().map_err(|error| SchedulerError::Transport {
            message: format!(
                "failed to fsync temporary metadata store '{}': {error}",
                tmp_path.display()
            ),
        })?;
        drop(file);
        std::fs::rename(&tmp_path, &self.path).map_err(|error| SchedulerError::Transport {
            message: format!(
                "failed to atomically replace metadata store '{}': {error}",
                self.path.display()
            ),
        })?;
        if let Some(parent) = self.path.parent() {
            std::fs::File::open(parent)
                .and_then(|dir| dir.sync_all())
                .map_err(|error| SchedulerError::Transport {
                    message: format!(
                        "failed to fsync metadata store directory '{}': {error}",
                        parent.display()
                    ),
                })?;
        }
        Ok(())
    }
}

impl MetadataStore for JsonFileMetadataStore {
    fn append_event(&mut self, event: EventLogEvent) -> SchedulerResult<()> {
        self.events.push(event);
        self.persist()
    }

    fn events(&self) -> &[EventLogEvent] {
        &self.events
    }

    fn save_job(&mut self, record: &JobRecord) -> SchedulerResult<()> {
        if let Some(existing) = self.jobs.iter_mut().find(|j| j.job_id() == record.job_id()) {
            *existing = record.clone();
        } else {
            self.jobs.push(record.clone());
        }
        self.persist()
    }

    fn jobs(&self) -> &[JobRecord] {
        &self.jobs
    }
}

// ── SqliteMetadataStore ───────────────────────────────────────────────────────

/// SQLite-backed metadata store for durable coordinator recovery.
///
/// Feature-gated behind `--features sqlite`.  Uses a bundled SQLite binary so
/// no system library is required.  Records are serialized as JSON blobs in the
/// `events` and `jobs` tables and loaded into memory on `open`; subsequent
/// `save_job`/`append_event` calls update both the in-memory cache and the
/// on-disk database atomically via transactions.
#[cfg(feature = "sqlite")]
pub struct SqliteMetadataStore {
    conn: std::sync::Mutex<rusqlite::Connection>,
    events: Vec<EventLogEvent>,
    jobs: Vec<JobRecord>,
}

#[cfg(feature = "sqlite")]
impl SqliteMetadataStore {
    /// Open (or create) a SQLite metadata store at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> SchedulerResult<Self> {
        let conn = rusqlite::Connection::open(path).map_err(|e| SchedulerError::Transport {
            message: format!("sqlite open error: {e}"),
        })?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS jobs   (job_id TEXT PRIMARY KEY, payload TEXT NOT NULL);",
        )
        .map_err(|e| SchedulerError::Transport {
            message: format!("sqlite schema init error: {e}"),
        })?;

        // Load existing events.
        let events = {
            let mut stmt = conn
                .prepare("SELECT payload FROM events ORDER BY id")
                .map_err(|e| SchedulerError::Transport {
                    message: format!("sqlite events query error: {e}"),
                })?;
            stmt.query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| SchedulerError::Transport {
                    message: format!("sqlite events iter error: {e}"),
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| SchedulerError::Transport {
                    message: format!("sqlite events row error: {e}"),
                })?
                .into_iter()
                .map(|json| {
                    let pe: PersistedEvent =
                        serde_json::from_str(&json).map_err(|e| SchedulerError::InvalidJob {
                            message: format!("sqlite event decode error: {e}"),
                        })?;
                    EventLogEvent::try_from(pe)
                })
                .collect::<SchedulerResult<Vec<_>>>()?
        };

        // Load existing jobs.
        let jobs = {
            let mut stmt = conn.prepare("SELECT payload FROM jobs").map_err(|e| {
                SchedulerError::Transport {
                    message: format!("sqlite jobs query error: {e}"),
                }
            })?;
            stmt.query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| SchedulerError::Transport {
                    message: format!("sqlite jobs iter error: {e}"),
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| SchedulerError::Transport {
                    message: format!("sqlite jobs row error: {e}"),
                })?
                .into_iter()
                .map(|json| {
                    let pj: PersistedJobRecord =
                        serde_json::from_str(&json).map_err(|e| SchedulerError::InvalidJob {
                            message: format!("sqlite job decode error: {e}"),
                        })?;
                    JobRecord::try_from(pj)
                })
                .collect::<SchedulerResult<Vec<_>>>()?
        };

        Ok(Self {
            conn: std::sync::Mutex::new(conn),
            events,
            jobs,
        })
    }
}

#[cfg(feature = "sqlite")]
impl MetadataStore for SqliteMetadataStore {
    fn append_event(&mut self, event: EventLogEvent) -> SchedulerResult<()> {
        let json = serde_json::to_string(&PersistedEvent::from(&event)).map_err(|e| {
            SchedulerError::Transport {
                message: format!("sqlite event encode error: {e}"),
            }
        })?;
        self.conn
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .execute("INSERT INTO events (payload) VALUES (?1)", [&json])
            .map_err(|e| SchedulerError::Transport {
                message: format!("sqlite event insert error: {e}"),
            })?;
        self.events.push(event);
        Ok(())
    }

    fn events(&self) -> &[EventLogEvent] {
        &self.events
    }

    fn save_job(&mut self, record: &JobRecord) -> SchedulerResult<()> {
        let job_id = record.job_id().to_string();
        let json = serde_json::to_string(&PersistedJobRecord::from(record)).map_err(|e| {
            SchedulerError::Transport {
                message: format!("sqlite job encode error: {e}"),
            }
        })?;
        self.conn
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .execute(
                "INSERT INTO jobs (job_id, payload) VALUES (?1, ?2)
                 ON CONFLICT(job_id) DO UPDATE SET payload = excluded.payload",
                rusqlite::params![job_id, json],
            )
            .map_err(|e| SchedulerError::Transport {
                message: format!("sqlite job upsert error: {e}"),
            })?;
        if let Some(existing) = self.jobs.iter_mut().find(|j| j.job_id() == record.job_id()) {
            *existing = record.clone();
        } else {
            self.jobs.push(record.clone());
        }
        Ok(())
    }

    fn jobs(&self) -> &[JobRecord] {
        &self.jobs
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedMetadata {
    #[serde(default = "default_json_metadata_schema_version")]
    schema_version: u32,
    #[serde(default = "default_json_metadata_store_kind")]
    store_kind: String,
    pub(crate) events: Vec<PersistedEvent>,
    pub(crate) jobs: Vec<PersistedJobRecord>,
}

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

fn default_json_metadata_schema_version() -> u32 {
    JSON_METADATA_SCHEMA_VERSION
}

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

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedJobRecord {
    pub(crate) spec: PersistedJobSpec,
    pub(crate) state: String,
    pub(crate) max_stage_retries: u32,
    pub(crate) stages: Vec<PersistedStageRecord>,
    /// Accumulated resource consumption. `None` in records written before R7.1.
    #[serde(default)]
    pub(crate) resource_usage: Option<ResourceUsage>,
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
        }
    }
}

impl TryFrom<PersistedJobRecord> for JobRecord {
    type Error = SchedulerError;

    fn try_from(value: PersistedJobRecord) -> SchedulerResult<Self> {
        Ok(Self {
            spec: JobSpec::try_from(value.spec)?,
            state: parse_job_state(&value.state)?,
            max_stage_retries: value.max_stage_retries,
            stages: value
                .stages
                .into_iter()
                .map(StageRecord::try_from)
                .collect::<SchedulerResult<Vec<_>>>()?,
            // Shuffle output metadata is not persisted; it is rebuilt from
            // executor task status updates after coordinator restart.
            shuffle_output: HashMap::new(),
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
            failure_count: 0,
            // Streaming state is not persisted in R5.1; executors re-report it on re-attach.
            last_watermark_ms: None,
            last_source_offset: None,
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
pub(crate) fn encode_metadata_snapshot(
    events: &[EventLogEvent],
    jobs: &[JobRecord],
) -> SchedulerResult<Vec<u8>> {
    let persisted = PersistedMetadata {
        schema_version: JSON_METADATA_SCHEMA_VERSION,
        store_kind: String::from("krishiv.scheduler.metadata"),
        events: events.iter().map(PersistedEvent::from).collect(),
        jobs: jobs.iter().map(PersistedJobRecord::from).collect(),
    };
    serde_json::to_vec_pretty(&persisted).map_err(|error| SchedulerError::Transport {
        message: format!("failed to encode metadata snapshot: {error}"),
    })
}

/// Restore coordinator metadata from a serialized snapshot blob.
pub(crate) fn decode_metadata_snapshot(
    bytes: &[u8],
) -> SchedulerResult<(Vec<EventLogEvent>, Vec<JobRecord>)> {
    if bytes.is_empty() {
        return Ok((Vec::new(), Vec::new()));
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
    Ok((events, jobs))
}
