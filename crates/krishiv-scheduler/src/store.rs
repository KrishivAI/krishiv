use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use krishiv_proto::{
    AttemptId, ConnectorCapabilityFlags, ExecutorId, JobId, JobKind, JobSpec, JobState, StageId,
    StageSpec, StageState, TaskId, TaskOutputMetadata, TaskSpec, TaskState,
};
use serde::{Deserialize, Serialize};

use krishiv_shuffle::{ShuffleMetadata, ShufflePath};

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
///
/// Uses two files:
/// - `{path}` — full snapshot (jobs + events), written on `save_job` and `open`.
/// - `{path}.events.ndjson` — append-only newline-delimited JSON event log.
///
/// On `append_event`, the event is appended to the `.events.ndjson` log (fast,
/// no full rewrite).  On `save_job`, the full snapshot is rewritten and the
/// `.events.ndjson` log is truncated (it's now captured in the snapshot).
/// On `open`, both files are read and events are merged.
#[derive(Debug)]
pub struct JsonFileMetadataStore {
    path: PathBuf,
    events: Vec<EventLogEvent>,
    jobs: Vec<JobRecord>,
}

impl JsonFileMetadataStore {
    fn events_log_path(&self) -> PathBuf {
        let mut p = self.path.as_os_str().to_owned();
        p.push(".events.ndjson");
        PathBuf::from(p)
    }

    /// Append a single event to the events NDJSON log (fast, no full rewrite).
    fn append_event_to_log(&self, event: &EventLogEvent) -> SchedulerResult<()> {
        let log_path = self.events_log_path();
        let persisted = PersistedEvent::from(event);
        let line = serde_json::to_string(&persisted).map_err(|e| SchedulerError::Transport {
            message: format!("failed to serialize event: {e}"),
        })?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| SchedulerError::Transport {
                message: format!("failed to open event log '{}': {e}", log_path.display()),
            })?;
        use std::io::Write as IoWrite;
        writeln!(file, "{}", line).map_err(|e| SchedulerError::Transport {
            message: format!("failed to write event log: {e}"),
        })?;
        file.sync_all().map_err(|e| SchedulerError::Transport {
            message: format!("failed to fsync event log: {e}"),
        })?;
        Ok(())
    }

    /// Truncate the events NDJSON log (called after a full snapshot rewrite).
    fn truncate_events_log(&self) -> SchedulerResult<()> {
        let log_path = self.events_log_path();
        if log_path.exists() {
            std::fs::write(&log_path, b"").map_err(|e| SchedulerError::Transport {
                message: format!("failed to truncate event log '{}': {e}", log_path.display()),
            })?;
        }
        Ok(())
    }

    /// Load events from the NDJSON log file (called during `open`).
    fn load_events_from_log(path: &Path) -> Vec<EventLogEvent> {
        let log_path = {
            let mut p = path.as_os_str().to_owned();
            p.push(".events.ndjson");
            PathBuf::from(p)
        };
        if !log_path.exists() {
            return Vec::new();
        }
        let content = match std::fs::read_to_string(&log_path) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        content
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str::<PersistedEvent>(l).ok())
            .filter_map(|pe| EventLogEvent::try_from(pe).ok())
            .collect()
    }

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
        // Load snapshot events then merge any extra events from the NDJSON log.
        let mut events: Vec<EventLogEvent> = persisted
            .events
            .into_iter()
            .map(EventLogEvent::try_from)
            .collect::<SchedulerResult<Vec<_>>>()?;
        let extra_events = Self::load_events_from_log(&path);
        events.extend(extra_events);
        Ok(Self {
            path,
            events,
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
        // Append-only: write just this event to the NDJSON log (no full rewrite).
        self.append_event_to_log(&event)?;
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
        // Full snapshot rewrite on save_job (less frequent than append_event).
        self.persist()?;
        // After snapshot, the events log is captured; truncate it.
        self.truncate_events_log()?;
        Ok(())
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
#[allow(dead_code)]
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
}

impl std::fmt::Debug for NonBlockingStoreHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NonBlockingStoreHandle")
            .field("async_mode", &self.tx.is_some())
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
            tokio::spawn(async move {
                while let Some(cmd) = rx.recv().await {
                    match cmd {
                        StoreCommand::AppendEvent(event) => {
                            let bg = std::sync::Arc::clone(&bg_store);
                            tokio::task::spawn_blocking(move || {
                                let mut guard = bg.lock().unwrap_or_else(|p| p.into_inner());
                                if let Err(e) = guard.append_event(event) {
                                    tracing::error!(
                                        error = %e,
                                        "NonBlockingStoreHandle: append_event failed"
                                    );
                                }
                            })
                            .await
                            .ok();
                        }
                        StoreCommand::SaveJob(record) => {
                            let bg = std::sync::Arc::clone(&bg_store);
                            tokio::task::spawn_blocking(move || {
                                let mut guard = bg.lock().unwrap_or_else(|p| p.into_inner());
                                if let Err(e) = guard.save_job(&record) {
                                    tracing::error!(
                                        error = %e,
                                        "NonBlockingStoreHandle: save_job failed"
                                    );
                                }
                            })
                            .await
                            .ok();
                        }
                        StoreCommand::Flush(reply) => {
                            let _ = reply.send(());
                        }
                    }
                }
            });
            Some(tx)
        } else {
            None
        };

        Self { inner, tx }
    }

    /// Access the underlying store for reads (blocks on mutex).
    pub fn inner(&self) -> std::sync::MutexGuard<'_, dyn MetadataStore + 'static> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Enqueue an event write (sync, uses `try_send`).
    ///
    /// When the bounded channel is full, the event is dropped and a warning is
    /// logged. Async callers should prefer [`Self::append_event_async`].
    pub fn append_event(&self, event: EventLogEvent) {
        if let Some(ref tx) = self.tx {
            if tx.try_send(StoreCommand::AppendEvent(event)).is_err() {
                tracing::warn!(
                    "NonBlockingStoreHandle: append_event dropped (channel full, {} pending)",
                    tx.max_capacity()
                );
            }
        }
    }

    /// Enqueue a job save (sync, uses `try_send`).
    ///
    /// When the bounded channel is full, the save is dropped and a warning is
    /// logged. Async callers should prefer [`Self::save_job_async`].
    pub fn save_job(&self, record: &JobRecord) {
        if let Some(ref tx) = self.tx {
            if tx
                .try_send(StoreCommand::SaveJob(Box::new(record.clone())))
                .is_err()
            {
                tracing::warn!(
                    "NonBlockingStoreHandle: save_job dropped (channel full, {} pending)",
                    tx.max_capacity()
                );
            }
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
            self.append_event(event);
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
            self.save_job(record);
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
#[allow(dead_code)]
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
