//! etcd-backed durable metadata store for distributed cluster recovery.
//!
//! # Key layout
//!
//! Each record is stored under its own etcd key instead of a single snapshot
//! blob.  This eliminates the O(total_jobs) re-encode on every write and
//! removes the 1.5 MiB single-key size ceiling.
//!
//! | Prefix | Content |
//! |--------|---------|
//! | `/krishiv/jobs/<job_id>` | JSON-encoded `PersistedJobRecord` |
//! | `/krishiv/executors/<executor_id>` | JSON-encoded `PersistedExecutorDescriptor` |
//! | `/krishiv/continuous/<job_id>` | Binary `ContinuousSnapshot` payload |
//! | `/krishiv/history/<job_id>` | JSON-encoded terminal `JobHistoryRecord` |
//!
//! Events are not persisted — they are audit-only and kept in-memory.

// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges the sync MetadataStore trait to the async etcd client.
#![allow(clippy::disallowed_methods)]
//!
//! # Persist mechanism
//!
//! `MetadataStore` is a sync trait called from within the coordinator's async
//! write-lock.  `krishiv_common::async_util::block_on` bridges the sync trait
//! to the async etcd client, using `block_in_place` on multi-thread runtimes
//! and a fallback runtime on current-thread or no-runtime callers.

use etcd_client::{Client, GetOptions};

use crate::store::{
    ContinuousSnapshot, EventLogEvent, JobHistoryRecord, MAX_JOB_HISTORY, MetadataStore,
    PersistedExecutorDescriptor, PersistedJobRecord,
};
use crate::{JobRecord, SchedulerError, SchedulerResult};

const JOB_KEY_PREFIX: &str = "/krishiv/jobs/";
const EXECUTOR_KEY_PREFIX: &str = "/krishiv/executors/";
const CONTINUOUS_KEY_PREFIX: &str = "/krishiv/continuous/";
const HISTORY_KEY_PREFIX: &str = "/krishiv/history/";

/// Durable metadata store backed by per-record etcd keys.
///
/// Each job and executor descriptor lives under its own key so writes are
/// O(1) regardless of cluster size, and the 1.5 MiB etcd value limit only
/// applies per-record rather than to the full metadata snapshot.
///
/// # Cache contract
///
/// `startup_jobs` and `startup_executors` are populated once at `connect()` time
/// and are **never mutated** afterwards.  All writes (`save_job`, `save_executor`,
/// `remove_executor`) go directly to etcd; the in-memory fields are not touched.
///
/// This eliminates split-brain between the in-memory view and etcd that would
/// otherwise arise when a network timeout causes `put` to return an error even
/// though the server committed the write.  `jobs()` and `executors()` are called
/// only during coordinator startup (recovery), where the startup snapshot is
/// authoritative.  For all other in-session state, the coordinator's own
/// `job_coordinators` map is the source of truth.
pub struct EtcdMetadataStore {
    client: std::sync::Mutex<Client>,
    events: Vec<EventLogEvent>,
    /// Startup-time snapshot loaded from etcd.  Read-only after construction.
    startup_jobs: Vec<JobRecord>,
    /// Startup-time snapshot loaded from etcd.  Read-only after construction.
    startup_executors: Vec<krishiv_proto::ExecutorDescriptor>,
    continuous_snapshots: std::collections::HashMap<String, ContinuousSnapshot>,
    history: Vec<JobHistoryRecord>,
}

impl EtcdMetadataStore {
    /// Connect to etcd and load all job and executor records from their
    /// individual keys.
    pub async fn connect(endpoints: Vec<String>) -> SchedulerResult<Self> {
        let mut client =
            Client::connect(endpoints, None)
                .await
                .map_err(|e| SchedulerError::Transport {
                    message: format!("etcd metadata connect failed: {e}"),
                })?;

        let jobs = load_prefix::<PersistedJobRecord, JobRecord>(&mut client, JOB_KEY_PREFIX)
            .await
            .map_err(|e| SchedulerError::Transport {
                message: format!("etcd jobs load failed: {e}"),
            })?;

        let executor_descriptors = load_prefix::<
            PersistedExecutorDescriptor,
            krishiv_proto::ExecutorDescriptor,
        >(&mut client, EXECUTOR_KEY_PREFIX)
        .await
        .map_err(|e| SchedulerError::Transport {
            message: format!("etcd executors load failed: {e}"),
        })?;

        let continuous_snapshots = load_continuous_snapshots(&mut client).await.map_err(|e| {
            SchedulerError::Transport {
                message: format!("etcd continuous snapshots load failed: {e}"),
            }
        })?;

        let history = load_json_prefix::<JobHistoryRecord>(&mut client, HISTORY_KEY_PREFIX)
            .await
            .map_err(|e| SchedulerError::Transport {
                message: format!("etcd job history load failed: {e}"),
            })?;

        Ok(Self {
            client: std::sync::Mutex::new(client),
            events: Vec::new(),
            startup_jobs: jobs,
            startup_executors: executor_descriptors,
            continuous_snapshots,
            history: truncate_history(sort_history(history)),
        })
    }

    /// Write a single key to etcd.
    fn put_key(&self, key: String, value: Vec<u8>) -> SchedulerResult<()> {
        let mut client = self
            .client
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        krishiv_common::async_util::block_on(client.put(key, value, None)).map_err(|e| {
            SchedulerError::Transport {
                message: format!("etcd put failed: {e}"),
            }
        })?;
        Ok(())
    }

    /// Delete a single key from etcd.
    fn delete_key(&self, key: String) -> SchedulerResult<()> {
        let mut client = self
            .client
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        krishiv_common::async_util::block_on(client.delete(key, None)).map_err(|e| {
            SchedulerError::Transport {
                message: format!("etcd delete failed: {e}"),
            }
        })?;
        Ok(())
    }
}

impl MetadataStore for EtcdMetadataStore {
    fn append_event(&mut self, event: EventLogEvent) -> SchedulerResult<()> {
        // Events are audit-only; not persisted to etcd (see module-level docs).
        self.events.push(event);
        Ok(())
    }

    fn events(&self) -> &[EventLogEvent] {
        &self.events
    }

    fn save_job(&mut self, record: &JobRecord) -> SchedulerResult<()> {
        let key = format!("{JOB_KEY_PREFIX}{}", record.job_id().as_str());
        let persisted = PersistedJobRecord::from(record);
        let bytes = serde_json::to_vec(&persisted).map_err(|e| SchedulerError::Transport {
            message: format!("etcd job encode failed for {}: {e}", record.job_id()),
        })?;
        self.put_key(key, bytes)
    }

    fn jobs(&self) -> &[JobRecord] {
        &self.startup_jobs
    }

    fn save_executor(
        &mut self,
        descriptor: &krishiv_proto::ExecutorDescriptor,
    ) -> SchedulerResult<()> {
        let key = format!("{EXECUTOR_KEY_PREFIX}{}", descriptor.executor_id().as_str());
        let persisted = PersistedExecutorDescriptor::from(descriptor);
        let bytes = serde_json::to_vec(&persisted).map_err(|e| SchedulerError::Transport {
            message: format!(
                "etcd executor encode failed for {}: {e}",
                descriptor.executor_id()
            ),
        })?;
        self.put_key(key, bytes)
    }

    fn executors(&self) -> Vec<krishiv_proto::ExecutorDescriptor> {
        self.startup_executors.clone()
    }

    fn remove_executor(&mut self, executor_id: &krishiv_proto::ExecutorId) -> SchedulerResult<()> {
        let key = format!("{EXECUTOR_KEY_PREFIX}{}", executor_id.as_str());
        self.delete_key(key)
    }

    fn save_continuous_snapshot(
        &mut self,
        job_id: &str,
        snapshot: ContinuousSnapshot,
    ) -> SchedulerResult<()> {
        let key = continuous_key(job_id);
        let bytes = snapshot.encode()?;
        self.put_key(key, bytes)?;
        self.continuous_snapshots
            .insert(job_id.to_owned(), snapshot);
        Ok(())
    }

    fn load_continuous_snapshot(&self, job_id: &str) -> Option<ContinuousSnapshot> {
        self.continuous_snapshots.get(job_id).cloned()
    }

    fn remove_continuous_snapshot(&mut self, job_id: &str) -> SchedulerResult<()> {
        self.delete_key(continuous_key(job_id))?;
        self.continuous_snapshots.remove(job_id);
        Ok(())
    }

    fn save_job_history(&mut self, record: JobHistoryRecord) -> SchedulerResult<()> {
        let bytes = serde_json::to_vec(&record).map_err(|e| SchedulerError::Transport {
            message: format!("etcd job history encode failed for {}: {e}", record.job_id),
        })?;
        self.put_key(history_key(&record.job_id), bytes)?;
        self.history.retain(|r| r.job_id != record.job_id);
        self.history.insert(0, record);
        self.history = sort_history(std::mem::take(&mut self.history));
        while self.history.len() > MAX_JOB_HISTORY {
            if let Some(evicted) = self.history.pop() {
                self.delete_key(history_key(&evicted.job_id))?;
            }
        }
        Ok(())
    }

    fn list_job_history(&self) -> Vec<JobHistoryRecord> {
        self.history.clone()
    }

    fn get_job_history(&self, job_id: &str) -> Option<JobHistoryRecord> {
        self.history.iter().find(|r| r.job_id == job_id).cloned()
    }
}

fn continuous_key(job_id: &str) -> String {
    format!("{CONTINUOUS_KEY_PREFIX}{job_id}")
}

fn history_key(job_id: &str) -> String {
    format!("{HISTORY_KEY_PREFIX}{job_id}")
}

fn sort_history(mut history: Vec<JobHistoryRecord>) -> Vec<JobHistoryRecord> {
    history.sort_by_key(|record| std::cmp::Reverse(record.completed_at_ms));
    history
}

fn truncate_history(mut history: Vec<JobHistoryRecord>) -> Vec<JobHistoryRecord> {
    history.truncate(MAX_JOB_HISTORY);
    history
}

/// Load all values under `prefix` from etcd, deserializing each as `P` then
/// converting to `T` via `TryFrom`.
async fn load_prefix<P, T>(client: &mut Client, prefix: &str) -> Result<Vec<T>, String>
where
    P: serde::de::DeserializeOwned,
    T: TryFrom<P>,
    <T as TryFrom<P>>::Error: std::fmt::Display,
{
    let resp = client
        .get(prefix, Some(GetOptions::new().with_prefix()))
        .await
        .map_err(|e| format!("etcd get prefix {prefix} failed: {e}"))?;

    let mut results = Vec::with_capacity(resp.kvs().len());
    for kv in resp.kvs() {
        let persisted: P = serde_json::from_slice(kv.value()).map_err(|e| {
            format!(
                "etcd decode failed for key {}: {e}",
                kv.key_str().unwrap_or("?")
            )
        })?;
        let record = T::try_from(persisted).map_err(|e| {
            format!(
                "etcd record convert failed for key {}: {e}",
                kv.key_str().unwrap_or("?")
            )
        })?;
        results.push(record);
    }
    Ok(results)
}

async fn load_json_prefix<T>(client: &mut Client, prefix: &str) -> Result<Vec<T>, String>
where
    T: serde::de::DeserializeOwned,
{
    let resp = client
        .get(prefix, Some(GetOptions::new().with_prefix()))
        .await
        .map_err(|e| format!("etcd get prefix {prefix} failed: {e}"))?;

    let mut results = Vec::with_capacity(resp.kvs().len());
    for kv in resp.kvs() {
        let record: T = serde_json::from_slice(kv.value()).map_err(|e| {
            format!(
                "etcd decode failed for key {}: {e}",
                kv.key_str().unwrap_or("?")
            )
        })?;
        results.push(record);
    }
    Ok(results)
}

async fn load_continuous_snapshots(
    client: &mut Client,
) -> Result<std::collections::HashMap<String, ContinuousSnapshot>, String> {
    let resp = client
        .get(CONTINUOUS_KEY_PREFIX, Some(GetOptions::new().with_prefix()))
        .await
        .map_err(|e| format!("etcd get prefix {CONTINUOUS_KEY_PREFIX} failed: {e}"))?;

    let mut snapshots = std::collections::HashMap::with_capacity(resp.kvs().len());
    for kv in resp.kvs() {
        let key = kv.key_str().unwrap_or("?");
        let job_id = key
            .strip_prefix(CONTINUOUS_KEY_PREFIX)
            .ok_or_else(|| format!("etcd continuous snapshot key has wrong prefix: {key}"))?;
        let snapshot = ContinuousSnapshot::decode(kv.value())
            .map_err(|e| format!("etcd continuous snapshot decode failed for {key}: {e}"))?;
        snapshots.insert(job_id.to_owned(), snapshot);
    }
    Ok(snapshots)
}

#[cfg(feature = "etcd")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{JobHistoryRecord, PersistedExecutorDescriptor, PersistedJobRecord};
    use krishiv_proto::{ExecutorDescriptor, ExecutorId, JobId};

    #[test]
    fn job_key_has_correct_prefix() {
        let job_id = "my-job-123";
        let key = format!("{JOB_KEY_PREFIX}{job_id}");
        assert_eq!(key, "/krishiv/jobs/my-job-123");
    }

    #[test]
    fn executor_key_has_correct_prefix() {
        let executor_id = "exec-0";
        let key = format!("{EXECUTOR_KEY_PREFIX}{executor_id}");
        assert_eq!(key, "/krishiv/executors/exec-0");
    }

    #[test]
    fn continuous_and_history_keys_have_correct_prefixes() {
        assert_eq!(
            continuous_key("job-stream-1"),
            "/krishiv/continuous/job-stream-1"
        );
        assert_eq!(history_key("job-batch-1"), "/krishiv/history/job-batch-1");
    }

    #[test]
    fn history_sorting_is_most_recent_first_and_bounded() {
        let mut records = Vec::new();
        for i in 0..(MAX_JOB_HISTORY + 2) {
            records.push(JobHistoryRecord {
                job_id: format!("job-{i}"),
                job_kind: "batch".into(),
                final_state: "succeeded".into(),
                completed_at_ms: i as u64,
                stage_count: 1,
                task_count: 1,
                succeeded_task_count: 1,
                failed_task_count: 0,
                cpu_nanos: 0,
                memory_peak_task_bytes: 0,
                namespace_id: None,
                priority: 0,
            });
        }

        let sorted = truncate_history(sort_history(records));

        assert_eq!(sorted.len(), MAX_JOB_HISTORY);
        assert_eq!(sorted[0].completed_at_ms, (MAX_JOB_HISTORY + 1) as u64);
        assert_eq!(sorted[MAX_JOB_HISTORY - 1].completed_at_ms, 2);
    }

    #[test]
    fn job_record_serializes_and_deserializes() {
        let job_id = JobId::try_new("roundtrip-job").unwrap();
        let spec = crate::job_spec_from_logical_plan(
            job_id.clone(),
            &krishiv_plan::LogicalPlan::new("p", krishiv_plan::ExecutionKind::Batch),
        )
        .unwrap();
        let record = JobRecord::from_spec(spec, 1);
        let persisted = PersistedJobRecord::from(&record);
        let bytes = serde_json::to_vec(&persisted).unwrap();
        let decoded: PersistedJobRecord = serde_json::from_slice(&bytes).unwrap();
        let restored = JobRecord::try_from(decoded).unwrap();
        assert_eq!(restored.job_id(), record.job_id());
    }

    #[test]
    fn executor_descriptor_serializes_and_deserializes() {
        let exec = ExecutorDescriptor::new(ExecutorId::try_new("exec-a").unwrap(), "host-a", 4)
            .with_task_endpoint("http://host-a:9010")
            .with_barrier_endpoint("http://host-a:9011");
        let persisted = PersistedExecutorDescriptor::from(&exec);
        let bytes = serde_json::to_vec(&persisted).unwrap();
        let decoded: PersistedExecutorDescriptor = serde_json::from_slice(&bytes).unwrap();
        let restored = ExecutorDescriptor::try_from(decoded).unwrap();
        assert_eq!(restored.executor_id(), exec.executor_id());
        assert_eq!(restored.task_endpoint(), exec.task_endpoint());
    }
}
