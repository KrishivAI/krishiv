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
//!
//! Events are not persisted — they are audit-only and kept in-memory.
//!
//! # Persist mechanism
//!
//! `MetadataStore` is a sync trait called from within the coordinator's async
//! write-lock.  `krishiv_common::async_util::block_on` bridges the sync trait
//! to the async etcd client, using `block_in_place` on multi-thread runtimes
//! and a fallback runtime on current-thread or no-runtime callers.

use etcd_client::{Client, GetOptions};

use crate::store::{
    ContinuousSnapshot, EventLogEvent, MetadataStore, PersistedExecutorDescriptor,
    PersistedJobRecord,
};
use crate::{JobRecord, SchedulerError, SchedulerResult};

const JOB_KEY_PREFIX: &str = "/krishiv/jobs/";
const EXECUTOR_KEY_PREFIX: &str = "/krishiv/executors/";

/// Durable metadata store backed by per-record etcd keys.
///
/// Each job and executor descriptor lives under its own key so writes are
/// O(1) regardless of cluster size, and the 1.5 MiB etcd value limit only
/// applies per-record rather than to the full metadata snapshot.
pub struct EtcdMetadataStore {
    client: std::sync::Mutex<Client>,
    events: Vec<EventLogEvent>,
    jobs: Vec<JobRecord>,
    executor_descriptors: Vec<krishiv_proto::ExecutorDescriptor>,
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

        Ok(Self {
            client: std::sync::Mutex::new(client),
            events: Vec::new(),
            jobs,
            executor_descriptors,
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
        self.put_key(key, bytes)?;
        // Update in-memory view.
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
        self.put_key(key, bytes)?;
        // Update in-memory view.
        if let Some(pos) = self
            .executor_descriptors
            .iter()
            .position(|e| e.executor_id() == descriptor.executor_id())
        {
            self.executor_descriptors[pos] = descriptor.clone();
        } else {
            self.executor_descriptors.push(descriptor.clone());
        }
        Ok(())
    }

    fn executors(&self) -> Vec<krishiv_proto::ExecutorDescriptor> {
        self.executor_descriptors.clone()
    }

    fn remove_executor(&mut self, executor_id: &krishiv_proto::ExecutorId) -> SchedulerResult<()> {
        let key = format!("{EXECUTOR_KEY_PREFIX}{}", executor_id.as_str());
        self.delete_key(key)?;
        self.executor_descriptors
            .retain(|e| e.executor_id() != executor_id);
        Ok(())
    }

    fn save_continuous_snapshot(
        &mut self,
        job_id: &str,
        _snapshot: ContinuousSnapshot,
    ) -> SchedulerResult<()> {
        // Continuous window snapshots may exceed per-record etcd size limits for
        // large key cardinalities. Operators using the etcd backend should persist
        // continuous job state to an external object store and restore manually.
        tracing::warn!(
            job_id = %job_id,
            "EtcdMetadataStore: continuous snapshot persistence is not supported; \
             window state will not survive coordinator restart on this backend"
        );
        Ok(())
    }

    fn load_continuous_snapshot(&self, _job_id: &str) -> Option<ContinuousSnapshot> {
        None
    }

    fn remove_continuous_snapshot(&mut self, _job_id: &str) -> SchedulerResult<()> {
        Ok(())
    }

    fn save_job_history(&mut self, record: crate::store::JobHistoryRecord) -> SchedulerResult<()> {
        tracing::warn!(
            job_id = %record.job_id,
            "EtcdMetadataStore: job history persistence is not supported; \
             history will not survive coordinator restart on this backend"
        );
        Ok(())
    }

    fn list_job_history(&self) -> Vec<crate::store::JobHistoryRecord> {
        Vec::new()
    }

    fn get_job_history(&self, _job_id: &str) -> Option<crate::store::JobHistoryRecord> {
        None
    }
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

#[cfg(feature = "etcd")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{EventLogEvent, PersistedExecutorDescriptor, PersistedJobRecord};
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
