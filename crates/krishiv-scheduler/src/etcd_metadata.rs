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
//! | `/krishiv/ivm/<job_id>` | Binary complete IVM job snapshot |
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

use etcd_client::{Client, GetOptions, KeyValue, KvClient, SortOrder, SortTarget};

use crate::store::{
    ContinuousSnapshot, EventLogEvent, JobHistoryRecord, MAX_JOB_HISTORY, MetadataStore,
    PersistedExecutorDescriptor, PersistedJobRecord,
};
use crate::{JobRecord, SchedulerError, SchedulerResult};

const JOB_KEY_PREFIX: &str = "/krishiv/jobs/";
const EXECUTOR_KEY_PREFIX: &str = "/krishiv/executors/";
const CONTINUOUS_KEY_PREFIX: &str = "/krishiv/continuous/";
const IVM_KEY_PREFIX: &str = "/krishiv/ivm/";
const HISTORY_KEY_PREFIX: &str = "/krishiv/history/";

/// Durable metadata store backed by per-record etcd keys.
///
/// Each job and executor descriptor lives under its own key so writes are
/// O(1) regardless of cluster size, and the 1.5 MiB etcd value limit only
/// applies per-record rather than to the full metadata snapshot.
///
/// # Cache contract
///
/// `startup_jobs` and `startup_executors` are populated at `connect()` time and
/// refreshed atomically immediately before a standby is promoted. All writes
/// (`save_job`, `save_executor`, `remove_executor`) go directly to etcd; the
/// in-memory fields are not touched between recovery refreshes.
///
/// This eliminates split-brain between the in-memory view and etcd that would
/// otherwise arise when a network timeout causes `put` to return an error even
/// though the server committed the write.  `jobs()` and `executors()` are called
/// only during coordinator recovery, where the freshly loaded snapshot is
/// authoritative. For all other in-session state, the coordinator's own
/// `job_coordinators` map is the source of truth.
pub struct EtcdMetadataStore {
    client: std::sync::Mutex<Client>,
    events: Vec<EventLogEvent>,
    /// Startup-time snapshot loaded from etcd.  Read-only after construction.
    startup_jobs: Vec<JobRecord>,
    /// Startup-time snapshot loaded from etcd.  Read-only after construction.
    startup_executors: Vec<krishiv_proto::ExecutorDescriptor>,
    continuous_snapshots: std::collections::HashMap<String, ContinuousSnapshot>,
    ivm_snapshots: std::collections::HashMap<String, Vec<u8>>,
    history: Vec<JobHistoryRecord>,
}

impl EtcdMetadataStore {
    /// Connect to etcd and load all job and executor records from their
    /// individual keys.
    pub async fn connect(endpoints: Vec<String>) -> SchedulerResult<Self> {
        let client =
            Client::connect(endpoints, None)
                .await
                .map_err(|e| SchedulerError::Transport {
                    message: format!("etcd metadata connect failed: {e}"),
                })?;
        // Metadata reads go through a kv client with a raised decode cap
        // (see wide_kv / ETCD_MAX_DECODE_BYTES); the plain `client` below is
        // kept only for the O(1) per-key put/delete writes.
        let mut kv = wide_kv(&client);

        let jobs = load_prefix::<PersistedJobRecord, JobRecord>(&mut kv, JOB_KEY_PREFIX)
            .await
            .map_err(|e| SchedulerError::Transport {
                message: format!("etcd jobs load failed: {e}"),
            })?;

        let executor_descriptors = load_prefix::<
            PersistedExecutorDescriptor,
            krishiv_proto::ExecutorDescriptor,
        >(&mut kv, EXECUTOR_KEY_PREFIX)
        .await
        .map_err(|e| SchedulerError::Transport {
            message: format!("etcd executors load failed: {e}"),
        })?;

        let continuous_snapshots = load_continuous_snapshots(&mut kv).await.map_err(|e| {
            SchedulerError::Transport {
                message: format!("etcd continuous snapshots load failed: {e}"),
            }
        })?;

        let ivm_snapshots = load_binary_prefix(&mut kv, IVM_KEY_PREFIX)
            .await
            .map_err(|e| SchedulerError::Transport {
                message: format!("etcd IVM snapshots load failed: {e}"),
            })?;

        let history = load_json_prefix::<JobHistoryRecord>(&mut kv, HISTORY_KEY_PREFIX)
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
            ivm_snapshots,
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
    fn refresh(&mut self) -> SchedulerResult<()> {
        let client = self
            .client
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let (jobs, executors, snapshots, ivm_snapshots, history) =
            krishiv_common::async_util::block_on(async move {
                let mut kv = wide_kv(&client);
                let jobs =
                    load_prefix::<PersistedJobRecord, JobRecord>(&mut kv, JOB_KEY_PREFIX)
                        .await
                        .map_err(|e| SchedulerError::Transport {
                            message: format!("etcd jobs refresh failed: {e}"),
                        })?;
                let executors = load_prefix::<
                    PersistedExecutorDescriptor,
                    krishiv_proto::ExecutorDescriptor,
                >(&mut kv, EXECUTOR_KEY_PREFIX)
                .await
                .map_err(|e| SchedulerError::Transport {
                    message: format!("etcd executors refresh failed: {e}"),
                })?;
                let snapshots = load_continuous_snapshots(&mut kv).await.map_err(|e| {
                    SchedulerError::Transport {
                        message: format!("etcd continuous snapshots refresh failed: {e}"),
                    }
                })?;
                let ivm_snapshots = load_binary_prefix(&mut kv, IVM_KEY_PREFIX)
                    .await
                    .map_err(|e| SchedulerError::Transport {
                        message: format!("etcd IVM snapshots refresh failed: {e}"),
                    })?;
                let history = load_json_prefix::<JobHistoryRecord>(&mut kv, HISTORY_KEY_PREFIX)
                    .await
                    .map_err(|e| SchedulerError::Transport {
                        message: format!("etcd job history refresh failed: {e}"),
                    })?;
                Ok::<_, SchedulerError>((jobs, executors, snapshots, ivm_snapshots, history))
            })?;

        // Replace the recovery cache only after every prefix loaded successfully;
        // a partial etcd read must never become a promotable coordinator view.
        self.startup_jobs = jobs;
        self.startup_executors = executors;
        self.continuous_snapshots = snapshots;
        self.ivm_snapshots = ivm_snapshots;
        self.history = truncate_history(sort_history(history));
        Ok(())
    }

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

    fn save_ivm_snapshot(&mut self, job_id: &str, snapshot: Vec<u8>) -> SchedulerResult<()> {
        self.put_key(ivm_key(job_id), snapshot.clone())?;
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
        self.delete_key(ivm_key(job_id))?;
        self.ivm_snapshots.remove(job_id);
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

fn ivm_key(job_id: &str) -> String {
    format!("{IVM_KEY_PREFIX}{job_id}")
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
/// Raised gRPC decode cap for the etcd metadata reads (default is 4 MiB).
/// A single IVM snapshot value is already ~1.5 MiB, and even one paged
/// response can carry several — plus a lone snapshot can itself exceed
/// 4 MiB — so the coordinator must accept large decoded messages or it
/// crash-loops on startup the moment durable state grows (observed
/// 2026-07-20 on the Phase 58 HA cluster: five IVM snapshots totalling
/// ~5 MB under `/krishiv/ivm/` → "decoded message length too large: found
/// 5027816 bytes, the limit is 4194304"; a fresh or failover coordinator
/// could then never load state — a hard availability cliff). 256 MiB is a
/// cap, not a preallocation (only the actual response is allocated), and
/// stays well under the coordinator's 2 GiB memory limit for a realistic
/// prefix. Pagination below caps per-message size in the healthy case;
/// the raised decode limit is what makes an oversized single response —
/// or a single oversized value — decodable at all.
const ETCD_MAX_DECODE_BYTES: usize = 256 * 1024 * 1024;

/// Key-values fetched per etcd range page — pagination bounds per-message
/// memory in the common case (a large decode cap alone would let one
/// response balloon). Kept small because individual snapshots are ~MiB.
const ETCD_PAGE_LIMIT: i64 = 8;

/// A [`KvClient`] with the raised decode cap applied. `Client::get` uses a
/// private 4 MiB-capped kv client, but `kv_client()` hands back a clone on
/// the same channel that we can reconfigure — so every metadata load goes
/// through this, both at `connect` and on `refresh`.
fn wide_kv(client: &Client) -> KvClient {
    client
        .kv_client()
        .max_decoding_message_size(ETCD_MAX_DECODE_BYTES)
}

/// Lexicographic end of a prefix range: the smallest key strictly greater
/// than every key under `prefix` (increment the last byte below 0xff,
/// dropping trailing 0xff bytes). An all-0xff (or empty) prefix has no
/// finite end → empty vec, which etcd range semantics read as "to the end
/// of the keyspace".
fn prefix_range_end(prefix: &str) -> Vec<u8> {
    let mut end = prefix.as_bytes().to_vec();
    while let Some(&last) = end.last() {
        if last < 0xff {
            *end.last_mut().unwrap() = last + 1;
            return end;
        }
        end.pop();
    }
    Vec::new()
}

/// Read every key-value under `prefix` in bounded, key-ascending pages so
/// no single etcd range response can exceed the gRPC decode limit. Each
/// page resumes strictly past the previous page's last key.
async fn get_prefix_paged(client: &mut KvClient, prefix: &str) -> Result<Vec<KeyValue>, String> {
    let range_end = prefix_range_end(prefix);
    let mut start = prefix.as_bytes().to_vec();
    let mut out: Vec<KeyValue> = Vec::new();
    loop {
        let opts = GetOptions::new()
            .with_range(range_end.clone())
            .with_limit(ETCD_PAGE_LIMIT)
            .with_sort(SortTarget::Key, SortOrder::Ascend);
        let resp = client
            .get(start.clone(), Some(opts))
            .await
            .map_err(|e| format!("etcd get prefix {prefix} failed: {e}"))?;
        let kvs = resp.kvs();
        if kvs.is_empty() {
            break;
        }
        let page_len = kvs.len();
        let last_key = kvs[page_len - 1].key().to_vec();
        out.extend(kvs.iter().cloned());
        if (page_len as i64) < ETCD_PAGE_LIMIT {
            break;
        }
        // Resume strictly after the last key returned (append a 0 byte).
        start = last_key;
        start.push(0);
    }
    Ok(out)
}

async fn load_prefix<P, T>(client: &mut KvClient, prefix: &str) -> Result<Vec<T>, String>
where
    P: serde::de::DeserializeOwned,
    T: TryFrom<P>,
    <T as TryFrom<P>>::Error: std::fmt::Display,
{
    let kvs = get_prefix_paged(client, prefix).await?;

    let mut results = Vec::with_capacity(kvs.len());
    for kv in &kvs {
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

async fn load_json_prefix<T>(client: &mut KvClient, prefix: &str) -> Result<Vec<T>, String>
where
    T: serde::de::DeserializeOwned,
{
    let kvs = get_prefix_paged(client, prefix).await?;

    let mut results = Vec::with_capacity(kvs.len());
    for kv in &kvs {
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
    client: &mut KvClient,
) -> Result<std::collections::HashMap<String, ContinuousSnapshot>, String> {
    let kvs = get_prefix_paged(client, CONTINUOUS_KEY_PREFIX).await?;

    let mut snapshots = std::collections::HashMap::with_capacity(kvs.len());
    for kv in &kvs {
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

async fn load_binary_prefix(
    client: &mut KvClient,
    prefix: &str,
) -> Result<std::collections::HashMap<String, Vec<u8>>, String> {
    let kvs = get_prefix_paged(client, prefix).await?;
    let mut snapshots = std::collections::HashMap::with_capacity(kvs.len());
    for kv in &kvs {
        let key = kv.key_str().unwrap_or("?");
        let job_id = key
            .strip_prefix(prefix)
            .ok_or_else(|| format!("etcd snapshot key has wrong prefix: {key}"))?;
        snapshots.insert(job_id.to_owned(), kv.value().to_vec());
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
    fn prefix_range_end_covers_every_key_under_the_prefix() {
        // The end must be strictly greater than every "<prefix><suffix>" key
        // and strictly less than the next unrelated prefix, so a paged range
        // scan reads exactly the prefix's keys and stops.
        let end = prefix_range_end(IVM_KEY_PREFIX);
        assert_eq!(end, b"/krishiv/ivm0"); // '/' (0x2f) -> '0' (0x30)
        let under = b"/krishiv/ivm/some-job-id".to_vec();
        assert!(under.as_slice() < end.as_slice(), "a prefixed key sorts before the range end");
        assert!(IVM_KEY_PREFIX.as_bytes() < end.as_slice(), "the prefix itself sorts before its end");
    }

    #[test]
    fn prefix_range_end_increments_the_last_byte() {
        // The last byte is incremented (0xff never appears in a &str prefix,
        // so the carry/pop branch is defensive-only). Every real prefix here
        // ends in '/', which increments to '0'.
        assert_eq!(prefix_range_end("ab"), b"ac");
        assert_eq!(prefix_range_end(JOB_KEY_PREFIX), b"/krishiv/jobs0");
        assert_eq!(prefix_range_end(HISTORY_KEY_PREFIX), b"/krishiv/history0");
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
