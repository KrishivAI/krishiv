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
//! | `/krishiv/ivm/<job_id>` | IVM snapshot manifest (magic header; compressed data inline when small) |
//! | `/krishiv/ivm/<job_id>#<index>` | IVM snapshot chunk (only when the compressed snapshot exceeds one etcd value) |
//! | `/krishiv/history/<job_id>` | JSON-encoded terminal `JobHistoryRecord` |
//!
//! Events are not persisted — they are audit-only and kept in-memory.
//!
//! # IVM snapshot size
//!
//! Per-record keys keep every *other* record small, but a single IVM job
//! snapshot is one record that can itself exceed etcd's 1.5 MiB
//! `--max-request-bytes` write ceiling (observed 2026-07-20: a ~1.57 MiB
//! snapshot rejected with "etcdserver: request is too large", failing the
//! IVM write path with HTTP 503 under the Phase 58 HA chaos gate). IVM
//! snapshots are therefore zstd-compressed and, if the compressed value
//! still exceeds one etcd value, split across `#<index>` chunk keys —
//! bounding every PUT well under the ceiling with no fixed size cliff. See
//! [`save_ivm_snapshot`] / [`reassemble_ivm_snapshot`].

// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges the sync MetadataStore trait to the async etcd client.
#![allow(clippy::disallowed_methods)]
//!
//! # Persist mechanism
//!
//! `MetadataStore` is a sync trait called from within the coordinator's async
//! write-lock. [`etcd_block_on`] bridges the sync trait to the async etcd
//! client by running every etcd future on a **dedicated etcd runtime** and
//! blocking the caller on a channel — never driving etcd I/O on the caller's
//! scheduling runtime, which would deadlock the coordinator under load (see
//! [`ETCD_RUNTIME`]).

use etcd_client::{Client, DeleteOptions, GetOptions, KeyValue, KvClient, SortOrder, SortTarget};

use crate::store::{
    ContinuousSnapshot, EventLogEvent, JobHistoryRecord, MAX_JOB_HISTORY, MetadataStore,
    PersistedExecutorDescriptor, PersistedJobRecord,
};
use crate::{JobRecord, SchedulerError, SchedulerResult};

/// Dedicated Tokio runtime for **all** etcd I/O.
///
/// The `MetadataStore` trait is synchronous but the etcd client is async, so
/// every call bridges sync→async by blocking. If that bridge drove the etcd
/// future on the *caller's* runtime (via `block_in_place(handle.block_on(..))`),
/// it deadlocks the coordinator under load: enough concurrent metadata writes
/// (executor churn during a chaos fault) park every worker thread in
/// `block_in_place`, leaving no thread to drive the etcd I/O reactor those
/// same threads are blocked waiting on — the whole coordinator wedges, so
/// `/leaderz` stops answering and the pod is dropped from Service routing with
/// no leader endpoint (observed 2026-07-20 on the Phase 58 HA chaos gate: all
/// coordinators frozen for minutes while `/healthz` on the dedicated liveness
/// thread stayed up). Running every etcd future on this separate runtime keeps
/// the etcd reactor always live, independent of how saturated the scheduling
/// runtime is; the caller only ever blocks on a plain channel receive.
static ETCD_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

fn etcd_runtime() -> &'static tokio::runtime::Runtime {
    ETCD_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("krishiv-etcd")
            .build()
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "failed to build the dedicated etcd runtime; aborting");
                std::process::abort()
            })
    })
}

/// Drive an etcd future to completion on [`etcd_runtime`], blocking the caller
/// for the result. The future runs on the dedicated runtime (its reactor is
/// never starved by the scheduling runtime); the caller waits on a std channel,
/// wrapped in `block_in_place` on a multi-thread worker so that runtime is told
/// the worker is parked rather than silently losing it. A current-thread caller
/// blocks directly — safe here because the result is produced by a *different*
/// runtime, so there is no self-deadlock.
fn etcd_block_on<F>(fut: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    etcd_runtime().spawn(async move {
        let _ = tx.send(fut.await);
    });
    let recv = move || rx.recv().expect("etcd runtime dropped the task before sending a result");
    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(recv),
        _ => recv(),
    }
}

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

        let ivm_snapshots = load_ivm_snapshots(&mut kv)
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
        etcd_block_on(async move { client.put(key, value, None).await }).map_err(|e| {
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
        etcd_block_on(async move { client.delete(key, None).await }).map_err(|e| {
            SchedulerError::Transport {
                message: format!("etcd delete failed: {e}"),
            }
        })?;
        Ok(())
    }

    /// Delete every key in the half-open range `[start, end)` from etcd. Used
    /// to sweep IVM snapshot chunk keys (`/krishiv/ivm/<job>#…`) — both when a
    /// snapshot shrinks (surplus higher-index chunks) and when it is removed.
    fn delete_range(&self, start: Vec<u8>, end: Vec<u8>) -> SchedulerResult<()> {
        let mut client = self
            .client
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let opts = DeleteOptions::new().with_range(end);
        etcd_block_on(async move { client.delete(start, Some(opts)).await }).map_err(|e| {
            SchedulerError::Transport {
                message: format!("etcd delete range failed: {e}"),
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
            etcd_block_on(async move {
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
                let ivm_snapshots = load_ivm_snapshots(&mut kv)
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
        // A single IVM snapshot can exceed etcd's 1.5 MiB per-value write limit
        // (see module docs). Compress, then store the compressed payload inline
        // in the manifest when it fits one etcd value, else split it across
        // bounded `#<index>` chunk keys. Chunks are written before the manifest
        // so the manifest (the commit point) only becomes visible once its data
        // is durable; a crash mid-write leaves the manifest pointing at a
        // consistent prior/partial set that recovery detects and skips rather
        // than silently corrupting.
        let raw_len = snapshot.len() as u64;
        let (codec, payload) = compress_ivm_payload(&snapshot);
        let chunk_prefix = ivm_chunk_prefix(job_id);
        let chunk_prefix_end = prefix_range_end(&chunk_prefix);

        if payload.len() <= IVM_INLINE_MAX {
            // Inline: manifest carries the whole (compressed) payload.
            let mut value = ivm_manifest_header(codec, 0, raw_len);
            value.extend_from_slice(&payload);
            self.put_key(ivm_key(job_id), value)?;
            // Sweep any chunk keys left by a previous, larger snapshot.
            self.delete_range(chunk_prefix.into_bytes(), chunk_prefix_end)?;
        } else {
            let chunks: Vec<&[u8]> = payload.chunks(IVM_CHUNK_BYTES).collect();
            let chunk_count = chunks.len() as u32;
            for (i, chunk) in chunks.iter().enumerate() {
                self.put_key(ivm_chunk_key(job_id, i as u32), chunk.to_vec())?;
            }
            // Commit: publish the manifest only after every chunk is durable.
            self.put_key(ivm_key(job_id), ivm_manifest_header(codec, chunk_count, raw_len))?;
            // Sweep surplus chunks (index >= chunk_count) from a larger prior
            // snapshot; the just-written chunks 0..chunk_count are untouched.
            self.delete_range(
                ivm_chunk_key(job_id, chunk_count).into_bytes(),
                chunk_prefix_end,
            )?;
        }
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
        // Also drop any chunk keys the snapshot spilled into.
        let chunk_prefix = ivm_chunk_prefix(job_id);
        let chunk_prefix_end = prefix_range_end(&chunk_prefix);
        self.delete_range(chunk_prefix.into_bytes(), chunk_prefix_end)?;
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

/// Separator between an IVM job id and a chunk index in a chunk key. `#`
/// (0x23) sorts below every character a job id can contain (`[A-Za-z0-9_-]`,
/// all ≥ 0x2d), so a job's chunk keys always sort immediately after its
/// manifest key and never interleave with another job's keys.
const IVM_CHUNK_SEP: char = '#';

/// Prefix under which one IVM job's snapshot chunks live: `/krishiv/ivm/<job>#`.
fn ivm_chunk_prefix(job_id: &str) -> String {
    format!("{IVM_KEY_PREFIX}{job_id}{IVM_CHUNK_SEP}")
}

/// Chunk key `/krishiv/ivm/<job>#<index:08x>` — zero-padded hex so keys sort
/// in ascending chunk-index order under an etcd range scan.
fn ivm_chunk_key(job_id: &str, index: u32) -> String {
    format!("{IVM_KEY_PREFIX}{job_id}{IVM_CHUNK_SEP}{index:08x}")
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

// ── IVM snapshot codec (compression + chunking) ──────────────────────────
//
// A single IVM snapshot can exceed etcd's 1.5 MiB per-value write ceiling.
// Snapshots are zstd-compressed and, when still too large, split across
// bounded chunk keys. The manifest at `/krishiv/ivm/<job>` is self-describing
// (magic-prefixed) so a legacy uncompressed value (no magic) is still read
// verbatim, keeping recovery working across a rolling upgrade.

/// Magic prefix identifying a chunked/compressed IVM manifest value. Legacy
/// raw snapshots (written before this format) will not start with it.
const IVM_MANIFEST_MAGIC: &[u8] = b"KIVMSNP";
const IVM_MANIFEST_VERSION: u8 = 1;
const IVM_CODEC_NONE: u8 = 0;
const IVM_CODEC_ZSTD: u8 = 1;
/// magic(7) + version(1) + codec(1) + flags(1) + chunk_count(u32) + raw_len(u64).
const IVM_HEADER_LEN: usize = IVM_MANIFEST_MAGIC.len() + 1 + 1 + 1 + 4 + 8;
/// Max bytes per etcd value we write — well under etcd's 1.5 MiB
/// `--max-request-bytes` ceiling to leave room for key + gRPC framing.
const IVM_CHUNK_BYTES: usize = 1024 * 1024;
/// Largest compressed payload we inline into the manifest value (header + this
/// must stay one etcd value).
const IVM_INLINE_MAX: usize = IVM_CHUNK_BYTES - IVM_HEADER_LEN;
/// zstd level: fast, since this is on the coordinator write path.
const IVM_ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IvmManifestHeader {
    codec: u8,
    chunk_count: u32,
    raw_len: u64,
}

/// Compress a raw snapshot, falling back to storing it uncompressed if zstd
/// fails or would expand it (incompressible payloads).
fn compress_ivm_payload(raw: &[u8]) -> (u8, Vec<u8>) {
    match zstd::encode_all(raw, IVM_ZSTD_LEVEL) {
        Ok(compressed) if compressed.len() < raw.len() => (IVM_CODEC_ZSTD, compressed),
        _ => (IVM_CODEC_NONE, raw.to_vec()),
    }
}

/// Build the fixed-size manifest header. `chunk_count == 0` means the payload
/// is inlined after this header in the same value; otherwise it lives in
/// `chunk_count` chunk keys.
fn ivm_manifest_header(codec: u8, chunk_count: u32, raw_len: u64) -> Vec<u8> {
    let mut header = Vec::with_capacity(IVM_HEADER_LEN);
    header.extend_from_slice(IVM_MANIFEST_MAGIC);
    header.push(IVM_MANIFEST_VERSION);
    header.push(codec);
    header.push(0); // flags (reserved)
    header.extend_from_slice(&chunk_count.to_le_bytes());
    header.extend_from_slice(&raw_len.to_le_bytes());
    header
}

/// Parse a manifest header. Returns `None` when the value is not our format
/// (no magic, truncated, or an unknown version) so the caller can decide
/// between "legacy raw value" and "corrupt".
fn parse_ivm_manifest_header(value: &[u8]) -> Option<IvmManifestHeader> {
    if value.len() < IVM_HEADER_LEN || !value.starts_with(IVM_MANIFEST_MAGIC) {
        return None;
    }
    let mut p = IVM_MANIFEST_MAGIC.len();
    if value[p] != IVM_MANIFEST_VERSION {
        return None;
    }
    p += 1;
    let codec = value[p];
    p += 2; // skip codec + flags
    let chunk_count = u32::from_le_bytes(value[p..p + 4].try_into().ok()?);
    p += 4;
    let raw_len = u64::from_le_bytes(value[p..p + 8].try_into().ok()?);
    Some(IvmManifestHeader {
        codec,
        chunk_count,
        raw_len,
    })
}

/// Reconstruct a raw IVM snapshot from its manifest value and (if chunked) its
/// chunk keys. A value without our magic is a legacy uncompressed snapshot and
/// is returned verbatim. Any inconsistency (missing chunk, bad codec, length
/// mismatch, decode failure) returns `Err` so recovery skips the snapshot
/// rather than surfacing a corrupt one.
fn reassemble_ivm_snapshot(
    manifest_value: &[u8],
    chunks: Option<&std::collections::BTreeMap<u32, Vec<u8>>>,
) -> Result<Vec<u8>, String> {
    if !manifest_value.starts_with(IVM_MANIFEST_MAGIC) {
        // Legacy pre-format snapshot: the value IS the raw snapshot.
        return Ok(manifest_value.to_vec());
    }
    let header = parse_ivm_manifest_header(manifest_value)
        .ok_or_else(|| "corrupt or unsupported IVM manifest header".to_string())?;

    let payload = if header.chunk_count == 0 {
        manifest_value[IVM_HEADER_LEN..].to_vec()
    } else {
        let chunks = chunks.ok_or_else(|| {
            format!(
                "IVM manifest declares {} chunk(s) but none are present",
                header.chunk_count
            )
        })?;
        let mut buf = Vec::new();
        for i in 0..header.chunk_count {
            let chunk = chunks
                .get(&i)
                .ok_or_else(|| format!("missing IVM chunk {i} of {}", header.chunk_count))?;
            buf.extend_from_slice(chunk);
        }
        buf
    };

    let raw = match header.codec {
        IVM_CODEC_NONE => payload,
        IVM_CODEC_ZSTD => zstd::decode_all(&payload[..])
            .map_err(|e| format!("IVM snapshot zstd decode failed: {e}"))?,
        other => return Err(format!("unknown IVM snapshot codec {other}")),
    };
    if raw.len() as u64 != header.raw_len {
        return Err(format!(
            "IVM snapshot length mismatch: reassembled {} bytes, manifest declared {}",
            raw.len(),
            header.raw_len
        ));
    }
    Ok(raw)
}

/// Load every IVM snapshot under `/krishiv/ivm/`, reassembling chunked and
/// decompressing compressed values. Manifest keys (`<job>`) and chunk keys
/// (`<job>#<index>`) share the prefix; they are partitioned by the `#`
/// separator. An unrecoverable snapshot is logged and skipped so one bad
/// record never blocks the coordinator from loading the rest.
async fn load_ivm_snapshots(
    client: &mut KvClient,
) -> Result<std::collections::HashMap<String, Vec<u8>>, String> {
    use std::collections::{BTreeMap, HashMap};
    let kvs = get_prefix_paged(client, IVM_KEY_PREFIX).await?;

    let mut manifests: HashMap<String, Vec<u8>> = HashMap::new();
    let mut chunks: HashMap<String, BTreeMap<u32, Vec<u8>>> = HashMap::new();
    for kv in &kvs {
        let key = kv.key_str().unwrap_or("?");
        let remainder = key
            .strip_prefix(IVM_KEY_PREFIX)
            .ok_or_else(|| format!("etcd IVM key has wrong prefix: {key}"))?;
        match remainder.split_once(IVM_CHUNK_SEP) {
            Some((job_id, index_hex)) => {
                let index = u32::from_str_radix(index_hex, 16)
                    .map_err(|e| format!("etcd IVM chunk key {key} has a bad index: {e}"))?;
                chunks
                    .entry(job_id.to_owned())
                    .or_default()
                    .insert(index, kv.value().to_vec());
            }
            None => {
                manifests.insert(remainder.to_owned(), kv.value().to_vec());
            }
        }
    }

    let mut snapshots = HashMap::with_capacity(manifests.len());
    for (job_id, manifest_value) in manifests {
        match reassemble_ivm_snapshot(&manifest_value, chunks.get(&job_id)) {
            Ok(raw) => {
                snapshots.insert(job_id, raw);
            }
            Err(error) => {
                tracing::warn!(
                    job_id = %job_id,
                    %error,
                    "skipping unrecoverable IVM snapshot during etcd load"
                );
            }
        }
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

    // ── IVM snapshot codec (compression + chunking) ──────────────────────

    /// Deterministic, effectively-incompressible bytes (xorshift) so a large
    /// payload actually spills across chunk keys instead of compressing to
    /// inline size.
    fn pseudo_random(len: usize) -> Vec<u8> {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x & 0xff) as u8
            })
            .collect()
    }

    /// Emulate exactly what `save_ivm_snapshot` writes to etcd: a manifest
    /// value plus (when chunked) the chunk-index → bytes map.
    fn encode_ivm_for_test(raw: &[u8]) -> (Vec<u8>, std::collections::BTreeMap<u32, Vec<u8>>) {
        let (codec, payload) = compress_ivm_payload(raw);
        let mut chunks = std::collections::BTreeMap::new();
        let manifest = if payload.len() <= IVM_INLINE_MAX {
            let mut value = ivm_manifest_header(codec, 0, raw.len() as u64);
            value.extend_from_slice(&payload);
            value
        } else {
            let parts: Vec<&[u8]> = payload.chunks(IVM_CHUNK_BYTES).collect();
            for (i, part) in parts.iter().enumerate() {
                chunks.insert(i as u32, part.to_vec());
            }
            ivm_manifest_header(codec, parts.len() as u32, raw.len() as u64)
        };
        (manifest, chunks)
    }

    #[test]
    fn ivm_snapshot_inline_round_trips() {
        let raw = b"a small IVM snapshot that compresses well".repeat(4);
        let (manifest, chunks) = encode_ivm_for_test(&raw);
        // Small + compressible => inline (no chunk keys), magic-prefixed.
        assert!(chunks.is_empty(), "small snapshot must inline");
        assert!(manifest.starts_with(IVM_MANIFEST_MAGIC));
        let restored = reassemble_ivm_snapshot(&manifest, Some(&chunks)).unwrap();
        assert_eq!(restored, raw);
    }

    #[test]
    fn ivm_snapshot_multi_chunk_round_trips() {
        // ~3 MiB incompressible => exceeds one etcd value => multiple chunks,
        // each bounded well under etcd's 1.5 MiB write ceiling.
        let raw = pseudo_random(3 * 1024 * 1024);
        let (manifest, chunks) = encode_ivm_for_test(&raw);
        assert!(chunks.len() >= 3, "large snapshot must span chunks: {}", chunks.len());
        for (idx, chunk) in &chunks {
            assert!(
                chunk.len() <= IVM_CHUNK_BYTES,
                "chunk {idx} is {} bytes, over the per-value bound",
                chunk.len()
            );
        }
        let restored = reassemble_ivm_snapshot(&manifest, Some(&chunks)).unwrap();
        assert_eq!(restored, raw);
    }

    #[test]
    fn ivm_snapshot_legacy_raw_value_is_read_verbatim() {
        // A pre-format value (no magic) must still load, unchanged.
        let legacy = vec![0u8, 1, 2, 3, 200, 201, 202];
        assert!(!legacy.starts_with(IVM_MANIFEST_MAGIC));
        let restored = reassemble_ivm_snapshot(&legacy, None).unwrap();
        assert_eq!(restored, legacy);
    }

    #[test]
    fn ivm_snapshot_missing_chunk_is_an_error_not_corruption() {
        let raw = pseudo_random(3 * 1024 * 1024);
        let (manifest, mut chunks) = encode_ivm_for_test(&raw);
        chunks.remove(&1); // simulate a chunk lost to a mid-write crash
        let err = reassemble_ivm_snapshot(&manifest, Some(&chunks)).unwrap_err();
        assert!(err.contains("missing IVM chunk"), "got: {err}");
    }

    #[test]
    fn ivm_snapshot_length_mismatch_is_rejected() {
        let raw = b"snapshot bytes".repeat(3);
        let (mut manifest, chunks) = encode_ivm_for_test(&raw);
        // Corrupt the declared raw_len in the header (last 8 bytes of header).
        let len_off = IVM_HEADER_LEN - 8;
        manifest[len_off] = manifest[len_off].wrapping_add(1);
        let err = reassemble_ivm_snapshot(&manifest, Some(&chunks)).unwrap_err();
        assert!(err.contains("length mismatch"), "got: {err}");
    }

    #[test]
    fn ivm_chunk_keys_sort_after_manifest_and_before_sibling_jobs() {
        // Contiguous grouping under an ascending key scan is what lets the
        // loader partition manifests from chunks reliably.
        let manifest = ivm_key("job");
        let c0 = ivm_chunk_key("job", 0);
        let c1 = ivm_chunk_key("job", 1);
        let sibling = ivm_key("job-2"); // '-' (0x2d) > '#' (0x23)
        assert!(manifest.as_str() < c0.as_str(), "manifest sorts before its chunks");
        assert!(c0.as_str() < c1.as_str(), "chunks sort by ascending index");
        assert!(c1.as_str() < sibling.as_str(), "a job's chunks sort before a sibling job");
        assert_eq!(ivm_chunk_key("job", 255), "/krishiv/ivm/job#000000ff");
        assert!(ivm_chunk_prefix("job").starts_with(&ivm_key("job")));
    }

    #[test]
    fn compress_ivm_payload_falls_back_to_none_for_incompressible_data() {
        let compressible = vec![7u8; 4096];
        let (codec, _) = compress_ivm_payload(&compressible);
        assert_eq!(codec, IVM_CODEC_ZSTD);
        let incompressible = pseudo_random(4096);
        let (codec, payload) = compress_ivm_payload(&incompressible);
        assert_eq!(codec, IVM_CODEC_NONE);
        assert_eq!(payload, incompressible, "NONE codec stores raw bytes");
    }

    // ── dedicated etcd runtime bridge ────────────────────────────────────

    #[test]
    fn etcd_block_on_drives_future_on_the_dedicated_runtime() {
        // From a plain (no ambient runtime) thread: exercises ETCD_RUNTIME
        // creation, spawn, and the blocking channel receive.
        assert_eq!(etcd_block_on(async { 21u32 * 2 }), 42);
        // A second call reuses the same runtime and still returns.
        assert_eq!(etcd_block_on(async { format!("{}-{}", 4, 2) }), "4-2");
    }

    #[test]
    fn etcd_block_on_result_comes_from_a_separate_runtime_thread() {
        // The future must run on the etcd runtime, never the caller thread, so
        // the caller's runtime can never starve the etcd reactor. Assert the
        // future executes on a `krishiv-etcd`-named thread.
        let name = etcd_block_on(async {
            std::thread::current()
                .name()
                .unwrap_or_default()
                .to_string()
        });
        assert!(
            name.starts_with("krishiv-etcd"),
            "etcd future must run on the dedicated runtime, ran on {name:?}"
        );
    }
}
