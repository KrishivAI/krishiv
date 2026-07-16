use crate::error::{SchedulerError, SchedulerResult};
use crate::job::JobRecord;
use crate::store::{
    ContinuousSnapshot, EventLogEvent, JobHistoryRecord, MetadataStore, PersistedEvent,
    PersistedExecutorDescriptor, PersistedJobRecord,
};
use krishiv_proto::{ExecutorDescriptor, ExecutorId};
use rocksdb::{ColumnFamilyDescriptor, DB, Options, WriteOptions};
use std::path::Path;

const CF_EVENTS: &str = "events";
const CF_JOBS: &str = "jobs";
const CF_EXECUTORS: &str = "executors";
const CF_METADATA: &str = "metadata";
const CF_CONTINUOUS: &str = "continuous_snapshots";
const CF_IVM: &str = "ivm_snapshots";
const CF_HISTORY: &str = "job_history";

fn all_cfs() -> Vec<ColumnFamilyDescriptor> {
    [
        CF_EVENTS,
        CF_JOBS,
        CF_EXECUTORS,
        CF_METADATA,
        CF_CONTINUOUS,
        CF_IVM,
        CF_HISTORY,
    ]
    .iter()
    .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
    .collect()
}

/// RocksDB-backed durable metadata store for the coordinator.
///
/// Seven column families — events, jobs, executors, metadata, continuous snapshots,
/// IVM snapshots, and job history — are created on first open.
pub struct RocksDbMetadataStore {
    db: DB,
    events: Vec<EventLogEvent>,
    jobs: Vec<JobRecord>,
    executors: Vec<ExecutorDescriptor>,
    continuous_snapshots: std::collections::HashMap<String, ContinuousSnapshot>,
    next_event_id: u64,
    history: Vec<JobHistoryRecord>,
    /// DUR-6: when true, state-advancing writes issue `WriteOptions::set_sync`
    /// so the RocksDB WAL is `fsync`'d before the write is acknowledged. Set on
    /// fail-closed durability profiles, whose contract is that an acknowledged
    /// metadata write survives host/power loss (default `put_cf` only survives a
    /// process crash, not a machine crash).
    sync_writes: bool,
}

impl RocksDbMetadataStore {
    fn store_err(msg: impl std::fmt::Display) -> SchedulerError {
        SchedulerError::Store {
            message: msg.to_string(),
        }
    }

    /// Create an ephemeral in-memory store backed by a temp directory.
    pub fn in_memory() -> SchedulerResult<Self> {
        let dir = tempfile::tempdir().map_err(Self::store_err)?;
        let path = dir.path().to_path_buf();
        Self::open_at(&path, Some(dir))
    }

    /// Open or create a durable store at `path`.
    pub fn open(path: impl AsRef<Path>) -> SchedulerResult<Self> {
        let p: &Path = path.as_ref();
        Self::open_at(p, None)
    }

    fn open_at(path: &Path, _tempdir: Option<tempfile::TempDir>) -> SchedulerResult<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = DB::open_cf_descriptors(&opts, path, all_cfs()).map_err(Self::store_err)?;

        let mut events = Vec::new();
        let mut jobs = Vec::new();
        let mut executors = Vec::new();
        let mut continuous_snapshots = std::collections::HashMap::new();
        let mut next_event_id = 0u64;

        // Load events
        {
            let cf = db
                .cf_handle(CF_EVENTS)
                .ok_or_else(|| Self::store_err("missing events CF"))?;
            let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
            for item in iter {
                let (k, v) = item.map_err(Self::store_err)?;
                let id = u64::from_le_bytes(
                    k.get(..8)
                        .ok_or_else(|| Self::store_err("corrupt event key: too short"))?
                        .try_into()
                        .map_err(|_| Self::store_err("corrupt event key"))?,
                );
                if id >= next_event_id {
                    next_event_id = id + 1;
                }
                if let Ok(pe) = serde_json::from_slice::<PersistedEvent>(&v)
                    && let Ok(evt) = EventLogEvent::try_from(pe)
                {
                    events.push(evt);
                } else {
                    tracing::warn!("failed to deserialize event record from events CF, skipping");
                }
            }
        }

        // Load jobs
        {
            let cf = db
                .cf_handle(CF_JOBS)
                .ok_or_else(|| Self::store_err("missing jobs CF"))?;
            let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
            for item in iter {
                let (_, v) = item.map_err(Self::store_err)?;
                if let Ok(pj) = serde_json::from_slice::<PersistedJobRecord>(&v)
                    && let Ok(job) = JobRecord::try_from(pj)
                {
                    jobs.push(job);
                } else {
                    tracing::warn!("failed to deserialize job record from jobs CF, skipping");
                }
            }
        }

        // Load executors
        {
            let cf = db
                .cf_handle(CF_EXECUTORS)
                .ok_or_else(|| Self::store_err("missing executors CF"))?;
            let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
            for item in iter {
                let (_, v) = item.map_err(Self::store_err)?;
                if let Ok(pe) = serde_json::from_slice::<PersistedExecutorDescriptor>(&v)
                    && let Ok(ex) = ExecutorDescriptor::try_from(pe)
                {
                    executors.push(ex);
                } else {
                    tracing::warn!(
                        "failed to deserialize executor record from executors CF, skipping"
                    );
                }
            }
        }

        // Load continuous snapshots
        {
            let cf = db
                .cf_handle(CF_CONTINUOUS)
                .ok_or_else(|| Self::store_err("missing continuous_snapshots CF"))?;
            let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
            for item in iter {
                let (k, v) = item.map_err(Self::store_err)?;
                let key = String::from_utf8_lossy(&k).into_owned();
                if let Ok(snapshot) = ContinuousSnapshot::decode(&v) {
                    continuous_snapshots.insert(key, snapshot);
                } else {
                    tracing::warn!(key = %key, "failed to decode continuous snapshot from CF, skipping");
                }
            }
        }

        // Load job history
        let mut history = Vec::new();
        {
            let cf = db
                .cf_handle(CF_HISTORY)
                .ok_or_else(|| Self::store_err("missing job_history CF"))?;
            let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
            for item in iter {
                let (_, v) = item.map_err(Self::store_err)?;
                if let Ok(rec) = serde_json::from_slice::<JobHistoryRecord>(&v) {
                    history.push(rec);
                } else {
                    tracing::warn!(
                        "failed to deserialize job history record from job_history CF, skipping"
                    );
                }
            }
            // Most-recent first: sort descending by completed_at_ms
            history.sort_by_key(|b| std::cmp::Reverse(b.completed_at_ms));
        }

        Ok(Self {
            db,
            events,
            jobs,
            executors,
            continuous_snapshots,
            next_event_id,
            history,
            sync_writes: false,
        })
    }

    /// DUR-6: enable synchronous (fsync'd) metadata writes. Set on fail-closed
    /// durability profiles so an acknowledged write survives host/power loss.
    /// The default (`false`) keeps the faster WAL-only durability that survives
    /// a process crash but not a machine crash.
    pub fn set_sync_writes(&mut self, sync: bool) {
        self.sync_writes = sync;
    }

    /// Whether synchronous metadata writes are enabled.
    pub fn sync_writes(&self) -> bool {
        self.sync_writes
    }

    /// Write options for a state-advancing put: `set_sync(true)` when running a
    /// fail-closed profile so the WAL is fsync'd before the write is acked.
    fn write_opts(&self) -> WriteOptions {
        let mut opts = WriteOptions::default();
        opts.set_sync(self.sync_writes);
        opts
    }
}

impl MetadataStore for RocksDbMetadataStore {
    fn append_event(&mut self, event: EventLogEvent) -> SchedulerResult<()> {
        let pe = PersistedEvent::from(&event);
        let bytes = serde_json::to_vec(&pe).map_err(Self::store_err)?;
        let cf = self
            .db
            .cf_handle(CF_EVENTS)
            .ok_or_else(|| Self::store_err("missing events CF"))?;
        let id = self.next_event_id;
        self.db
            .put_cf_opt(&cf, id.to_le_bytes(), bytes, &self.write_opts())
            .map_err(Self::store_err)?;
        self.next_event_id += 1;
        self.events.push(event);
        Ok(())
    }

    fn events(&self) -> &[EventLogEvent] {
        &self.events
    }

    fn save_job(&mut self, record: &JobRecord) -> SchedulerResult<()> {
        let pj = PersistedJobRecord::from(record);
        let bytes = serde_json::to_vec(&pj).map_err(Self::store_err)?;
        let cf = self
            .db
            .cf_handle(CF_JOBS)
            .ok_or_else(|| Self::store_err("missing jobs CF"))?;
        self.db
            .put_cf_opt(&cf, record.job_id().as_str(), bytes, &self.write_opts())
            .map_err(Self::store_err)?;
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

    fn save_executor(&mut self, descriptor: &ExecutorDescriptor) -> SchedulerResult<()> {
        let pe = PersistedExecutorDescriptor::from(descriptor);
        let bytes = serde_json::to_vec(&pe).map_err(Self::store_err)?;
        let cf = self
            .db
            .cf_handle(CF_EXECUTORS)
            .ok_or_else(|| Self::store_err("missing executors CF"))?;
        self.db
            .put_cf_opt(
                &cf,
                descriptor.executor_id().as_str(),
                bytes,
                &self.write_opts(),
            )
            .map_err(Self::store_err)?;
        if let Some(existing) = self
            .executors
            .iter_mut()
            .find(|e| e.executor_id() == descriptor.executor_id())
        {
            *existing = descriptor.clone();
        } else {
            self.executors.push(descriptor.clone());
        }
        Ok(())
    }

    fn executors(&self) -> Vec<ExecutorDescriptor> {
        self.executors.clone()
    }

    fn remove_executor(&mut self, executor_id: &ExecutorId) -> SchedulerResult<()> {
        let cf = self
            .db
            .cf_handle(CF_EXECUTORS)
            .ok_or_else(|| Self::store_err("missing executors CF"))?;
        self.db
            .delete_cf(&cf, executor_id.as_str())
            .map_err(Self::store_err)?;
        self.executors.retain(|e| e.executor_id() != executor_id);
        Ok(())
    }

    fn save_continuous_snapshot(
        &mut self,
        job_id: &str,
        snapshot: ContinuousSnapshot,
    ) -> SchedulerResult<()> {
        let encoded = snapshot.encode()?;
        let cf = self
            .db
            .cf_handle(CF_CONTINUOUS)
            .ok_or_else(|| Self::store_err("missing continuous_snapshots CF"))?;
        self.db
            .put_cf_opt(&cf, job_id, encoded, &self.write_opts())
            .map_err(Self::store_err)?;
        self.continuous_snapshots
            .insert(job_id.to_owned(), snapshot);
        Ok(())
    }

    fn load_continuous_snapshot(&self, job_id: &str) -> Option<ContinuousSnapshot> {
        self.continuous_snapshots.get(job_id).cloned()
    }

    fn remove_continuous_snapshot(&mut self, job_id: &str) -> SchedulerResult<()> {
        let cf = self
            .db
            .cf_handle(CF_CONTINUOUS)
            .ok_or_else(|| Self::store_err("missing continuous_snapshots CF"))?;
        self.db.delete_cf(&cf, job_id).map_err(Self::store_err)?;
        self.continuous_snapshots.remove(job_id);
        Ok(())
    }

    fn save_ivm_snapshot(&mut self, job_id: &str, snapshot: Vec<u8>) -> SchedulerResult<()> {
        let cf = self
            .db
            .cf_handle(CF_IVM)
            .ok_or_else(|| Self::store_err("missing IVM snapshots CF"))?;
        self.db
            .put_cf_opt(&cf, job_id, snapshot, &self.write_opts())
            .map_err(Self::store_err)
    }

    fn load_ivm_snapshot(&self, job_id: &str) -> Option<Vec<u8>> {
        let cf = self.db.cf_handle(CF_IVM)?;
        self.db.get_cf(&cf, job_id).ok().flatten()
    }

    fn list_ivm_snapshots(&self) -> Vec<(String, Vec<u8>)> {
        let Some(cf) = self.db.cf_handle(CF_IVM) else {
            return Vec::new();
        };
        self.db
            .iterator_cf(&cf, rocksdb::IteratorMode::Start)
            .filter_map(|item| {
                let (key, value) = item.ok()?;
                Some((String::from_utf8(key.to_vec()).ok()?, value.to_vec()))
            })
            .collect()
    }

    fn remove_ivm_snapshot(&mut self, job_id: &str) -> SchedulerResult<()> {
        let cf = self
            .db
            .cf_handle(CF_IVM)
            .ok_or_else(|| Self::store_err("missing IVM snapshots CF"))?;
        self.db
            .delete_cf_opt(&cf, job_id, &self.write_opts())
            .map_err(Self::store_err)
    }

    fn save_job_history(&mut self, record: JobHistoryRecord) -> SchedulerResult<()> {
        let bytes = serde_json::to_vec(&record).map_err(Self::store_err)?;
        let cf = self
            .db
            .cf_handle(CF_HISTORY)
            .ok_or_else(|| Self::store_err("missing job_history CF"))?;
        self.db
            .put_cf_opt(&cf, record.job_id.as_str(), bytes, &self.write_opts())
            .map_err(Self::store_err)?;
        self.history.retain(|r| r.job_id != record.job_id);
        self.history.insert(0, record);
        // Bound the archive: evict oldest records past the cap from both the
        // in-memory view and the column family so disk usage stays bounded.
        while self.history.len() > crate::store::MAX_JOB_HISTORY {
            if let Some(evicted) = self.history.pop() {
                let _ = self.db.delete_cf(&cf, evicted.job_id.as_str());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job_spec_from_logical_plan;
    use krishiv_plan::{ExecutionKind, LogicalPlan};
    use krishiv_proto::{ExecutorDescriptor, ExecutorId, JobId};

    fn job_record(id: &str) -> JobRecord {
        let job_id = JobId::try_new(id).unwrap();
        let spec =
            job_spec_from_logical_plan(job_id, &LogicalPlan::new("test", ExecutionKind::Batch))
                .unwrap();
        JobRecord::from_spec(spec, 1)
    }

    fn executor(id: &str) -> ExecutorDescriptor {
        ExecutorDescriptor::new(ExecutorId::try_new(id).unwrap(), format!("{id}.local"), 2)
            .with_task_endpoint(format!("http://{id}.local:9010"))
            .with_barrier_endpoint(format!("http://{id}.local:9011"))
    }

    #[test]
    fn rocksdb_metadata_in_memory_roundtrip() {
        let mut store = RocksDbMetadataStore::in_memory().unwrap();
        let job = job_record("job-a");
        let exec = executor("exec-a");
        let event = EventLogEvent::JobSubmitted {
            job_id: job.job_id().clone(),
        };

        store.append_event(event.clone()).unwrap();
        store.save_job(&job).unwrap();
        store.save_executor(&exec).unwrap();

        assert_eq!(store.events(), &[event]);
        assert_eq!(store.jobs(), &[job]);
        assert_eq!(store.executors(), vec![exec]);
    }

    #[test]
    fn rocksdb_metadata_reopens_jobs_events_executors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scheduler.rocksdb");
        let job = job_record("job-a");
        let exec = executor("exec-a");
        let event = EventLogEvent::JobSubmitted {
            job_id: job.job_id().clone(),
        };

        {
            let mut store = RocksDbMetadataStore::open(&path).unwrap();
            store.append_event(event.clone()).unwrap();
            store.save_job(&job).unwrap();
            store.save_executor(&exec).unwrap();
        }

        let reopened = RocksDbMetadataStore::open(&path).unwrap();
        assert_eq!(reopened.events(), &[event]);
        assert_eq!(reopened.jobs(), &[job]);
        assert_eq!(reopened.executors(), vec![exec]);
    }

    /// DUR-6: with synchronous writes enabled (fail-closed profiles), writes
    /// still succeed and are durable across a reopen. The flag is opt-in and
    /// off by default.
    #[test]
    fn rocksdb_metadata_sync_writes_persist_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scheduler.rocksdb");
        let job = job_record("job-sync");

        {
            let mut store = RocksDbMetadataStore::open(&path).unwrap();
            assert!(!store.sync_writes(), "sync writes are off by default");
            store.set_sync_writes(true);
            assert!(store.sync_writes());
            // Every state-advancing write now goes through fsync'd WriteOptions.
            store.save_job(&job).unwrap();
        }

        let reopened = RocksDbMetadataStore::open(&path).unwrap();
        assert_eq!(reopened.jobs(), &[job]);
    }

    #[test]
    fn rocksdb_metadata_remove_executor_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scheduler.rocksdb");
        let exec = executor("exec-remove");
        let exec_id = exec.executor_id().clone();

        {
            let mut store = RocksDbMetadataStore::open(&path).unwrap();
            store.save_executor(&exec).unwrap();
            store.remove_executor(&exec_id).unwrap();
        }

        let reopened = RocksDbMetadataStore::open(&path).unwrap();
        assert!(reopened.executors().is_empty());
    }

    #[test]
    fn rocksdb_continuous_snapshot_roundtrip() {
        let mut store = RocksDbMetadataStore::in_memory().unwrap();
        let snap = ContinuousSnapshot {
            snapshot_bytes: b"checkpoint".to_vec(),
            watermark_ms: 12345,
        };
        store
            .save_continuous_snapshot("job-1", snap.clone())
            .unwrap();
        assert_eq!(
            store
                .load_continuous_snapshot("job-1")
                .unwrap()
                .watermark_ms,
            12345
        );
        store.remove_continuous_snapshot("job-1").unwrap();
        assert!(store.load_continuous_snapshot("job-1").is_none());
    }
}
