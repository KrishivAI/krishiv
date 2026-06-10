use crate::error::{SchedulerError, SchedulerResult};
use crate::job::JobRecord;
use crate::store::{
    ContinuousSnapshot, EventLogEvent, MetadataStore, PersistedEvent, PersistedExecutorDescriptor,
    PersistedJobRecord,
};
use krishiv_proto::{ExecutorDescriptor, ExecutorId};
use rocksdb::{ColumnFamilyDescriptor, DB, Options, WriteBatch};
use std::path::Path;

const CF_EVENTS: &str = "events";
const CF_JOBS: &str = "jobs";
const CF_EXECUTORS: &str = "executors";
const CF_METADATA: &str = "metadata";
const CF_CONTINUOUS: &str = "continuous_snapshots";

fn all_cfs() -> Vec<ColumnFamilyDescriptor> {
    [CF_EVENTS, CF_JOBS, CF_EXECUTORS, CF_METADATA, CF_CONTINUOUS]
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
        .collect()
}

/// RocksDB-backed durable metadata store for the coordinator.
///
/// Five column families — events, jobs, executors, metadata, continuous_snapshots —
/// are created on first open and must exist on subsequent opens.
pub struct RocksDbMetadataStore {
    db: DB,
    events: Vec<EventLogEvent>,
    jobs: Vec<JobRecord>,
    executors: Vec<ExecutorDescriptor>,
    continuous_snapshots: std::collections::HashMap<String, ContinuousSnapshot>,
    next_event_id: u64,
}

impl RocksDbMetadataStore {
    fn store_err(msg: impl std::fmt::Display) -> SchedulerError {
        SchedulerError::Store {
            message: msg.to_string(),
        }
    }

    /// Create an ephemeral in-memory store backed by a temp directory.
    pub fn in_memory() -> SchedulerResult<Self> {
        let dir = tempfile::tempdir().map_err(|e| Self::store_err(e))?;
        Self::open_at(dir.path(), Some(dir))
    }

    /// Open or create a durable store at `path`.
    pub fn open(path: impl AsRef<Path>) -> SchedulerResult<Self> {
        Self::open_at(path.as_ref(), None)
    }

    fn open_at(
        path: &Path,
        _tempdir: Option<tempfile::TempDir>,
    ) -> SchedulerResult<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = DB::open_cf_descriptors(&opts, path, all_cfs())
            .map_err(|e| Self::store_err(e))?;

        let mut events = Vec::new();
        let mut jobs = Vec::new();
        let mut executors = Vec::new();
        let mut continuous_snapshots = std::collections::HashMap::new();
        let mut next_event_id = 0u64;

        // Load events
        {
            let cf = db.cf_handle(CF_EVENTS).ok_or_else(|| Self::store_err("missing events CF"))?;
            let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);
            for item in iter {
                let (k, v) = item.map_err(|e| Self::store_err(e))?;
                let id = u64::from_le_bytes(
                    k[..8].try_into().map_err(|_| Self::store_err("corrupt event key"))?,
                );
                if id >= next_event_id {
                    next_event_id = id + 1;
                }
                if let Ok(pe) = serde_json::from_slice::<PersistedEvent>(&v)
                    && let Ok(evt) = EventLogEvent::try_from(pe)
                {
                    events.push(evt);
                }
            }
        }

        // Load jobs
        {
            let cf = db.cf_handle(CF_JOBS).ok_or_else(|| Self::store_err("missing jobs CF"))?;
            let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);
            for item in iter {
                let (_, v) = item.map_err(|e| Self::store_err(e))?;
                if let Ok(pj) = serde_json::from_slice::<PersistedJobRecord>(&v)
                    && let Ok(job) = JobRecord::try_from(pj)
                {
                    jobs.push(job);
                }
            }
        }

        // Load executors
        {
            let cf = db
                .cf_handle(CF_EXECUTORS)
                .ok_or_else(|| Self::store_err("missing executors CF"))?;
            let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);
            for item in iter {
                let (_, v) = item.map_err(|e| Self::store_err(e))?;
                if let Ok(pe) = serde_json::from_slice::<PersistedExecutorDescriptor>(&v)
                    && let Ok(ex) = ExecutorDescriptor::try_from(pe)
                {
                    executors.push(ex);
                }
            }
        }

        // Load continuous snapshots
        {
            let cf = db
                .cf_handle(CF_CONTINUOUS)
                .ok_or_else(|| Self::store_err("missing continuous_snapshots CF"))?;
            let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);
            for item in iter {
                let (k, v) = item.map_err(|e| Self::store_err(e))?;
                let key = String::from_utf8_lossy(&k).into_owned();
                if let Ok(snapshot) = ContinuousSnapshot::decode(&v) {
                    continuous_snapshots.insert(key, snapshot);
                }
            }
        }

        Ok(Self {
            db,
            events,
            jobs,
            executors,
            continuous_snapshots,
            next_event_id,
        })
    }
}

impl MetadataStore for RocksDbMetadataStore {
    fn append_event(&mut self, event: EventLogEvent) -> SchedulerResult<()> {
        let pe = PersistedEvent::from(&event);
        let bytes = serde_json::to_vec(&pe).map_err(|e| Self::store_err(e))?;
        let cf = self
            .db
            .cf_handle(CF_EVENTS)
            .ok_or_else(|| Self::store_err("missing events CF"))?;
        let id = self.next_event_id;
        self.db
            .put_cf(cf, id.to_le_bytes(), bytes)
            .map_err(|e| Self::store_err(e))?;
        self.next_event_id += 1;
        self.events.push(event);
        Ok(())
    }

    fn events(&self) -> &[EventLogEvent] {
        &self.events
    }

    fn save_job(&mut self, record: &JobRecord) -> SchedulerResult<()> {
        let pj = PersistedJobRecord::from(record);
        let bytes = serde_json::to_vec(&pj).map_err(|e| Self::store_err(e))?;
        let cf = self
            .db
            .cf_handle(CF_JOBS)
            .ok_or_else(|| Self::store_err("missing jobs CF"))?;
        self.db
            .put_cf(cf, record.job_id().as_str(), bytes)
            .map_err(|e| Self::store_err(e))?;
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
        let bytes = serde_json::to_vec(&pe).map_err(|e| Self::store_err(e))?;
        let cf = self
            .db
            .cf_handle(CF_EXECUTORS)
            .ok_or_else(|| Self::store_err("missing executors CF"))?;
        self.db
            .put_cf(cf, descriptor.executor_id().as_str(), bytes)
            .map_err(|e| Self::store_err(e))?;
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
            .delete_cf(cf, executor_id.as_str())
            .map_err(|e| Self::store_err(e))?;
        self.executors.retain(|e| e.executor_id() != executor_id);
        Ok(())
    }

    fn save_continuous_snapshot(
        &mut self,
        job_id: &str,
        snapshot: ContinuousSnapshot,
    ) -> SchedulerResult<()> {
        let encoded = snapshot.encode();
        let cf = self
            .db
            .cf_handle(CF_CONTINUOUS)
            .ok_or_else(|| Self::store_err("missing continuous_snapshots CF"))?;
        self.db
            .put_cf(cf, job_id, encoded)
            .map_err(|e| Self::store_err(e))?;
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
        self.db
            .delete_cf(cf, job_id)
            .map_err(|e| Self::store_err(e))?;
        self.continuous_snapshots.remove(job_id);
        Ok(())
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
            store.load_continuous_snapshot("job-1").unwrap().watermark_ms,
            12345
        );
        store.remove_continuous_snapshot("job-1").unwrap();
        assert!(store.load_continuous_snapshot("job-1").is_none());
    }
}
