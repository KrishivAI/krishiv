use crate::error::{SchedulerError, SchedulerResult};
use crate::job::JobRecord;
use crate::store::{
    EventLogEvent, MetadataStore, PersistedEvent, PersistedExecutorDescriptor, PersistedJobRecord,
};
use krishiv_proto::{ExecutorDescriptor, ExecutorId};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::path::Path;

const EVENTS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("events");
const JOBS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("jobs");
const EXECUTORS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("executors");
const METADATA_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");

#[derive(Debug)]
pub struct RedbMetadataStore {
    db: Database,
    events: Vec<EventLogEvent>,
    jobs: Vec<JobRecord>,
    executors: Vec<ExecutorDescriptor>,
}

impl RedbMetadataStore {
    pub fn in_memory() -> SchedulerResult<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| SchedulerError::Store {
                message: format!("failed to open in-memory redb: {e}"),
            })?;

        let events = Vec::new();
        let jobs = Vec::new();
        let executors = Vec::new();

        let tx = db.begin_write().map_err(|e| SchedulerError::Store {
            message: format!("failed to begin redb write tx: {e}"),
        })?;

        {
            let _ = tx.open_table(EVENTS_TABLE);
            let _ = tx.open_table(JOBS_TABLE);
            let _ = tx.open_table(EXECUTORS_TABLE);
            let _ = tx.open_table(METADATA_TABLE);
        }
        tx.commit().map_err(|e| SchedulerError::Store {
            message: format!("failed to commit redb write tx: {e}"),
        })?;

        Ok(Self {
            db,
            events,
            jobs,
            executors,
        })
    }

    pub fn open(path: impl AsRef<Path>) -> SchedulerResult<Self> {
        let db = Database::create(path.as_ref()).map_err(|e| SchedulerError::Store {
            message: format!("failed to open redb metadata store: {e}"),
        })?;

        let mut events = Vec::new();
        let mut jobs = Vec::new();
        let mut executors = Vec::new();

        let tx = db.begin_write().map_err(|e| SchedulerError::Store {
            message: format!("failed to begin redb write tx: {e}"),
        })?;

        {
            let _ = tx.open_table(EVENTS_TABLE);
            let _ = tx.open_table(JOBS_TABLE);
            let _ = tx.open_table(EXECUTORS_TABLE);
            let _ = tx.open_table(METADATA_TABLE);
        }
        tx.commit().map_err(|e| SchedulerError::Store {
            message: format!("failed to commit redb tables tx: {e}"),
        })?;

        let rx = db.begin_read().map_err(|e| SchedulerError::Store {
            message: format!("failed to begin redb read tx: {e}"),
        })?;

        if let Ok(table) = rx.open_table(EVENTS_TABLE) {
            for result in table.iter().map_err(|e| SchedulerError::Store {
                message: format!("failed to iterate redb events: {e}"),
            })? {
                if let Ok((_, value)) = result {
                    if let Ok(pe) = serde_json::from_slice::<PersistedEvent>(value.value()) {
                        if let Ok(evt) = EventLogEvent::try_from(pe) {
                            events.push(evt);
                        }
                    }
                }
            }
        }

        if let Ok(table) = rx.open_table(JOBS_TABLE) {
            for result in table.iter().map_err(|e| SchedulerError::Store {
                message: format!("failed to iterate redb jobs: {e}"),
            })? {
                if let Ok((_, value)) = result {
                    if let Ok(pj) = serde_json::from_slice::<PersistedJobRecord>(value.value()) {
                        if let Ok(job) = JobRecord::try_from(pj) {
                            jobs.push(job);
                        }
                    }
                }
            }
        }

        if let Ok(table) = rx.open_table(EXECUTORS_TABLE) {
            for result in table.iter().map_err(|e| SchedulerError::Store {
                message: format!("failed to iterate redb executors: {e}"),
            })? {
                if let Ok((_, value)) = result {
                    if let Ok(pe) =
                        serde_json::from_slice::<PersistedExecutorDescriptor>(value.value())
                    {
                        if let Ok(ex) = ExecutorDescriptor::try_from(pe) {
                            executors.push(ex);
                        }
                    }
                }
            }
        }

        Ok(Self {
            db,
            events,
            jobs,
            executors,
        })
    }
}

impl MetadataStore for RedbMetadataStore {
    fn append_event(&mut self, event: EventLogEvent) -> SchedulerResult<()> {
        let pe = PersistedEvent::from(&event);
        let bytes = serde_json::to_vec(&pe).map_err(|e| SchedulerError::Store {
            message: format!("failed to serialize event: {e}"),
        })?;

        let tx = self.db.begin_write().map_err(|e| SchedulerError::Store {
            message: format!("failed to begin redb write tx: {e}"),
        })?;
        {
            let mut table = tx
                .open_table(EVENTS_TABLE)
                .map_err(|e| SchedulerError::Store {
                    message: format!("failed to open redb events table: {e}"),
                })?;
            let id = self.events.len() as u64;
            table
                .insert(id, bytes.as_slice())
                .map_err(|e| SchedulerError::Store {
                    message: format!("failed to insert into redb events: {e}"),
                })?;
        }
        tx.commit().map_err(|e| SchedulerError::Store {
            message: format!("failed to commit redb tx: {e}"),
        })?;

        self.events.push(event);
        Ok(())
    }

    fn events(&self) -> &[EventLogEvent] {
        &self.events
    }

    fn save_job(&mut self, record: &JobRecord) -> SchedulerResult<()> {
        let pj = PersistedJobRecord::from(record);
        let bytes = serde_json::to_vec(&pj).map_err(|e| SchedulerError::Store {
            message: format!("failed to serialize job: {e}"),
        })?;

        let tx = self.db.begin_write().map_err(|e| SchedulerError::Store {
            message: format!("failed to begin redb write tx: {e}"),
        })?;
        {
            let mut table = tx
                .open_table(JOBS_TABLE)
                .map_err(|e| SchedulerError::Store {
                    message: format!("failed to open redb jobs table: {e}"),
                })?;
            table
                .insert(record.job_id().as_str(), bytes.as_slice())
                .map_err(|e| SchedulerError::Store {
                    message: format!("failed to insert into redb jobs: {e}"),
                })?;
        }
        tx.commit().map_err(|e| SchedulerError::Store {
            message: format!("failed to commit redb tx: {e}"),
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

    fn save_executor(&mut self, descriptor: &ExecutorDescriptor) -> SchedulerResult<()> {
        let pe = PersistedExecutorDescriptor::from(descriptor);
        let bytes = serde_json::to_vec(&pe).map_err(|e| SchedulerError::Store {
            message: format!("failed to serialize executor: {e}"),
        })?;

        let tx = self.db.begin_write().map_err(|e| SchedulerError::Store {
            message: format!("failed to begin redb write tx: {e}"),
        })?;
        {
            let mut table = tx
                .open_table(EXECUTORS_TABLE)
                .map_err(|e| SchedulerError::Store {
                    message: format!("failed to open redb executors table: {e}"),
                })?;
            table
                .insert(descriptor.executor_id().as_str(), bytes.as_slice())
                .map_err(|e| SchedulerError::Store {
                    message: format!("failed to insert into redb executors: {e}"),
                })?;
        }
        tx.commit().map_err(|e| SchedulerError::Store {
            message: format!("failed to commit redb tx: {e}"),
        })?;

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
        let tx = self.db.begin_write().map_err(|e| SchedulerError::Store {
            message: format!("failed to begin redb write tx: {e}"),
        })?;
        {
            let mut table = tx
                .open_table(EXECUTORS_TABLE)
                .map_err(|e| SchedulerError::Store {
                    message: format!("failed to open redb executors table: {e}"),
                })?;
            table
                .remove(executor_id.as_str())
                .map_err(|e| SchedulerError::Store {
                    message: format!("failed to remove from redb executors: {e}"),
                })?;
        }
        tx.commit().map_err(|e| SchedulerError::Store {
            message: format!("failed to commit redb tx: {e}"),
        })?;

        self.executors.retain(|e| e.executor_id() != executor_id);
        Ok(())
    }
}

#[cfg(all(feature = "redb", test))]
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
    fn redb_metadata_reopens_jobs_events_and_executors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scheduler.redb");
        let job = job_record("job-a");
        let executor = executor("exec-a");
        let event = EventLogEvent::JobSubmitted {
            job_id: job.job_id().clone(),
        };

        {
            let mut store = RedbMetadataStore::open(&path).unwrap();
            store.append_event(event.clone()).unwrap();
            store.save_job(&job).unwrap();
            store.save_executor(&executor).unwrap();
        }

        let reopened = RedbMetadataStore::open(&path).unwrap();
        assert_eq!(reopened.events(), &[event]);
        assert_eq!(reopened.jobs(), &[job]);
        assert_eq!(reopened.executors(), vec![executor]);
    }

    #[test]
    fn redb_metadata_remove_executor_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scheduler.redb");
        let executor = executor("exec-remove");
        let executor_id = executor.executor_id().clone();

        {
            let mut store = RedbMetadataStore::open(&path).unwrap();
            store.save_executor(&executor).unwrap();
            store.remove_executor(&executor_id).unwrap();
        }

        let reopened = RedbMetadataStore::open(&path).unwrap();
        assert!(reopened.executors().is_empty());
    }
}
