//! etcd-backed durable metadata store for bare-metal cluster recovery.

use std::sync::Mutex;

use etcd_client::Client;

use crate::store::{
    EventLogEvent, MetadataStore, decode_metadata_snapshot, encode_metadata_snapshot,
};
use crate::{JobRecord, SchedulerError, SchedulerResult};

const METADATA_SNAPSHOT_KEY: &str = "/krishiv/metadata/snapshot";

/// Durable metadata store backed by a single etcd snapshot key.
pub struct EtcdMetadataStore {
    client: Mutex<Client>,
    events: Vec<EventLogEvent>,
    jobs: Vec<JobRecord>,
}

impl EtcdMetadataStore {
    /// Connect to etcd and load the metadata snapshot if present.
    pub async fn connect(endpoints: Vec<String>) -> SchedulerResult<Self> {
        let mut client =
            Client::connect(endpoints, None)
                .await
                .map_err(|e| SchedulerError::Transport {
                    message: format!("etcd metadata connect failed: {e}"),
                })?;
        let (events, jobs) = match client.get(METADATA_SNAPSHOT_KEY, None).await {
            Ok(resp) => {
                let value = resp.kvs().first().map(|kv| kv.value());
                match value {
                    Some(bytes) if !bytes.is_empty() => decode_metadata_snapshot(bytes)?,
                    _ => (Vec::new(), Vec::new()),
                }
            }
            Err(e) => {
                return Err(SchedulerError::Transport {
                    message: format!("etcd metadata snapshot read failed: {e}"),
                });
            }
        };
        Ok(Self {
            client: Mutex::new(client),
            events,
            jobs,
        })
    }

    fn persist(&self) -> SchedulerResult<()> {
        let bytes = encode_metadata_snapshot(&self.events, &self.jobs)?;
        let mut client = self.client.lock().unwrap_or_else(|p| p.into_inner());
        futures::executor::block_on(client.put(METADATA_SNAPSHOT_KEY, bytes, None)).map_err(
            |e| SchedulerError::Transport {
                message: format!("etcd metadata snapshot write failed: {e}"),
            },
        )?;
        Ok(())
    }
}

impl MetadataStore for EtcdMetadataStore {
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
