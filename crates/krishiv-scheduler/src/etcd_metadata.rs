//! etcd-backed durable metadata store for bare-metal cluster recovery.

use etcd_client::Client;
use tokio::sync::Mutex;

use crate::store::{
    EventLogEvent, MetadataStore, decode_metadata_snapshot, encode_metadata_snapshot,
};
use crate::{JobRecord, SchedulerError, SchedulerResult};

const METADATA_SNAPSHOT_KEY: &str = "/krishiv/metadata/snapshot";

/// Durable metadata store backed by a single etcd snapshot key.
///
/// # Size limit
///
/// etcd has a default maximum value size of **1.5 MiB**.  If the encoded
/// metadata snapshot exceeds this limit the `persist()` call will fail with
/// an etcd `RequestTooLarge` error.  A `tracing::warn!` is emitted when the
/// snapshot exceeds 1 MiB so operators can detect approaching the limit
/// before writes start failing.
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
        // etcd default max value size is 1.5 MiB; warn early at 1 MiB.
        const WARN_THRESHOLD: usize = 1024 * 1024;
        if bytes.len() > WARN_THRESHOLD {
            tracing::warn!(
                size_bytes = bytes.len(),
                "etcd metadata snapshot exceeds 1 MiB; etcd default limit is 1.5 MiB"
            );
        }
        let mut client = self.client.blocking_lock().clone();
        let handle = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(client.put(METADATA_SNAPSHOT_KEY, bytes, None))
                .map_err(|e| SchedulerError::Transport {
                    message: format!("etcd metadata snapshot write failed: {e}"),
                })
        });
        krishiv_async_util::block_on(handle).map_err(|e| SchedulerError::Transport {
            message: format!("spawn_blocking join failed: {e}"),
        })??;
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
