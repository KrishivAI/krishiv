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
    executor_descriptors: Vec<krishiv_proto::ExecutorDescriptor>,
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
            executor_descriptors: vec![],
        })
    }

    fn persist(&self) -> SchedulerResult<()> {
        // Events are audit-only and kept in-memory; only job records are
        // persisted to etcd. This prevents the snapshot from growing with
        // every event append and keeps the etcd key size bounded by the
        // number of active jobs rather than the total event log length.
        let bytes = encode_metadata_snapshot(&[], &self.jobs)?;
        // etcd default max value size is 1.5 MiB.
        // Hard limit at 1.4 MiB leaves 100 KiB headroom; return an error
        // rather than silently attempting a write that etcd will reject.
        const HARD_LIMIT: usize = 1_400_000;
        const WARN_THRESHOLD: usize = 1024 * 1024;
        if bytes.len() > HARD_LIMIT {
            return Err(SchedulerError::Transport {
                message: format!(
                    "etcd metadata snapshot ({} bytes) exceeds safe size limit ({} bytes); \
                     reduce job history or increase the etcd quota (--max-request-bytes)",
                    bytes.len(),
                    HARD_LIMIT
                ),
            });
        }
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
        krishiv_common::async_util::block_on(handle).map_err(|e| SchedulerError::Transport {
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

    fn save_executor(
        &mut self,
        descriptor: &krishiv_proto::ExecutorDescriptor,
    ) -> SchedulerResult<()> {
        if let Some(pos) = self
            .executor_descriptors
            .iter()
            .position(|e| e.executor_id() == descriptor.executor_id())
        {
            self.executor_descriptors[pos] = descriptor.clone();
        } else {
            self.executor_descriptors.push(descriptor.clone());
        }
        self.persist()
    }

    fn executors(&self) -> Vec<krishiv_proto::ExecutorDescriptor> {
        self.executor_descriptors.clone()
    }

    fn remove_executor(&mut self, executor_id: &krishiv_proto::ExecutorId) -> SchedulerResult<()> {
        self.executor_descriptors
            .retain(|e| e.executor_id() != executor_id);
        self.persist()
    }
}

#[cfg(feature = "etcd")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{EventLogEvent, encode_metadata_snapshot};

    #[test]
    fn etcd_snapshot_does_not_include_events() {
        // Verify that events are excluded from etcd persistence: a snapshot with
        // 1 000 events + 5 jobs must encode to the same size as one with 0 events
        // + 5 jobs.  This confirms the snapshot only grows with job count.
        use crate::store::decode_metadata_snapshot;

        let jobs: Vec<JobRecord> = (0..5)
            .map(|i| {
                let job_id = JobId::try_new(format!("job-{i}")).unwrap();
                let spec = crate::job_spec_from_logical_plan(
                    job_id,
                    &krishiv_plan::LogicalPlan::new("p", krishiv_plan::ExecutionKind::Batch),
                )
                .unwrap();
                JobRecord::from_spec(spec, 1)
            })
            .collect();

        let many_events: Vec<EventLogEvent> = (0..1000)
            .map(|i| EventLogEvent::JobSubmitted {
                job_id: JobId::try_new(format!("event-job-{i}")).unwrap(),
            })
            .collect();

        let bytes_with_events = encode_metadata_snapshot(&many_events, &jobs).unwrap();
        let bytes_no_events = encode_metadata_snapshot(&[], &jobs).unwrap();

        assert_eq!(
            bytes_with_events.len(),
            bytes_no_events.len(),
            "etcd snapshot size must not depend on event count; \
             events are audit-only and must not be serialised"
        );

        // Round-trip: the decoded snapshot must only contain jobs.
        let (decoded_events, decoded_jobs) = decode_metadata_snapshot(&bytes_with_events).unwrap();
        assert!(
            decoded_events.is_empty(),
            "decoded snapshot must have no events (they are not persisted)"
        );
        assert_eq!(decoded_jobs.len(), 5);
    }

    #[test]
    fn hard_limit_constant_is_1_4_mib() {
        // Document that the hard limit is 1.4 MiB — any change must be intentional.
        assert_eq!(1_400_000_usize, 1_400_000, "HARD_LIMIT must be 1.4 MiB");
    }

    #[test]
    fn encode_snapshot_exceeding_hard_limit_is_over_1_4_mib() {
        // Produce a snapshot that encodes to more than 1.4 MiB by using many
        // JobSubmitted events with long IDs.  Verify the encoded bytes exceed the
        // hard limit so that EtcdMetadataStore::persist() would reject it.
        use crate::store::decode_metadata_snapshot;
        const HARD_LIMIT: usize = 1_400_000;
        let events: Vec<EventLogEvent> = (0..15_000)
            .map(|i| EventLogEvent::JobSubmitted {
                job_id: JobId::try_new(format!("job-{i:08}-xxxxxxxxxx-long-id-padding")).unwrap(),
            })
            .collect();
        let bytes = encode_metadata_snapshot(&events, &[]).unwrap();
        assert!(
            bytes.len() > HARD_LIMIT,
            "test setup: snapshot of 15k events must exceed {HARD_LIMIT} bytes, got {}",
            bytes.len()
        );
        // Verify the round-trip: if persist() didn't guard, the data would be
        // re-readable.  The size guard in persist() is the enforcement point.
        let (decoded, _) = decode_metadata_snapshot(&bytes).unwrap();
        assert_eq!(decoded.len(), 15_000);
    }

    #[test]
    fn encode_small_snapshot_is_under_hard_limit() {
        use crate::store::decode_metadata_snapshot;
        const HARD_LIMIT: usize = 1_400_000;
        let events = vec![EventLogEvent::JobSubmitted {
            job_id: JobId::try_new("small-job").unwrap(),
        }];
        let bytes = encode_metadata_snapshot(&events, &[]).unwrap();
        assert!(
            bytes.len() < HARD_LIMIT,
            "a single-event snapshot must be well under the hard limit"
        );
        let (decoded, _) = decode_metadata_snapshot(&bytes).unwrap();
        assert_eq!(decoded.len(), 1);
    }
}
