// RdkafkaTransactionalSink: wraps rdkafka's transactional producer for exactly-once
// Kafka writes.  Implements TwoPhaseCommitSink where Handle = String (transaction ID).
//
// Construction: takes bootstrap_servers, topic, transactional_id.
// prepare(epoch, batch): serialize batch as Arrow IPC bytes, begin transaction if
//   not already open, send staged messages (NOT committed yet), return handle.
// commit(handle): commit_transaction on the producer.
// abort(handle): abort_transaction on the producer.

#![cfg(feature = "kafka")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use rdkafka::ClientConfig;
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};

use crate::{ConnectorCapabilities, ConnectorError, ConnectorResult, TwoPhaseCommitSink};

static HANDLE_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Timeout for Kafka transaction operations (init, begin, commit, abort).
const TRANSACTION_TIMEOUT: Duration = Duration::from_secs(10);

/// An rdkafka-backed exactly-once Kafka sink.
///
/// Uses Kafka transactions (EOS) to implement [`TwoPhaseCommitSink`].
/// `prepare` stages messages under an open transaction; `commit` finalises it;
/// `abort` rolls it back.
///
/// # Configuration
///
/// The producer is configured with:
/// - `transactional.id` = `transactional_id` (unique per task slot)
/// - `enable.idempotence` = `true`
///
/// # Handle
///
/// The `Handle` is a `String` formatted as `"{epoch}-{uuid}"` to give the
/// coordinator a human-readable correlation ID for log tracing.
pub struct RdkafkaTransactionalSink {
    producer: ThreadedProducer<rdkafka::producer::DefaultProducerContext>,
    topic: String,
    /// True when a Kafka transaction has been opened but not yet committed/aborted.
    transaction_open: bool,
}

impl RdkafkaTransactionalSink {
    /// Build an exactly-once transactional sink.
    ///
    /// Calls `init_transactions()` during construction so the producer is
    /// immediately ready to begin transactions.
    pub fn new(
        bootstrap_servers: impl AsRef<str>,
        topic: impl Into<String>,
        transactional_id: impl AsRef<str>,
    ) -> ConnectorResult<Self> {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", bootstrap_servers.as_ref())
            .set("transactional.id", transactional_id.as_ref())
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "30000");

        let producer: ThreadedProducer<rdkafka::producer::DefaultProducerContext> =
            cfg.create().map_err(|e| ConnectorError::Kafka {
                message: format!("rdkafka transactional producer creation failed: {e}"),
                retriable: false,
            })?;

        producer
            .init_transactions(TRANSACTION_TIMEOUT)
            .map_err(|e| ConnectorError::Kafka {
                message: format!("rdkafka init_transactions failed: {e}"),
                retriable: false,
            })?;

        Ok(Self {
            producer,
            topic: topic.into(),
            transaction_open: false,
        })
    }
}

impl TwoPhaseCommitSink for RdkafkaTransactionalSink {
    /// The handle is `"{epoch}-{uuid}"` for correlation in coordinator logs.
    type Handle = String;

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_unbounded()
            .with_two_phase_commit()
    }

    /// Serialize `batch` as Arrow IPC bytes and send it to Kafka inside an open
    /// transaction.  Begins a new transaction if one is not already open.
    fn prepare(&mut self, epoch: u64, batch: &RecordBatch) -> ConnectorResult<Self::Handle> {
        if !self.transaction_open {
            self.producer
                .begin_transaction()
                .map_err(|e| ConnectorError::Kafka {
                    message: format!("rdkafka begin_transaction failed: {e}"),
                    retriable: false,
                })?;
            self.transaction_open = true;
        }

        // Serialize the batch as Arrow IPC stream bytes.
        let mut ipc_buf: Vec<u8> = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut ipc_buf, batch.schema().as_ref()).map_err(|e| {
                    ConnectorError::Schema {
                        message: format!("Arrow IPC writer creation failed: {e}"),
                    }
                })?;
            writer.write(batch).map_err(|e| ConnectorError::Schema {
                message: format!("Arrow IPC write failed: {e}"),
            })?;
            writer.finish().map_err(|e| ConnectorError::Schema {
                message: format!("Arrow IPC finish failed: {e}"),
            })?;
        }

        let handle = format!("{epoch}-{}", HANDLE_COUNTER.fetch_add(1, Ordering::Relaxed));
        let record: BaseRecord<'_, str, [u8]> = BaseRecord::to(&self.topic)
            .key(handle.as_str())
            .payload(&ipc_buf);

        self.producer
            .send(record)
            .map_err(|(e, _)| ConnectorError::Kafka {
                message: format!("rdkafka transactional send failed: {e}"),
                retriable: true,
            })?;

        Ok(handle)
    }

    /// Commit the open Kafka transaction, making all staged messages visible to
    /// downstream consumers configured with `isolation.level=read_committed`.
    fn commit(&mut self, _handle: Self::Handle) -> ConnectorResult<()> {
        if !self.transaction_open {
            // Already committed — idempotent.
            return Ok(());
        }
        self.producer
            .commit_transaction(TRANSACTION_TIMEOUT)
            .map_err(|e| ConnectorError::Kafka {
                message: format!("rdkafka commit_transaction failed: {e}"),
                retriable: true,
            })?;
        self.transaction_open = false;
        Ok(())
    }

    /// Abort the open Kafka transaction, discarding all staged messages.
    fn abort(&mut self, _handle: Self::Handle) -> ConnectorResult<()> {
        if !self.transaction_open {
            // Nothing staged — idempotent.
            return Ok(());
        }
        self.producer
            .abort_transaction(TRANSACTION_TIMEOUT)
            .map_err(|e| ConnectorError::Kafka {
                message: format!("rdkafka abort_transaction failed: {e}"),
                retriable: true,
            })?;
        self.transaction_open = false;
        Ok(())
    }
}
