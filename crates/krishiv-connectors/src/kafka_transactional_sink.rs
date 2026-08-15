// RdkafkaTransactionalSink: wraps rdkafka's transactional producer for exactly-once
// Kafka writes.  Implements TwoPhaseCommitSink where Handle = String (transaction ID).
//
// Construction: takes bootstrap_servers, topic, transactional_id.
// prepare(epoch, batch): serialize batch as Arrow IPC bytes, begin transaction if
//   not already open, send staged messages (NOT committed yet), return handle.
// commit(handle): commit_transaction on the producer.
// abort(handle): abort_transaction on the producer.

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
/// The `Handle` is a `String` formatted as `"{epoch}-{counter}"`, where the
/// counter is a process-local atomic sequence (unique within a process
/// lifetime), giving the coordinator a human-readable correlation ID for log
/// tracing.
pub struct RdkafkaTransactionalSink {
    producer: ThreadedProducer<rdkafka::producer::DefaultProducerContext>,
    topic: String,
    /// True when a Kafka transaction has been opened but not yet committed/aborted.
    transaction_open: bool,
    /// The epoch of the currently open transaction (if any).
    current_epoch: Option<u64>,
    /// The handle of the currently open transaction (if any). `commit`/`abort`
    /// verify the caller's handle against this to reject stale duplicates.
    open_handle: Option<String>,
    /// The epoch of the last *committed* transaction. Persists across
    /// commit so `prepare` can reject duplicate or stale (non-monotonic)
    /// epoch retries.
    last_finalized_epoch: Option<u64>,
    /// Transaction timeout in milliseconds. Must be ≤ broker
    /// `transaction.max.timeout.ms` (default 15 min).
    transaction_timeout_ms: u32,
}

impl RdkafkaTransactionalSink {
    /// Build an exactly-once transactional sink.
    ///
    /// Calls `init_transactions()` during construction so the producer is
    /// immediately ready to begin transactions.
    ///
    /// `transactional_id` must be stable across epochs for the same task slot
    /// (e.g. `"{job_id}/{task_slot}"`) — this ensures Kafka's zombie fencing
    /// works correctly. Per-epoch IDs would break fencing.
    ///
    /// `transaction_timeout_ms` defaults to 30 seconds. Must be ≤ the broker's
    /// `transaction.max.timeout.ms` setting.
    pub fn new(
        bootstrap_servers: impl AsRef<str>,
        topic: impl Into<String>,
        transactional_id: impl AsRef<str>,
    ) -> ConnectorResult<Self> {
        Self::with_timeout(
            bootstrap_servers,
            topic,
            transactional_id,
            Duration::from_secs(30),
        )
    }

    /// Like [`Self::new`] with an explicit transaction timeout.
    pub fn with_timeout(
        bootstrap_servers: impl AsRef<str>,
        topic: impl Into<String>,
        transactional_id: impl AsRef<str>,
        transaction_timeout: Duration,
    ) -> ConnectorResult<Self> {
        let timeout_ms: u32 =
            transaction_timeout
                .as_millis()
                .try_into()
                .map_err(|_| ConnectorError::Config {
                    message: format!(
                        "transaction timeout {transaction_timeout:?} exceeds u32::MAX ms"
                    ),
                })?;

        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", bootstrap_servers.as_ref())
            .set("transactional.id", transactional_id.as_ref())
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", timeout_ms.to_string())
            .set("transaction.timeout.ms", timeout_ms.to_string());

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
            current_epoch: None,
            open_handle: None,
            last_finalized_epoch: None,
            transaction_timeout_ms: timeout_ms,
        })
    }

    /// Validate a `prepare(epoch)` call against the sink's transaction state.
    ///
    /// Rejects a call while a transaction is still open, and rejects an epoch
    /// that is not strictly greater than the last committed epoch (a
    /// duplicate or stale retry).
    fn validate_prepare(
        transaction_open: bool,
        current_epoch: Option<u64>,
        last_finalized_epoch: Option<u64>,
        epoch: u64,
    ) -> ConnectorResult<()> {
        if transaction_open {
            return Err(ConnectorError::Protocol {
                message: format!(
                    "transaction for epoch {} is still open; commit or abort it first",
                    current_epoch.unwrap_or(0)
                ),
            });
        }
        if let Some(finalized) = last_finalized_epoch
            && epoch <= finalized
        {
            return Err(ConnectorError::Config {
                message: format!(
                    "prepare epoch {epoch} is not greater than last committed epoch {finalized}"
                ),
            });
        }
        Ok(())
    }

    /// Validate a `commit`/`abort` handle against the open transaction.
    ///
    /// Returns `Ok(false)` when no transaction is open (idempotent no-op),
    /// `Ok(true)` when the handle matches the open transaction, and an error
    /// when the handle belongs to a different (stale) transaction.
    fn validate_finalize(
        transaction_open: bool,
        open_handle: Option<&str>,
        handle: &str,
        op: &str,
    ) -> ConnectorResult<bool> {
        if !transaction_open {
            return Ok(false);
        }
        match open_handle {
            Some(open) if open == handle => Ok(true),
            _ => Err(ConnectorError::Protocol {
                message: format!(
                    "{op} handle {handle} does not match the open transaction handle {}",
                    open_handle.unwrap_or("<none>")
                ),
            }),
        }
    }

    /// Derive a stable `transactional.id` from `{job_id}/{task_slot}`.
    ///
    /// The transactional ID must be **stable across epochs** for the same task
    /// slot so that Kafka's zombie fencing correctly rejects stale producers.
    /// Per-epoch IDs (`{job_id}/{task_slot}/{epoch}`) would allow a zombie
    /// with an older epoch to commit after the current producer has progressed.
    pub fn transactional_id(job_id: &str, task_slot: &str) -> String {
        format!("krishiv-kafka-txn/{job_id}/{task_slot}")
    }
}

impl TwoPhaseCommitSink for RdkafkaTransactionalSink {
    /// The handle is `"{epoch}-{counter}"` (process-local sequence, unique
    /// within a process lifetime) for correlation in coordinator logs.
    type Handle = String;

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_unbounded()
            .with_two_phase_commit()
    }

    /// Serialize `batch` as Arrow IPC bytes and send it to Kafka inside an open
    /// transaction.  Begins a new transaction if one is not already open.
    ///
    /// # One-outstanding-handle semantics
    ///
    /// Kafka's EOS protocol allows only one open transaction per producer at a
    /// time. This sink enforces that: a second `prepare` while a transaction is
    /// still open returns `ConnectorError::TransactionBusy`. The coordinator
    /// must `commit` or `abort` the current handle before calling `prepare`
    /// again. This matches the `TwoPhaseCommitSink` contract: per-handle
    /// isolation.
    fn prepare(&mut self, epoch: u64, batch: &RecordBatch) -> ConnectorResult<Self::Handle> {
        // Validate epoch monotonicity against the last committed epoch to
        // catch duplicate or stale retries.
        Self::validate_prepare(
            self.transaction_open,
            self.current_epoch,
            self.last_finalized_epoch,
            epoch,
        )?;

        self.producer
            .begin_transaction()
            .map_err(|e| ConnectorError::Kafka {
                message: format!("rdkafka begin_transaction failed: {e}"),
                retriable: false,
            })?;
        self.transaction_open = true;
        self.current_epoch = Some(epoch);

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

        self.open_handle = Some(handle.clone());
        Ok(handle)
    }

    /// Commit the open Kafka transaction, making all staged messages visible to
    /// downstream consumers configured with `isolation.level=read_committed`.
    fn commit(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        if !Self::validate_finalize(
            self.transaction_open,
            self.open_handle.as_deref(),
            &handle,
            "commit",
        )? {
            // Already committed — idempotent.
            return Ok(());
        }
        let timeout = Duration::from_millis(self.transaction_timeout_ms as u64);
        self.producer
            .commit_transaction(timeout)
            .map_err(|e| ConnectorError::Kafka {
                message: format!("rdkafka commit_transaction failed: {e}"),
                retriable: true,
            })?;
        self.transaction_open = false;
        self.last_finalized_epoch = self.current_epoch;
        self.current_epoch = None;
        self.open_handle = None;
        Ok(())
    }

    /// Abort the open Kafka transaction, discarding all staged messages.
    fn abort(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        if !Self::validate_finalize(
            self.transaction_open,
            self.open_handle.as_deref(),
            &handle,
            "abort",
        )? {
            // Nothing staged — idempotent.
            return Ok(());
        }
        let timeout = Duration::from_millis(self.transaction_timeout_ms as u64);
        self.producer
            .abort_transaction(timeout)
            .map_err(|e| ConnectorError::Kafka {
                message: format!("rdkafka abort_transaction failed: {e}"),
                retriable: true,
            })?;
        // An aborted epoch may legitimately be retried, so it does not
        // advance `last_finalized_epoch` — only a commit does.
        self.transaction_open = false;
        self.current_epoch = None;
        self.open_handle = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::RdkafkaTransactionalSink as Sink;

    /// A prepare for an epoch at or below the last committed epoch is a
    /// duplicate/stale retry and must be rejected — even though no
    /// transaction is currently open.
    #[test]
    fn prepare_rejects_epoch_not_greater_than_last_committed() {
        // State after prepare(5) + commit: closed, last committed epoch 5.
        assert!(Sink::validate_prepare(false, None, Some(5), 4).is_err());
        assert!(Sink::validate_prepare(false, None, Some(5), 5).is_err());
        assert!(Sink::validate_prepare(false, None, Some(5), 6).is_ok());
        // First-ever prepare: nothing committed yet.
        assert!(Sink::validate_prepare(false, None, None, 1).is_ok());
    }

    /// A second prepare while a transaction is open stays rejected.
    #[test]
    fn prepare_rejects_while_transaction_open() {
        assert!(Sink::validate_prepare(true, Some(6), Some(5), 7).is_err());
    }

    /// Commit/abort of a handle that does not match the open transaction is
    /// a stale duplicate and must error rather than finalize the wrong
    /// transaction.
    #[test]
    fn finalize_rejects_mismatched_handle() {
        // prepare -> commit -> prepare(new): open handle "6-2", stale "5-1".
        let err = Sink::validate_finalize(true, Some("6-2"), "5-1", "commit")
            .expect_err("stale handle must not commit the open transaction");
        assert!(err.to_string().contains("5-1"));
        assert!(Sink::validate_finalize(true, Some("6-2"), "5-1", "abort").is_err());
    }

    /// The matching handle finalizes; with no open transaction the call is
    /// an idempotent no-op regardless of handle.
    #[test]
    fn finalize_accepts_matching_handle_and_is_idempotent_when_closed() {
        assert!(Sink::validate_finalize(true, Some("6-2"), "6-2", "commit").expect("matching"));
        assert!(
            !Sink::validate_finalize(false, None, "6-2", "commit")
                .expect("duplicate commit after close is idempotent"),
        );
    }
}
