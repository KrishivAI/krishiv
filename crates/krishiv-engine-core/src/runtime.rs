//! Placement-provided runtime services.
//!
//! [`EngineRuntime`] is the seam that makes one engine run unchanged across
//! embedded, single-node, and distributed placements: the engine codes only
//! against these traits, and each placement injects concrete implementations.
//! Moving a job from in-process to a cluster swaps the implementations, not the
//! engine code.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use krishiv_proto::JobId;

use crate::changelog::ChangelogBatch;
use crate::error::EngineResult;
use crate::job::{CompiledJob, SinkSpec, SourceSpec};

/// A wakeup primitive the streaming engine uses to shorten the idle-poll floor.
///
/// The default idle poll was `tokio::time::sleep(5ms)` (see
/// `krishiv_api::engines::STREAMING_IDLE_TICK_MS`). That made a source that
/// emits one record per second see a p99 latency of 5ms. Sources that
/// implement [`SourceReader::data_notify`] can call `notify_one()` after each
/// successful `next()`, dropping the floor to the microsecond range while
/// keeping a small sleep as a safety net for spurious wakeups.
pub type DataNotify = Arc<tokio::sync::Notify>;

/// Where a job's data-plane work runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    /// In the caller's process (tests, examples, embedded API).
    Embedded,
    /// One host — local daemon or in-process cluster.
    SingleNode,
    /// Remote coordinator/executor cluster.
    Distributed,
}

/// Opens readers for a job's sources.
#[async_trait]
pub trait SourceProvider: Send + Sync {
    /// Open a reader for `spec`.
    async fn open(&self, spec: &SourceSpec) -> EngineResult<Box<dyn SourceReader>>;
}

/// Reads input batches for one source.
#[async_trait]
pub trait SourceReader: Send {
    /// The next batch; `None` at end-of-input for bounded sources or when an
    /// unbounded source is momentarily idle.
    async fn next(&mut self) -> EngineResult<Option<RecordBatch>>;

    /// The next change set as a [`ChangelogBatch`] — the change-data-capture
    /// view of the source, carrying per-row insert/delete semantics.
    ///
    /// The default treats every row of [`next`](Self::next) as an insertion, so
    /// append-only sources need no extra work. CDC connectors override this to
    /// surface deletes and updates, which the incremental engine applies as
    /// retractions.
    async fn next_changelog(&mut self) -> EngineResult<Option<ChangelogBatch>> {
        Ok(self.next().await?.map(ChangelogBatch::inserts))
    }

    /// Encoded checkpoint offset, or `None` if the source cannot checkpoint.
    fn checkpoint_offset(&self) -> Option<Vec<u8>> {
        None
    }

    /// Restore the source to a previously encoded offset.
    fn restore_offset(&mut self, _encoded: &[u8]) -> EngineResult<()> {
        Ok(())
    }

    /// Return a [`DataNotify`] the streaming engine can `await` instead of
    /// sleeping on an idle poll tick. The default returns `None`, in which
    /// case the engine falls back to a time-based sleep (5 ms) — the
    /// historical behavior. Implementations that drive a real underlying
    /// transport (Kafka, Kinesis, Pulsar, CDC consumers) return `Some(notify)`
    /// and call `notify_one()` after each successful poll, dropping the
    /// detection latency from milliseconds to microseconds.
    fn data_notify(&self) -> Option<DataNotify> {
        None
    }

    /// In-flight batches that the source has already produced but that the
    /// engine has not yet emitted to downstream operators.
    ///
    /// Persisted alongside operator state in a checkpoint so a restart
    /// restores both the operator window and the records that were "in
    /// flight" at the barrier boundary — the property a distributed
    /// **unaligned** checkpoint needs for exactly-once replay (see
    /// [`crate::runtime::CheckpointPayload::in_flight`]).
    ///
    /// The default returns `None` (no in-flight records to persist); sources
    /// that have already drained their internal prefetch buffer into the
    /// engine's `next()` queue return `None` too — only sources that hold
    /// back records in their own buffer (a Kafka consumer with prefetched
    /// but unconsumed records, an `mpsc::Sender` with queued batches)
    /// return `Some(_)`. Returning `Some` with the engine's own queue would
    /// double-persist the same records.
    ///
    /// The returned `Vec<Vec<u8>>` is opaque to the engine — opaque bytes the
    /// source encodes and decodes however it wants — but **the same source
    /// must produce the same bytes across save/restore**. Implementations
    /// that cannot meet that contract should return `None` (effectively
    /// at-least-once for in-flight records; the operator state and source
    /// offset are still exactly-once).
    fn snapshot_in_flight(&self) -> Option<Vec<Vec<u8>>> {
        None
    }

    /// Restore the in-flight buffer from bytes previously returned by
    /// [`snapshot_in_flight`](Self::snapshot_in_flight). Default is a no-op
    /// matching `snapshot_in_flight = None`; a source that returns
    /// `Some(_)` from `snapshot_in_flight` must override this and re-enqueue
    /// the records **before** [`next`](Self::next) is called again.
    ///
    /// Failure to re-enqueue leaves the engine at the restored operator
    /// state with no in-flight records — i.e. the checkpoint is treated as
    /// having no in-flight buffer. The fix is the same as for any other
    /// source error: log + propagate.
    fn restore_in_flight(&mut self, _bytes: &[Vec<u8>]) -> EngineResult<()> {
        Ok(())
    }

    /// Preferred upper bound on the row count of a single batch.
    ///
    /// Sources whose transport hands them a contiguous byte range (Kafka
    /// `max.poll.records`, an in-memory `Vec<RecordBatch>` from a
    /// `ContinuousTableInput`) override this to surface the natural unit.
    /// The default is `None` (the engine uses its own default of 65 536
    /// rows). Returning `Some(n)` lets the source's natural unit propagate
    /// all the way to the window operator, which is the
    /// highest-throughput, lowest-allocation path.
    ///
    /// Note: this is a **hint**, not a strict cap — sources whose next poll
    /// returns more than `n` rows (Kafka rebalance, a queued burst) should
    /// still emit the larger batch; the engine just uses `n` for its
    /// backpressure decision when present.
    fn preferred_batch_size(&self) -> Option<usize> {
        None
    }
}

/// Opens writers for a job's sinks.
#[async_trait]
pub trait SinkProvider: Send + Sync {
    /// Open a writer for `spec`.
    async fn open(&self, spec: &SinkSpec) -> EngineResult<Box<dyn SinkWriter>>;
}

/// Writes engine output. Batch writes insert-only batches; incremental and
/// streaming write full changelogs.
#[async_trait]
pub trait SinkWriter: Send {
    /// Write one changelog batch. The historical API takes `ChangelogBatch`
    /// by value, which forces the streaming engine to clone the batch per
    /// sink when fanning an output to multiple sinks. New code should prefer
    /// [`write_arc`](Self::write_arc) when the engine has an `Arc<ChangelogBatch>`
    /// ready — that's the zero-allocation fan-out path.
    async fn write(&mut self, changes: ChangelogBatch) -> EngineResult<()>;

    /// Fast fan-out path: the streaming engine builds one `Arc<ChangelogBatch>`
    /// per output and `Arc::clone`s it once per sink. The default delegates
    /// to `write((*batch).clone())` so existing sinks work unchanged; new
    /// sinks that store the batch in an internal buffer (like
    /// [`InMemorySinkWriter`](crate::mem::InMemorySinkWriter)) should override
    /// this and just push the `Arc` — no `RecordBatch::clone` is ever paid.
    async fn write_arc(&mut self, batch: Arc<ChangelogBatch>) -> EngineResult<()> {
        match Arc::try_unwrap(batch) {
            Ok(owned) => self.write(owned).await,
            Err(arc) => self.write((*arc).clone()).await,
        }
    }

    /// Flush any buffered output.
    async fn flush(&mut self) -> EngineResult<()> {
        Ok(())
    }
}

/// Opens keyed-state backends for stateful engines.
pub trait StateBackendFactory: Send + Sync {
    /// Open (or create) the keyed state store for `namespace`.
    fn open_keyed(&self, namespace: &str) -> EngineResult<Box<dyn KeyedState>>;
}

/// A keyed state store scoped to one operator/namespace.
pub trait KeyedState: Send {
    /// Read the value for `key`.
    fn get(&self, key: &[u8]) -> EngineResult<Option<Vec<u8>>>;
    /// Write `value` for `key`.
    fn put(&mut self, key: &[u8], value: &[u8]) -> EngineResult<()>;
    /// Remove `key`.
    fn delete(&mut self, key: &[u8]) -> EngineResult<()>;
    /// Serialize the full store for a checkpoint.
    fn snapshot(&self) -> EngineResult<Vec<u8>>;
    /// Replace the store's contents from a snapshot produced by [`snapshot`](Self::snapshot).
    fn restore(&mut self, bytes: &[u8]) -> EngineResult<()>;
}

/// What one operator/task persists at one checkpoint epoch.
///
/// Source offsets travel **with** operator state so a restore rewinds sources
/// to exactly the position the snapshotted state reflects — the consistency the
/// current executor path lacks. `sink_transactions` is added when the unified
/// streaming checkpoint protocol lands.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CheckpointPayload {
    /// The epoch this payload belongs to.
    pub epoch: u64,
    /// Serialized operator state (from [`KeyedState::snapshot`]).
    pub operator_state: Vec<u8>,
    /// Source name → encoded source offset at this epoch.
    pub source_offsets: Vec<(String, Vec<u8>)>,
    /// **Unaligned-checkpoint** in-flight buffers: for each not-yet-barriered
    /// input channel `(channel_index, arrow_ipc_bytes)`, the records that were
    /// in flight when the barrier overtook them (the dataflow
    /// `AlignmentMode::Unaligned` path, once wired through the operator
    /// runtime). Replayed on recovery so an unaligned snapshot is exactly-once
    /// without an alignment stall. Empty for aligned checkpoints.
    /// `#[serde(default)]` keeps checkpoints written before this field readable.
    #[serde(default)]
    pub in_flight: Vec<(u32, Vec<u8>)>,
    /// **Per-source in-flight records** (B-5 fix). Carries the source's
    /// `snapshot_in_flight` opaque bytes — the records the source had
    /// internally prefetched but had not yet emitted to the engine. Restored
    /// on the source's `restore_in_flight` so a restart replays the same
    /// records (exactly-once, in the strict sense) rather than dropping them
    /// (at-least-once). Empty for sources that opt out of in-flight
    /// persistence.
    /// `#[serde(default)]` keeps checkpoints written before this field
    /// readable; older restores behave as if the source had no in-flight
    /// records to re-enqueue.
    #[serde(default)]
    pub source_in_flight: Vec<(String, Vec<Vec<u8>>)>,
}

/// Coordinates durable checkpoints. Embedded persists locally; distributed
/// routes through the scheduler's checkpoint coordinator.
#[async_trait]
pub trait CheckpointService: Send + Sync {
    /// Persist `payload` for `job` as the latest committed checkpoint.
    async fn persist(&self, job: &JobId, payload: &CheckpointPayload) -> EngineResult<()>;
    /// Load the latest committed checkpoint for `job`, if any.
    async fn restore_latest(&self, job: &JobId) -> EngineResult<Option<CheckpointPayload>>;
}

/// Repartitions data across tasks. `None` in embedded placement (one task).
///
/// [`partition_by_key`](Self::partition_by_key) is the data-movement primitive:
/// it splits a batch into per-partition batches by a value-based hash of the key
/// columns. The in-memory implementation ([`InMemoryShuffle`](crate::mem::InMemoryShuffle))
/// keeps the partitions in process; a distributed placement implements the same
/// contract over the network (e.g. Flight), so a stateful operator's repartition
/// step is identical regardless of where the downstream task runs.
pub trait ShuffleService: Send + Sync {
    /// Number of partitions this shuffle fans out to.
    fn partitions(&self) -> usize;

    /// Hash-partition `batch` by the values in `key_indices` into
    /// [`partitions`](Self::partitions) buckets, returning one [`RecordBatch`]
    /// per partition in partition order (a bucket with no rows is an empty
    /// batch with the same schema).
    ///
    /// Partitioning is **deterministic and value-based**: equal key values
    /// always map to the same partition, across processes — so a given logical
    /// key is routed to the same downstream task no matter which upstream task
    /// produced the row. This is the property keyed state and windowed
    /// aggregation rely on after a repartition.
    fn partition_by_key(
        &self,
        batch: &RecordBatch,
        key_indices: &[usize],
    ) -> EngineResult<Vec<RecordBatch>>;

    /// Serialize one partition to bytes for transport to a downstream task.
    ///
    /// The default uses the **Arrow IPC stream format** ([`encode_batch_ipc`]):
    /// a columnar, length-delimited layout that ships the batch's buffers as-is,
    /// with no row-by-row re-encode and no schema repetition per row. A
    /// distributed placement sends these bytes over its transport (e.g. Flight)
    /// and the receiver reconstructs the batch with [`decode_partition`](Self::decode_partition)
    /// — so the on-wire representation is identical to the in-memory one. In-process
    /// shuffles never call this (they pass `RecordBatch` by `Arc`).
    fn encode_partition(&self, batch: &RecordBatch) -> EngineResult<Vec<u8>> {
        encode_batch_ipc(batch)
    }

    /// Reconstruct a partition produced by [`encode_partition`](Self::encode_partition).
    fn decode_partition(&self, bytes: &[u8]) -> EngineResult<RecordBatch> {
        decode_batch_ipc(bytes)
    }
}

/// Encode a `RecordBatch` to Arrow IPC **stream** bytes — the columnar wire
/// format used to move a shuffle partition across the network without a
/// row-by-row re-encode. Round-trips with [`decode_batch_ipc`].
pub fn encode_batch_ipc(batch: &RecordBatch) -> EngineResult<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| crate::error::EngineError::Runtime(format!("ipc writer init: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| crate::error::EngineError::Runtime(format!("ipc write: {e}")))?;
        writer
            .finish()
            .map_err(|e| crate::error::EngineError::Runtime(format!("ipc finish: {e}")))?;
    }
    Ok(buf)
}

/// Decode Arrow IPC stream bytes produced by [`encode_batch_ipc`] back into a
/// single `RecordBatch`. Errors if the stream carries no batch or more than one.
pub fn decode_batch_ipc(bytes: &[u8]) -> EngineResult<RecordBatch> {
    let mut reader =
        arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| crate::error::EngineError::Runtime(format!("ipc reader init: {e}")))?;
    let batch = match reader.next() {
        Some(Ok(b)) => b,
        Some(Err(e)) => return Err(crate::error::EngineError::Runtime(format!("ipc read: {e}"))),
        None => {
            return Err(crate::error::EngineError::Runtime(
                "ipc stream contained no batch".into(),
            ));
        }
    };
    if reader.next().is_some() {
        return Err(crate::error::EngineError::Runtime(
            "ipc stream contained more than one batch; decode_batch_ipc expects exactly one".into(),
        ));
    }
    Ok(batch)
}

/// A stream of [`RecordBatch`]es from a [`QueryExecutor`].
pub type BatchOutputStream = BoxStream<'static, EngineResult<RecordBatch>>;

/// Executes a job's query off-engine — the placement seam for batch execution.
///
/// This is what lets the batch engine run unchanged across placements. When a
/// runtime carries no executor (`EngineRuntime::query_executor == None`), the
/// engine uses its built-in in-process path (drain sources, run DataFusion
/// locally). When a placement injects one, the engine hands the whole job to it
/// instead: a single-node or distributed executor registers the job's sources
/// with a coordinator and runs the query on the cluster, returning a stream of
/// result batches. The engine writes each batch to sinks as it arrives, so the
/// full result set is never buffered in the client process.
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    /// Plan the job and return a stream of result batches. The executor owns
    /// how the job's sources are reached (e.g. path registration with a remote
    /// coordinator), so it takes the whole [`CompiledJob`].
    async fn execute_batch(&self, job: &CompiledJob) -> EngineResult<BatchOutputStream>;
}

/// Time source — wall clock in production, controllable in tests.
pub trait Clock: Send + Sync {
    /// Current time in epoch milliseconds.
    fn now_ms(&self) -> i64;
}

/// Wall-clock implementation of [`Clock`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        krishiv_common::async_util::unix_now_ms()
    }
}

/// Placement-provided services handed to a [`ComputeEngine`](crate::ComputeEngine)
/// at run time.
///
/// The engine never knows whether it is embedded or distributed — it sees only
/// these trait objects.
#[derive(Clone)]
pub struct EngineRuntime {
    /// Where the data plane runs.
    pub placement: Placement,
    /// Source readers.
    pub sources: Arc<dyn SourceProvider>,
    /// Sink writers.
    pub sinks: Arc<dyn SinkProvider>,
    /// Keyed-state backends.
    pub state: Arc<dyn StateBackendFactory>,
    /// Checkpoint coordination.
    pub checkpoint: Arc<dyn CheckpointService>,
    /// Cross-task shuffle, absent in embedded placement.
    pub shuffle: Option<Arc<dyn ShuffleService>>,
    /// Off-engine query executor. `None` ⇒ the engine runs the query in-process;
    /// `Some` ⇒ a placement-provided (single-node / distributed) executor runs it.
    pub query_executor: Option<Arc<dyn QueryExecutor>>,
    /// Base directory for durable, file-backed operator state (per-job
    /// subdirectories). `None` ⇒ ephemeral in-memory operator state (embedded);
    /// `Some` ⇒ single-node / distributed durable state that survives a restart.
    /// The streaming engine threads this into its window operator's state backend.
    pub state_dir: Option<std::path::PathBuf>,
    /// Time source.
    pub clock: Arc<dyn Clock>,
}

impl EngineRuntime {
    /// Whether this runtime runs the data plane on a remote cluster.
    pub fn is_distributed(&self) -> bool {
        matches!(self.placement, Placement::Distributed)
    }
}

#[cfg(test)]
mod ipc_codec_tests {
    use super::{RecordBatch, decode_batch_ipc, encode_batch_ipc};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn ipc_roundtrip_preserves_schema_and_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let bytes = encode_batch_ipc(&batch).expect("encode");
        let decoded = decode_batch_ipc(&bytes).expect("decode");
        assert_eq!(decoded.schema(), schema);
        assert_eq!(decoded.num_rows(), 3);
        let v = decoded
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(v.values(), &[10, 20, 30]);
    }

    #[test]
    fn ipc_roundtrip_handles_empty_partition() {
        // An empty partition still carries its schema across the wire.
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let empty = RecordBatch::new_empty(schema.clone());
        let bytes = encode_batch_ipc(&empty).expect("encode empty");
        let decoded = decode_batch_ipc(&bytes).expect("decode empty");
        assert_eq!(decoded.schema(), schema);
        assert_eq!(decoded.num_rows(), 0);
    }

    #[test]
    fn decode_rejects_empty_bytes() {
        assert!(decode_batch_ipc(&[]).is_err());
    }
}
