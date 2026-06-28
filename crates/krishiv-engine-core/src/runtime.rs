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
use krishiv_proto::JobId;

use crate::changelog::ChangelogBatch;
use crate::error::EngineResult;
use crate::job::{CompiledJob, SinkSpec, SourceSpec};

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
    /// Write one changelog batch.
    async fn write(&mut self, changes: ChangelogBatch) -> EngineResult<()>;

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
/// current executor path lacks. `sink_transactions` and unaligned in-flight
/// buffers are added in Phase 5 when the unified streaming checkpoint protocol
/// lands.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CheckpointPayload {
    /// The epoch this payload belongs to.
    pub epoch: u64,
    /// Serialized operator state (from [`KeyedState::snapshot`]).
    pub operator_state: Vec<u8>,
    /// Source name → encoded source offset at this epoch.
    pub source_offsets: Vec<(String, Vec<u8>)>,
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
/// Phase 2 fills in the data-movement methods; the trait exists now so the seam
/// is in place.
pub trait ShuffleService: Send + Sync {
    /// Number of partitions this shuffle fans out to.
    fn partitions(&self) -> usize;
}

/// Executes a job's query off-engine — the placement seam for batch execution.
///
/// This is what lets the batch engine run unchanged across placements. When a
/// runtime carries no executor (`EngineRuntime::query_executor == None`), the
/// engine uses its built-in in-process path (drain sources, run DataFusion
/// locally). When a placement injects one, the engine hands the whole job to it
/// instead: a single-node or distributed executor registers the job's sources
/// with a coordinator and runs the query on the cluster, returning the result
/// batches. The engine code does not change — only the injected service does.
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    /// Run `job`'s query and return its result batches. The executor owns how
    /// the job's sources are reached (e.g. by path registration with a remote
    /// coordinator), so it takes the whole [`CompiledJob`].
    async fn execute_batch(&self, job: &CompiledJob) -> EngineResult<Vec<RecordBatch>>;
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
