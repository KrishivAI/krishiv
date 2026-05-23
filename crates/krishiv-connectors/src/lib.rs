#![forbid(unsafe_code)]

//! Connector contracts for Krishiv.
//!
//! This crate defines the `Source` and `Sink` traits, connector capability
//! flags, offset persistence boundary, commit semantics, configuration
//! validation, and a certification test harness stub.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

pub mod cdc;
pub mod cdc_router;
pub mod kafka;
pub mod parquet;
pub mod s3;
pub mod transactional;
pub mod transactional_kafka;
pub mod two_phase_parquet_s3;

// ---------------------------------------------------------------------------
// Error and Result
// ---------------------------------------------------------------------------

/// Errors produced by connector operations.
#[non_exhaustive]
#[derive(Debug)]
pub enum ConnectorError {
    /// Configuration problem (missing required property, bad value, etc.).
    Config { message: String },
    /// Kafka-specific error (connection, produce, consume).
    Kafka { message: String, retriable: bool },
    /// Parquet read/write error.
    Parquet(String),
    /// Object-store (S3/GCS/Azure) error with optional HTTP status code.
    ObjectStore { message: String, status: Option<u16> },
    /// CDC (change-data-capture) pipeline error.
    Cdc(String),
    /// Typed I/O error from the operating system.
    Io(std::io::Error),
    /// Schema mismatch or incompatible field types.
    Schema { message: String },
    /// Operation is not supported by this connector.
    Unsupported { message: String },
    /// A certification test assertion failed.
    CertificationFailed { reason: String },
    /// Migration alias: callers that previously used `Io { message }` form.
    #[allow(non_camel_case_types)]
    IoStr { message: String },
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorError::Config { message } => write!(f, "connector config error: {message}"),
            ConnectorError::Kafka { message, retriable } => write!(
                f,
                "connector Kafka error (retriable={retriable}): {message}"
            ),
            ConnectorError::Parquet(message) => write!(f, "connector Parquet error: {message}"),
            ConnectorError::ObjectStore { message, status } => match status {
                Some(code) => write!(f, "connector object-store error (HTTP {code}): {message}"),
                None => write!(f, "connector object-store error: {message}"),
            },
            ConnectorError::Cdc(message) => write!(f, "connector CDC error: {message}"),
            ConnectorError::Io(e) => write!(f, "connector I/O error: {e}"),
            ConnectorError::Schema { message } => write!(f, "connector schema error: {message}"),
            ConnectorError::Unsupported { message } => {
                write!(f, "connector unsupported: {message}")
            }
            ConnectorError::CertificationFailed { reason } => {
                write!(f, "connector certification failed: {reason}")
            }
            ConnectorError::IoStr { message } => write!(f, "connector I/O error: {message}"),
        }
    }
}

impl std::error::Error for ConnectorError {}

/// Convenience result alias for connector operations.
pub type ConnectorResult<T> = Result<T, ConnectorError>;

// ---------------------------------------------------------------------------
// ConnectorCapabilities
// ---------------------------------------------------------------------------

/// Describes what guarantees and modes a connector supports.
///
/// All flags default to `false`. Use the builder methods to opt-in to
/// capabilities the connector actually provides.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnectorCapabilities {
    bounded: bool,
    unbounded: bool,
    rewindable: bool,
    transactional: bool,
    idempotent: bool,
    /// Can participate in the barrier checkpoint protocol (R6).
    supports_checkpoint: bool,
    /// Implements `TwoPhaseCommitSink` for exactly-once delivery (R6).
    supports_two_phase_commit: bool,
}

impl ConnectorCapabilities {
    /// Create a new capabilities instance with all flags disabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the connector as producing a bounded (finite) data stream.
    ///
    /// Clears the `unbounded` flag: a connector cannot be both bounded and unbounded.
    #[must_use]
    pub fn with_bounded(mut self) -> Self {
        self.bounded = true;
        self.unbounded = false;
        debug_assert!(!self.bounded || !self.unbounded);
        self
    }

    /// Mark the connector as producing an unbounded (infinite) data stream.
    ///
    /// Clears the `bounded` flag: a connector cannot be both bounded and unbounded.
    #[must_use]
    pub fn with_unbounded(mut self) -> Self {
        self.unbounded = true;
        self.bounded = false;
        debug_assert!(!self.bounded || !self.unbounded);
        self
    }

    /// Validate capability invariants.
    ///
    /// Returns an error if both `bounded` and `unbounded` are set simultaneously.
    pub fn validate(&self) -> ConnectorResult<()> {
        if self.bounded && self.unbounded {
            return Err(ConnectorError::Config {
                message: "connector capabilities: bounded and unbounded cannot both be true".into(),
            });
        }
        Ok(())
    }

    /// Mark the connector as supporting rewind to a previous offset.
    #[must_use]
    pub fn with_rewindable(mut self) -> Self {
        self.rewindable = true;
        self
    }

    /// Mark the connector as supporting transactional commits.
    #[must_use]
    pub fn with_transactional(mut self) -> Self {
        self.transactional = true;
        self
    }

    /// Mark the connector as supporting idempotent writes.
    #[must_use]
    pub fn with_idempotent(mut self) -> Self {
        self.idempotent = true;
        self
    }

    /// Mark the connector as capable of participating in the barrier checkpoint protocol.
    #[must_use]
    pub fn with_checkpoint(mut self) -> Self {
        self.supports_checkpoint = true;
        self
    }

    /// Mark the connector as implementing two-phase commit for exactly-once delivery.
    #[must_use]
    pub fn with_two_phase_commit(mut self) -> Self {
        self.supports_two_phase_commit = true;
        self
    }

    /// Returns `true` if the data stream is bounded (finite).
    pub fn is_bounded(&self) -> bool {
        self.bounded
    }

    /// Returns `true` if the data stream is unbounded (infinite).
    pub fn is_unbounded(&self) -> bool {
        self.unbounded
    }

    /// Returns `true` if the connector supports rewind to a previous offset.
    pub fn is_rewindable(&self) -> bool {
        self.rewindable
    }

    /// Returns `true` if the connector supports transactional commits.
    pub fn is_transactional(&self) -> bool {
        self.transactional
    }

    /// Returns `true` if writes are idempotent (safe to replay).
    pub fn is_idempotent(&self) -> bool {
        self.idempotent
    }

    /// Returns `true` if the connector can participate in the barrier checkpoint protocol.
    pub fn is_checkpoint_capable(&self) -> bool {
        self.supports_checkpoint
    }

    /// Returns `true` if the connector implements two-phase commit for exactly-once delivery.
    pub fn is_two_phase_commit_capable(&self) -> bool {
        self.supports_two_phase_commit
    }

    /// Returns `true` if at least one capability flag is set.
    pub fn has_any(&self) -> bool {
        self.bounded
            || self.unbounded
            || self.rewindable
            || self.transactional
            || self.idempotent
            || self.supports_checkpoint
            || self.supports_two_phase_commit
    }
}

// ---------------------------------------------------------------------------
// Offset
// ---------------------------------------------------------------------------

/// Serialisable cursor into a connector's data stream.
///
/// Implementors must be able to round-trip through `encode`/`decode` without
/// loss of information.
pub trait Offset {
    /// Serialise this offset to a byte vector.
    fn encode(&self) -> Vec<u8>;

    /// Deserialise an offset from a byte slice.
    fn decode(bytes: &[u8]) -> ConnectorResult<Self>
    where
        Self: Sized;
}

// ---------------------------------------------------------------------------
// CommitHandle
// ---------------------------------------------------------------------------

/// Handle returned by a sink after a batch is buffered, allowing the caller
/// to drive the two-phase commit boundary.
pub trait CommitHandle {
    /// Durably commit the buffered output.
    fn commit(&self) -> impl Future<Output = ConnectorResult<()>> + Send;
}

// ---------------------------------------------------------------------------
// OffsetCommitter
// ---------------------------------------------------------------------------

/// Commits a source offset after downstream output is durable.
///
/// This is intentionally separate from [`Source`] so executors can keep source
/// reads, sink writes, and offset commits ordered explicitly.  For at-least-once
/// delivery, callers must commit offsets only after the corresponding sink
/// output has been written and flushed.
pub trait OffsetCommitter<O: Offset> {
    /// Persist `offset` as consumed.
    fn commit_offset(&mut self, offset: O) -> impl Future<Output = ConnectorResult<()>> + Send;
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// An async, pull-based data source that emits Arrow [`RecordBatch`] values.
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
pub trait Source {
    /// Return the capabilities this source supports.
    fn capabilities(&self) -> ConnectorCapabilities;

    /// Pull the next batch from the source.
    ///
    /// Returns `Ok(None)` when the source is exhausted (bounded sources only).
    fn read_batch(
        &mut self,
    ) -> impl Future<Output = ConnectorResult<Option<arrow::record_batch::RecordBatch>>> + Send;

    /// Return the current read offset, if available.
    ///
    /// The returned value is connector-specific and should be downcast by the
    /// caller if it needs the concrete offset type.
    fn current_offset(&self) -> Option<Box<dyn Any + Send>>;

    /// Rewind the source to its initial position.
    ///
    /// The default implementation is a no-op; sources that advertise
    /// [`ConnectorCapabilities::is_rewindable`] **must** override this method.
    /// A debug-mode assertion fires if a rewindable source does not override,
    /// catching capability mismatches during development.
    fn reset(&mut self) {
        debug_assert!(
            !self.capabilities().is_rewindable(),
            "source advertises rewindable capability but does not override reset()"
        );
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// An async, push-based data sink that accepts Arrow [`RecordBatch`] values.
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
pub trait Sink {
    /// Return the capabilities this sink supports.
    fn capabilities(&self) -> ConnectorCapabilities;

    /// Write a single batch to the sink.
    fn write_batch(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> impl Future<Output = ConnectorResult<()>> + Send;

    /// Flush any buffered data and close the sink.
    fn flush(&mut self) -> impl Future<Output = ConnectorResult<()>> + Send;
}

/// Dyn-compatible version of [`Sink`] that boxes the async return types.
///
/// Because [`Sink`] uses `impl Future` returns it is not object-safe.  This
/// trait provides a blanket implementation over every `T: Sink + Send` and can
/// be used as `Box<dyn DynSink>` wherever dynamic dispatch is needed.
pub trait DynSink: Send {
    fn write_batch_dyn(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<()>> + Send + '_>>;

    fn flush_dyn(&mut self) -> Pin<Box<dyn Future<Output = ConnectorResult<()>> + Send + '_>>;
}

impl<T: Sink + Send> DynSink for T {
    fn write_batch_dyn(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<()>> + Send + '_>> {
        Box::pin(self.write_batch(batch))
    }

    fn flush_dyn(&mut self) -> Pin<Box<dyn Future<Output = ConnectorResult<()>> + Send + '_>> {
        Box::pin(self.flush())
    }
}

// ---------------------------------------------------------------------------
// ConnectorConfig
// ---------------------------------------------------------------------------

/// Key/value configuration bag for connector instantiation.
///
/// Properties are stored in a sorted map to make serialisation deterministic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorConfig {
    /// Logical name for this connector instance.
    pub name: String,
    /// Connector kind identifier (e.g., `"parquet"`, `"kafka"`, `"s3"`).
    pub kind: String,
    properties: BTreeMap<String, String>,
}

impl ConnectorConfig {
    /// Create a new config with the given name and kind, and no properties.
    pub fn new(name: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: kind.into(),
            properties: BTreeMap::new(),
        }
    }

    /// Add a property and return the updated config (builder style).
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Look up a property by key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(String::as_str)
    }

    /// Look up a required property, returning a [`ConnectorError::Config`] if
    /// it is absent.
    pub fn required(&self, key: &str) -> ConnectorResult<&str> {
        self.get(key).ok_or_else(|| ConnectorError::Config {
            message: format!(
                "required property '{key}' is missing from connector '{}'",
                self.name
            ),
        })
    }
}

// ---------------------------------------------------------------------------
// ParquetOffset
// ---------------------------------------------------------------------------

/// Cursor into a Parquet-backed source: index of the next batch to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetOffset {
    pub batch_index: usize,
}

impl Offset for ParquetOffset {
    fn encode(&self) -> Vec<u8> {
        (self.batch_index as u64).to_le_bytes().to_vec()
    }

    fn decode(bytes: &[u8]) -> ConnectorResult<Self> {
        if bytes.len() < 8 {
            return Err(ConnectorError::IoStr {
                message: "ParquetOffset decode: expected 8 bytes".into(),
            });
        }
        let n = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        Ok(Self {
            batch_index: n as usize,
        })
    }
}

// ---------------------------------------------------------------------------
// AtLeastOnceSinkContract
// ---------------------------------------------------------------------------

/// Documents the at-least-once sink delivery contract.
///
/// A sink operating under at-least-once semantics guarantees:
/// - Every input batch is written to the downstream store at least once.
/// - On executor reassignment or crash-recovery, batches that were written
///   but not yet acknowledged may be replayed. Idempotent sinks (`is_idempotent()`)
///   handle replays safely. Non-idempotent sinks may produce duplicates.
/// - `flush()` must be called and awaited before an offset is committed to
///   the source. Committing the source offset before `flush()` completes risks
///   data loss on crash.
pub struct AtLeastOnceSinkContract;

/// Drives the R3.2 post-write offset commit protocol.
///
/// The protocol order is:
///
/// 1. write the output batch to the sink,
/// 2. flush the sink so the output is durable,
/// 3. commit the source offset.
///
/// If either the write or flush fails, the offset is not committed.  This keeps
/// reassignment/replay at-least-once: a task may write duplicate output to a
/// non-idempotent sink, but it does not acknowledge data that was not durably
/// written.
#[derive(Debug, Default, Clone, Copy)]
pub struct PostWriteOffsetCommitProtocol;

impl PostWriteOffsetCommitProtocol {
    /// Write `batch`, flush `sink`, and then commit `offset`.
    pub async fn write_flush_commit<S, C, O>(
        sink: &mut S,
        committer: &mut C,
        batch: arrow::record_batch::RecordBatch,
        offset: O,
    ) -> ConnectorResult<()>
    where
        S: Sink,
        C: OffsetCommitter<O>,
        O: Offset,
    {
        sink.write_batch(batch).await?;
        sink.flush().await?;
        committer.commit_offset(offset).await
    }
}

// ---------------------------------------------------------------------------
// CertificationSuite
// ---------------------------------------------------------------------------

/// Stub test harness for connector certification.
///
/// A real certification suite would drive the connector through its full
/// lifecycle. This stub checks only the capability invariants that every
/// connector must satisfy.
pub struct CertificationSuite {
    /// Human-readable name of the suite being run.
    pub name: String,
}

impl CertificationSuite {
    /// Create a new certification suite with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Verify that a source declares at least one capability.
    pub fn run_source_capabilities_test(source: &impl Source) -> ConnectorResult<()> {
        let caps = source.capabilities();
        if !caps.has_any() {
            return Err(ConnectorError::Unsupported {
                message: "source must declare at least one capability flag".into(),
            });
        }
        Ok(())
    }

    /// Verify that a sink declares at least one capability.
    pub fn run_sink_capabilities_test(sink: &impl Sink) -> ConnectorResult<()> {
        let caps = sink.capabilities();
        if !caps.has_any() {
            return Err(ConnectorError::Unsupported {
                message: "sink must declare at least one capability flag".into(),
            });
        }
        Ok(())
    }

    /// Drain a bounded source and verify it returns `None` on exhaustion.
    ///
    /// Returns [`ConnectorError::Unsupported`] if the source is not bounded.
    /// Returns [`ConnectorError::Unsupported`] if the source does not exhaust
    /// within 100,000 batches (guards against infinite sources that
    /// misreport bounded capability).
    pub async fn run_bounded_exhaustion_test(source: &mut impl Source) -> ConnectorResult<()> {
        if !source.capabilities().is_bounded() {
            return Err(ConnectorError::Unsupported {
                message: "exhaustion test requires a bounded source".into(),
            });
        }
        let mut count = 0usize;
        loop {
            match source.read_batch().await? {
                Some(_) => count += 1,
                None => break,
            }
            if count > 100_000 {
                return Err(ConnectorError::Unsupported {
                    message: "source did not exhaust after 100_000 batches".into(),
                });
            }
        }
        Ok(())
    }

    /// Encode then decode `offset` and verify the round-trip produces an equal value.
    ///
    /// `O` must implement both `Offset` and `PartialEq + std::fmt::Debug` so the
    /// failure message can produce a useful description.
    pub fn run_offset_round_trip_test<O>(offset: O) -> ConnectorResult<()>
    where
        O: Offset + PartialEq + std::fmt::Debug,
    {
        let encoded = offset.encode();
        let decoded = O::decode(&encoded)?;
        if offset != decoded {
            return Err(ConnectorError::CertificationFailed {
                reason: format!(
                    "offset round-trip failed: original={offset:?}, decoded={decoded:?}"
                ),
            });
        }
        Ok(())
    }

    /// Write `batches` to `sink`, flush it, and verify the sink declared
    /// idempotent.
    ///
    /// Returns [`ConnectorError::Unsupported`] if the sink is not idempotent.
    pub async fn run_idempotent_sink_test(
        sink: &mut impl Sink,
        batches: &[arrow::record_batch::RecordBatch],
    ) -> ConnectorResult<()> {
        if !sink.capabilities().is_idempotent() {
            return Err(ConnectorError::Unsupported {
                message: "idempotent sink test requires idempotent capability".into(),
            });
        }
        for batch in batches {
            sink.write_batch(batch.clone()).await?;
        }
        sink.flush().await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// TwoPhaseCommitSink
// ---------------------------------------------------------------------------

/// Sink that participates in two-phase checkpoint commit (R6).
///
/// The caller drives the protocol:
/// 1. Call `prepare(epoch, batch)` — the sink buffers the batch under a
///    staging key tied to `epoch` and returns an opaque `Handle`.
/// 2. After all operators in the job acknowledge the barrier for `epoch`,
///    call `commit(handle)` — the sink makes the buffered output durable
///    (e.g., an atomic rename from a staging prefix to the final key).
/// 3. If the checkpoint is aborted, call `abort(handle)` — the sink discards
///    the staged output without making it visible.
///
/// `commit` and `abort` are mutually exclusive for a given handle.
/// Calling `commit` after `abort`, or vice versa, is a logic error and
/// implementations may panic.
///
/// The certified R6 sink is `S3/Parquet` (object-level atomic rename).
/// `InMemoryTwoPhaseCommitSink` is provided for deterministic testing.
pub trait TwoPhaseCommitSink: Send {
    /// Opaque handle returned by `prepare`.
    type Handle: Send;

    /// Buffer `batch` under a staging area keyed to `epoch`.
    ///
    /// Returns a `Handle` that identifies this staged write.
    fn prepare(
        &mut self,
        epoch: u64,
        batch: &arrow::record_batch::RecordBatch,
    ) -> ConnectorResult<Self::Handle>;

    /// Make the staged output for `handle` durable and visible.
    fn commit(&mut self, handle: Self::Handle) -> ConnectorResult<()>;

    /// Discard the staged output for `handle` without making it visible.
    fn abort(&mut self, handle: Self::Handle) -> ConnectorResult<()>;
}

// ---------------------------------------------------------------------------
// InMemoryTwoPhaseCommitSink
// ---------------------------------------------------------------------------

/// In-memory two-phase commit sink for deterministic testing.
///
/// `prepare` stages a batch under `(epoch, handle_id)`.
/// `commit` moves it to the committed list.
/// `abort` drops it.
#[derive(Debug, Default)]
pub struct InMemoryTwoPhaseCommitSink {
    staged: std::collections::BTreeMap<u64, Vec<arrow::record_batch::RecordBatch>>,
    committed: Vec<(u64, arrow::record_batch::RecordBatch)>,
    next_handle: u64,
}

impl InMemoryTwoPhaseCommitSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// All committed `(epoch, batch)` pairs, in commit order.
    pub fn committed(&self) -> &[(u64, arrow::record_batch::RecordBatch)] {
        &self.committed
    }

    /// Number of batches currently staged but not yet committed or aborted.
    pub fn staged_count(&self) -> usize {
        self.staged.values().map(|v| v.len()).sum()
    }
}

/// Handle for a staged write in `InMemoryTwoPhaseCommitSink`.
#[derive(Debug, Clone, Copy)]
pub struct InMemoryCommitHandle {
    epoch: u64,
    handle_id: u64,
}

impl TwoPhaseCommitSink for InMemoryTwoPhaseCommitSink {
    type Handle = InMemoryCommitHandle;

    fn prepare(
        &mut self,
        epoch: u64,
        batch: &arrow::record_batch::RecordBatch,
    ) -> ConnectorResult<Self::Handle> {
        let handle_id = self.next_handle;
        self.next_handle += 1;
        self.staged
            .entry(handle_id)
            .or_default()
            .push(batch.clone());
        Ok(InMemoryCommitHandle { epoch, handle_id })
    }

    fn commit(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        if let Some(batches) = self.staged.remove(&handle.handle_id) {
            for batch in batches {
                self.committed.push((handle.epoch, batch));
            }
        }
        Ok(())
    }

    fn abort(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        self.staged.remove(&handle.handle_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// LocalParquetTwoPhaseCommitSink
// ---------------------------------------------------------------------------

/// Handle for a staged Parquet write.
///
/// Carries the `.tmp` staging path and the final target path so `commit` can
/// atomically rename and `abort` can delete the staging file.
#[derive(Debug, Clone)]
pub struct ParquetCommitHandle {
    pub epoch: u64,
    /// Path to the `.tmp` file written during `prepare`.
    pub staging_path: std::path::PathBuf,
    /// Final target path (after rename on `commit`).
    pub final_path: std::path::PathBuf,
}

/// Parquet-backed two-phase commit sink.
///
/// `prepare(epoch, batch)` serializes `batch` to a `.tmp` file named
/// `<epoch>-<handle_id>.parquet.tmp` inside `output_dir`.
/// `commit(handle)` renames the `.tmp` file to its final `.parquet` name.
/// `abort(handle)` deletes the `.tmp` file.
///
/// The rename in `commit` is atomic on POSIX filesystems, providing
/// exactly-once delivery guarantees for local storage.
pub struct LocalParquetTwoPhaseCommitSink {
    output_dir: std::path::PathBuf,
    next_handle: u64,
    quality_config: Option<DataQualityConfig>,
}

impl LocalParquetTwoPhaseCommitSink {
    /// Create a sink that writes Parquet files to `output_dir`.
    /// The directory must already exist.
    pub fn new(output_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            output_dir: output_dir.into(),
            next_handle: 0,
            quality_config: None,
        }
    }

    /// Attach a data quality configuration. Quality checks run during `prepare()`.
    /// Rows failing a `Reject` rule are excluded from the written output.
    /// A `Fail` rule aborts the entire prepare with an error.
    #[must_use]
    pub fn with_quality_config(mut self, config: DataQualityConfig) -> Self {
        self.quality_config = Some(config);
        self
    }
}

impl TwoPhaseCommitSink for LocalParquetTwoPhaseCommitSink {
    type Handle = ParquetCommitHandle;

    fn prepare(
        &mut self,
        epoch: u64,
        batch: &arrow::record_batch::RecordBatch,
    ) -> ConnectorResult<Self::Handle> {
        // Run quality checks if a config is attached.
        let filtered: arrow::record_batch::RecordBatch;
        let batch = if let Some(ref qc) = self.quality_config {
            use arrow::array::BooleanArray;
            let result = check_batch(batch, qc)?;
            if result.failed {
                return Err(ConnectorError::IoStr {
                    message: format!("data quality Fail action triggered at epoch {}", epoch),
                });
            }
            if result.accepted_indices.len() == batch.num_rows() {
                batch // No rows rejected — use original batch
            } else {
                let keep_mask: BooleanArray = (0..batch.num_rows())
                    .map(|i| Some(result.accepted_indices.contains(&i)))
                    .collect();
                filtered = arrow::compute::filter_record_batch(batch, &keep_mask).map_err(|e| {
                    ConnectorError::IoStr {
                        message: e.to_string(),
                    }
                })?;
                &filtered
            }
        } else {
            batch
        };

        let (staging_path, final_path, file) = loop {
            let handle_id = self.next_handle;
            self.next_handle += 1;
            let staging_name = format!("{epoch}-{handle_id}.parquet.tmp");
            let final_name = format!("{epoch}-{handle_id}.parquet");
            let staging_path = self.output_dir.join(&staging_name);
            let final_path = self.output_dir.join(&final_name);
            if final_path.exists() {
                continue;
            }
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staging_path)
            {
                Ok(file) => break (staging_path, final_path, file),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => {
                    return Err(ConnectorError::IoStr {
                        message: format!("parquet 2pc prepare: cannot create {staging_name}: {e}"),
                    });
                }
            }
        };

        let mut writer = ::parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
            .map_err(|e| ConnectorError::IoStr {
                message: format!("parquet 2pc prepare: cannot create writer: {e}"),
            })?;
        writer.write(batch).map_err(|e| ConnectorError::IoStr {
            message: format!("parquet 2pc prepare: write error: {e}"),
        })?;
        writer.close().map_err(|e| ConnectorError::IoStr {
            message: format!("parquet 2pc prepare: close error: {e}"),
        })?;

        Ok(ParquetCommitHandle {
            epoch,
            staging_path,
            final_path,
        })
    }

    fn commit(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        match std::fs::hard_link(&handle.staging_path, &handle.final_path) {
            Ok(()) => std::fs::remove_file(&handle.staging_path).map_err(|e| ConnectorError::IoStr {
                message: format!(
                    "parquet 2pc commit: remove staging {:?}: {e}",
                    handle.staging_path
                ),
            }),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = std::fs::remove_file(&handle.staging_path);
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && handle.final_path.exists() => {
                Ok(())
            }
            Err(e) => Err(ConnectorError::IoStr {
                message: format!(
                    "parquet 2pc commit: link {:?} to {:?}: {e}",
                    handle.staging_path, handle.final_path
                ),
            }),
        }
    }

    fn abort(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        use std::io::ErrorKind;
        match std::fs::remove_file(&handle.staging_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ConnectorError::IoStr {
                message: format!("parquet 2pc abort: remove {:?}: {e}", handle.staging_path),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// DataQualityRule
// ---------------------------------------------------------------------------

/// A predicate applied per row against a column value.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum DataQualityRule {
    /// Column must not be null.
    NotNull { column: String },
    /// Numeric column must be within [min, max] inclusive.
    Range { column: String, min: f64, max: f64 },
    /// String column must match the regex pattern.
    Regex { column: String, pattern: String },
}

// ---------------------------------------------------------------------------
// CompiledQualityRule / CompiledDataQualityConfig   (P2.8)
// ---------------------------------------------------------------------------

/// A data quality rule with any regex pre-compiled.
///
/// Build via [`DataQualityConfig::compile`] so that regex compilation happens
/// once per config, not once per batch.
pub enum CompiledQualityRule {
    /// Column must not be null.
    NotNull { column: String },
    /// Numeric column must be within [min, max] inclusive.
    Range { column: String, min: f64, max: f64 },
    /// String column must match the pre-compiled regex.
    Regex {
        column: String,
        pattern: String,
        compiled: regex::Regex,
    },
}

/// A fully compiled data quality configuration.
///
/// Created by [`DataQualityConfig::compile`]. Pass this to [`check_batch_compiled`]
/// to avoid recompiling regexes on every call.
pub struct CompiledDataQualityConfig {
    pub rules: Vec<(CompiledQualityRule, QualityAction)>,
}

impl DataQualityConfig {
    /// Compile all regex patterns in this config.
    ///
    /// Returns a [`CompiledDataQualityConfig`] that can be used with
    /// [`check_batch_compiled`] to avoid recompiling regexes on every batch.
    pub fn compile(self) -> ConnectorResult<CompiledDataQualityConfig> {
        let mut compiled_rules = Vec::with_capacity(self.rules.len());
        for (rule, action) in self.rules {
            let compiled_rule = match rule {
                DataQualityRule::NotNull { column } => CompiledQualityRule::NotNull { column },
                DataQualityRule::Range { column, min, max } => {
                    CompiledQualityRule::Range { column, min, max }
                }
                DataQualityRule::Regex { column, pattern } => {
                    let compiled =
                        regex::Regex::new(&pattern).map_err(|e| ConnectorError::Config {
                            message: format!("invalid regex pattern '{pattern}': {e}"),
                        })?;
                    CompiledQualityRule::Regex {
                        column,
                        pattern,
                        compiled,
                    }
                }
            };
            compiled_rules.push((compiled_rule, action));
        }
        Ok(CompiledDataQualityConfig {
            rules: compiled_rules,
        })
    }
}

/// Run all pre-compiled quality rules against `batch`. Returns a [`DataQualityCheckResult`].
///
/// Prefer this over [`check_batch`] when the same config is used across multiple batches,
/// since regex compilation only happens once.
pub fn check_batch_compiled(
    batch: &arrow::record_batch::RecordBatch,
    config: &CompiledDataQualityConfig,
) -> ConnectorResult<DataQualityCheckResult> {
    let nrows = batch.num_rows();
    let mut rejected_rows: Vec<usize> = Vec::new();
    let mut rejected_meta: Vec<RejectedRow> = Vec::new();
    let mut failed = false;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    for (rule, action) in &config.rules {
        let (col_name, violations) = find_violations_compiled(batch, rule)?;
        for row_idx in violations {
            if rejected_rows.contains(&row_idx) {
                continue;
            }
            match action {
                QualityAction::Fail => {
                    failed = true;
                }
                QualityAction::Reject => {
                    rejected_rows.push(row_idx);
                    rejected_meta.push(RejectedRow {
                        batch_row_index: row_idx,
                        rule_violated: format!("{:?}", col_name),
                        column_name: col_name.clone(),
                        timestamp_ms: now_ms,
                    });
                }
                QualityAction::Warn => {
                    tracing::warn!(
                        column = %col_name,
                        row_index = row_idx,
                        "data quality warning: rule violated"
                    );
                }
            }
        }
    }

    let accepted_indices: Vec<usize> = (0..nrows).filter(|i| !rejected_rows.contains(i)).collect();

    Ok(DataQualityCheckResult {
        accepted_indices,
        rejected: rejected_meta,
        failed,
    })
}

fn find_violations_compiled(
    batch: &arrow::record_batch::RecordBatch,
    rule: &CompiledQualityRule,
) -> ConnectorResult<(String, Vec<usize>)> {
    use arrow::array::{Array, Float64Array};

    match rule {
        CompiledQualityRule::NotNull { column } => {
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|e| ConnectorError::Schema {
                    message: format!("column '{column}' not found: {e}"),
                })?;
            let col = batch.column(col_idx);
            let violations: Vec<usize> =
                (0..batch.num_rows()).filter(|&i| col.is_null(i)).collect();
            Ok((column.clone(), violations))
        }
        CompiledQualityRule::Range { column, min, max } => {
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|e| ConnectorError::Schema {
                    message: format!("column '{column}' not found: {e}"),
                })?;
            let col = batch.column(col_idx);
            let float_col = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                ConnectorError::Schema {
                    message: format!("column '{column}' is not Float64 for Range rule"),
                }
            })?;
            let violations: Vec<usize> = (0..batch.num_rows())
                .filter(|&i| {
                    if float_col.is_null(i) {
                        return true;
                    }
                    let v = float_col.value(i);
                    v < *min || v > *max
                })
                .collect();
            Ok((column.clone(), violations))
        }
        CompiledQualityRule::Regex {
            column,
            pattern: _,
            compiled,
        } => {
            use arrow::array::StringArray;
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|e| ConnectorError::Schema {
                    message: format!("column '{column}' not found: {e}"),
                })?;
            let col = batch.column(col_idx);
            let str_col = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ConnectorError::Schema {
                    message: format!("column '{column}' is not Utf8 for Regex rule"),
                }
            })?;
            let violations: Vec<usize> = (0..batch.num_rows())
                .filter(|&i| {
                    if str_col.is_null(i) {
                        return true;
                    }
                    !compiled.is_match(str_col.value(i))
                })
                .collect();
            Ok((column.clone(), violations))
        }
    }
}

// ---------------------------------------------------------------------------
// QualityAction
// ---------------------------------------------------------------------------

/// Action taken when a data quality rule is violated.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualityAction {
    /// Abort the entire batch.
    Fail,
    /// Route the violating row to the rejected-row output.
    Reject,
    /// Increment a counter metric and pass the row through.
    Warn,
}

// ---------------------------------------------------------------------------
// DataQualityConfig
// ---------------------------------------------------------------------------

/// Data quality configuration attached to a sink.
#[derive(Debug, Clone, Default)]
pub struct DataQualityConfig {
    pub rules: Vec<(DataQualityRule, QualityAction)>,
}

impl DataQualityConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_rule(mut self, rule: DataQualityRule, action: QualityAction) -> Self {
        self.rules.push((rule, action));
        self
    }
}

// ---------------------------------------------------------------------------
// RejectedRow
// ---------------------------------------------------------------------------

/// A row rejected by a data quality check, with metadata.
#[derive(Debug, Clone)]
pub struct RejectedRow {
    pub batch_row_index: usize,
    pub rule_violated: String, // display name of the rule
    pub column_name: String,
    pub timestamp_ms: i64, // Unix epoch milliseconds
}

// ---------------------------------------------------------------------------
// DataQualityCheckResult
// ---------------------------------------------------------------------------

/// Result of running data quality checks on a batch.
pub struct DataQualityCheckResult {
    /// Rows accepted (indices into original batch).
    pub accepted_indices: Vec<usize>,
    /// Rejected rows.
    pub rejected: Vec<RejectedRow>,
    /// True if a Fail action was triggered.
    pub failed: bool,
}

// ---------------------------------------------------------------------------
// check_batch / find_violations
// ---------------------------------------------------------------------------

/// Run all quality rules against `batch`. Returns a `DataQualityCheckResult`.
pub fn check_batch(
    batch: &arrow::record_batch::RecordBatch,
    config: &DataQualityConfig,
) -> ConnectorResult<DataQualityCheckResult> {
    let nrows = batch.num_rows();
    let mut rejected_rows: Vec<usize> = Vec::new();
    let mut rejected_meta: Vec<RejectedRow> = Vec::new();
    let mut failed = false;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    for (rule, action) in &config.rules {
        let (col_name, violations) = find_violations(batch, rule)?;
        for row_idx in violations {
            if rejected_rows.contains(&row_idx) {
                continue;
            }
            match action {
                QualityAction::Fail => {
                    failed = true;
                }
                QualityAction::Reject => {
                    rejected_rows.push(row_idx);
                    rejected_meta.push(RejectedRow {
                        batch_row_index: row_idx,
                        rule_violated: format!("{:?}", rule),
                        column_name: col_name.clone(),
                        timestamp_ms: now_ms,
                    });
                }
                QualityAction::Warn => {
                    tracing::warn!(
                        rule = ?rule,
                        row_index = row_idx,
                        "data quality warning: rule violated"
                    );
                }
            }
        }
    }

    let accepted_indices: Vec<usize> = (0..nrows).filter(|i| !rejected_rows.contains(i)).collect();

    Ok(DataQualityCheckResult {
        accepted_indices,
        rejected: rejected_meta,
        failed,
    })
}

fn find_violations(
    batch: &arrow::record_batch::RecordBatch,
    rule: &DataQualityRule,
) -> ConnectorResult<(String, Vec<usize>)> {
    use arrow::array::{Array, Float64Array};

    match rule {
        DataQualityRule::NotNull { column } => {
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|e| ConnectorError::Schema {
                    message: format!("column '{}' not found: {}", column, e),
                })?;
            let col = batch.column(col_idx);
            let violations: Vec<usize> =
                (0..batch.num_rows()).filter(|&i| col.is_null(i)).collect();
            Ok((column.clone(), violations))
        }
        DataQualityRule::Range { column, min, max } => {
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|e| ConnectorError::Schema {
                    message: format!("column '{}' not found: {}", column, e),
                })?;
            let col = batch.column(col_idx);
            let float_col = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                ConnectorError::Schema {
                    message: format!("column '{}' is not Float64 for Range rule", column),
                }
            })?;
            let violations: Vec<usize> = (0..batch.num_rows())
                .filter(|&i| {
                    if float_col.is_null(i) {
                        return true;
                    }
                    let v = float_col.value(i);
                    v < *min || v > *max
                })
                .collect();
            Ok((column.clone(), violations))
        }
        DataQualityRule::Regex { column, pattern } => {
            use arrow::array::StringArray;
            let re = regex::Regex::new(pattern).map_err(|e| ConnectorError::Config {
                message: format!("invalid regex pattern '{}': {}", pattern, e),
            })?;
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|e| ConnectorError::Schema {
                    message: format!("column '{}' not found: {}", column, e),
                })?;
            let col = batch.column(col_idx);
            let str_col = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ConnectorError::Schema {
                    message: format!("column '{}' is not Utf8 for Regex rule", column),
                }
            })?;
            let violations: Vec<usize> = (0..batch.num_rows())
                .filter(|&i| {
                    if str_col.is_null(i) {
                        return true; // null = violation
                    }
                    !re.is_match(str_col.value(i))
                })
                .collect();
            Ok((column.clone(), violations))
        }
    }
}

// ---------------------------------------------------------------------------
// DeadLetterSink
// ---------------------------------------------------------------------------

/// Wraps a sink and writes rejected rows plus metadata to a secondary output.
///
/// The primary output receives only accepted rows. Rejected rows are written
/// to the dead-letter output with error metadata appended as extra columns.
pub struct DeadLetterSink {
    /// Name of the dead-letter sink (used in metrics/logs).
    pub name: String,
    /// Quality configuration applied before writing to the primary sink.
    pub quality_config: DataQualityConfig,
    /// Optional secondary sink that receives rejected rows with error metadata.
    secondary: Option<Box<dyn DynSink>>,
}

impl std::fmt::Debug for DeadLetterSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeadLetterSink")
            .field("name", &self.name)
            .field("has_secondary", &self.secondary.is_some())
            .finish()
    }
}

impl DeadLetterSink {
    pub fn new(name: impl Into<String>, quality_config: DataQualityConfig) -> Self {
        Self {
            name: name.into(),
            quality_config,
            secondary: None,
        }
    }

    /// Attach a secondary sink that receives rejected rows with an appended
    /// `_error: Utf8` column containing the violation reason.
    #[must_use]
    pub fn with_secondary_sink(mut self, sink: impl Sink + Send + 'static) -> Self {
        self.secondary = Some(Box::new(sink));
        self
    }

    /// Run quality checks and return `(accepted_batch, rejected_rows)`.
    ///
    /// If a secondary sink is attached, rejected rows are written to it with an
    /// additional `_error` column.  Because that forwarding is async, the whole
    /// method is `async`.
    pub async fn process_batch(
        &mut self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> ConnectorResult<(arrow::record_batch::RecordBatch, Vec<RejectedRow>)> {
        use arrow::array::{BooleanArray, StringArray};
        use arrow::datatypes::{DataType, Field};

        let result = check_batch(batch, &self.quality_config)?;

        if result.failed {
            return Err(ConnectorError::IoStr {
                message: format!("sink '{}': data quality Fail action triggered", self.name),
            });
        }

        // Build accepted batch (rows not in the rejected set).
        let keep_mask: BooleanArray = (0..batch.num_rows())
            .map(|i| Some(result.accepted_indices.contains(&i)))
            .collect();
        let accepted = arrow::compute::filter_record_batch(batch, &keep_mask).map_err(|e| {
            ConnectorError::IoStr {
                message: e.to_string(),
            }
        })?;

        // Forward rejected rows to the secondary (dead-letter) sink if present.
        if let Some(ref mut secondary) = self.secondary
            && !result.rejected.is_empty()
        {
            let reject_mask: BooleanArray = (0..batch.num_rows())
                .map(|i| Some(!result.accepted_indices.contains(&i)))
                .collect();
            let rejected_batch =
                arrow::compute::filter_record_batch(batch, &reject_mask).map_err(|e| {
                    ConnectorError::IoStr {
                        message: e.to_string(),
                    }
                })?;

            // Build _error column keyed by original row index so the error string
            // is always attached to the correct rejected row even when multiple
            // rules fire on different rows in non-contiguous order (RC2).
            let mut error_by_row: std::collections::HashMap<usize, &str> =
                std::collections::HashMap::new();
            for meta in &result.rejected {
                error_by_row.insert(meta.batch_row_index, meta.rule_violated.as_str());
            }
            // Rejected rows appear in original row order because filter_record_batch
            // preserves order; walk the original indices to assign errors.
            let mut rejected_row_cursor = 0usize;
            let mut error_strings: Vec<Option<&str>> = vec![None; rejected_batch.num_rows()];
            for orig_row in 0..batch.num_rows() {
                if !result.accepted_indices.contains(&orig_row) {
                    if rejected_row_cursor < error_strings.len() {
                        error_strings[rejected_row_cursor] = error_by_row.get(&orig_row).copied();
                    }
                    rejected_row_cursor += 1;
                }
            }
            let error_col: StringArray = error_strings.into_iter().collect();

            let mut new_fields: Vec<Field> = rejected_batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            new_fields.push(Field::new("_error", DataType::Utf8, true));
            let new_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(new_fields));
            let mut new_cols: Vec<std::sync::Arc<dyn arrow::array::Array>> =
                rejected_batch.columns().to_vec();
            new_cols.push(std::sync::Arc::new(error_col));
            let dlq_batch = arrow::record_batch::RecordBatch::try_new(new_schema, new_cols)
                .map_err(|e| ConnectorError::IoStr {
                    message: format!("failed to build dead-letter batch: {e}"),
                })?;

            secondary.write_batch_dyn(dlq_batch).await?;
        }

        Ok((accepted, result.rejected))
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    // -----------------------------------------------------------------------
    // ConnectorCapabilities builder
    // -----------------------------------------------------------------------

    #[test]
    fn connector_capabilities_builder_sets_flags() {
        let caps = ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable()
            .with_idempotent();

        assert!(caps.is_bounded());
        assert!(caps.is_rewindable());
        assert!(caps.is_idempotent());
        assert!(!caps.is_unbounded());
        assert!(!caps.is_transactional());
        assert!(caps.has_any());
    }

    #[test]
    fn connector_capabilities_default_all_false() {
        let caps = ConnectorCapabilities::new();
        assert!(!caps.has_any());
    }

    #[test]
    fn connector_capabilities_checkpoint_flag() {
        let caps = ConnectorCapabilities::new().with_checkpoint();
        assert!(caps.is_checkpoint_capable());
        assert!(!caps.is_two_phase_commit_capable());
        assert!(caps.has_any());
    }

    #[test]
    fn connector_capabilities_two_phase_commit_flag() {
        let caps = ConnectorCapabilities::new().with_two_phase_commit();
        assert!(caps.is_two_phase_commit_capable());
        assert!(!caps.is_checkpoint_capable());
        assert!(caps.has_any());
    }

    // -----------------------------------------------------------------------
    // ConnectorConfig
    // -----------------------------------------------------------------------

    #[test]
    fn connector_config_required_returns_error_when_missing() {
        let config = ConnectorConfig::new("my-source", "parquet");
        let err = config.required("path").unwrap_err();
        match err {
            ConnectorError::Config { message } => {
                assert!(message.contains("path"), "expected 'path' in: {message}");
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }

    #[test]
    fn connector_config_required_returns_value_when_present() {
        let config = ConnectorConfig::new("my-source", "parquet")
            .with_property("path", "/data/file.parquet");
        let value = config.required("path").unwrap();
        assert_eq!(value, "/data/file.parquet");
    }

    // -----------------------------------------------------------------------
    // CertificationSuite: source with no capabilities
    // -----------------------------------------------------------------------

    struct NullSource;

    impl Source for NullSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new() // all false
        }

        async fn read_batch(
            &mut self,
        ) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
            Ok(None)
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            None
        }
    }

    #[test]
    fn certification_suite_rejects_source_with_no_capabilities() {
        let source = NullSource;
        let result = CertificationSuite::run_source_capabilities_test(&source);
        assert!(result.is_err());
        match result.unwrap_err() {
            ConnectorError::Unsupported { .. } => {}
            other => panic!("expected Unsupported, got: {other}"),
        }
    }

    // -----------------------------------------------------------------------
    // CertificationSuite: bounded exhaustion test
    // -----------------------------------------------------------------------

    struct ThreeBatchSource {
        count: usize,
    }

    impl Source for ThreeBatchSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new().with_bounded()
        }

        async fn read_batch(
            &mut self,
        ) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
            if self.count < 3 {
                self.count += 1;
                Ok(Some(arrow::record_batch::RecordBatch::new_empty(
                    std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                )))
            } else {
                Ok(None)
            }
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            None
        }
    }

    struct UnboundedSource;

    impl Source for UnboundedSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new().with_unbounded()
        }

        async fn read_batch(
            &mut self,
        ) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
            Ok(None)
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            None
        }
    }

    #[tokio::test]
    async fn certification_exhaustion_test_passes_for_bounded_source() {
        let mut source = ThreeBatchSource { count: 0 };
        let result = CertificationSuite::run_bounded_exhaustion_test(&mut source).await;
        assert!(result.is_ok(), "bounded source exhaustion test should pass");
    }

    // -----------------------------------------------------------------------
    // ParquetOffset + AtLeastOnceSinkContract + CertificationSuite offset test
    // -----------------------------------------------------------------------

    #[test]
    fn parquet_offset_encode_decode_roundtrip() {
        let original = ParquetOffset { batch_index: 42 };
        let encoded = original.encode();
        let decoded = ParquetOffset::decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn at_least_once_contract_exists() {
        let _ = AtLeastOnceSinkContract;
    }

    #[test]
    fn certification_offset_round_trip_passes_for_parquet_offset() {
        let offset = ParquetOffset { batch_index: 7 };
        CertificationSuite::run_offset_round_trip_test(offset).unwrap();
    }

    #[tokio::test]
    async fn certification_exhaustion_test_rejects_unbounded_source() {
        let mut source = UnboundedSource;
        let err = CertificationSuite::run_bounded_exhaustion_test(&mut source)
            .await
            .unwrap_err();
        match err {
            ConnectorError::Unsupported { .. } => {}
            other => panic!("expected Unsupported, got: {other}"),
        }
    }

    // -----------------------------------------------------------------------
    // PostWriteOffsetCommitProtocol
    // -----------------------------------------------------------------------

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestOffset(u64);

    impl Offset for TestOffset {
        fn encode(&self) -> Vec<u8> {
            self.0.to_be_bytes().to_vec()
        }

        fn decode(bytes: &[u8]) -> ConnectorResult<Self> {
            if bytes.len() != 8 {
                return Err(ConnectorError::Config {
                    message: format!("expected 8 offset bytes, got {}", bytes.len()),
                });
            }
            let mut value = [0u8; 8];
            value.copy_from_slice(bytes);
            Ok(Self(u64::from_be_bytes(value)))
        }
    }

    #[derive(Default)]
    struct RecordingCommitter {
        committed: Vec<TestOffset>,
    }

    impl OffsetCommitter<TestOffset> for RecordingCommitter {
        async fn commit_offset(&mut self, offset: TestOffset) -> ConnectorResult<()> {
            self.committed.push(offset);
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        events: Vec<&'static str>,
        fail_write: bool,
        fail_flush: bool,
    }

    impl Sink for RecordingSink {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new().with_idempotent()
        }

        async fn write_batch(
            &mut self,
            _batch: arrow::record_batch::RecordBatch,
        ) -> ConnectorResult<()> {
            self.events.push("write");
            if self.fail_write {
                return Err(ConnectorError::IoStr {
                    message: "injected write failure".into(),
                });
            }
            Ok(())
        }

        async fn flush(&mut self) -> ConnectorResult<()> {
            self.events.push("flush");
            if self.fail_flush {
                return Err(ConnectorError::IoStr {
                    message: "injected flush failure".into(),
                });
            }
            Ok(())
        }
    }

    fn one_row_batch() -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap()
    }

    #[tokio::test]
    async fn post_write_protocol_commits_after_write_and_flush() {
        let mut sink = RecordingSink::default();
        let mut committer = RecordingCommitter::default();

        PostWriteOffsetCommitProtocol::write_flush_commit(
            &mut sink,
            &mut committer,
            one_row_batch(),
            TestOffset(9),
        )
        .await
        .unwrap();

        assert_eq!(sink.events, vec!["write", "flush"]);
        assert_eq!(committer.committed, vec![TestOffset(9)]);
    }

    #[tokio::test]
    async fn post_write_protocol_does_not_commit_when_write_fails() {
        let mut sink = RecordingSink {
            fail_write: true,
            ..RecordingSink::default()
        };
        let mut committer = RecordingCommitter::default();

        let err = PostWriteOffsetCommitProtocol::write_flush_commit(
            &mut sink,
            &mut committer,
            one_row_batch(),
            TestOffset(9),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ConnectorError::IoStr { .. }));
        assert_eq!(sink.events, vec!["write"]);
        assert!(committer.committed.is_empty());
    }

    #[tokio::test]
    async fn post_write_protocol_does_not_commit_when_flush_fails() {
        let mut sink = RecordingSink {
            fail_flush: true,
            ..RecordingSink::default()
        };
        let mut committer = RecordingCommitter::default();

        let err = PostWriteOffsetCommitProtocol::write_flush_commit(
            &mut sink,
            &mut committer,
            one_row_batch(),
            TestOffset(9),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ConnectorError::IoStr { .. }));
        assert_eq!(sink.events, vec!["write", "flush"]);
        assert!(committer.committed.is_empty());
    }

    // -----------------------------------------------------------------------
    // TwoPhaseCommitSink
    // -----------------------------------------------------------------------

    #[test]
    fn two_phase_commit_sink_prepare_commit_roundtrip() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))])
                .unwrap();

        let mut sink = InMemoryTwoPhaseCommitSink::new();
        let handle = sink.prepare(1, &batch).unwrap();
        assert_eq!(sink.staged_count(), 1);
        assert_eq!(sink.committed().len(), 0);
        sink.commit(handle).unwrap();
        assert_eq!(sink.staged_count(), 0);
        assert_eq!(sink.committed().len(), 1);
        assert_eq!(sink.committed()[0].0, 1); // epoch
    }

    #[test]
    fn two_phase_commit_sink_abort_discards() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42i64]))]).unwrap();

        let mut sink = InMemoryTwoPhaseCommitSink::new();
        let handle = sink.prepare(2, &batch).unwrap();
        sink.abort(handle).unwrap();
        assert_eq!(sink.staged_count(), 0);
        assert_eq!(sink.committed().len(), 0);
    }

    #[test]
    fn two_phase_commit_sink_multiple_epochs() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let make_batch = |v: i64| -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![v]))],
            )
            .unwrap()
        };

        let mut sink = InMemoryTwoPhaseCommitSink::new();
        let h1 = sink.prepare(1, &make_batch(10)).unwrap();
        let h2 = sink.prepare(2, &make_batch(20)).unwrap();
        sink.commit(h1).unwrap();
        sink.abort(h2).unwrap();
        assert_eq!(sink.committed().len(), 1);
        assert_eq!(sink.committed()[0].0, 1);
    }

    // ── LocalParquetTwoPhaseCommitSink ────────────────────────────────────────

    fn make_int32_batch(values: Vec<i32>) -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(values)) as _],
        )
        .unwrap()
    }

    #[test]
    fn parquet_2pc_prepare_commit_creates_final_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());

        let batch = make_int32_batch(vec![1, 2, 3]);
        let handle = sink.prepare(1, &batch).unwrap();

        assert!(
            handle.staging_path.exists(),
            "staging .tmp file must exist after prepare"
        );
        assert!(
            !handle.final_path.exists(),
            "final .parquet file must not exist before commit"
        );

        sink.commit(handle).unwrap();

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(
            files
                .iter()
                .any(|f| f.ends_with(".parquet") && !f.ends_with(".tmp")),
            "final .parquet file must exist after commit"
        );
        assert!(
            !files.iter().any(|f| f.ends_with(".tmp")),
            "staging .tmp file must be gone after commit"
        );
    }

    #[test]
    fn parquet_2pc_abort_deletes_staging_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());

        let batch = make_int32_batch(vec![10, 20]);
        let handle = sink.prepare(2, &batch).unwrap();
        assert!(handle.staging_path.exists(), "staging file must exist");

        sink.abort(handle).unwrap();

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(files.is_empty(), "abort must remove staging file");
    }

    #[test]
    fn parquet_2pc_abort_is_idempotent_when_file_missing() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());

        let batch = make_int32_batch(vec![1]);
        let handle = sink.prepare(3, &batch).unwrap();
        // Remove the staging file manually before calling abort.
        std::fs::remove_file(&handle.staging_path).unwrap();

        // abort on a missing file must not error.
        sink.abort(handle).unwrap();
    }

    #[test]
    fn parquet_2pc_restart_does_not_overwrite_existing_commit() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_int32_batch(vec![1]);
        let first_final = {
            let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());
            let handle = sink.prepare(7, &batch).unwrap();
            let final_path = handle.final_path.clone();
            sink.commit(handle).unwrap();
            final_path
        };

        let mut restarted = LocalParquetTwoPhaseCommitSink::new(dir.path());
        let handle = restarted.prepare(7, &make_int32_batch(vec![2])).unwrap();
        assert_ne!(handle.final_path, first_final);
        restarted.commit(handle).unwrap();

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
            .collect();
        assert_eq!(files.len(), 2, "restart must allocate a fresh final path");
    }
}

#[cfg(test)]
mod quality_tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("score", DataType::Float64, true),
            Field::new("name", DataType::Utf8, false),
        ]));
        let score = Float64Array::from(vec![Some(85.0), None, Some(110.0), Some(50.0)]);
        let name = StringArray::from(vec!["alice", "bob", "carol", "dave"]);
        RecordBatch::try_new(schema, vec![Arc::new(score), Arc::new(name)]).unwrap()
    }

    #[test]
    fn notnull_rejects_null_rows() {
        let batch = make_batch();
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::NotNull {
                column: "score".into(),
            },
            QualityAction::Reject,
        );
        let result = check_batch(&batch, &config).unwrap();
        assert_eq!(result.rejected.len(), 1);
        assert_eq!(result.rejected[0].batch_row_index, 1);
        assert!(!result.failed);
    }

    #[test]
    fn range_rejects_out_of_range_rows() {
        let batch = make_batch();
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::Range {
                column: "score".into(),
                min: 0.0,
                max: 100.0,
            },
            QualityAction::Reject,
        );
        let result = check_batch(&batch, &config).unwrap();
        // row 1 (null) and row 2 (110.0 > 100) should be rejected
        assert_eq!(result.rejected.len(), 2);
    }

    #[test]
    fn fail_action_sets_failed_flag() {
        let batch = make_batch();
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::NotNull {
                column: "score".into(),
            },
            QualityAction::Fail,
        );
        let result = check_batch(&batch, &config).unwrap();
        assert!(result.failed);
    }

    #[tokio::test]
    async fn dead_letter_sink_splits_accepted_and_rejected() {
        let batch = make_batch();
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::NotNull {
                column: "score".into(),
            },
            QualityAction::Reject,
        );
        let mut sink = DeadLetterSink::new("test_sink", config);
        let (accepted, rejected) = sink.process_batch(&batch).await.unwrap();
        assert_eq!(accepted.num_rows(), 3); // rows 0, 2, 3
        assert_eq!(rejected.len(), 1); // row 1 (null score)
    }

    #[test]
    fn regex_rule_rejects_non_matching_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("email", DataType::Utf8, true)]));
        let emails = StringArray::from(vec![
            Some("alice@example.com"),
            Some("not-an-email"),
            None,
            Some("bob@corp.org"),
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(emails)]).unwrap();

        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::Regex {
                column: "email".into(),
                pattern: r"^[^@]+@[^@]+\.[^@]+$".into(),
            },
            QualityAction::Reject,
        );
        let result = check_batch(&batch, &config).unwrap();
        // "not-an-email" (idx 1) and None (idx 2) should be rejected
        assert_eq!(result.rejected.len(), 2);
        let rejected_indices: Vec<usize> =
            result.rejected.iter().map(|r| r.batch_row_index).collect();
        assert!(rejected_indices.contains(&1));
        assert!(rejected_indices.contains(&2));
    }

    #[test]
    fn regex_rule_invalid_pattern_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let col = StringArray::from(vec!["hello"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::Regex {
                column: "v".into(),
                pattern: "[invalid((".into(),
            },
            QualityAction::Reject,
        );
        assert!(check_batch(&batch, &config).is_err());
    }

    #[test]
    fn parquet_2pc_quality_check_rejects_null_rows() {
        use arrow::array::Float64Array;

        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        // Row 0: 1.0, Row 1: null — null should be rejected by NotNull rule
        let col = Float64Array::from(vec![Some(1.0), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();

        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::NotNull { column: "v".into() },
            QualityAction::Reject,
        );
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path()).with_quality_config(config);

        let handle = sink.prepare(1, &batch).unwrap();
        sink.commit(handle).unwrap();

        // Read back the written parquet file and verify only 1 row was written
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().map_or(false, |e| e == "parquet"))
            .collect();
        assert_eq!(files.len(), 1, "exactly one .parquet file should exist");

        // Use the parquet module in this crate (which re-exports ParquetSource)
        // to read back and count rows.
        use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let file = std::fs::File::open(&files[0]).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let total_rows: usize = reader
            .map(|b: Result<RecordBatch, _>| b.unwrap().num_rows())
            .sum();
        assert_eq!(total_rows, 1, "only the non-null row should be written");
    }

    #[test]
    fn parquet_2pc_quality_fail_action_aborts_prepare() {
        use arrow::array::Float64Array;

        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let col = Float64Array::from(vec![None::<f64>]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();

        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::NotNull { column: "v".into() },
            QualityAction::Fail,
        );
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path()).with_quality_config(config);

        let result = sink.prepare(1, &batch);
        assert!(result.is_err(), "Fail action must abort prepare");
    }
}
