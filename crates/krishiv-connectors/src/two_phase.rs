//! Two-phase commit.

use crate::error::{ConnectorError, ConnectorResult};
use crate::quality::{DataQualityConfig, check_batch};

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
            Ok(()) => {
                std::fs::remove_file(&handle.staging_path).map_err(|e| ConnectorError::IoStr {
                    message: format!(
                        "parquet 2pc commit: remove staging {:?}: {e}",
                        handle.staging_path
                    ),
                })
            }
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
