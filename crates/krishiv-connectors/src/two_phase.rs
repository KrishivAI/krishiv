//! Two-phase commit trait, in-memory test impl, local Parquet impl, and epoch transaction log.

use crate::capabilities::ConnectorCapabilities;
use crate::error::{ConnectorError, ConnectorResult};
use crate::quality::{CompiledDataQualityConfig, DataQualityConfig, check_batch_compiled};

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
/// Coordinator delivery can be retried after an uncertain response. Therefore
/// repeated `commit` and repeated `abort` calls for the same cloned handle must
/// be idempotent and must return typed errors rather than panic. A conflicting
/// decision after the opposite outcome must never reverse visible data.
///
/// The certified R6 sink is `S3/Parquet` (object-level atomic rename).
/// `InMemoryTwoPhaseCommitSink` is provided for deterministic testing.
pub trait TwoPhaseCommitSink: Send {
    /// Opaque handle returned by `prepare`.
    type Handle: Clone + Send;

    /// Return the capabilities implemented by this sink.
    fn capabilities(&self) -> ConnectorCapabilities;

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

/// **Testing only**: In-memory implementation for unit tests. Not for production use.
///
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

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_two_phase_commit()
    }

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
    /// Pre-compiled quality config so regex compilation happens once at
    /// construction, not once per `prepare()` call.
    quality_config: Option<CompiledDataQualityConfig>,
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
    ///
    /// The config is compiled immediately (regex patterns pre-compiled) so
    /// repeated `prepare()` calls do not pay regex-compilation overhead.
    pub fn with_quality_config(mut self, config: DataQualityConfig) -> ConnectorResult<Self> {
        self.quality_config = Some(config.compile()?);
        Ok(self)
    }

    /// Test-only: seed `next_handle` directly so overflow behaviour can be
    /// exercised without iterating `prepare()` up to `u64::MAX` times.
    #[cfg(test)]
    pub(crate) fn with_next_handle_for_test(mut self, next_handle: u64) -> Self {
        self.next_handle = next_handle;
        self
    }
}

impl TwoPhaseCommitSink for LocalParquetTwoPhaseCommitSink {
    type Handle = ParquetCommitHandle;

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_two_phase_commit()
    }

    fn prepare(
        &mut self,
        epoch: u64,
        batch: &arrow::record_batch::RecordBatch,
    ) -> ConnectorResult<Self::Handle> {
        // Run quality checks if a config is attached.
        let filtered: arrow::record_batch::RecordBatch;
        let batch = if let Some(ref qc) = self.quality_config {
            use arrow::array::BooleanArray;
            let result = check_batch_compiled(batch, qc)?;
            if result.failed {
                return Err(ConnectorError::Quality {
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
                    ConnectorError::Schema {
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
            self.next_handle = self.next_handle.checked_add(1).ok_or_else(|| {
                ConnectorError::Parquet("parquet 2pc prepare: handle ID overflow".into())
            })?;
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
                    return Err(ConnectorError::Parquet(format!(
                        "parquet 2pc prepare: cannot create {staging_name}: {e}"
                    )));
                }
            }
        };

        let mut writer = ::parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
            .map_err(|e| {
                ConnectorError::Parquet(format!("parquet 2pc prepare: cannot create writer: {e}"))
            })?;
        writer.write(batch).map_err(|e| {
            ConnectorError::Parquet(format!("parquet 2pc prepare: write error: {e}"))
        })?;
        writer.close().map_err(|e| {
            ConnectorError::Parquet(format!("parquet 2pc prepare: close error: {e}"))
        })?;

        Ok(ParquetCommitHandle {
            epoch,
            staging_path,
            final_path,
        })
    }

    fn commit(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        // `rename` is atomic on POSIX: the final path becomes visible only when
        // the rename succeeds.  If the staging file is missing and the final path
        // already exists, the commit was completed by a prior attempt (idempotent).
        use std::io::ErrorKind;
        match std::fs::rename(&handle.staging_path, &handle.final_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                // Staging file is gone — either already committed (final exists)
                // or an unexpected race.  Accept if the final target exists.
                if handle.final_path.exists() {
                    Ok(())
                } else {
                    Err(ConnectorError::Io(std::io::Error::new(
                        e.kind(),
                        format!(
                            "parquet 2pc commit: rename {:?} to {:?}: staging missing and final absent: {e}",
                            handle.staging_path, handle.final_path
                        ),
                    )))
                }
            }
            Err(e) => Err(ConnectorError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "parquet 2pc commit: rename {:?} to {:?}: {e}",
                    handle.staging_path, handle.final_path
                ),
            ))),
        }
    }

    fn abort(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        use std::io::ErrorKind;
        match std::fs::remove_file(&handle.staging_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ConnectorError::Io(std::io::Error::new(
                e.kind(),
                format!("parquet 2pc abort: remove {:?}: {e}", handle.staging_path),
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Checkpoint-aligned transaction log
// ---------------------------------------------------------------------------

/// Dyn-safe participant in checkpoint-aligned two-phase commit.
///
/// Lifecycle (mirrors Flink's `TwoPhaseCommitSinkFunction`):
///
/// 1. [`stage`] — output produced between barriers accumulates in the open
///    transaction buffer.
/// 2. [`pre_commit`]`(epoch)` — at the checkpoint barrier, the open buffer is
///    durably staged under `epoch` (e.g. written as `.tmp` Parquet files).
///    Must complete *before* the checkpoint ack so a committed checkpoint
///    always has its sink output staged.
/// 3. [`commit_through`]`(epoch)` — when the coordinator notifies that
///    `epoch` is durably committed, every prepared transaction at or before
///    `epoch` becomes visible.  Completion notifications are best-effort: a
///    missed notification is repaired by the next one (commit-through covers
///    earlier epochs) or by restore (recover-and-commit).
/// 4. [`abort_after`]`(epoch)` — on restore to `epoch`, prepared transactions
///    after it are rolled back; prepared transactions at or before it must be
///    committed first (their data is covered by the restored checkpoint and
///    sources resume past it).
pub trait TransactionalSinkParticipant: Send {
    /// Stage a batch into the open (pre-barrier) transaction buffer.
    fn stage(&mut self, batch: &arrow::record_batch::RecordBatch) -> ConnectorResult<()>;

    /// Durably prepare the open buffer under `epoch`.
    ///
    /// `epoch` must be greater than every previously prepared epoch.
    fn pre_commit(&mut self, epoch: u64) -> ConnectorResult<()>;

    /// Commit every prepared transaction with epoch ≤ `epoch`.
    /// Returns the number of committed staged writes.
    fn commit_through(&mut self, epoch: u64) -> ConnectorResult<usize>;

    /// Abort every prepared transaction with epoch > `epoch` and discard the
    /// open buffer (its data will be re-delivered by the rewound source).
    /// Returns the number of aborted staged writes.
    fn abort_after(&mut self, epoch: u64) -> ConnectorResult<usize>;

    /// Rows currently accumulated in the open transaction buffer.
    fn open_rows(&self) -> usize;

    /// Epochs with prepared-but-uncommitted transactions, ascending.
    fn prepared_epochs(&self) -> Vec<u64>;
}

/// Checkpoint-aligned transaction log over any [`TwoPhaseCommitSink`].
///
/// Buffers staged batches in memory between barriers; the buffer is bounded by
/// the checkpoint interval (back-to-back barriers flush it).  `pre_commit`
/// converts the buffer into durable staged writes via [`TwoPhaseCommitSink::prepare`].
pub struct EpochTransactionLog<S: TwoPhaseCommitSink> {
    sink: S,
    open: Vec<arrow::record_batch::RecordBatch>,
    prepared: std::collections::BTreeMap<u64, Vec<S::Handle>>,
}

impl<S: TwoPhaseCommitSink> EpochTransactionLog<S> {
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            open: Vec::new(),
            prepared: std::collections::BTreeMap::new(),
        }
    }

    /// Borrow the underlying sink (testing/inspection).
    pub fn sink(&self) -> &S {
        &self.sink
    }
}

impl<S: TwoPhaseCommitSink> TransactionalSinkParticipant for EpochTransactionLog<S> {
    fn stage(&mut self, batch: &arrow::record_batch::RecordBatch) -> ConnectorResult<()> {
        if batch.num_rows() > 0 {
            self.open.push(batch.clone());
        }
        Ok(())
    }

    fn pre_commit(&mut self, epoch: u64) -> ConnectorResult<()> {
        if let Some((&max_prepared, _)) = self.prepared.iter().next_back()
            && epoch <= max_prepared
        {
            return Err(ConnectorError::Protocol {
                message: format!(
                    "pre_commit epoch {epoch} is not greater than already prepared epoch \
                     {max_prepared}; checkpoint epochs must be monotonic"
                ),
            });
        }
        if self.open.is_empty() {
            return Ok(());
        }
        let mut handles = Vec::with_capacity(self.open.len());
        for batch in &self.open {
            handles.push(self.sink.prepare(epoch, batch)?);
        }
        // Only clear the open buffer after every prepare succeeded so a
        // failed pre_commit can be retried for a later epoch without loss.
        self.open.clear();
        self.prepared.insert(epoch, handles);
        Ok(())
    }

    fn commit_through(&mut self, epoch: u64) -> ConnectorResult<usize> {
        let mut later = self.prepared.split_off(&(epoch.saturating_add(1)));
        let to_commit = std::mem::take(&mut self.prepared);
        self.prepared.append(&mut later);

        let mut committed = 0usize;
        for (txn_epoch, handles) in to_commit {
            let mut remaining = handles.into_iter();
            for handle in remaining.by_ref() {
                if let Err(error) = self.sink.commit(handle.clone()) {
                    // Re-queue the failed handle and the rest for retry —
                    // commit is idempotent, so retrying is safe.
                    let mut requeue = vec![handle];
                    requeue.extend(remaining);
                    self.prepared.insert(txn_epoch, requeue);
                    return Err(error);
                }
                committed += 1;
            }
        }
        Ok(committed)
    }

    fn abort_after(&mut self, epoch: u64) -> ConnectorResult<usize> {
        self.open.clear();
        let to_abort = self.prepared.split_off(&(epoch.saturating_add(1)));
        let mut aborted = 0usize;
        for (txn_epoch, handles) in to_abort {
            let mut remaining = handles.into_iter();
            for handle in remaining.by_ref() {
                if let Err(error) = self.sink.abort(handle.clone()) {
                    let mut requeue = vec![handle];
                    requeue.extend(remaining);
                    self.prepared.insert(txn_epoch, requeue);
                    return Err(error);
                }
                aborted += 1;
            }
        }
        Ok(aborted)
    }

    fn open_rows(&self) -> usize {
        self.open.iter().map(|b| b.num_rows()).sum()
    }

    fn prepared_epochs(&self) -> Vec<u64> {
        self.prepared.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap()
    }

    // ── EpochTransactionLog lifecycle ───────────────────────────────────────

    #[test]
    fn transaction_log_stage_precommit_commit_lifecycle() {
        let mut log = EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new());
        log.stage(&make_batch()).unwrap();
        log.stage(&make_batch()).unwrap();
        assert_eq!(log.open_rows(), 4);

        // Barrier for epoch 1: open buffer becomes a prepared transaction.
        log.pre_commit(1).unwrap();
        assert_eq!(log.open_rows(), 0);
        assert_eq!(log.prepared_epochs(), vec![1]);
        assert_eq!(log.sink().staged_count(), 2);
        assert!(log.sink().committed().is_empty());

        // More data, barrier for epoch 2.
        log.stage(&make_batch()).unwrap();
        log.pre_commit(2).unwrap();
        assert_eq!(log.prepared_epochs(), vec![1, 2]);

        // Coordinator committed epoch 1: only epoch-1 output becomes visible.
        assert_eq!(log.commit_through(1).unwrap(), 2);
        assert_eq!(log.prepared_epochs(), vec![2]);
        assert_eq!(log.sink().committed().len(), 2);
        assert!(log.sink().committed().iter().all(|(e, _)| *e == 1));

        // Completion for epoch 2 covers everything remaining.
        assert_eq!(log.commit_through(2).unwrap(), 1);
        assert!(log.prepared_epochs().is_empty());
        assert_eq!(log.sink().committed().len(), 3);
    }

    #[test]
    fn transaction_log_restore_commits_covered_and_aborts_later_epochs() {
        let mut log = EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new());
        log.stage(&make_batch()).unwrap();
        log.pre_commit(3).unwrap();
        log.stage(&make_batch()).unwrap();
        log.pre_commit(4).unwrap();
        log.stage(&make_batch()).unwrap(); // open, never prepared

        // Restore to epoch 3: recover-and-commit ≤ 3, recover-and-abort > 3,
        // and the open buffer is discarded (rewound source re-delivers it).
        assert_eq!(log.commit_through(3).unwrap(), 1);
        assert_eq!(log.abort_after(3).unwrap(), 1);
        assert_eq!(log.open_rows(), 0);
        assert!(log.prepared_epochs().is_empty());
        assert_eq!(log.sink().committed().len(), 1);
        assert_eq!(log.sink().staged_count(), 0);
    }

    #[test]
    fn transaction_log_rejects_non_monotonic_pre_commit() {
        let mut log = EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new());
        log.stage(&make_batch()).unwrap();
        log.pre_commit(5).unwrap();
        log.stage(&make_batch()).unwrap();
        let err = log.pre_commit(5).expect_err("same epoch must be rejected");
        assert!(matches!(err, ConnectorError::Protocol { .. }));
        let err = log.pre_commit(4).expect_err("older epoch must be rejected");
        assert!(matches!(err, ConnectorError::Protocol { .. }));
        // The open buffer survives a rejected pre_commit for a later barrier.
        assert_eq!(log.open_rows(), 2);
        log.pre_commit(6).unwrap();
        assert_eq!(log.prepared_epochs(), vec![5, 6]);
    }

    #[test]
    fn transaction_log_empty_barrier_prepares_nothing() {
        let mut log = EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new());
        log.pre_commit(1).unwrap();
        assert!(log.prepared_epochs().is_empty());
        assert_eq!(log.commit_through(1).unwrap(), 0);
    }

    #[test]
    fn transaction_log_parquet_sink_files_visible_only_after_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = EpochTransactionLog::new(LocalParquetTwoPhaseCommitSink::new(dir.path()));
        log.stage(&make_batch()).unwrap();
        log.pre_commit(1).unwrap();

        let staged: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(staged.len(), 1);
        assert!(
            staged[0].ends_with(".parquet.tmp"),
            "pre-commit output must be staged, found {staged:?}"
        );

        log.commit_through(1).unwrap();
        let committed: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(committed.len(), 1);
        assert!(
            committed[0].ends_with(".parquet"),
            "committed output must be final, found {committed:?}"
        );
    }

    /// Regression: `prepare()` must report an error when `next_handle` would
    /// overflow `u64`, rather than wrapping back to handle 0 and risking a
    /// collision with an already-committed file (Phase 1 fix for unchecked
    /// `+= 1` accumulation).
    #[test]
    fn prepare_reports_error_on_handle_id_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink =
            LocalParquetTwoPhaseCommitSink::new(dir.path()).with_next_handle_for_test(u64::MAX);
        let batch = make_batch();

        let result = sink.prepare(0, &batch);
        match result {
            Err(ConnectorError::Parquet(message)) => {
                assert!(
                    message.contains("overflow"),
                    "expected an overflow error, got: {message}"
                );
            }
            other => panic!("expected ConnectorError::Parquet overflow error, got {other:?}"),
        }
    }
}
