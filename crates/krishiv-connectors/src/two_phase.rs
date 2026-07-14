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

    /// DUR-2 recovery: finalize a transaction that was prepared before a crash,
    /// reconstructing it from its durable `prepare_path` because the in-memory
    /// [`Self::Handle`] does not survive an executor restart. `commit` replays
    /// the two-phase second phase (make the staged output visible); `!commit`
    /// discards the staged output. Must be idempotent — recovery may re-run.
    ///
    /// The default rejects recovery: a sink that does not persist enough at
    /// `prepare` time to reconstruct the transaction cannot provide exactly-once
    /// output across a restart and must not be used under a durable profile.
    fn finalize_prepared(&mut self, prepare_path: &str, commit: bool) -> ConnectorResult<()> {
        let _ = commit;
        Err(ConnectorError::Unsupported {
            message: format!(
                "sink does not support prepared-transaction recovery (path {prepare_path}); \
                 it cannot deliver exactly-once output across a restart under a durable profile"
            ),
        })
    }

    /// The durable path that identifies a prepared `handle` for recovery — the
    /// value later passed to [`Self::finalize_prepared`]. Reported in the
    /// checkpoint ack (DUR-2) so the coordinator can persist it and drive
    /// commit-or-abort after a crash.
    ///
    /// Must be globally unique per prepared transaction (it is the recovery
    /// dedup identity). The default returns `None`: a sink with no durable
    /// staging cannot be recovered and reports nothing, so its transactions
    /// never enter the durable recovery plan.
    fn prepare_path_of(&self, handle: &Self::Handle) -> Option<String> {
        let _ = handle;
        None
    }
}

/// A prepared-but-unfinalized sink transaction, reported by a participant at
/// checkpoint time (DUR-2). `prepare_path` is the durable identity used to
/// reconstruct the transaction during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSinkRef {
    /// Checkpoint epoch under which the transaction was staged.
    pub epoch: u64,
    /// Durable staging path — recovery finalizes the transaction from this.
    pub prepare_path: String,
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

    /// DUR-2 recovery: the staging file written at `prepare` is durable on disk,
    /// so a prepared transaction is fully reconstructable from its staging path
    /// after a crash. The final target is the staging path with the `.tmp`
    /// suffix removed (see `prepare`), so `commit`/`abort` — both idempotent —
    /// can finalize it without the original in-memory handle.
    fn finalize_prepared(&mut self, prepare_path: &str, commit: bool) -> ConnectorResult<()> {
        let final_str = prepare_path.strip_suffix(".tmp").ok_or_else(|| {
            ConnectorError::Parquet(format!(
                "parquet 2pc recovery: staging path {prepare_path} does not end in .tmp"
            ))
        })?;
        let handle = ParquetCommitHandle {
            epoch: 0,
            staging_path: std::path::PathBuf::from(prepare_path),
            final_path: std::path::PathBuf::from(final_str),
        };
        if commit {
            self.commit(handle)
        } else {
            self.abort(handle)
        }
    }

    /// The durable `.tmp` staging path is the recovery identity — it is what
    /// `finalize_prepared` reconstructs the transaction from. Globally unique
    /// because `prepare` names each staging file `<epoch>-<handle_id>.parquet.tmp`
    /// with a monotonic `handle_id`.
    fn prepare_path_of(&self, handle: &Self::Handle) -> Option<String> {
        Some(handle.staging_path.to_string_lossy().into_owned())
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

    /// Record the latest source offsets observed for the open buffer.
    ///
    /// Sinks that can bind source progress to committed output (e.g. Iceberg
    /// snapshot summary properties) persist these at commit so recovery can
    /// resume the source from the last visible write. Default: ignored.
    fn stage_source_offsets(
        &mut self,
        _offsets: &std::collections::BTreeMap<String, i64>,
    ) -> ConnectorResult<()> {
        Ok(())
    }

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

    /// DUR-2 reporting: the prepared-but-unfinalized transactions to record in
    /// the checkpoint ack, each with the durable `prepare_path` recovery will
    /// reconstruct it from. Default: none (a sink with no durable staging
    /// reports nothing and so never enters the recovery plan).
    fn prepared_refs(&self) -> Vec<PreparedSinkRef> {
        Vec::new()
    }

    /// DUR-2 recovery: finalize a transaction reconstructed from its durable
    /// `prepare_path` when the in-memory prepared handles were lost to an
    /// executor crash. `commit` replays the two-phase second phase; `!commit`
    /// rolls it back. Idempotent. Default: unsupported (the underlying sink
    /// cannot reconstruct from durable state).
    fn finalize_prepared(&mut self, prepare_path: &str, commit: bool) -> ConnectorResult<()> {
        let _ = (prepare_path, commit);
        Err(ConnectorError::Unsupported {
            message: String::from(
                "participant does not support prepared-transaction recovery from a durable ref",
            ),
        })
    }
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

    fn prepared_refs(&self) -> Vec<PreparedSinkRef> {
        let mut refs = Vec::new();
        for (&epoch, handles) in &self.prepared {
            for handle in handles {
                if let Some(prepare_path) = self.sink.prepare_path_of(handle) {
                    refs.push(PreparedSinkRef {
                        epoch,
                        prepare_path,
                    });
                }
            }
        }
        refs
    }

    fn finalize_prepared(&mut self, prepare_path: &str, commit: bool) -> ConnectorResult<()> {
        // Delegate to the underlying sink, which reconstructs the prepared
        // transaction from its durable path (DUR-2 recovery after an executor
        // crash lost the in-memory `prepared` handles).
        self.sink.finalize_prepared(prepare_path, commit)
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

    /// DUR-2: after a crash the in-memory handle is gone but the durable
    /// staging file survives, so `finalize_prepared` reconstructs the txn from
    /// its staging path and commits (publish) or aborts (delete) idempotently.
    #[test]
    fn parquet_finalize_prepared_recovers_across_crash() {
        let dir = tempfile::tempdir().unwrap();

        // Prepare, capture the staging path, then DROP the sink (simulated crash).
        let staging_commit = {
            let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());
            let h = sink.prepare(7, &make_batch()).unwrap();
            h.staging_path.to_string_lossy().into_owned()
        };
        assert!(
            std::path::Path::new(&staging_commit).exists(),
            "staging file must survive the crash"
        );
        let final_commit = staging_commit.strip_suffix(".tmp").unwrap().to_string();

        // A fresh sink (post-restart) recovers the prepared txn by commit.
        let mut recovered = LocalParquetTwoPhaseCommitSink::new(dir.path());
        recovered.finalize_prepared(&staging_commit, true).unwrap();
        assert!(
            std::path::Path::new(&final_commit).exists(),
            "recovery-commit must publish the output"
        );
        assert!(!std::path::Path::new(&staging_commit).exists());
        // Idempotent: re-running recovery-commit is a no-op.
        recovered.finalize_prepared(&staging_commit, true).unwrap();

        // A separate prepared txn is aborted on recovery.
        let staging_abort = {
            let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());
            sink.prepare(8, &make_batch())
                .unwrap()
                .staging_path
                .to_string_lossy()
                .into_owned()
        };
        let final_abort = staging_abort.strip_suffix(".tmp").unwrap().to_string();
        recovered.finalize_prepared(&staging_abort, false).unwrap();
        assert!(!std::path::Path::new(&staging_abort).exists());
        assert!(
            !std::path::Path::new(&final_abort).exists(),
            "aborted txn must never be published"
        );
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
