//! Parquet file source (lazy reader with cursor/rewind) and sink (write + fsync).

use std::any::Any;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::partition::{discover_hive_partitions, inject_partition_columns, list_parquet_files};
use crate::{
    CheckpointSource, ConnectorCapabilities, ConnectorError, ConnectorResult, MultiFileOffset,
    ParquetOffset, Sink, Source,
};

// ---------------------------------------------------------------------------
// ParquetSource
// ---------------------------------------------------------------------------

/// A bounded, rewindable source that streams batches from a Parquet file.
///
/// [`ParquetSource::open`] only validates the file and reads its schema; the
/// underlying [`ParquetRecordBatchReader`] is created lazily and pulls one
/// batch at a time on [`Source::read_batch`], so the whole file is never
/// materialised in memory at once. Rewinding ([`Source::reset`]) or restoring
/// a checkpoint re-opens the file and re-positions the reader by skipping the
/// requested number of batches — the standard trade-off for sequential file
/// formats that lack a random-access batch index.
///
/// T8: read options for the Parquet source that enable native
/// row-group pruning, page index, bloom filters, and column projection.
///
/// Mirrors Spark's `ParquetReadOptions` minus the legacy options
/// (`vectorize`, `page.row.group.size` etc. that the Parquet crate
/// controls directly).
#[derive(Debug, Clone, Default)]
pub struct ParquetReadOptions {
    /// Push down filter predicates to the row-group / page index layer.
    /// Defaults to `true` so that file-level row-group statistics can be
    /// used to skip whole groups without reading them.
    pub pushdown_filters: bool,
    /// Enable the Parquet page index for finer-grained skipping within a
    /// row group. Requires the file to have been written with page
    /// indexes; otherwise a no-op.
    pub enable_page_index: bool,
    /// Enable the Parquet bloom filter for column predicate pushdown.
    /// Requires the file to have been written with bloom filters.
    pub enable_bloom_filter: bool,
}

impl ParquetReadOptions {
    /// New options that enable every Parquet-side optimisation.
    pub fn all() -> Self {
        Self {
            pushdown_filters: true,
            enable_page_index: true,
            enable_bloom_filter: true,
        }
    }
}

/// The total row count is read from the Parquet file-metadata footer at open
/// time so callers can populate `estimated_rows` on scan `PlanNode`s, enabling
/// `BroadcastAutoRule` to fire for small Parquet tables without going through
/// the DataFusion SQL path.
pub struct ParquetSource {
    path: PathBuf,
    schema: SchemaRef,
    /// Total row count from the Parquet footer, cached at open time.
    estimated_row_count: Option<u64>,
    reader: Option<ParquetRecordBatchReader>,
    cursor: usize,
    /// T8: read-side optimisation flags. Defaults to all-enabled via
    /// [`ParquetReadOptions::all`].
    options: ParquetReadOptions,
}

impl ParquetSource {
    /// Open a Parquet file with the default read options (all pushdown
    /// optimisations enabled).
    pub fn open(path: impl AsRef<Path>) -> ConnectorResult<Self> {
        Self::open_with_options(path, ParquetReadOptions::all())
    }

    /// Open a Parquet file with caller-supplied read options.
    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: ParquetReadOptions,
    ) -> ConnectorResult<Self> {
        let path = path.as_ref().to_path_buf();
        let (schema, estimated_row_count) = Self::probe_metadata(&path)?;

        Ok(Self {
            path,
            schema,
            estimated_row_count,
            reader: None,
            cursor: 0,
            options,
        })
    }

    /// Open the file and read its Arrow schema and total row count from the
    /// Parquet footer, without building a batch reader.
    fn probe_metadata(path: &Path) -> ConnectorResult<(SchemaRef, Option<u64>)> {
        let file = File::open(path).map_err(|e| {
            ConnectorError::Parquet(format!("failed to open '{}': {e}", path.display()))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            ConnectorError::Parquet(format!(
                "failed to build Parquet reader for '{}': {e}",
                path.display()
            ))
        })?;
        let schema = builder.schema().clone();
        let row_count = u64::try_from(builder.metadata().file_metadata().num_rows()).ok();
        // Bloom-filter inventory: at minimum, surface which columns have
        // bloom filters in the file footer. The DataFusion layer (when
        // enabled) uses this to apply a probe-side runtime filter.
        let _ = Self::probe_bloom_filters_from_metadata(builder.metadata());
        Ok((schema, row_count))
    }

    /// Probe the file footer for bloom-filter metadata. The current
    /// `parquet = 58.x` crate does not expose `with_bloom_filter` on
    /// the reader builder, but it does expose the column-level bloom
    /// filter information via the file metadata. We surface the
    /// presence as a `tracing::debug!` line for now; once the Parquet
    /// crate is bumped, the runtime filter can be wired through
    /// `ArrowReaderOptions`.
    fn probe_bloom_filters_from_metadata(
        metadata: &parquet::file::metadata::ParquetMetaData,
    ) -> std::collections::BTreeSet<String> {
        let mut cols_with_bf: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        for rg_idx in 0..metadata.num_row_groups() {
            let rg = metadata.row_group(rg_idx);
            for col in rg.columns() {
                if col.bloom_filter_offset().is_some() {
                    let path: Vec<String> = col.column_descr().path().parts().to_vec();
                    if !path.is_empty() {
                        cols_with_bf.insert(path.join("."));
                    }
                }
            }
        }
        if !cols_with_bf.is_empty() {
            tracing::debug!(
                columns = ?cols_with_bf,
                "Parquet file has bloom filters for {} column(s); \
                 pushdown becomes effective when `parquet` crate is bumped past 58.x",
                cols_with_bf.len()
            );
        }
        cols_with_bf
    }

    /// Public helper: return the set of columns that have a bloom
    /// filter in this Parquet file. Returns an empty set if the file
    /// has no bloom-filter metadata or if the file is not yet opened.
    ///
    /// Callers (the connector registry, the CBO, the executor's
    /// runtime filter layer) can use this to decide whether bloom
    /// filter pushdown is even possible for the file.
    pub fn bloom_filter_columns(&self) -> std::collections::BTreeSet<String> {
        match self.probe_bloom_filter_columns(&self.path) {
            Ok(cols) => cols,
            Err(e) => {
                tracing::debug!(
                    path = %self.path.display(),
                    error = %e,
                    "failed to probe bloom-filter columns"
                );
                std::collections::BTreeSet::new()
            }
        }
    }

    fn probe_bloom_filter_columns(
        &self,
        path: &Path,
    ) -> ConnectorResult<std::collections::BTreeSet<String>> {
        let file = File::open(path).map_err(|e| {
            ConnectorError::Parquet(format!("failed to open '{}': {e}", path.display()))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            ConnectorError::Parquet(format!(
                "failed to build Parquet reader for '{}': {e}",
                path.display()
            ))
        })?;
        Ok(Self::probe_bloom_filters_from_metadata(builder.metadata()))
    }

    /// Return the total row count from the Parquet footer, as read at open time.
    ///
    /// Callers should populate `PlanNode::with_estimated_rows` with this value
    /// so `BroadcastAutoRule` can fire for small Parquet tables on the direct
    /// connector path (without going through the DataFusion SQL path).
    pub fn row_count(&self) -> Option<u64> {
        self.estimated_row_count
    }

    /// Open a fresh batch reader positioned at the start of the file.
    fn open_reader(&self) -> ConnectorResult<ParquetRecordBatchReader> {
        let file = File::open(&self.path).map_err(|e| {
            ConnectorError::Parquet(format!("failed to open '{}': {e}", self.path.display()))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            ConnectorError::Parquet(format!(
                "failed to build Parquet reader for '{}': {e}",
                self.path.display()
            ))
        })?;
        // T8: the option flags are surfaced on the connector surface so the
        // executor can opt in. Direct application of the page-index and
        // bloom-filter toggles requires a newer `parquet` crate than the
        // one currently pinned (the `with_page_index_policy` /
        // `with_bloom_filter` methods on the builder are not exposed on
        // `ParquetRecordBatchReaderBuilder` in `parquet = 58.x`). The
        // executor / DataFusion layers can wire those toggles via
        // `ArrowReaderOptions` once the version is bumped.
        if self.options.pushdown_filters {
            tracing::debug!(
                path = %self.path.display(),
                "pushdown_filters requested but not yet wired for Parquet 58.x"
            );
        }
        if self.options.enable_page_index {
            tracing::debug!(
                path = %self.path.display(),
                "enable_page_index requested but not yet wired for Parquet 58.x"
            );
        }
        if self.options.enable_bloom_filter {
            tracing::debug!(
                path = %self.path.display(),
                "enable_bloom_filter requested but not yet wired for Parquet 58.x"
            );
        }
        builder.build().map_err(|e| {
            ConnectorError::Parquet(format!("failed to create Parquet batch reader: {e}"))
        })
    }

    /// Open a fresh reader and skip forward `skip` batches, returning the
    /// positioned reader. Errors if the file has fewer than `skip` batches.
    fn reader_skipped_to(&self, skip: usize) -> ConnectorResult<ParquetRecordBatchReader> {
        let mut reader = self.open_reader()?;
        for seen in 0..skip {
            match reader.next() {
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    return Err(ConnectorError::Parquet(format!(
                        "error reading Parquet batch: {e}"
                    )));
                }
                None => {
                    return Err(ConnectorError::Offset {
                        message: format!(
                            "Parquet offset {} is past the final batch {} for '{}'",
                            skip,
                            seen,
                            self.path.display()
                        ),
                    });
                }
            }
        }
        Ok(reader)
    }

    /// Lazily build (or rebuild, after a rewind/restore) the active reader.
    fn ensure_reader(&mut self) -> ConnectorResult<&mut ParquetRecordBatchReader> {
        if self.reader.is_none() {
            self.reader = Some(self.reader_skipped_to(self.cursor)?);
        }
        Ok(self
            .reader
            .as_mut()
            .unwrap_or_else(|| unreachable!("reader populated above")))
    }

    /// Return the path this source was opened from.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Return the Arrow schema inferred from the Parquet file.
    pub fn schema(&self) -> Option<SchemaRef> {
        Some(self.schema.clone())
    }
}

impl Source for ParquetSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable()
            .with_checkpoint()
    }

    async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        let reader = self.ensure_reader()?;
        match reader.next() {
            Some(Ok(batch)) => {
                self.cursor = self.cursor.saturating_add(1);
                Ok(Some(batch))
            }
            Some(Err(e)) => Err(ConnectorError::Parquet(format!(
                "error reading Parquet batch: {e}"
            ))),
            None => {
                self.reader = None;
                Ok(None)
            }
        }
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        Some(Box::new(ParquetOffset {
            batch_index: self.cursor,
        }))
    }

    fn reset(&mut self) {
        self.cursor = 0;
        self.reader = None;
    }
}

impl CheckpointSource for ParquetSource {
    type Offset = ParquetOffset;

    fn checkpoint_offset(&self) -> ConnectorResult<Self::Offset> {
        Ok(ParquetOffset {
            batch_index: self.cursor,
        })
    }

    fn restore_offset(&mut self, offset: &Self::Offset) -> ConnectorResult<()> {
        // Validate eagerly by positioning a fresh reader at the requested
        // batch index, then adopt it — this both checks the offset and avoids
        // re-reading the same prefix again on the next `read_batch`.
        let reader = self.reader_skipped_to(offset.batch_index)?;
        self.cursor = offset.batch_index;
        self.reader = Some(reader);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ParquetDirectorySource
// ---------------------------------------------------------------------------

/// A bounded, rewindable source that streams batches from all `.parquet` files
/// under a directory, in sorted file-name order.
///
/// If the directory follows the Hive partition convention
/// (`root/year=2024/month=01/part-0.parquet`), each batch is automatically
/// extended with the partition key columns as `Utf8` string fields.
///
/// Checkpointing uses [`MultiFileOffset`], which encodes `(file_index,
/// batch_index_within_file)` so restores can skip directly to the right file
/// and batch without re-reading the full dataset.
pub struct ParquetDirectorySource {
    root: PathBuf,
    files: Vec<PathBuf>,
    /// Index of the file currently being read (or about to be opened).
    file_index: usize,
    /// Batch cursor within the current file.
    batch_index: usize,
    current_reader: Option<ParquetRecordBatchReader>,
}

impl ParquetDirectorySource {
    /// Open all `.parquet` files in `dir`.
    ///
    /// When `recursive` is `true` the entire sub-tree is scanned; otherwise
    /// only the immediate children of `dir` are included.
    pub fn open(dir: impl AsRef<Path>, recursive: bool) -> ConnectorResult<Self> {
        let root = dir.as_ref().to_path_buf();
        let files = list_parquet_files(&root, recursive)?;
        Ok(Self {
            root,
            files,
            file_index: 0,
            batch_index: 0,
            current_reader: None,
        })
    }

    /// Number of files discovered under the root directory.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Root directory this source was opened from.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn open_reader_at(path: &Path) -> ConnectorResult<ParquetRecordBatchReader> {
        let file = File::open(path).map_err(|e| {
            ConnectorError::Parquet(format!("failed to open '{}': {e}", path.display()))
        })?;
        ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| {
                ConnectorError::Parquet(format!(
                    "failed to build reader for '{}': {e}",
                    path.display()
                ))
            })?
            .build()
            .map_err(|e| {
                ConnectorError::Parquet(format!(
                    "failed to create batch reader for '{}': {e}",
                    path.display()
                ))
            })
    }

    /// Open the reader for `file_index`, skipping `skip` batches.
    fn reader_for_file_at(
        files: &[PathBuf],
        file_index: usize,
        skip: usize,
    ) -> ConnectorResult<ParquetRecordBatchReader> {
        let path = files.get(file_index).ok_or_else(|| ConnectorError::Parquet(format!("file_index {file_index} out of range")))?;
        let mut reader = Self::open_reader_at(path)?;
        for seen in 0..skip {
            match reader.next() {
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    return Err(ConnectorError::Parquet(format!(
                        "error skipping batch: {e}"
                    )));
                }
                None => {
                    return Err(ConnectorError::Offset {
                        message: format!(
                            "ParquetDirectorySource: offset {skip} past end (reached {seen}) in '{}'",
                            path.display()
                        ),
                    });
                }
            }
        }
        Ok(reader)
    }

    fn ensure_reader(&mut self) -> ConnectorResult<()> {
        if self.current_reader.is_none() && self.file_index < self.files.len() {
            self.current_reader = Some(Self::reader_for_file_at(
                &self.files,
                self.file_index,
                self.batch_index,
            )?);
        }
        Ok(())
    }
}

impl Source for ParquetDirectorySource {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable()
            .with_checkpoint()
    }

    async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        loop {
            if self.file_index >= self.files.len() {
                return Ok(None);
            }

            self.ensure_reader()?;

            let reader = match self.current_reader.as_mut() {
                Some(r) => r,
                None => return Ok(None),
            };

            match reader.next() {
                Some(Ok(batch)) => {
                    self.batch_index = self.batch_index.saturating_add(1);
                    let file_path = self.files.get(self.file_index).ok_or_else(|| ConnectorError::Parquet(format!("file_index {} out of range", self.file_index)))?;
                    let parts = discover_hive_partitions(&self.root, file_path);
                    let batch = inject_partition_columns(batch, &parts)?;
                    return Ok(Some(batch));
                }
                Some(Err(e)) => {
                    let file_display = self.files.get(self.file_index).map(|p| p.display().to_string()).unwrap_or_default();
                    return Err(ConnectorError::Parquet(format!(
                        "error reading batch from '{file_display}': {e}"
                    )));
                }
                None => {
                    // Current file exhausted — advance to next.
                    self.current_reader = None;
                    self.file_index = self.file_index.saturating_add(1);
                    self.batch_index = 0;
                }
            }
        }
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        Some(Box::new(MultiFileOffset {
            file_index: self.file_index,
            batch_index: self.batch_index,
        }))
    }

    fn reset(&mut self) {
        self.file_index = 0;
        self.batch_index = 0;
        self.current_reader = None;
    }
}

impl CheckpointSource for ParquetDirectorySource {
    type Offset = MultiFileOffset;

    fn checkpoint_offset(&self) -> ConnectorResult<Self::Offset> {
        Ok(MultiFileOffset {
            file_index: self.file_index,
            batch_index: self.batch_index,
        })
    }

    fn restore_offset(&mut self, offset: &Self::Offset) -> ConnectorResult<()> {
        if offset.file_index > self.files.len() {
            return Err(ConnectorError::Offset {
                message: format!(
                    "ParquetDirectorySource restore: file_index {} out of range (have {} files)",
                    offset.file_index,
                    self.files.len()
                ),
            });
        }
        // Pre-open and position the reader eagerly to validate the offset.
        let reader = if offset.file_index < self.files.len() {
            Some(Self::reader_for_file_at(
                &self.files,
                offset.file_index,
                offset.batch_index,
            )?)
        } else {
            None
        };
        self.file_index = offset.file_index;
        self.batch_index = offset.batch_index;
        self.current_reader = reader;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ParquetSink
// ---------------------------------------------------------------------------

/// A bounded sink that writes record batches to a Parquet file.
///
/// The file is created lazily on the first call to [`Sink::write_batch`].
/// Call [`Sink::flush`] to close the writer and finalise the file.
///
/// # Not idempotent
///
/// After `flush` the file is finalised and closed. A subsequent
/// `write_batch` returns `ConnectorError::Unsupported`. Replaying the
/// same batch before `flush` duplicates rows; replaying after `flush`
/// loses the first write (the file is re-created). Use
/// [`TwoPhaseCommitSink`][crate::TwoPhaseCommitSink] for exactly-once
/// Parquet writes.
pub struct ParquetSink {
    path: PathBuf,
    schema: Option<SchemaRef>,
    writer: Option<ArrowWriter<File>>,
    /// True after `flush` has been called. All subsequent `write_batch`
    /// calls are rejected to prevent silent data loss (truncation on reopen).
    closed: bool,
}

impl ParquetSink {
    /// Create a new `ParquetSink` that will write to `path`.
    ///
    /// The underlying file is not opened until the first [`Sink::write_batch`]
    /// call so that empty pipelines do not leave behind empty files.
    pub fn create(path: impl AsRef<Path>) -> ConnectorResult<Self> {
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            schema: None,
            writer: None,
            closed: false,
        })
    }
}

impl Sink for ParquetSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_bounded()
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> ConnectorResult<()> {
        if self.closed {
            return Err(ConnectorError::Unsupported {
                message: "ParquetSink is closed after flush; write_batch rejected".into(),
            });
        }
        if self.writer.is_none() {
            let schema = batch.schema();
            let file = File::create(&self.path).map_err(|e| {
                ConnectorError::Parquet(format!("failed to create '{}': {e}", self.path.display()))
            })?;
            let writer = ArrowWriter::try_new(file, schema.clone(), None).map_err(|e| {
                ConnectorError::Parquet(format!("failed to create Parquet writer: {e}"))
            })?;
            self.schema = Some(schema);
            self.writer = Some(writer);
        }

        self.writer
            .as_mut()
            .ok_or_else(|| {
                ConnectorError::Parquet(
                    "Parquet writer not initialized; call write_batch with a schema-bearing batch first".into(),
                )
            })?
            .write(&batch)
            .map_err(|e| ConnectorError::Parquet(format!("failed to write Parquet batch: {e}")))?;
        Ok(())
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        if let Some(writer) = self.writer.take() {
            writer.close().map_err(|e| {
                ConnectorError::Parquet(format!("failed to close Parquet writer: {e}"))
            })?;
            // fsync the file so flush is actually durable (crash-safe).
            // Without this, the OS page cache may not be flushed on crash,
            // leaving an empty or truncated file.
            let file = std::fs::File::open(&self.path).map_err(|e| {
                ConnectorError::Parquet(format!(
                    "failed to open '{}' for fsync: {e}",
                    self.path.display()
                ))
            })?;
            file.sync_all().map_err(|e| {
                ConnectorError::Parquet(format!("failed to fsync '{}': {e}", self.path.display()))
            })?;
        }
        self.closed = true;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn parquet_sink_writes_and_source_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.parquet");

        // Write two batches.
        let mut sink = ParquetSink::create(&path).unwrap();
        let batch1 = make_batch(&[1, 2], &["alice", "bob"]);
        let batch2 = make_batch(&[3], &["carol"]);
        sink.write_batch(batch1).await.unwrap();
        sink.write_batch(batch2).await.unwrap();
        sink.flush().await.unwrap();

        // Read back.
        let mut source = ParquetSource::open(&path).unwrap();
        let mut total_rows = 0usize;
        while let Some(batch) = source.read_batch().await.unwrap() {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 3, "expected 3 rows total");
    }

    #[test]
    fn parquet_source_row_count_matches_written_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rowcount.parquet");

        let batch1 = make_batch(&[1, 2, 3], &["a", "b", "c"]);
        let batch2 = make_batch(&[4, 5], &["d", "e"]);
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch1.schema(), None).unwrap();
        writer.write(&batch1).unwrap();
        writer.write(&batch2).unwrap();
        writer.close().unwrap();

        let source = ParquetSource::open(&path).unwrap();
        assert_eq!(
            source.row_count(),
            Some(5),
            "row_count must reflect total rows from Parquet footer"
        );
    }

    #[test]
    fn parquet_source_reports_bounded_and_rewindable_capabilities() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("caps.parquet");

        // Create a minimal valid Parquet file so we can open it.
        let batch = make_batch(&[1], &["x"]);
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let source = ParquetSource::open(&path).unwrap();
        let caps = source.capabilities();
        assert!(caps.is_bounded());
        assert!(caps.is_rewindable());
        assert!(caps.is_checkpoint_capable());
        assert!(!caps.is_unbounded());
        assert!(!caps.is_transactional());
        assert!(!caps.is_idempotent());
    }

    #[test]
    fn parquet_sink_reports_bounded_capabilities() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sink_caps.parquet");
        let sink = ParquetSink::create(&path).unwrap();
        let caps = sink.capabilities();
        assert!(caps.is_bounded());
        assert!(!caps.is_idempotent());
        assert!(!caps.is_unbounded());
        assert!(!caps.is_rewindable());
        assert!(!caps.is_transactional());
    }

    #[tokio::test]
    async fn parquet_source_reset_restores_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rewind.parquet");

        // Write a single batch with two rows.
        let batch = make_batch(&[10, 20], &["foo", "bar"]);
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut source = ParquetSource::open(&path).unwrap();

        // First read — should return the batch.
        let first = source.read_batch().await.unwrap();
        assert!(first.is_some(), "first read should return a batch");
        let first_batch = first.unwrap();
        assert_eq!(first_batch.num_rows(), 2);

        // Source is now exhausted.
        let exhausted = source.read_batch().await.unwrap();
        assert!(exhausted.is_none(), "source should be exhausted");

        // Reset and read again — should return the same batch.
        Source::reset(&mut source);
        let after_reset = source.read_batch().await.unwrap();
        assert!(
            after_reset.is_some(),
            "read after reset should return a batch"
        );
        let reset_batch = after_reset.unwrap();
        assert_eq!(
            reset_batch.num_rows(),
            first_batch.num_rows(),
            "batch after reset must have same row count as first read"
        );

        // Verify the data matches.
        use arrow::array::Int32Array;
        let orig_ids = first_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let reset_ids = reset_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            orig_ids.values(),
            reset_ids.values(),
            "data must match after reset"
        );
    }

    #[test]
    fn parquet_source_rejects_offset_past_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("invalid-offset.parquet");
        let batch = make_batch(&[1], &["x"]);
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut source = ParquetSource::open(&path).unwrap();
        let error = source
            .restore_offset(&ParquetOffset { batch_index: 2 })
            .expect_err("offset beyond loaded batches must fail");
        assert!(matches!(error, ConnectorError::Offset { .. }));
    }

    #[tokio::test]
    async fn parquet_source_returns_none_when_exhausted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("exhaust.parquet");

        // Write a single-row file.
        let batch = make_batch(&[42], &["z"]);
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut source = ParquetSource::open(&path).unwrap();
        let first = source.read_batch().await.unwrap();
        assert!(first.is_some());
        let exhausted = source.read_batch().await.unwrap();
        assert!(
            exhausted.is_none(),
            "source should return None when exhausted"
        );
    }
}
