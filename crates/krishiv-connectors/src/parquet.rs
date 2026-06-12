//! Parquet source and sink implementations.

use std::any::Any;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::{
    CheckpointSource, ConnectorCapabilities, ConnectorError, ConnectorResult, ParquetOffset, Sink,
    Source,
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
}

impl ParquetSource {
    /// Open a Parquet file, validating it and reading its schema and row count.
    ///
    /// Batches are not read until [`Source::read_batch`] is called.
    pub fn open(path: impl AsRef<Path>) -> ConnectorResult<Self> {
        let path = path.as_ref().to_path_buf();
        let (schema, estimated_row_count) = Self::probe_metadata(&path)?;

        Ok(Self {
            path,
            schema,
            estimated_row_count,
            reader: None,
            cursor: 0,
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
        Ok((schema, row_count))
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
        Ok(self.reader.as_mut().expect("reader populated above"))
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
                self.cursor += 1;
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
// ParquetSink
// ---------------------------------------------------------------------------

/// A bounded, idempotent sink that writes record batches to a Parquet file.
///
/// The file is created lazily on the first call to [`Sink::write_batch`].
/// Call [`Sink::flush`] to close the writer and finalise the file.
pub struct ParquetSink {
    path: PathBuf,
    schema: Option<SchemaRef>,
    writer: Option<ArrowWriter<File>>,
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
        })
    }
}

impl Sink for ParquetSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_idempotent()
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> ConnectorResult<()> {
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
        }
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
    fn parquet_sink_reports_bounded_and_idempotent_capabilities() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sink_caps.parquet");
        let sink = ParquetSink::create(&path).unwrap();
        let caps = sink.capabilities();
        assert!(caps.is_bounded());
        assert!(caps.is_idempotent());
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
