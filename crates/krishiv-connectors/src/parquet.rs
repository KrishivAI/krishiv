//! Parquet source and sink implementations.

use std::any::Any;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{ConnectorCapabilities, ConnectorError, ConnectorResult, Sink, Source};

// ---------------------------------------------------------------------------
// ParquetSource
// ---------------------------------------------------------------------------

/// A bounded, rewindable source that reads all batches from a Parquet file.
///
/// The file is read eagerly on [`ParquetSource::open`]; subsequent calls to
/// [`Source::read_batch`] iterate over the in-memory batch vector.
pub struct ParquetSource {
    path: PathBuf,
    schema: Option<SchemaRef>,
    batches: Vec<RecordBatch>,
    cursor: usize,
}

impl ParquetSource {
    /// Open a Parquet file and eagerly load all record batches.
    pub fn open(path: impl AsRef<Path>) -> ConnectorResult<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path).map_err(|e| ConnectorError::IoStr {
            message: format!("failed to open '{}': {e}", path.display()),
        })?;

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| ConnectorError::IoStr {
                message: format!(
                    "failed to build Parquet reader for '{}': {e}",
                    path.display()
                ),
            })?;

        let schema = builder.schema().clone();
        let reader = builder.build().map_err(|e| ConnectorError::IoStr {
            message: format!("failed to create Parquet batch reader: {e}"),
        })?;

        let mut batches = Vec::new();
        for result in reader {
            let batch = result.map_err(|e| ConnectorError::IoStr {
                message: format!("error reading Parquet batch: {e}"),
            })?;
            batches.push(batch);
        }

        Ok(Self {
            path,
            schema: Some(schema),
            batches,
            cursor: 0,
        })
    }

    /// Return the path this source was opened from.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Return the Arrow schema inferred from the Parquet file, if opened.
    pub fn schema(&self) -> Option<SchemaRef> {
        self.schema.clone()
    }
}

impl Source for ParquetSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable()
    }

    async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        if self.cursor >= self.batches.len() {
            return Ok(None);
        }
        let batch = self.batches[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(batch))
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        Some(Box::new(crate::ParquetOffset {
            batch_index: self.cursor,
        }))
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
            let file = File::create(&self.path).map_err(|e| ConnectorError::IoStr {
                message: format!("failed to create '{}': {e}", self.path.display()),
            })?;
            let writer = ArrowWriter::try_new(file, schema.clone(), None).map_err(|e| {
                ConnectorError::IoStr {
                    message: format!("failed to create Parquet writer: {e}"),
                }
            })?;
            self.schema = Some(schema);
            self.writer = Some(writer);
        }

        self.writer
            .as_mut()
            .expect("writer is set above")
            .write(&batch)
            .map_err(|e| ConnectorError::IoStr {
                message: format!("failed to write Parquet batch: {e}"),
            })?;
        Ok(())
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        if let Some(writer) = self.writer.take() {
            writer.close().map_err(|e| ConnectorError::IoStr {
                message: format!("failed to close Parquet writer: {e}"),
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
