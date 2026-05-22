//! S3-compatible object store source and sink implementations.
//!
//! Uses the `object_store` crate to read/write Parquet data from/to any
//! S3-compatible object store (AWS S3, MinIO, GCS, Azure Blob via the
//! object_store abstraction layer).

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload, path::Path};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{ConnectorCapabilities, ConnectorError, ConnectorResult, Sink, Source};

// ---------------------------------------------------------------------------
// S3Source
// ---------------------------------------------------------------------------

/// A bounded, rewindable source that reads a Parquet object from an
/// [`ObjectStore`].
///
/// The object is downloaded eagerly on [`S3Source::open`]; subsequent calls to
/// [`Source::read_batch`] iterate over the in-memory batch vector.
pub struct S3Source {
    // Retained to keep the store alive and for future rewind/re-read operations.
    #[allow(dead_code)]
    store: Arc<dyn ObjectStore>,
    path: Path,
    schema: Option<SchemaRef>,
    batches: Vec<RecordBatch>,
    cursor: usize,
}

impl S3Source {
    /// Download the object at `path` from `store` and eagerly load all Parquet
    /// record batches.
    pub async fn open(store: Arc<dyn ObjectStore>, path: impl Into<Path>) -> ConnectorResult<Self> {
        let path = path.into();

        // Download the full object.
        let get_result = store.get(&path).await.map_err(|e| ConnectorError::IoStr {
            message: format!("failed to get object '{}': {e}", path),
        })?;
        let raw: Bytes = get_result.bytes().await.map_err(|e| ConnectorError::IoStr {
            message: format!("failed to read bytes from '{}': {e}", path),
        })?;

        // Parse as Parquet.
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(raw).map_err(|e| ConnectorError::IoStr {
                message: format!("failed to build Parquet reader for '{}': {e}", path),
            })?;
        let schema = builder.schema().clone();
        let reader = builder.build().map_err(|e| ConnectorError::IoStr {
            message: format!("failed to build Parquet batch reader for '{}': {e}", path),
        })?;

        let mut batches = Vec::new();
        for result in reader {
            let batch = result.map_err(|e| ConnectorError::IoStr {
                message: format!("error reading Parquet batch from '{}': {e}", path),
            })?;
            batches.push(batch);
        }

        Ok(Self {
            store,
            path,
            schema: Some(schema),
            batches,
            cursor: 0,
        })
    }

    /// Return the object path this source was opened from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the Arrow schema inferred from the Parquet object.
    pub fn schema(&self) -> Option<SchemaRef> {
        self.schema.clone()
    }
}

impl Source for S3Source {
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
        Some(Box::new(self.cursor))
    }

    /// Reset the cursor to the beginning so the source can be read again.
    fn reset(&mut self) {
        self.cursor = 0;
    }
}

// ---------------------------------------------------------------------------
// S3Sink
// ---------------------------------------------------------------------------

/// A bounded, idempotent sink that writes record batches to a Parquet object
/// in an [`ObjectStore`].
///
/// Batches are buffered in memory.  On [`Sink::flush`] all batches are
/// serialised to a Parquet byte buffer and uploaded atomically via
/// [`ObjectStore::put`].
pub struct S3Sink {
    store: Arc<dyn ObjectStore>,
    path: Path,
    schema: Option<SchemaRef>,
    pending: Vec<RecordBatch>,
}

impl S3Sink {
    /// Create a new `S3Sink` that will write to `path` in `store`.
    pub fn new(store: Arc<dyn ObjectStore>, path: impl Into<Path>) -> Self {
        Self {
            store,
            path: path.into(),
            schema: None,
            pending: Vec::new(),
        }
    }

    /// Return the object path this sink will write to.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Sink for S3Sink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_idempotent()
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> ConnectorResult<()> {
        if self.schema.is_none() {
            self.schema = Some(batch.schema());
        }
        self.pending.push(batch);
        Ok(())
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let schema = self.schema.clone().ok_or_else(|| ConnectorError::IoStr {
            message: "S3Sink::flush: schema not set (no batches written)".into(),
        })?;

        // Serialise all pending batches to a Parquet byte buffer.
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer =
                ArrowWriter::try_new(&mut buf, schema, None).map_err(|e| ConnectorError::IoStr {
                    message: format!("failed to create Parquet writer: {e}"),
                })?;
            for batch in &self.pending {
                writer.write(batch).map_err(|e| ConnectorError::IoStr {
                    message: format!("failed to write Parquet batch: {e}"),
                })?;
            }
            writer.close().map_err(|e| ConnectorError::IoStr {
                message: format!("failed to close Parquet writer: {e}"),
            })?;
        }

        // Upload.
        let payload = PutPayload::from_bytes(Bytes::from(buf));
        self.store
            .put(&self.path, payload)
            .await
            .map_err(|e| ConnectorError::IoStr {
                message: format!("failed to put object '{}': {e}", self.path),
            })?;

        self.pending.clear();
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
    use object_store::local::LocalFileSystem;

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
    async fn s3_sink_writes_and_source_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let path = Path::from("data.parquet");

        // Write two batches.
        let mut sink = S3Sink::new(Arc::clone(&store), path.clone());
        let batch1 = make_batch(&[1, 2], &["alice", "bob"]);
        let batch2 = make_batch(&[3], &["carol"]);
        sink.write_batch(batch1).await.unwrap();
        sink.write_batch(batch2).await.unwrap();
        sink.flush().await.unwrap();

        // Read back.
        let mut source = S3Source::open(Arc::clone(&store), path).await.unwrap();
        let mut total_rows = 0usize;
        while let Some(batch) = source.read_batch().await.unwrap() {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 3, "expected 3 rows total");
    }

    #[tokio::test]
    async fn s3_source_capabilities() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let path = Path::from("caps.parquet");

        // Write a minimal file so we can open it.
        let batch = make_batch(&[1], &["x"]);
        let mut sink = S3Sink::new(Arc::clone(&store), path.clone());
        sink.write_batch(batch).await.unwrap();
        sink.flush().await.unwrap();

        let source = S3Source::open(store, path).await.unwrap();
        let caps = source.capabilities();
        assert!(caps.is_bounded());
        assert!(caps.is_rewindable());
        assert!(!caps.is_unbounded());
        assert!(!caps.is_transactional());
        assert!(!caps.is_idempotent());
    }

    #[tokio::test]
    async fn s3_sink_capabilities() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let path = Path::from("sink_caps.parquet");
        let sink = S3Sink::new(store, path);
        let caps = sink.capabilities();
        assert!(caps.is_bounded());
        assert!(caps.is_idempotent());
        assert!(!caps.is_unbounded());
        assert!(!caps.is_rewindable());
        assert!(!caps.is_transactional());
    }
}
