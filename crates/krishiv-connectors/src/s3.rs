//! S3-compatible object store source and sink implementations.
//!
//! Uses the `object_store` crate to read/write Parquet data from/to any
//! S3-compatible object store (AWS S3, MinIO, GCS, Azure Blob via the
//! object_store abstraction layer).

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload, path::Path};
use parquet::arrow::ArrowWriter;
use parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};

use crate::{
    CheckpointSource, ConnectorCapabilities, ConnectorError, ConnectorResult, ParquetOffset, Sink,
    Source,
};

// ---------------------------------------------------------------------------
// S3Source
// ---------------------------------------------------------------------------

type ObjectBatchStream = Pin<Box<ParquetRecordBatchStream<ParquetObjectReader>>>;

/// A bounded, rewindable source that streams Parquet record batches from an
/// object in an [`ObjectStore`].
///
/// [`S3Source::open`] only fetches the object's size and Arrow schema; the
/// underlying [`ParquetRecordBatchStream`] is created lazily and pulls one
/// batch at a time over the network on [`Source::read_batch`], so the whole
/// object is never downloaded into memory at once. Rewinding
/// ([`Source::reset`]) or restoring a checkpoint re-opens the stream and
/// re-positions it by skipping the requested number of batches — the standard
/// trade-off for sequential formats that lack a random-access batch index.
pub struct S3Source {
    store: Arc<dyn ObjectStore>,
    path: Path,
    schema: SchemaRef,
    file_size: u64,
    stream: Option<ObjectBatchStream>,
    cursor: usize,
}

impl S3Source {
    /// Open a Parquet object, validating it and reading its schema.
    ///
    /// Batches are not downloaded until [`Source::read_batch`] is called.
    pub async fn open(store: Arc<dyn ObjectStore>, path: impl Into<Path>) -> ConnectorResult<Self> {
        let path = path.into();

        let meta = store
            .head(&path)
            .await
            .map_err(|e| ConnectorError::ObjectStore {
                message: format!("failed to stat object '{}': {e}", path),
                status: None,
            })?;
        let file_size = meta.size;

        let schema = Self::probe_schema(&store, &path, file_size).await?;

        Ok(Self {
            store,
            path,
            schema,
            file_size,
            stream: None,
            cursor: 0,
        })
    }

    /// Open a streaming reader and read just its Arrow schema, without
    /// building the batch stream.
    async fn probe_schema(
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        file_size: u64,
    ) -> ConnectorResult<SchemaRef> {
        let reader =
            ParquetObjectReader::new(Arc::clone(store), path.clone()).with_file_size(file_size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| {
                ConnectorError::Parquet(format!(
                    "failed to build Parquet stream reader for '{}': {e}",
                    path
                ))
            })?;
        Ok(builder.schema().clone())
    }

    /// Open a fresh batch stream positioned at the start of the object.
    ///
    /// Takes owned `store`/`path`/`file_size` (rather than `&self`) so the
    /// returned future does not capture a `&S3Source` — the stream's inner
    /// decoder types are `Send` but not `Sync`, which would otherwise make
    /// `&S3Source` (and thus `read_batch`'s future) non-`Send`.
    async fn open_stream(
        store: Arc<dyn ObjectStore>,
        path: Path,
        file_size: u64,
    ) -> ConnectorResult<ObjectBatchStream> {
        let reader = ParquetObjectReader::new(store, path.clone()).with_file_size(file_size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| {
                ConnectorError::Parquet(format!(
                    "failed to build Parquet stream reader for '{}': {e}",
                    path
                ))
            })?;
        let stream = builder.build().map_err(|e| {
            ConnectorError::Parquet(format!("failed to create Parquet batch stream: {e}"))
        })?;
        Ok(Box::pin(stream))
    }

    /// Open a fresh stream and skip forward `skip` batches, returning the
    /// positioned stream. Errors if the object has fewer than `skip` batches.
    async fn stream_skipped_to(
        store: Arc<dyn ObjectStore>,
        path: Path,
        file_size: u64,
        skip: usize,
    ) -> ConnectorResult<ObjectBatchStream> {
        let mut stream = Self::open_stream(store, path.clone(), file_size).await?;
        for seen in 0..skip {
            match stream.next().await {
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    return Err(ConnectorError::Parquet(format!(
                        "error reading Parquet batch: {e}"
                    )));
                }
                None => {
                    return Err(ConnectorError::Offset {
                        message: format!(
                            "object-store Parquet offset {} is past the final batch {} for '{}'",
                            skip, seen, path
                        ),
                    });
                }
            }
        }
        Ok(stream)
    }

    /// Lazily build (or rebuild, after a rewind/restore) the active stream.
    async fn ensure_stream(&mut self) -> ConnectorResult<&mut ObjectBatchStream> {
        if self.stream.is_none() {
            let stream = Self::stream_skipped_to(
                Arc::clone(&self.store),
                self.path.clone(),
                self.file_size,
                self.cursor,
            )
            .await?;
            self.stream = Some(stream);
        }
        Ok(self.stream.as_mut().expect("stream populated above"))
    }

    /// Return the object path this source was opened from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the Arrow schema inferred from the Parquet object.
    pub fn schema(&self) -> Option<SchemaRef> {
        Some(self.schema.clone())
    }
}

impl Source for S3Source {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable()
            .with_checkpoint()
    }

    async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        let stream = self.ensure_stream().await?;
        match stream.next().await {
            Some(Ok(batch)) => {
                self.cursor += 1;
                Ok(Some(batch))
            }
            Some(Err(e)) => Err(ConnectorError::Parquet(format!(
                "error reading Parquet batch: {e}"
            ))),
            None => {
                self.stream = None;
                Ok(None)
            }
        }
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        Some(Box::new(ParquetOffset {
            batch_index: self.cursor,
        }))
    }

    /// Reset the cursor to the beginning so the source can be read again.
    fn reset(&mut self) {
        self.cursor = 0;
        self.stream = None;
    }
}

impl CheckpointSource for S3Source {
    type Offset = ParquetOffset;

    fn checkpoint_offset(&self) -> ConnectorResult<Self::Offset> {
        Ok(ParquetOffset {
            batch_index: self.cursor,
        })
    }

    fn restore_offset(&mut self, offset: &Self::Offset) -> ConnectorResult<()> {
        // `restore_offset` is synchronous, but positioning a network-backed
        // stream requires async I/O. Adopt the offset and drop the active
        // stream so the next `read_batch` lazily rebuilds and re-validates it
        // — `stream_skipped_to` returns `ConnectorError::Offset` if the
        // object has fewer batches than the requested index.
        self.cursor = offset.batch_index;
        self.stream = None;
        Ok(())
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
/// [`ObjectStore::put`]. Pending batches are capped both by count and by
/// total in-memory byte size to avoid unbounded memory growth from a small
/// number of very large batches.
const MAX_PENDING_BATCHES: usize = 1_024;

/// Default cap on the total in-memory size (bytes, per [`RecordBatch::get_array_memory_size`])
/// of batches buffered before [`Sink::flush`] must be called. Bounds memory
/// when a handful of large batches would otherwise stay under
/// [`MAX_PENDING_BATCHES`] but still exhaust memory.
const DEFAULT_MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;

pub struct S3Sink {
    store: Arc<dyn ObjectStore>,
    path: Path,
    schema: Option<SchemaRef>,
    pending: Vec<RecordBatch>,
    pending_bytes: usize,
    max_pending_bytes: usize,
}

impl S3Sink {
    /// Create a new `S3Sink` that will write to `path` in `store`.
    pub fn new(store: Arc<dyn ObjectStore>, path: impl Into<Path>) -> Self {
        Self {
            store,
            path: path.into(),
            schema: None,
            pending: Vec::new(),
            pending_bytes: 0,
            max_pending_bytes: DEFAULT_MAX_PENDING_BYTES,
        }
    }

    /// Override the maximum total in-memory size (bytes) of batches buffered
    /// before [`Sink::flush`] must be called.
    pub fn with_max_pending_bytes(mut self, max_pending_bytes: usize) -> Self {
        self.max_pending_bytes = max_pending_bytes;
        self
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
        if self.pending.len() >= MAX_PENDING_BATCHES {
            return Err(ConnectorError::ObjectStore {
                message: format!(
                    "S3Sink pending batch limit ({MAX_PENDING_BATCHES}) exceeded; flush before writing more"
                ),
                status: None,
            });
        }
        let batch_bytes = batch.get_array_memory_size();
        if !self.pending.is_empty()
            && self.pending_bytes.saturating_add(batch_bytes) > self.max_pending_bytes
        {
            return Err(ConnectorError::ObjectStore {
                message: format!(
                    "S3Sink pending byte limit ({} bytes) exceeded; flush before writing more",
                    self.max_pending_bytes
                ),
                status: None,
            });
        }
        self.pending_bytes = self.pending_bytes.saturating_add(batch_bytes);
        self.pending.push(batch);
        Ok(())
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let schema = self.schema.clone().ok_or_else(|| {
            ConnectorError::Parquet("S3Sink::flush: schema not set (no batches written)".into())
        })?;

        // Serialise all pending batches to a Parquet byte buffer.
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None).map_err(|e| {
                ConnectorError::Parquet(format!("failed to create Parquet writer: {e}"))
            })?;
            for batch in &self.pending {
                writer.write(batch).map_err(|e| {
                    ConnectorError::Parquet(format!("failed to write Parquet batch: {e}"))
                })?;
            }
            writer.close().map_err(|e| {
                ConnectorError::Parquet(format!("failed to close Parquet writer: {e}"))
            })?;
        }

        // Upload.
        let payload = PutPayload::from_bytes(Bytes::from(buf));
        self.store
            .put(&self.path, payload)
            .await
            .map_err(|e| ConnectorError::ObjectStore {
                message: format!("failed to put object '{}': {e}", self.path),
                status: None,
            })?;

        self.pending.clear();
        self.pending_bytes = 0;
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
        assert!(caps.is_checkpoint_capable());
        assert!(!caps.is_unbounded());
        assert!(!caps.is_transactional());
        assert!(!caps.is_idempotent());
    }

    #[tokio::test]
    async fn s3_source_restores_typed_checkpoint_offset() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let path = Path::from("checkpoint.parquet");
        let mut sink = S3Sink::new(Arc::clone(&store), path.clone());
        sink.write_batch(make_batch(&[1, 2], &["a", "b"]))
            .await
            .unwrap();
        sink.flush().await.unwrap();

        let mut source = S3Source::open(store, path).await.unwrap();
        crate::CertificationSuite::run_checkpoint_restore_test(&mut source)
            .await
            .expect("S3Source must restore typed Parquet offsets");
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
