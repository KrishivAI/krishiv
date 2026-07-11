//! Lakehouse connector drivers: Iceberg (fs), Delta, and Hudi.
//!
//! Each driver reads a `path` key from [`ConnectorConfig`] which points to the
//! table root directory.  All three formats are bounded + rewindable (full scan).

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::{ConnectorError, ConnectorResult};
use crate::lakehouse::{
    DeltaWriteMode, HudiCowWriter, HudiSnapshotReader, IcebergFsTable, IcebergScanOptions,
    IcebergTableRef, LakehouseError, LakehouseTable, SchemaVersion, write_delta,
};
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::{SinkDriver, SourceDriver};
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::sink::DynSink;
use crate::source::DynSource;

fn map_lh(e: LakehouseError) -> ConnectorError {
    ConnectorError::Config {
        message: e.to_string(),
    }
}

fn require_path(config: &ConnectorConfig) -> ConnectorResult<PathBuf> {
    Ok(PathBuf::from(config.required("path")?))
}

/// Streams a snapshot's parquet files one batch at a time (Phase 52 #194).
///
/// Holds the resolved file list and at most one open file reader, so a
/// bounded lakehouse scan never materializes the whole table the way the
/// previous scan-everything-at-open sources did. `rewind` restarts from the
/// first file, making the advertised rewindable capability real.
struct ParquetFilesSource {
    files: Vec<PathBuf>,
    next_file: usize,
    current: Option<parquet::arrow::arrow_reader::ParquetRecordBatchReader>,
}

impl ParquetFilesSource {
    fn new(files: Vec<PathBuf>) -> Self {
        Self {
            files,
            next_file: 0,
            current: None,
        }
    }

    fn next_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let io_err = |e: String| ConnectorError::Io(std::io::Error::other(e));
        loop {
            if let Some(reader) = self.current.as_mut() {
                match reader.next() {
                    Some(batch) => return batch.map(Some).map_err(|e| io_err(e.to_string())),
                    None => {
                        self.current = None;
                        continue;
                    }
                }
            }
            let Some(path) = self.files.get(self.next_file).cloned() else {
                return Ok(None);
            };
            self.next_file += 1;
            // A file listed by the snapshot but since vacuumed is skipped,
            // matching the pre-streaming read path.
            if !path.exists() {
                continue;
            }
            let file = std::fs::File::open(&path).map_err(|e| io_err(e.to_string()))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| io_err(e.to_string()))?
                .build()
                .map_err(|e| io_err(e.to_string()))?;
            self.current = Some(reader);
        }
    }

    fn rewind(&mut self) {
        self.next_file = 0;
        self.current = None;
    }
}

// ── Iceberg ──────────────────────────────────────────────────────────────────

struct IcebergSource {
    batches: std::collections::VecDeque<RecordBatch>,
}

impl IcebergSource {
    async fn open(path: PathBuf) -> ConnectorResult<Self> {
        let table_ref = IcebergTableRef::new("default", "default", path.to_string_lossy().as_ref());
        let table = IcebergFsTable::new(
            &path,
            table_ref,
            SchemaVersion {
                schema_id: 0,
                fields: vec![],
            },
        )
        .map_err(map_lh)?;
        let batches = table
            .scan(&IcebergScanOptions::new())
            .await
            .map_err(map_lh)?;
        Ok(Self {
            batches: batches.into(),
        })
    }
}

impl crate::source::Source for IcebergSource {
    fn capabilities(&self) -> crate::capabilities::ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable()
    }

    fn read_batch(&mut self) -> impl Future<Output = ConnectorResult<Option<RecordBatch>>> + Send {
        let batch = self.batches.pop_front();
        async move { Ok(batch) }
    }

    fn current_offset(&self) -> Option<Box<dyn std::any::Any + Send>> {
        None
    }
}

struct IcebergSink {
    table: Arc<IcebergFsTable>,
    pending: Vec<RecordBatch>,
}

impl IcebergSink {
    fn open(path: PathBuf) -> ConnectorResult<Self> {
        let table_ref = IcebergTableRef::new("default", "default", path.to_string_lossy().as_ref());
        let table = IcebergFsTable::new(
            &path,
            table_ref,
            SchemaVersion {
                schema_id: 0,
                fields: vec![],
            },
        )
        .map_err(map_lh)?;
        Ok(Self {
            table: Arc::new(table),
            pending: Vec::new(),
        })
    }
}

impl crate::sink::Sink for IcebergSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_idempotent()
    }

    fn write_batch(
        &mut self,
        batch: RecordBatch,
    ) -> impl Future<Output = ConnectorResult<()>> + Send {
        self.pending.push(batch);
        async move { Ok(()) }
    }

    fn flush(&mut self) -> impl Future<Output = ConnectorResult<()>> + Send {
        let batches = std::mem::take(&mut self.pending);
        let table = Arc::clone(&self.table);
        async move {
            if !batches.is_empty() {
                table.append(batches).await.map_err(map_lh)?;
            }
            Ok(())
        }
    }
}

pub struct IcebergSourceDriver;

impl SourceDriver for IcebergSourceDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Iceberg,
            ConnectorRole::Source,
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = require_path(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSource>>> + Send + 'a>> {
        let path = require_path(config);
        Box::pin(async move {
            let src = IcebergSource::open(path?).await?;
            Ok(Box::new(src) as Box<dyn DynSource>)
        })
    }
}

pub struct IcebergSinkDriver;

impl SinkDriver for IcebergSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Iceberg,
            ConnectorRole::Sink,
            ConnectorCapabilities::new()
                .with_bounded()
                .with_idempotent(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = require_path(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>> {
        let path = require_path(config);
        Box::pin(async move {
            let sink = IcebergSink::open(path?)?;
            Ok(Box::new(sink) as Box<dyn DynSink>)
        })
    }
}

// ── Delta ─────────────────────────────────────────────────────────────────────

struct DeltaSource {
    files: ParquetFilesSource,
}

impl DeltaSource {
    async fn open(path: PathBuf) -> ConnectorResult<Self> {
        // Resolve the current snapshot's file list from the log; batches
        // stream out one file at a time instead of being scanned up front.
        let files = crate::lakehouse::list_table_data_files(path.to_string_lossy().as_ref(), None)
            .map_err(map_lh)?;
        Ok(Self {
            files: ParquetFilesSource::new(files),
        })
    }
}

impl crate::source::Source for DeltaSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable()
    }

    fn read_batch(&mut self) -> impl Future<Output = ConnectorResult<Option<RecordBatch>>> + Send {
        let batch = self.files.next_batch();
        async move { batch }
    }

    fn current_offset(&self) -> Option<Box<dyn std::any::Any + Send>> {
        None
    }

    fn reset(&mut self) {
        self.files.rewind();
    }
}

struct DeltaSink {
    path: PathBuf,
    pending: Vec<RecordBatch>,
}

impl crate::sink::Sink for DeltaSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_bounded()
    }

    fn write_batch(
        &mut self,
        batch: RecordBatch,
    ) -> impl Future<Output = ConnectorResult<()>> + Send {
        self.pending.push(batch);
        async move { Ok(()) }
    }

    fn flush(&mut self) -> impl Future<Output = ConnectorResult<()>> + Send {
        let batches = std::mem::take(&mut self.pending);
        let path = self.path.clone();
        async move {
            if !batches.is_empty() {
                write_delta(
                    path.to_string_lossy().as_ref(),
                    batches,
                    DeltaWriteMode::Append,
                    false,
                )
                .await
                .map_err(map_lh)?;
            }
            Ok(())
        }
    }
}

pub struct DeltaSourceDriver;

impl SourceDriver for DeltaSourceDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Delta,
            ConnectorRole::Source,
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = require_path(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSource>>> + Send + 'a>> {
        let path = require_path(config);
        Box::pin(async move {
            let src = DeltaSource::open(path?).await?;
            Ok(Box::new(src) as Box<dyn DynSource>)
        })
    }
}

pub struct DeltaSinkDriver;

impl SinkDriver for DeltaSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Delta,
            ConnectorRole::Sink,
            ConnectorCapabilities::new().with_bounded(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = require_path(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>> {
        let path = require_path(config);
        Box::pin(async move {
            let sink = DeltaSink {
                path: path?,
                pending: Vec::new(),
            };
            Ok(Box::new(sink) as Box<dyn DynSink>)
        })
    }
}

// ── Hudi ──────────────────────────────────────────────────────────────────────

struct HudiSource {
    files: ParquetFilesSource,
}

impl HudiSource {
    fn open(path: PathBuf) -> ConnectorResult<Self> {
        // Resolve the snapshot's file list from commit metadata; batches
        // stream out one file at a time instead of being scanned up front.
        let reader = HudiSnapshotReader::open(&path);
        let files = reader.parquet_files().map_err(map_lh)?;
        Ok(Self {
            files: ParquetFilesSource::new(files),
        })
    }
}

impl crate::source::Source for HudiSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable()
    }

    fn read_batch(&mut self) -> impl Future<Output = ConnectorResult<Option<RecordBatch>>> + Send {
        let batch = self.files.next_batch();
        async move { batch }
    }

    fn current_offset(&self) -> Option<Box<dyn std::any::Any + Send>> {
        None
    }

    fn reset(&mut self) {
        self.files.rewind();
    }
}

struct HudiSink {
    writer: HudiCowWriter,
}

impl crate::sink::Sink for HudiSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_bounded()
    }

    fn write_batch(
        &mut self,
        batch: RecordBatch,
    ) -> impl Future<Output = ConnectorResult<()>> + Send {
        let result = self.writer.append(batch).map_err(map_lh);
        async move {
            result?;
            Ok(())
        }
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        Ok(())
    }
}

pub struct HudiSourceDriver;

impl SourceDriver for HudiSourceDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Hudi,
            ConnectorRole::Source,
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = require_path(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSource>>> + Send + 'a>> {
        let path = require_path(config);
        Box::pin(async move {
            let src = HudiSource::open(path?)?;
            Ok(Box::new(src) as Box<dyn DynSource>)
        })
    }
}

pub struct HudiSinkDriver;

impl SinkDriver for HudiSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Hudi,
            ConnectorRole::Sink,
            ConnectorCapabilities::new().with_bounded(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = require_path(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>> {
        let path = require_path(config);
        Box::pin(async move {
            let writer = HudiCowWriter::open(&path?);
            let sink = HudiSink { writer };
            Ok(Box::new(sink) as Box<dyn DynSink>)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::config::ConnectorConfig;
    use crate::registry::driver::{SinkDriver, SourceDriver};
    use crate::sink::DynSink;
    use crate::source::DynSource;

    use super::{IcebergSinkDriver, IcebergSourceDriver};

    fn make_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)) as _,
                Arc::new(StringArray::from(names)) as _,
            ],
        )
        .expect("valid batch")
    }

    /// Verify Iceberg end-to-end: sink write → flush → source read round-trips data.
    #[tokio::test]
    async fn iceberg_sink_insert_then_source_select() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();

        let config = ConnectorConfig::new("test_table", "iceberg").with_property("path", &path);

        // ── INSERT: write two batches through the sink driver ─────────────────
        let sink_driver = IcebergSinkDriver;
        sink_driver.validate(&config).expect("validate sink");

        let mut sink = sink_driver.open(&config).await.expect("open sink");
        sink.write_batch_dyn(make_batch(vec![1, 2, 3], vec!["a", "b", "c"]))
            .await
            .expect("write batch 1");
        sink.write_batch_dyn(make_batch(vec![4, 5], vec!["d", "e"]))
            .await
            .expect("write batch 2");
        sink.flush_dyn().await.expect("flush");
        drop(sink);

        // ── SELECT: read all rows back through the source driver ──────────────
        let source_driver = IcebergSourceDriver;
        source_driver.validate(&config).expect("validate source");

        let mut source = source_driver.open(&config).await.expect("open source");

        let mut total_rows = 0usize;
        let mut ids_seen: Vec<i64> = Vec::new();
        while let Some(batch) = source.read_batch_dyn().await.expect("read_batch") {
            total_rows += batch.num_rows();
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64");
            ids_seen.extend((0..id_col.len()).map(|i| id_col.value(i)));
        }

        assert_eq!(total_rows, 5, "expected 5 rows after two appends");
        ids_seen.sort_unstable();
        assert_eq!(ids_seen, vec![1, 2, 3, 4, 5]);
    }

    /// Verify that a second append creates a new snapshot and both are readable.
    #[tokio::test]
    async fn iceberg_two_commits_both_visible() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let config = ConnectorConfig::new("t", "iceberg").with_property("path", &path);

        // First commit
        let mut sink1 = IcebergSinkDriver.open(&config).await.expect("open sink 1");
        sink1
            .write_batch_dyn(make_batch(vec![10], vec!["x"]))
            .await
            .expect("write 1");
        sink1.flush_dyn().await.expect("flush 1");
        drop(sink1);

        // Second commit
        let mut sink2 = IcebergSinkDriver.open(&config).await.expect("open sink 2");
        sink2
            .write_batch_dyn(make_batch(vec![20], vec!["y"]))
            .await
            .expect("write 2");
        sink2.flush_dyn().await.expect("flush 2");
        drop(sink2);

        // Scan should see both rows
        let mut source = IcebergSourceDriver
            .open(&config)
            .await
            .expect("open source");
        let mut total = 0usize;
        while let Some(b) = source.read_batch_dyn().await.expect("read") {
            total += b.num_rows();
        }
        assert_eq!(total, 2, "both committed rows must be visible");
    }
}
