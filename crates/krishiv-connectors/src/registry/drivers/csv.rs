//! CSV file source and sink drivers.

use std::any::Any;
use std::fs::File;
use std::path::PathBuf;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::csv_json::{CsvOptions, CsvSource};
use crate::error::ConnectorResult;
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::{OpenSinkFuture, OpenSourceFuture, SinkDriver, SourceDriver};
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::sink::{DynSink, Sink};
use crate::source::{DynSource, Source};

fn require_path(config: &ConnectorConfig) -> ConnectorResult<PathBuf> {
    Ok(PathBuf::from(config.required("path")?))
}

struct CsvFileSource {
    inner: CsvSource,
}

impl Source for CsvFileSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        self.inner.capabilities()
    }

    fn source_schema(&self) -> Option<arrow::datatypes::SchemaRef> {
        Some(self.inner.schema().clone())
    }

    async fn read_batch(&mut self) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
        self.inner.read_batch()
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        None
    }

    fn reset(&mut self) {
        self.inner.reset();
    }
}

pub struct CsvSourceDriver;

impl SourceDriver for CsvSourceDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Csv,
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

    fn open<'a>(&'a self, config: &'a ConnectorConfig) -> OpenSourceFuture<'a> {
        Box::pin(async move {
            let path = require_path(config)?;
            let mut opts = CsvOptions::default();
            if let Some(value) = config.get("has_header") {
                opts = opts.with_has_header(value == "true" || value == "1");
            }
            if let Some(value) = config.get("delimiter") {
                let delimiter = value.as_bytes().first().copied().unwrap_or(b',');
                opts = opts.with_delimiter(delimiter);
            }
            if let Some(value) = config.get("batch_size")
                && let Ok(batch_size) = value.parse::<usize>()
            {
                opts = opts.with_batch_size(batch_size);
            }
            let file = File::open(&path).map_err(crate::error::ConnectorError::Io)?;
            let source = CsvSource::open(file, opts)?;
            Ok(Box::new(CsvFileSource { inner: source }) as Box<dyn DynSource>)
        })
    }
}

// ── Sink ──────────────────────────────────────────────────────────────────────

/// CSV file sink.
///
/// `ConnectorKind::Csv` had a registered *source* driver but no sink driver, so
/// every registry-generic surface (`sql_ddl`, and after #197 `sql_job` /
/// `distributed_job` / `python_sink`) failed with "no sink driver registered
/// for kind 'csv'" while the reachability matrix claimed CSV sinks were
/// reachable. The dedicated writer in `krishiv-api::connector_runtime` covered
/// only the ad-hoc SQL job path. This driver makes the claim true everywhere.
///
/// Config: `path` (required), `delimiter`, `has_header` (default true).
struct CsvFileSink {
    /// Kept open across flushes: `flush` syncs what has been written and leaves
    /// the sink usable, so a streaming job can flush every cycle (the
    /// `resumable_flush` capability). Taking the writer here would truncate the
    /// next cycle's writes.
    writer: arrow::csv::Writer<SyncedFile>,
    file: std::sync::Arc<std::sync::Mutex<File>>,
}

/// `Write` adapter that shares the underlying file with the sink so `flush` can
/// `sync_data()` it without consuming the arrow writer.
struct SyncedFile(std::sync::Arc<std::sync::Mutex<File>>);

impl std::io::Write for SyncedFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut file = self
            .0
            .lock()
            .map_err(|_| std::io::Error::other("csv sink file lock poisoned"))?;
        file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut file = self
            .0
            .lock()
            .map_err(|_| std::io::Error::other("csv sink file lock poisoned"))?;
        file.flush()
    }
}

impl Sink for CsvFileSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_bounded()
    }

    async fn write_batch(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> ConnectorResult<()> {
        self.writer
            .write(&batch)
            .map_err(|error| crate::error::ConnectorError::Schema {
                message: format!("csv sink write failed: {error}"),
            })
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        // arrow-csv writes rows straight through, so durability is the file's:
        // fsync it and leave the writer open for the next batch/cycle.
        let file = self
            .file
            .lock()
            .map_err(|_| crate::error::ConnectorError::Config {
                message: String::from("csv sink file lock poisoned"),
            })?;
        file.sync_data().map_err(crate::error::ConnectorError::Io)
    }
}

pub struct CsvSinkDriver;

impl SinkDriver for CsvSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Csv,
            ConnectorRole::Sink,
            ConnectorCapabilities::new()
                .with_bounded()
                .with_resumable_flush(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = require_path(config)?;
        Ok(())
    }

    fn open<'a>(&'a self, config: &'a ConnectorConfig) -> OpenSinkFuture<'a> {
        Box::pin(async move {
            let path = require_path(config)?;
            let mut builder = arrow::csv::WriterBuilder::new();
            if let Some(value) = config.get("has_header") {
                builder = builder.with_header(value == "true" || value == "1");
            }
            if let Some(value) = config.get("delimiter")
                && let Some(delimiter) = value.as_bytes().first().copied()
            {
                builder = builder.with_delimiter(delimiter);
            }
            let file = File::create(&path).map_err(crate::error::ConnectorError::Io)?;
            let shared = std::sync::Arc::new(std::sync::Mutex::new(file));
            Ok(Box::new(CsvFileSink {
                writer: builder.build(SyncedFile(shared.clone())),
                file: shared,
            }) as Box<dyn DynSink>)
        })
    }
}
