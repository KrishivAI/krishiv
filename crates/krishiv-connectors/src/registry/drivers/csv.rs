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
    /// `None` once flushed — a flushed sink rejects further writes rather than
    /// silently dropping them.
    writer: Option<arrow::csv::Writer<File>>,
}

impl Sink for CsvFileSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_bounded()
    }

    async fn write_batch(&mut self, batch: arrow::record_batch::RecordBatch) -> ConnectorResult<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| crate::error::ConnectorError::Config {
                message: String::from("csv sink already flushed"),
            })?;
        writer
            .write(&batch)
            .map_err(|error| crate::error::ConnectorError::Schema {
                message: format!("csv sink write failed: {error}"),
            })
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        // arrow-csv writes rows through on every `write`, so durability here is
        // the underlying file's flush; take the writer so the file is closed.
        if let Some(writer) = self.writer.take() {
            use std::io::Write as _;
            let mut file = writer.into_inner();
            file.flush().map_err(crate::error::ConnectorError::Io)?;
        }
        Ok(())
    }
}

pub struct CsvSinkDriver;

impl SinkDriver for CsvSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Csv,
            ConnectorRole::Sink,
            ConnectorCapabilities::new().with_bounded(),
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
            Ok(Box::new(CsvFileSink {
                writer: Some(builder.build(file)),
            }) as Box<dyn DynSink>)
        })
    }
}
