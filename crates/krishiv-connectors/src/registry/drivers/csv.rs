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

/// Strictly parse the optional `delimiter`: it must be exactly one byte.
/// `"||"` or `"\t"` typed as two characters silently splitting on `|` or
/// `\` would corrupt every row.
fn parse_delimiter(config: &ConnectorConfig) -> ConnectorResult<Option<u8>> {
    match config.get("delimiter") {
        None => Ok(None),
        Some(value) => match value.as_bytes() {
            [b] => Ok(Some(*b)),
            _ => Err(crate::error::ConnectorError::Config {
                message: format!(
                    "csv option 'delimiter' must be exactly one byte, got '{value}' \
                     ({} bytes)",
                    value.len()
                ),
            }),
        },
    }
}

/// Strictly parse the optional `batch_size`: a malformed value is an ERROR,
/// not a silent fall-back to the default.
fn parse_batch_size(config: &ConnectorConfig) -> ConnectorResult<Option<usize>> {
    match config.get("batch_size") {
        None => Ok(None),
        Some(value) => {
            value
                .parse::<usize>()
                .map(Some)
                .map_err(|_| crate::error::ConnectorError::Config {
                    message: format!(
                        "csv option 'batch_size' must be a positive integer, got '{value}'"
                    ),
                })
        }
    }
}

/// Strictly parse the optional `has_header`: only `true`/`false` (any case)
/// and `1`/`0` are accepted. It used to be `value == "true" || value == "1"`,
/// so `True`, `yes` or `TRUE` silently meant *false* — the header row was
/// ingested as data, or a file was written without one.
fn parse_has_header(config: &ConnectorConfig) -> ConnectorResult<Option<bool>> {
    match config.get("has_header") {
        None => Ok(None),
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Ok(Some(true)),
            "false" | "0" => Ok(Some(false)),
            _ => Err(crate::error::ConnectorError::Config {
                message: format!("csv option 'has_header' must be true or false, got '{value}'"),
            }),
        },
    }
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
        parse_has_header(config)?;
        let _ = require_path(config)?;
        let _ = parse_delimiter(config)?;
        let _ = parse_batch_size(config)?;
        Ok(())
    }

    fn open<'a>(&'a self, config: &'a ConnectorConfig) -> OpenSourceFuture<'a> {
        Box::pin(async move {
            let path = require_path(config)?;
            let mut opts = CsvOptions::default();
            if let Some(has_header) = parse_has_header(config)? {
                opts = opts.with_has_header(has_header);
            }
            if let Some(delimiter) = parse_delimiter(config)? {
                opts = opts.with_delimiter(delimiter);
            }
            if let Some(batch_size) = parse_batch_size(config)? {
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
        // `SyncedFile` exists precisely so `flush` fsyncs and leaves the sink
        // usable for the next cycle — the descriptor's resumable_flush claim.
        ConnectorCapabilities::new()
            .with_bounded()
            .with_resumable_flush()
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
        parse_has_header(config)?;
        let _ = require_path(config)?;
        let _ = parse_delimiter(config)?;
        Ok(())
    }

    fn open<'a>(&'a self, config: &'a ConnectorConfig) -> OpenSinkFuture<'a> {
        Box::pin(async move {
            let path = require_path(config)?;
            let mut builder = arrow::csv::WriterBuilder::new();
            if let Some(has_header) = parse_has_header(config)? {
                builder = builder.with_header(has_header);
            }
            if let Some(delimiter) = parse_delimiter(config)? {
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn config(pairs: &[(&str, &str)]) -> ConnectorConfig {
        let mut c = ConnectorConfig::new("probe", "csv");
        for (k, v) in pairs {
            c = c.with_property(*k, *v);
        }
        c
    }

    /// The descriptor's advertised capabilities must match what an opened
    /// sink instance actually reports (resumable flush via `SyncedFile`).
    #[tokio::test]
    async fn csv_sink_descriptor_matches_opened_instance_capabilities() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("caps.csv");
        let cfg = config(&[("path", path.to_str().expect("utf-8 path"))]);

        let sink = CsvSinkDriver.open(&cfg).await.expect("open csv sink");
        assert_eq!(
            CsvSinkDriver.descriptor().default_capabilities,
            sink.capabilities(),
            "descriptor and opened-instance capabilities must agree"
        );
    }

    /// `has_header` is strict like every other option here: `yes` / `True`
    /// used to silently mean *false*, ingesting the header row as data.
    #[test]
    fn has_header_is_parsed_strictly() {
        assert_eq!(
            parse_has_header(&config(&[("has_header", "TRUE")])).unwrap(),
            Some(true)
        );
        assert_eq!(
            parse_has_header(&config(&[("has_header", "0")])).unwrap(),
            Some(false)
        );
        assert!(parse_has_header(&config(&[("has_header", "yes")])).is_err());
        let bad = config(&[("path", "/tmp/x.csv"), ("has_header", "yes")]);
        assert!(
            CsvSourceDriver.validate(&bad).is_err(),
            "validate refuses it too"
        );
    }

    #[test]
    fn multi_byte_delimiter_is_a_validate_error() {
        let bad = config(&[("path", "/tmp/x.csv"), ("delimiter", "||")]);
        assert!(CsvSourceDriver.validate(&bad).is_err());

        let ok = config(&[("path", "/tmp/x.csv"), ("delimiter", "|")]);
        assert!(CsvSourceDriver.validate(&ok).is_ok());
        assert_eq!(parse_delimiter(&ok).unwrap(), Some(b'|'));
    }

    #[test]
    fn malformed_batch_size_is_a_validate_error() {
        let bad = config(&[("path", "/tmp/x.csv"), ("batch_size", "10k")]);
        assert!(CsvSourceDriver.validate(&bad).is_err());

        let ok = config(&[("path", "/tmp/x.csv"), ("batch_size", "256")]);
        assert!(CsvSourceDriver.validate(&ok).is_ok());
        assert_eq!(parse_batch_size(&ok).unwrap(), Some(256));
    }
}
