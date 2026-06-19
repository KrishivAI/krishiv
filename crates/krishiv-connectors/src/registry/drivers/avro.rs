//! Avro file source and sink drivers.

use std::any::Any;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

use crate::avro::{AvroSink, AvroSource};
use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::ConnectorResult;
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::{OpenSinkFuture, OpenSourceFuture, SinkDriver, SourceDriver};
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::sink::{DynSink, Sink};
use crate::source::{DynSource, Source};

fn require_path(config: &ConnectorConfig) -> ConnectorResult<PathBuf> {
    Ok(PathBuf::from(config.required("path")?))
}

fn batch_size(config: &ConnectorConfig) -> usize {
    config
        .get("batch_size")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1024)
        .max(1)
}

struct AvroFileSource {
    inner: AvroSource,
}

impl Source for AvroFileSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        self.inner.capabilities()
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

struct AvroFileSink {
    inner: Option<AvroSink<BufWriter<File>>>,
    path: PathBuf,
}

impl Sink for AvroFileSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_idempotent()
    }

    async fn write_batch(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> ConnectorResult<()> {
        if self.inner.is_none() {
            let file = File::create(&self.path).map_err(crate::error::ConnectorError::Io)?;
            let sink = AvroSink::new(BufWriter::new(file), batch.schema().as_ref())?;
            self.inner = Some(sink);
        }
        self.inner
            .as_mut()
            .expect("avro sink initialized on first batch")
            .write_batch(&batch)
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        if let Some(sink) = self.inner.take() {
            sink.flush()?;
        }
        Ok(())
    }
}

pub struct AvroSourceDriver;

impl SourceDriver for AvroSourceDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Avro,
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
            let file = File::open(&path).map_err(crate::error::ConnectorError::Io)?;
            let source = AvroSource::open(file, batch_size(config))?;
            Ok(Box::new(AvroFileSource { inner: source }) as Box<dyn DynSource>)
        })
    }
}

pub struct AvroSinkDriver;

impl SinkDriver for AvroSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Avro,
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

    fn open<'a>(&'a self, config: &'a ConnectorConfig) -> OpenSinkFuture<'a> {
        Box::pin(async move {
            let path = require_path(config)?;
            let sink = AvroFileSink { inner: None, path };
            Ok(Box::new(sink) as Box<dyn DynSink>)
        })
    }
}
