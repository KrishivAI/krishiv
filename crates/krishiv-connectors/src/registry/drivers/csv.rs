//! CSV file source driver.

use std::any::Any;
use std::fs::File;
use std::path::PathBuf;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::csv_json::{CsvOptions, CsvSource};
use crate::error::ConnectorResult;
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::{OpenSourceFuture, SourceDriver};
use crate::registry::kind::{ConnectorKind, ConnectorRole};
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
