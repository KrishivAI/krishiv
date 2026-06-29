//! Parquet source and sink drivers.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::ConnectorResult;
use crate::parquet::{ParquetDirectorySource, ParquetSink, ParquetSource};
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::{SinkDriver, SourceDriver};
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::sink::DynSink;
use crate::source::DynSource;

fn require_path(config: &ConnectorConfig) -> ConnectorResult<PathBuf> {
    Ok(PathBuf::from(config.required("path")?))
}

pub struct ParquetSourceDriver;

impl SourceDriver for ParquetSourceDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Parquet,
            ConnectorRole::Source,
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable()
                .with_checkpoint(),
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
        Box::pin(async move {
            let path = require_path(config)?;
            let source = ParquetSource::open(path)?;
            Ok(Box::new(source) as Box<dyn DynSource>)
        })
    }

    fn estimated_row_count(&self, config: &ConnectorConfig) -> Option<u64> {
        let path = require_path(config).ok()?;
        ParquetSource::open(path).ok()?.row_count()
    }
}

/// Driver for [`ParquetDirectorySource`] — opens all `.parquet` files under a
/// directory (optionally recursive) and reads them in sorted order.
///
/// Required config key: `path` — path to the root directory.
/// Optional config key: `recursive` — `"true"` to scan sub-directories.
pub struct ParquetDirectorySourceDriver;

impl SourceDriver for ParquetDirectorySourceDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::ParquetDirectory,
            ConnectorRole::Source,
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable()
                .with_checkpoint(),
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
        Box::pin(async move {
            let path = require_path(config)?;
            let recursive = config
                .get("recursive")
                .map(|v| v == "true")
                .unwrap_or(false);
            let source = ParquetDirectorySource::open(path, recursive)?;
            Ok(Box::new(source) as Box<dyn DynSource>)
        })
    }
}

pub struct ParquetSinkDriver;

impl SinkDriver for ParquetSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::Parquet,
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
        Box::pin(async move {
            let path = require_path(config)?;
            let sink = ParquetSink::create(path)?;
            Ok(Box::new(sink) as Box<dyn DynSink>)
        })
    }
}
