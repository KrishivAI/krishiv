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

/// Strictly parse the optional `recursive` flag: `"true"`/`"false"`
/// (case-insensitive) only. A malformed value is an ERROR, not a silent
/// non-recursive scan.
fn parse_recursive(config: &ConnectorConfig) -> ConnectorResult<bool> {
    match config.get("recursive") {
        None => Ok(false),
        Some(v) if v.eq_ignore_ascii_case("true") => Ok(true),
        Some(v) if v.eq_ignore_ascii_case("false") => Ok(false),
        Some(v) => Err(crate::error::ConnectorError::Config {
            message: format!("parquet option 'recursive' must be 'true' or 'false', got '{v}'"),
        }),
    }
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
        let _ = parse_recursive(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSource>>> + Send + 'a>> {
        Box::pin(async move {
            let path = require_path(config)?;
            let recursive = parse_recursive(config)?;
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn config(pairs: &[(&str, &str)]) -> ConnectorConfig {
        let mut c = ConnectorConfig::new("probe", "parquet_directory");
        for (k, v) in pairs {
            c = c.with_property(*k, *v);
        }
        c
    }

    #[test]
    fn malformed_recursive_flag_is_a_validate_error() {
        let bad = config(&[("path", "/tmp"), ("recursive", "yes")]);
        assert!(ParquetDirectorySourceDriver.validate(&bad).is_err());

        for (v, want) in [("true", true), ("FALSE", false)] {
            let cfg = config(&[("path", "/tmp"), ("recursive", v)]);
            assert!(ParquetDirectorySourceDriver.validate(&cfg).is_ok());
            assert_eq!(parse_recursive(&cfg).unwrap(), want);
        }
    }
}
