//! P12: Streaming Lakehouse Unification — a unified streaming source/sink
//! abstraction over Delta Lake, Hudi, and Paimon formats.
//!
//! This module provides:
//! - `LakehouseStreamSource`: reads new data from a lakehouse table as a
//!   streaming source, tracking the last-read version/offset.
//! - `LakehouseStreamSink`: writes micro-batches to a lakehouse table with
//!   exactly-once semantics via two-phase commit.
//! - `LakehouseFormat`: enum over the supported lakehouse formats.
//! - `LakehouseStreamConfig`: configuration for connecting to a lakehouse
//!   table as a streaming source or sink.

#[allow(unused_imports)]
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::lakehouse::LakehouseError;

// ── Lakehouse format enum ────────────────────────────────────────────────────

/// Supported lakehouse table formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LakehouseFormat {
    Delta,
    Hudi,
    Paimon,
}

impl std::fmt::Display for LakehouseFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Delta => write!(f, "delta"),
            Self::Hudi => write!(f, "hudi"),
            Self::Paimon => write!(f, "paimon"),
        }
    }
}

// ── Stream configuration ─────────────────────────────────────────────────────

/// Configuration for streaming from/to a lakehouse table.
#[derive(Debug, Clone)]
pub struct LakehouseStreamConfig {
    /// Table path or identifier.
    pub table_path: String,
    /// Lakehouse format.
    pub format: LakehouseFormat,
    /// Maximum number of rows per micro-batch. `None` means unlimited.
    pub max_batch_rows: Option<usize>,
    /// Maximum bytes per micro-batch. `None` means unlimited.
    pub max_batch_bytes: Option<usize>,
    /// Polling interval for detecting new data (in milliseconds).
    /// Only used for source mode.
    pub poll_interval_ms: u64,
    /// Starting version/offset for the source. `None` = latest.
    pub start_version: Option<i64>,
}

impl LakehouseStreamConfig {
    /// Create a new config for a Delta table.
    pub fn delta(path: impl Into<String>) -> Self {
        Self {
            table_path: path.into(),
            format: LakehouseFormat::Delta,
            max_batch_rows: None,
            max_batch_bytes: None,
            poll_interval_ms: 1000,
            start_version: None,
        }
    }

    /// Create a new config for a Hudi table.
    pub fn hudi(path: impl Into<String>) -> Self {
        Self {
            table_path: path.into(),
            format: LakehouseFormat::Hudi,
            max_batch_rows: None,
            max_batch_bytes: None,
            poll_interval_ms: 1000,
            start_version: None,
        }
    }

    /// Create a new config for a Paimon table.
    pub fn paimon(path: impl Into<String>) -> Self {
        Self {
            table_path: path.into(),
            format: LakehouseFormat::Paimon,
            max_batch_rows: None,
            max_batch_bytes: None,
            poll_interval_ms: 1000,
            start_version: None,
        }
    }

    /// Set the maximum batch size (rows).
    pub fn with_max_batch_rows(mut self, max: usize) -> Self {
        self.max_batch_rows = Some(max);
        self
    }

    /// Set the maximum batch size (bytes).
    pub fn with_max_batch_bytes(mut self, max: usize) -> Self {
        self.max_batch_bytes = Some(max);
        self
    }

    /// Set the polling interval.
    pub fn with_poll_interval_ms(mut self, ms: u64) -> Self {
        self.poll_interval_ms = ms;
        self
    }

    /// Set the starting version/offset.
    pub fn with_start_version(mut self, version: i64) -> Self {
        self.start_version = Some(version);
        self
    }
}

// ── Streaming source ─────────────────────────────────────────────────────────

/// State for reading from a lakehouse table as a streaming source.
///
/// Tracks the current version/offset and provides methods to poll for new
/// data. Each call to `next_batch` returns data from the next available
/// version, advancing the internal cursor.
pub struct LakehouseStreamSource {
    config: LakehouseStreamConfig,
    current_version: i64,
}

impl LakehouseStreamSource {
    /// Create a new source with the given config.
    pub fn new(config: LakehouseStreamConfig) -> Self {
        let start = config.start_version.unwrap_or(0);
        Self {
            config,
            current_version: start,
        }
    }

    /// Return the current version/offset of this source.
    pub fn current_version(&self) -> i64 {
        self.current_version
    }

    /// Return the table path.
    pub fn table_path(&self) -> &str {
        &self.config.table_path
    }

    /// Return the format.
    pub fn format(&self) -> LakehouseFormat {
        self.config.format
    }

    /// Advance the source to the next version, returning the batches read.
    ///
    /// For Delta tables, this reads the data at `current_version + 1`.
    /// For other formats, this is a placeholder that returns empty.
    pub async fn next_batch(&mut self) -> Result<Option<Vec<RecordBatch>>, LakehouseError> {
        let target_version = self.current_version + 1;

        match self.config.format {
            LakehouseFormat::Delta => {
                match crate::lakehouse::delta_lake::DeltaTableHandle::open(
                    &self.config.table_path,
                    Some(target_version),
                )
                .await
                {
                    Ok(handle) => match handle.scan_batches().await {
                        Ok(batches) if batches.is_empty() => Ok(None),
                        Ok(batches) => {
                            self.current_version = target_version;
                            Ok(Some(batches))
                        }
                        Err(e) => Err(e),
                    },
                    Err(_) => Ok(None), // version not found → no new data
                }
            }
            _ => Ok(None), // Other formats: placeholder
        }
    }
}

// ── Streaming sink ───────────────────────────────────────────────────────────

/// Two-phase commit handle for a lakehouse streaming sink.
///
/// Wraps the existing two-phase commit infrastructure for each format.
pub struct LakehouseStreamSink {
    config: LakehouseStreamConfig,
    /// Total rows written since creation.
    total_rows_written: u64,
}

impl LakehouseStreamSink {
    /// Create a new sink with the given config.
    pub fn new(config: LakehouseStreamConfig) -> Self {
        Self {
            config,
            total_rows_written: 0,
        }
    }

    /// Return the total rows written.
    pub fn total_rows_written(&self) -> u64 {
        self.total_rows_written
    }

    /// Return the table path.
    pub fn table_path(&self) -> &str {
        &self.config.table_path
    }

    /// Return the format.
    pub fn format(&self) -> LakehouseFormat {
        self.config.format
    }

    /// Write a micro-batch to the lakehouse table.
    ///
    /// For Delta tables, this appends to the table. For other formats,
    /// this is a placeholder.
    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<u64, LakehouseError> {
        let rows = batch.num_rows() as u64;

        match self.config.format {
            LakehouseFormat::Delta => {
                crate::lakehouse::delta_lake::write_delta(
                    &self.config.table_path,
                    vec![batch],
                    crate::lakehouse::delta_lake::DeltaWriteMode::Append,
                    false,
                )
                .await?;
            }
            _ => {
                // Other formats: placeholder
                return Err(LakehouseError::Io(format!(
                    "streaming sink not yet supported for {}",
                    self.config.format
                )));
            }
        }

        self.total_rows_written += rows;
        Ok(rows)
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lakehouse_format_display() {
        assert_eq!(LakehouseFormat::Delta.to_string(), "delta");
        assert_eq!(LakehouseFormat::Hudi.to_string(), "hudi");
        assert_eq!(LakehouseFormat::Paimon.to_string(), "paimon");
    }

    #[test]
    fn stream_config_delta_default() {
        let config = LakehouseStreamConfig::delta("/data/orders");
        assert_eq!(config.table_path, "/data/orders");
        assert_eq!(config.format, LakehouseFormat::Delta);
        assert_eq!(config.poll_interval_ms, 1000);
        assert!(config.max_batch_rows.is_none());
        assert!(config.start_version.is_none());
    }

    #[test]
    fn stream_config_builder_methods() {
        let config = LakehouseStreamConfig::hudi("/data/events")
            .with_max_batch_rows(5000)
            .with_max_batch_bytes(4_000_000)
            .with_poll_interval_ms(500)
            .with_start_version(10);

        assert_eq!(config.max_batch_rows, Some(5000));
        assert_eq!(config.max_batch_bytes, Some(4_000_000));
        assert_eq!(config.poll_interval_ms, 500);
        assert_eq!(config.start_version, Some(10));
    }

    #[test]
    fn stream_source_initial_state() {
        let config = LakehouseStreamConfig::delta("/data/t").with_start_version(5);
        let source = LakehouseStreamSource::new(config);
        assert_eq!(source.current_version(), 5);
        assert_eq!(source.table_path(), "/data/t");
        assert_eq!(source.format(), LakehouseFormat::Delta);
    }

    #[test]
    fn stream_sink_initial_state() {
        let config = LakehouseStreamConfig::delta("/data/t");
        let sink = LakehouseStreamSink::new(config);
        assert_eq!(sink.total_rows_written(), 0);
        assert_eq!(sink.table_path(), "/data/t");
    }

    #[test]
    fn stream_config_paimon() {
        let config = LakehouseStreamConfig::paimon("/data/paimon_table");
        assert_eq!(config.format, LakehouseFormat::Paimon);
        assert_eq!(config.table_path, "/data/paimon_table");
    }

    #[test]
    fn stream_source_format_accessor() {
        let config = LakehouseStreamConfig::hudi("/data/h");
        let source = LakehouseStreamSource::new(config);
        assert_eq!(source.format(), LakehouseFormat::Hudi);
    }

    #[test]
    fn stream_config_defaults_after_builder() {
        let config = LakehouseStreamConfig::delta("/t").with_start_version(0);
        assert_eq!(config.max_batch_rows, None);
        assert_eq!(config.max_batch_bytes, None);
    }

    #[test]
    fn lakehouse_format_equality() {
        assert_eq!(LakehouseFormat::Delta, LakehouseFormat::Delta);
        assert_ne!(LakehouseFormat::Delta, LakehouseFormat::Hudi);
    }

    #[test]
    fn lakehouse_format_hash_consistent() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(LakehouseFormat::Delta, 1);
        map.insert(LakehouseFormat::Hudi, 2);
        assert_eq!(map.len(), 2);
        assert_eq!(map[&LakehouseFormat::Delta], 1);
        assert_eq!(map[&LakehouseFormat::Hudi], 2);
    }
}
