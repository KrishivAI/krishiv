//! Typed source/sink and file-layout contracts shared by public builders.

use std::collections::BTreeMap;

use crate::error::{ConnectorError, ConnectorResult};

/// Common I/O endpoint family used for capability negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoConnectorKind {
    File,
    Kafka,
    Database,
    Iceberg,
}

/// Capabilities required before a common reader/writer may execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoCapabilities {
    pub bounded_read: bool,
    pub bounded_write: bool,
    pub streaming_read: bool,
    pub streaming_write: bool,
    pub atomic_commit: bool,
    pub schema_evolution: bool,
}

impl IoConnectorKind {
    pub fn capabilities(self) -> IoCapabilities {
        match self {
            Self::File => IoCapabilities {
                bounded_read: true,
                bounded_write: true,
                streaming_read: false,
                streaming_write: false,
                atomic_commit: true,
                schema_evolution: false,
            },
            Self::Kafka => IoCapabilities {
                bounded_read: false,
                bounded_write: false,
                streaming_read: true,
                streaming_write: true,
                atomic_commit: true,
                schema_evolution: false,
            },
            Self::Database => IoCapabilities {
                bounded_read: true,
                bounded_write: true,
                streaming_read: false,
                streaming_write: false,
                atomic_commit: false,
                schema_evolution: false,
            },
            Self::Iceberg => IoCapabilities {
                bounded_read: true,
                bounded_write: true,
                streaming_read: true,
                streaming_write: true,
                atomic_commit: true,
                schema_evolution: true,
            },
        }
    }
}

/// File encoding selected for a bounded source or sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileFormat {
    Parquet,
    Csv,
    Json,
}

/// Existing-target behavior for bounded writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WriteMode {
    #[default]
    ErrorIfExists,
    Append,
    Overwrite,
    Ignore,
    DynamicOverwrite,
}

/// Schema compatibility policy applied before a write is staged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SchemaEvolutionMode {
    #[default]
    Strict,
    Additive,
    Merge,
}

/// Distribution requested before files are materialized.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum WriteDistribution {
    #[default]
    Unspecified,
    Single,
    Hash {
        columns: Vec<String>,
        partitions: usize,
    },
}

/// Sort direction for a physical file-layout key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Typed sort key used by writers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SortField {
    pub column: String,
    pub direction: SortDirection,
}

/// Physical file-layout controls independent of a concrete connector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileLayout {
    pub partition_by: Vec<String>,
    pub sort_by: Vec<SortField>,
    pub distribution: WriteDistribution,
    pub target_file_size_bytes: Option<u64>,
    pub max_rows_per_file: Option<usize>,
}

impl Default for FileLayout {
    fn default() -> Self {
        Self {
            partition_by: Vec::new(),
            sort_by: Vec::new(),
            distribution: WriteDistribution::Unspecified,
            target_file_size_bytes: None,
            max_rows_per_file: None,
        }
    }
}

impl FileLayout {
    pub fn validate(&self) -> ConnectorResult<()> {
        if self
            .partition_by
            .iter()
            .any(|column| column.trim().is_empty())
        {
            return Err(ConnectorError::Config {
                message: "partition columns must be non-empty".into(),
            });
        }
        if self
            .sort_by
            .iter()
            .any(|field| field.column.trim().is_empty())
        {
            return Err(ConnectorError::Config {
                message: "sort columns must be non-empty".into(),
            });
        }
        if self.target_file_size_bytes == Some(0) || self.max_rows_per_file == Some(0) {
            return Err(ConnectorError::Config {
                message: "file sizing values must be greater than zero".into(),
            });
        }
        if let WriteDistribution::Hash {
            columns,
            partitions,
        } = &self.distribution
            && (columns.is_empty() || *partitions == 0)
        {
            return Err(ConnectorError::Config {
                message: "hash distribution requires columns and a positive partition count".into(),
            });
        }
        Ok(())
    }
}

/// Typed Kafka source configuration used by common reader builders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaIoOptions {
    pub bootstrap_servers: String,
    pub topic: String,
    pub group_id: String,
    pub properties: BTreeMap<String, String>,
}

impl KafkaIoOptions {
    pub fn validate(&self) -> ConnectorResult<()> {
        if self.bootstrap_servers.trim().is_empty()
            || self.topic.trim().is_empty()
            || self.group_id.trim().is_empty()
        {
            return Err(ConnectorError::Config {
                message: "Kafka bootstrap_servers, topic, and group_id are required".into(),
            });
        }
        Ok(())
    }
}

/// Typed JDBC-compatible database endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseIoOptions {
    pub url: String,
    pub table: String,
    pub fetch_size: Option<usize>,
    pub properties: BTreeMap<String, String>,
}

impl DatabaseIoOptions {
    pub fn validate(&self) -> ConnectorResult<()> {
        if self.url.trim().is_empty() || self.table.trim().is_empty() {
            return Err(ConnectorError::Config {
                message: "database URL and table are required".into(),
            });
        }
        if self.fetch_size == Some(0) {
            return Err(ConnectorError::Config {
                message: "database fetch_size must be greater than zero".into(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_layout_rejects_invalid_distribution_and_sizing() {
        let layout = FileLayout {
            distribution: WriteDistribution::Hash {
                columns: vec![],
                partitions: 0,
            },
            ..FileLayout::default()
        };
        assert!(layout.validate().is_err());

        let layout = FileLayout {
            max_rows_per_file: Some(0),
            ..FileLayout::default()
        };
        assert!(layout.validate().is_err());
    }
}
