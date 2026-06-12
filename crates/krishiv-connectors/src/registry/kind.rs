//! Typed connector identifiers used by the driver registry.

use crate::capabilities::ConnectorMaturity;
use crate::error::{ConnectorError, ConnectorResult};

/// Logical role of a registered connector driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectorRole {
    Source,
    Sink,
    TwoPhaseSink,
    #[cfg(feature = "vector-sinks")]
    VectorSink,
}

/// Stable connector kind identifiers.
///
/// External configuration may still use string `kind` values; parse them through
/// [`ConnectorKind::parse`] at registry boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectorKind {
    Parquet,
    S3,
    #[cfg(feature = "kafka")]
    Kafka,
    TwoPhaseParquet,
    #[cfg(feature = "vector-sinks")]
    MemoryVector,
    #[cfg(all(feature = "vector-sinks", feature = "qdrant"))]
    Qdrant,
    #[cfg(all(feature = "vector-sinks", feature = "pgvector"))]
    Pgvector,
    #[cfg(feature = "vector-sinks")]
    LanceDb,
    #[cfg(feature = "vector-sinks")]
    Weaviate,
    #[cfg(feature = "vector-sinks")]
    Pinecone,
    /// Lakehouse table I/O (implemented in `crate::lakehouse`, not opened via batch drivers).
    #[cfg(feature = "lakehouse")]
    Iceberg,
    #[cfg(feature = "lakehouse")]
    Delta,
    #[cfg(feature = "lakehouse")]
    Hudi,
}

impl ConnectorKind {
    /// Parse a configuration kind string into a typed connector kind.
    pub fn parse(raw: &str) -> ConnectorResult<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            "s3" | "object_store" | "object-store" => Ok(Self::S3),
            #[cfg(feature = "kafka")]
            "kafka" => Ok(Self::Kafka),
            #[cfg(not(feature = "kafka"))]
            "kafka" => Err(ConnectorError::Unsupported {
                message: "kafka connector requires the `kafka` feature".into(),
            }),
            "two-phase-parquet" | "two_phase_parquet" => Ok(Self::TwoPhaseParquet),
            #[cfg(feature = "vector-sinks")]
            "memory-vector" | "memory_vector" | "vector-memory" => Ok(Self::MemoryVector),
            #[cfg(all(feature = "vector-sinks", feature = "qdrant"))]
            "qdrant" => Ok(Self::Qdrant),
            #[cfg(all(feature = "vector-sinks", feature = "pgvector"))]
            "pgvector" => Ok(Self::Pgvector),
            #[cfg(feature = "vector-sinks")]
            "lancedb" | "lance-db" => Ok(Self::LanceDb),
            #[cfg(feature = "vector-sinks")]
            "weaviate" => Ok(Self::Weaviate),
            #[cfg(feature = "vector-sinks")]
            "pinecone" => Ok(Self::Pinecone),
            #[cfg(feature = "lakehouse")]
            "iceberg" => Ok(Self::Iceberg),
            #[cfg(feature = "lakehouse")]
            "delta" => Ok(Self::Delta),
            #[cfg(feature = "lakehouse")]
            "hudi" => Ok(Self::Hudi),
            other => Err(ConnectorError::Config {
                message: format!("unknown connector kind '{other}'"),
            }),
        }
    }

    /// Return the repository-published maturity for this connector kind.
    pub fn default_maturity(self) -> ConnectorMaturity {
        match self {
            Self::Parquet | Self::S3 => ConnectorMaturity::Preview,
            #[cfg(feature = "kafka")]
            Self::Kafka => ConnectorMaturity::Preview,
            Self::TwoPhaseParquet => ConnectorMaturity::Preview,
            #[cfg(feature = "lakehouse")]
            Self::Iceberg => ConnectorMaturity::Preview,
            #[cfg(feature = "lakehouse")]
            Self::Delta | Self::Hudi => ConnectorMaturity::Experimental,
            #[cfg(feature = "vector-sinks")]
            Self::MemoryVector | Self::LanceDb | Self::Weaviate | Self::Pinecone => {
                ConnectorMaturity::Experimental
            }
            #[cfg(all(feature = "vector-sinks", feature = "qdrant"))]
            Self::Qdrant => ConnectorMaturity::Experimental,
            #[cfg(all(feature = "vector-sinks", feature = "pgvector"))]
            Self::Pgvector => ConnectorMaturity::Experimental,
        }
    }

    /// Return the canonical configuration string for this kind.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::S3 => "s3",
            #[cfg(feature = "kafka")]
            Self::Kafka => "kafka",
            Self::TwoPhaseParquet => "two-phase-parquet",
            #[cfg(feature = "vector-sinks")]
            Self::MemoryVector => "memory-vector",
            #[cfg(all(feature = "vector-sinks", feature = "qdrant"))]
            Self::Qdrant => "qdrant",
            #[cfg(all(feature = "vector-sinks", feature = "pgvector"))]
            Self::Pgvector => "pgvector",
            #[cfg(feature = "vector-sinks")]
            Self::LanceDb => "lancedb",
            #[cfg(feature = "vector-sinks")]
            Self::Weaviate => "weaviate",
            #[cfg(feature = "vector-sinks")]
            Self::Pinecone => "pinecone",
            #[cfg(feature = "lakehouse")]
            Self::Iceberg => "iceberg",
            #[cfg(feature = "lakehouse")]
            Self::Delta => "delta",
            #[cfg(feature = "lakehouse")]
            Self::Hudi => "hudi",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_connector_maturity_is_published() {
        assert_eq!(
            ConnectorKind::Parquet.default_maturity(),
            ConnectorMaturity::Preview
        );
        assert_eq!(
            ConnectorKind::S3.default_maturity(),
            ConnectorMaturity::Preview
        );
        assert_eq!(
            ConnectorKind::TwoPhaseParquet.default_maturity(),
            ConnectorMaturity::Preview
        );
    }
}
