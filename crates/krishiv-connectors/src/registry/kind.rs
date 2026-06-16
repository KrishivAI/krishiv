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
    Csv,
    #[cfg(feature = "avro")]
    Avro,
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
    #[cfg(feature = "kafka")]
    KafkaTransactional,
    #[cfg(feature = "kinesis")]
    Kinesis,
    #[cfg(feature = "pulsar-source")]
    Pulsar,
    #[cfg(feature = "elasticsearch")]
    Elasticsearch,
    #[cfg(feature = "cassandra")]
    Cassandra,
    #[cfg(feature = "hbase")]
    HBase,
}

impl ConnectorKind {
    /// Parse a configuration kind string into a typed connector kind.
    pub fn parse(raw: &str) -> ConnectorResult<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            "csv" => Ok(Self::Csv),
            #[cfg(feature = "avro")]
            "avro" => Ok(Self::Avro),
            #[cfg(not(feature = "avro"))]
            "avro" => Err(ConnectorError::Unsupported {
                message: "avro connector requires the `avro` feature".into(),
            }),
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
            #[cfg(feature = "kafka")]
            "kafka-transactional" | "kafka_transactional" => Ok(Self::KafkaTransactional),
            #[cfg(not(feature = "kafka"))]
            "kafka-transactional" | "kafka_transactional" => Err(ConnectorError::Unsupported {
                message: "kafka-transactional connector requires the `kafka` feature".into(),
            }),
            #[cfg(feature = "kinesis")]
            "kinesis" => Ok(Self::Kinesis),
            #[cfg(not(feature = "kinesis"))]
            "kinesis" => Err(ConnectorError::Unsupported {
                message: "kinesis connector requires the `kinesis` feature".into(),
            }),
            #[cfg(feature = "pulsar-source")]
            "pulsar" => Ok(Self::Pulsar),
            #[cfg(not(feature = "pulsar-source"))]
            "pulsar" => Err(ConnectorError::Unsupported {
                message: "pulsar connector requires the `pulsar-source` feature".into(),
            }),
            #[cfg(feature = "elasticsearch")]
            "elasticsearch" | "opensearch" => Ok(Self::Elasticsearch),
            #[cfg(not(feature = "elasticsearch"))]
            "elasticsearch" | "opensearch" => Err(ConnectorError::Unsupported {
                message: "elasticsearch connector requires the `elasticsearch` feature".into(),
            }),
            #[cfg(feature = "cassandra")]
            "cassandra" | "scylladb" => Ok(Self::Cassandra),
            #[cfg(not(feature = "cassandra"))]
            "cassandra" | "scylladb" => Err(ConnectorError::Unsupported {
                message: "cassandra connector requires the `cassandra` feature".into(),
            }),
            #[cfg(feature = "hbase")]
            "hbase" => Ok(Self::HBase),
            #[cfg(not(feature = "hbase"))]
            "hbase" => Err(ConnectorError::Unsupported {
                message: "hbase connector requires the `hbase` feature".into(),
            }),
            other => Err(ConnectorError::Config {
                message: format!("unknown connector kind '{other}'"),
            }),
        }
    }

    /// Return the repository-published maturity for this connector kind.
    pub fn default_maturity(self) -> ConnectorMaturity {
        match self {
            Self::Parquet | Self::S3 | Self::Csv => ConnectorMaturity::Preview,
            #[cfg(feature = "avro")]
            Self::Avro => ConnectorMaturity::Preview,
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
            #[cfg(feature = "kafka")]
            Self::KafkaTransactional => ConnectorMaturity::Preview,
            #[cfg(feature = "kinesis")]
            Self::Kinesis => ConnectorMaturity::Experimental,
            #[cfg(feature = "pulsar-source")]
            Self::Pulsar => ConnectorMaturity::Experimental,
            #[cfg(feature = "elasticsearch")]
            Self::Elasticsearch => ConnectorMaturity::Experimental,
            #[cfg(feature = "cassandra")]
            Self::Cassandra => ConnectorMaturity::Experimental,
            #[cfg(feature = "hbase")]
            Self::HBase => ConnectorMaturity::Experimental,
        }
    }

    /// Return the canonical configuration string for this kind.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Csv => "csv",
            #[cfg(feature = "avro")]
            Self::Avro => "avro",
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
            #[cfg(feature = "kafka")]
            Self::KafkaTransactional => "kafka-transactional",
            #[cfg(feature = "kinesis")]
            Self::Kinesis => "kinesis",
            #[cfg(feature = "pulsar-source")]
            Self::Pulsar => "pulsar",
            #[cfg(feature = "elasticsearch")]
            Self::Elasticsearch => "elasticsearch",
            #[cfg(feature = "cassandra")]
            Self::Cassandra => "cassandra",
            #[cfg(feature = "hbase")]
            Self::HBase => "hbase",
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
