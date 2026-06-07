#![forbid(unsafe_code)]

//! Public facade for `krishiv-connectors`.
//!
//! User-facing source, sink, capabilities, transactional, and quality interfaces.

// Submodules with implementations
#[cfg(feature = "lakehouse")]
pub mod cdc;
#[cfg(all(feature = "lakehouse", feature = "kafka"))]
pub mod cdc_router;
pub mod feature_store;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod parquet;
pub mod s3;
#[cfg(feature = "schema-registry")]
pub mod schema_registry;
pub mod schema_normalize;
pub mod transactional;
#[cfg(feature = "kafka")]
pub mod transactional_kafka;
#[cfg(any(feature = "kafka", feature = "state"))]
pub mod two_phase_parquet_s3;

// Module facades
pub mod capabilities;
pub mod certification;
pub mod config;
pub mod error;
pub mod offset;
pub mod quality;
pub mod registry;
pub mod sink;
pub mod source;
pub mod two_phase;

#[cfg(feature = "vector-sinks")]
pub mod vector;

#[cfg(test)]
mod tests;

// Root re-exports for perfect compatibility
pub use capabilities::ConnectorCapabilities;
pub use certification::CertificationSuite;
pub use config::ConnectorConfig;
pub use error::{ConnectorError, ConnectorResult};
pub use offset::{CommitHandle, Offset, OffsetCommitter, ParquetOffset};
pub use quality::{
    CompiledDataQualityConfig, CompiledQualityRule, ConnectorQualityHook, DataQualityCheckResult,
    DataQualityConfig, DataQualityRule, DeadLetterSink, QualityAction, RejectedRow,
};
pub use registry::{
    ConnectorDescriptor, ConnectorKind, ConnectorRegistry, ConnectorRole, OpenedTwoPhaseSink,
    SinkDriver, SourceDriver, TwoPhaseSinkDriver, default_registry,
};
pub use schema_normalize::SchemaNormalizeOperator;
pub use sink::{AtLeastOnceSinkContract, DynSink, PostWriteOffsetCommitProtocol, Sink};
pub use source::{CheckpointSource, DynSource, Source};
pub use two_phase::{
    InMemoryCommitHandle, InMemoryTwoPhaseCommitSink, LocalParquetTwoPhaseCommitSink,
    ParquetCommitHandle, TwoPhaseCommitSink,
};

#[cfg(feature = "vector-sinks")]
pub use vector::{
    EmbeddingBatch, InMemoryVectorSink, LanceDbSink, PayloadFilter, PayloadValue, PineconeSink,
    ScoredChunk, VectorSink, VectorSinkConfig, VectorSinkError, VectorSinkRegistry, WeaviateSink,
    point_id_from_doc_epoch, validate_identifier,
};
#[cfg(all(feature = "vector-sinks", feature = "pgvector"))]
pub use vector::PgvectorSink;
#[cfg(all(feature = "vector-sinks", feature = "qdrant"))]
pub use vector::QdrantSink;

pub use feature_store::{FeatureRow, FeatureStoreSink, InMemoryFeatureStream};

#[cfg(all(feature = "state", feature = "lakehouse"))]
pub use cdc::CdcOffsetTracker;
