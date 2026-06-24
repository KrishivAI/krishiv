#![forbid(unsafe_code)]

//! Public facade for `krishiv-connectors`.
//!
//! User-facing source, sink, capabilities, transactional, and quality interfaces.

// Submodules with implementations
#[cfg(feature = "avro")]
pub mod avro;
#[cfg(feature = "cassandra")]
pub mod cassandra_sink;
#[cfg(feature = "lakehouse")]
pub mod cdc;
#[cfg(all(feature = "lakehouse", feature = "kafka"))]
pub mod cdc_router;
pub mod csv_json;
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch_sink;
#[cfg(feature = "hbase")]
pub mod hbase_connector;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "kafka")]
pub mod kafka_transactional_sink;
#[cfg(feature = "kinesis")]
pub mod kinesis;
#[cfg(feature = "lakehouse")]
pub mod lakehouse;
pub mod parquet;
#[cfg(feature = "pulsar-source")]
pub mod pulsar_connector;
pub mod s3;
pub mod schema_normalize;
#[cfg(feature = "schema-registry")]
pub mod schema_registry;
/// T9: SQL connector support (Postgres / MySQL / MSSQL / Oracle).
pub mod sql;
pub mod transactional;
#[cfg(feature = "kafka")]
pub mod transactional_kafka;
#[cfg(feature = "two-phase")]
pub mod two_phase_parquet_s3;

// Module facades
pub mod capabilities;
pub mod config;
pub mod error;
pub mod io_contract;
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

#[cfg(test)]
pub mod certification;

// Root re-exports for perfect compatibility
pub use capabilities::{ConnectorCapabilities, ConnectorMaturity, DeliveryGuarantee};
pub use config::ConnectorConfig;
pub use error::{ConnectorError, ConnectorResult};
#[cfg(feature = "kafka")]
pub use kafka_transactional_sink::RdkafkaTransactionalSink;
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
    EpochTransactionLog, InMemoryCommitHandle, InMemoryTwoPhaseCommitSink,
    LocalParquetTwoPhaseCommitSink, ParquetCommitHandle, TransactionalSinkParticipant,
    TwoPhaseCommitSink,
};

#[cfg(all(feature = "vector-sinks", feature = "pgvector"))]
pub use vector::PgvectorSink;
#[cfg(all(feature = "vector-sinks", feature = "qdrant"))]
pub use vector::QdrantSink;
#[cfg(feature = "vector-sinks")]
pub use vector::{
    EmbeddingBatch, InMemoryVectorSink, LanceDbSink, PayloadFilter, PayloadValue, PineconeSink,
    ScoredChunk, VectorSink, VectorSinkConfig, VectorSinkError, VectorSinkRegistry, WeaviateSink,
    point_id_from_doc_epoch, validate_identifier,
};

#[cfg(all(feature = "state", feature = "lakehouse"))]
pub use cdc::CdcOffsetTracker;

#[cfg(feature = "lakehouse")]
pub use lakehouse::{
    AsOfSpec, DeltaEntry, DeltaObjectStoreReader, DeltaOp, DeltaStore, DeltaTableHandle,
    DeltaWriteMode, DistributedIcebergCommitCoordinator, HudiCowWriter, HudiObjectStoreReader,
    HudiObjectStoreWriter, HudiQueryType, HudiSnapshotReader, HudiWriteResult, IcebergFsTable,
    IcebergReference, IcebergReferenceKind, IcebergScanOptions, IcebergTableRef,
    IcebergTwoPhaseCommit, KAFKA_OFFSETS_SUMMARY_KEY, LakehouseError, LakehouseResult,
    LakehouseTable, MemoryDeltaStore, MemoryIcebergTwoPhaseCommit, MemoryLakehouseTable,
    MergeDeltaResult, MultiWriterGuard, PartitionField, PartitionSpecResolver,
    PartitionSpecVersion, RedbDeltaStore, SchemaField, SchemaVersion, StagedSnapshot,
    check_write_precondition, kafka_offsets_json, merge_delta, parse_kafka_offsets_json,
    remove_merge_key_column, write_delta, write_hudi_cow_append, write_hudi_cow_fixture,
    write_hudi_cow_upsert,
};
#[cfg(all(feature = "lakehouse", feature = "kafka"))]
pub use lakehouse::{KafkaDeltaStore, RdkafkaDeltaStore};

pub use io_contract::{
    DatabaseIoOptions, FileFormat, FileLayout, IoCapabilities, IoConnectorKind, KafkaIoOptions,
    SchemaEvolutionMode, SortDirection as FileSortDirection, SortField, WriteDistribution,
    WriteMode,
};
