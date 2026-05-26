#![forbid(unsafe_code)]

//! Public facade for `krishiv-connectors`.
//!
//! User-facing source, sink, capabilities, transactional, and quality interfaces.

// Submodules with implementations
pub mod cdc;
pub mod cdc_router;
pub mod feature_store;
pub mod kafka;
pub mod parquet;
pub mod s3;
pub mod transactional;
pub mod transactional_kafka;
pub mod two_phase_parquet_s3;

// Module facades
pub mod capabilities;
pub mod certification;
pub mod config;
pub mod error;
pub mod offset;
pub mod quality;
pub mod sink;
pub mod source;
pub mod two_phase;

#[cfg(test)]
mod tests;

// Root re-exports for perfect compatibility
pub use capabilities::ConnectorCapabilities;
pub use certification::CertificationSuite;
pub use config::ConnectorConfig;
pub use error::{ConnectorError, ConnectorResult};
pub use offset::{CommitHandle, Offset, OffsetCommitter, ParquetOffset};
pub use quality::{
    CompiledDataQualityConfig, CompiledQualityRule, DataQualityCheckResult, DataQualityConfig,
    DataQualityRule, DeadLetterSink, QualityAction, RejectedRow,
};
pub use sink::{AtLeastOnceSinkContract, DynSink, PostWriteOffsetCommitProtocol, Sink};
pub use source::Source;
pub use two_phase::{
    InMemoryCommitHandle, InMemoryTwoPhaseCommitSink, LocalParquetTwoPhaseCommitSink,
    ParquetCommitHandle, TwoPhaseCommitSink,
};

pub use feature_store::{FeatureRow, FeatureStoreSink, InMemoryFeatureStream};

#[cfg(feature = "state")]
pub use cdc::CdcOffsetTracker;
