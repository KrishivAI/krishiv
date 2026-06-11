//! Built-in connector driver registrations.

mod parquet;
mod s3;
mod two_phase;

#[cfg(feature = "kafka")]
mod kafka;

#[cfg(feature = "lakehouse")]
mod lakehouse;

pub use parquet::{ParquetSinkDriver, ParquetSourceDriver};
pub use s3::{S3SinkDriver, S3SourceDriver};
pub use two_phase::LocalParquetTwoPhaseSinkDriver;

#[cfg(feature = "kafka")]
pub use kafka::{KafkaSinkDriver, KafkaSourceDriver};

#[cfg(feature = "lakehouse")]
pub use lakehouse::{
    DeltaSinkDriver, DeltaSourceDriver, HudiSinkDriver, HudiSourceDriver, IcebergSinkDriver,
    IcebergSourceDriver,
};
