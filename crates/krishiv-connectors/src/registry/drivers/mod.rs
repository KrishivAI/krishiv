//! Built-in connector driver registrations.

mod csv;
mod parquet;
mod s3;
mod two_phase;

#[cfg(feature = "avro")]
mod avro;

#[cfg(feature = "kafka")]
mod kafka;

#[cfg(feature = "lakehouse")]
mod lakehouse;

pub use csv::CsvSourceDriver;
pub use parquet::{ParquetSinkDriver, ParquetSourceDriver};
pub use s3::{S3SinkDriver, S3SourceDriver};
pub use two_phase::LocalParquetTwoPhaseSinkDriver;

#[cfg(feature = "avro")]
pub use avro::{AvroSinkDriver, AvroSourceDriver};

#[cfg(feature = "kafka")]
pub use kafka::{KafkaSinkDriver, KafkaSourceDriver};

#[cfg(feature = "lakehouse")]
pub use lakehouse::{
    DeltaSinkDriver, DeltaSourceDriver, HudiSinkDriver, HudiSourceDriver, IcebergSinkDriver,
    IcebergSourceDriver,
};
