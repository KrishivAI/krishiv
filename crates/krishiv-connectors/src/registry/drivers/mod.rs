//! Built-in connector driver registrations.

mod csv;
#[cfg(feature = "jdbc")]
mod jdbc;
mod parquet;
mod s3;
mod two_phase;

#[cfg(feature = "avro")]
mod avro;

#[cfg(feature = "kafka")]
mod kafka;

#[cfg(feature = "lakehouse")]
mod lakehouse;

#[cfg(feature = "kinesis")]
mod kinesis;

#[cfg(feature = "pulsar-source")]
mod pulsar;

#[cfg(feature = "elasticsearch")]
mod elasticsearch;

#[cfg(feature = "cassandra")]
mod cassandra;

#[cfg(feature = "hbase")]
mod hbase;

pub use csv::CsvSourceDriver;
#[cfg(feature = "jdbc")]
pub use jdbc::{JdbcSinkDriver, JdbcSourceDriver};
pub use parquet::{ParquetDirectorySourceDriver, ParquetSinkDriver, ParquetSourceDriver};
pub use s3::{S3PrefixSourceDriver, S3SinkDriver, S3SourceDriver};
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

#[cfg(feature = "kinesis")]
pub use kinesis::KinesisSourceDriver;

#[cfg(feature = "pulsar-source")]
pub use pulsar::PulsarSourceDriver;

#[cfg(feature = "elasticsearch")]
pub use elasticsearch::ElasticsearchSinkDriver;

#[cfg(feature = "cassandra")]
pub use cassandra::CassandraSinkDriver;

#[cfg(feature = "hbase")]
pub use hbase::HBaseSinkDriver;
