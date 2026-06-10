//! E6.4 — AWS Kinesis streaming source.
//!
//! Reads records from an Amazon Kinesis Data Stream and exposes them as Arrow
//! [`RecordBatch`] values.
//!
//! # Arrow schema
//!
//! Each Kinesis record becomes one row with fixed columns:
//!
//! | Column                 | Arrow type  | Notes                              |
//! |------------------------|-------------|------------------------------------|
//! | `sequence_number`      | `Utf8`      | Kinesis sequence number            |
//! | `partition_key`        | `Utf8`      | Kinesis partition key              |
//! | `data`                 | `Binary`    | Raw record payload bytes           |
//! | `arrival_timestamp_ms` | `Int64`     | Approx. arrival epoch ms; -1 if absent |
//!
//! # Usage
//!
//! ```no_run
//! # #[cfg(feature = "kinesis")]
//! # async fn example() -> anyhow::Result<()> {
//! use krishiv_connectors::kinesis::{KinesisConfig, KinesisSource, ShardPosition};
//!
//! let cfg = KinesisConfig {
//!     stream_name: "my-stream".into(),
//!     region: "us-east-1".into(),
//!     shard_id: "shardId-000000000000".into(),
//!     start: ShardPosition::TrimHorizon,
//!     batch_size: 100,
//! };
//! let mut src = KinesisSource::new(cfg).await?;
//! while let Some(batch) = src.next_batch().await? {
//!     println!("{} rows", batch.num_rows());
//! }
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use aws_sdk_kinesis::{
    Client,
    config::Region,
    types::{Record, ShardIteratorType},
};
use arrow::array::{BinaryBuilder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::capabilities::ConnectorCapabilities;
use crate::error::{ConnectorError, ConnectorResult};

// ── Schema ────────────────────────────────────────────────────────────────────

/// Fixed Arrow schema for Kinesis records.
pub fn kinesis_arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sequence_number", DataType::Utf8, false),
        Field::new("partition_key", DataType::Utf8, false),
        Field::new("data", DataType::Binary, false),
        Field::new("arrival_timestamp_ms", DataType::Int64, true),
    ]))
}

// ── Config ────────────────────────────────────────────────────────────────────

/// Starting position for a Kinesis shard iterator.
#[derive(Debug, Clone)]
pub enum ShardPosition {
    /// Read from the oldest available record.
    TrimHorizon,
    /// Read only records arriving after the source starts.
    Latest,
    /// Resume from after a specific sequence number.
    AfterSequenceNumber(String),
    /// Resume from (inclusive) a specific sequence number.
    AtSequenceNumber(String),
}

/// Configuration for a single-shard Kinesis source.
#[derive(Debug, Clone)]
pub struct KinesisConfig {
    /// Kinesis stream name or ARN.
    pub stream_name: String,
    /// AWS region (e.g. `"us-east-1"`).
    pub region: String,
    /// Shard to read (e.g. `"shardId-000000000000"`).
    pub shard_id: String,
    /// Where to begin reading.
    pub start: ShardPosition,
    /// Max records per `get_records` call (1–10 000).
    pub batch_size: i32,
}

impl KinesisConfig {
    /// Sensible defaults for a new stream consumer.
    pub fn new(stream_name: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            stream_name: stream_name.into(),
            region: region.into(),
            shard_id: "shardId-000000000000".into(),
            start: ShardPosition::TrimHorizon,
            batch_size: 500,
        }
    }

    pub fn with_shard_id(mut self, id: impl Into<String>) -> Self {
        self.shard_id = id.into();
        self
    }

    pub fn with_start(mut self, pos: ShardPosition) -> Self {
        self.start = pos;
        self
    }

    pub fn with_batch_size(mut self, n: i32) -> Self {
        self.batch_size = n.clamp(1, 10_000);
        self
    }
}

// ── Source ────────────────────────────────────────────────────────────────────

/// Reads records from a single Kinesis shard as Arrow [`RecordBatch`] values.
///
/// Each call to [`next_batch`][KinesisSource::next_batch] issues one
/// `GetRecords` API call and converts the returned records into a batch.
/// Returns `Ok(None)` when the iterator reaches the end of the stream (i.e.
/// `next_shard_iterator` is absent in the response).
pub struct KinesisSource {
    client: Client,
    config: KinesisConfig,
    schema: SchemaRef,
    shard_iterator: Option<String>,
}

impl KinesisSource {
    /// Create and initialise a `KinesisSource`.
    ///
    /// Calls `GetShardIterator` to prime the iterator.  The caller is
    /// responsible for constructing and configuring AWS credentials (via
    /// environment, EC2 instance profile, or explicit config).
    pub async fn new(config: KinesisConfig) -> ConnectorResult<Self> {
        let sdk_config = aws_config::defaults(
            aws_sdk_kinesis::config::BehaviorVersion::latest(),
        )
        .region(Region::new(config.region.clone()))
        .load()
        .await;

        let client = Client::new(&sdk_config);

        let (iter_type, seq) = match &config.start {
            ShardPosition::TrimHorizon => (ShardIteratorType::TrimHorizon, None),
            ShardPosition::Latest => (ShardIteratorType::Latest, None),
            ShardPosition::AfterSequenceNumber(s) => {
                (ShardIteratorType::AfterSequenceNumber, Some(s.clone()))
            }
            ShardPosition::AtSequenceNumber(s) => {
                (ShardIteratorType::AtSequenceNumber, Some(s.clone()))
            }
        };

        let mut req = client
            .get_shard_iterator()
            .stream_name(&config.stream_name)
            .shard_id(&config.shard_id)
            .shard_iterator_type(iter_type);

        if let Some(s) = seq {
            req = req.starting_sequence_number(s);
        }

        let resp = req.send().await.map_err(|e| {
            ConnectorError::Io(std::io::Error::other(e.to_string()))
        })?;

        let shard_iterator = resp.shard_iterator().map(str::to_owned);

        Ok(Self {
            client,
            config,
            schema: kinesis_arrow_schema(),
            shard_iterator,
        })
    }

    /// Arrow schema for Kinesis record batches.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Fetch the next batch of records from the shard.
    ///
    /// Returns `Ok(None)` when the stream is exhausted (end of shard or no
    /// more data and `MillisBehindLatest == 0` with no iterator).
    pub async fn next_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        let iterator = match self.shard_iterator.take() {
            Some(it) => it,
            None => return Ok(None),
        };

        let resp = self
            .client
            .get_records()
            .shard_iterator(&iterator)
            .limit(self.config.batch_size)
            .send()
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        // Advance the iterator for the next call.
        self.shard_iterator = resp.next_shard_iterator().map(str::to_owned);

        let records = resp.records();
        if records.is_empty() {
            return Ok(Some(RecordBatch::new_empty(self.schema.clone())));
        }

        Ok(Some(records_to_batch(&self.schema, records)))
    }

    /// Connector capabilities: unbounded streaming source.
    pub fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::default().with_unbounded()
    }
}

// ── Conversion ────────────────────────────────────────────────────────────────

/// Convert a slice of Kinesis [`Record`] values to an Arrow [`RecordBatch`].
pub fn records_to_batch(schema: &SchemaRef, records: &[Record]) -> RecordBatch {
    let n = records.len();
    let mut seq_col = StringBuilder::with_capacity(n, n * 32);
    let mut key_col = StringBuilder::with_capacity(n, n * 16);
    let mut data_col = BinaryBuilder::with_capacity(n, n * 256);
    let mut ts_col = Int64Builder::with_capacity(n);

    for r in records {
        seq_col.append_value(r.sequence_number());
        key_col.append_value(r.partition_key());
        data_col.append_value(r.data().as_ref());
        match r.approximate_arrival_timestamp() {
            Some(dt) => match dt.to_millis() {
                Ok(ms) => ts_col.append_value(ms),
                Err(_) => ts_col.append_null(),
            },
            None => ts_col.append_null(),
        }
    }

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(seq_col.finish()),
            Arc::new(key_col.finish()),
            Arc::new(data_col.finish()),
            Arc::new(ts_col.finish()),
        ],
    )
    .expect("schema matches builders — infallible")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_kinesis::primitives::Blob;

    fn make_record(seq: &str, key: &str, payload: &[u8]) -> Record {
        Record::builder()
            .sequence_number(seq)
            .partition_key(key)
            .data(Blob::new(payload.to_vec()))
            .build()
            .unwrap()
    }

    #[test]
    fn kinesis_schema_has_four_columns() {
        let schema = kinesis_arrow_schema();
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field_with_name("sequence_number").unwrap().data_type(), &DataType::Utf8);
        assert_eq!(schema.field_with_name("partition_key").unwrap().data_type(), &DataType::Utf8);
        assert_eq!(schema.field_with_name("data").unwrap().data_type(), &DataType::Binary);
        assert_eq!(schema.field_with_name("arrival_timestamp_ms").unwrap().data_type(), &DataType::Int64);
    }

    #[test]
    fn records_to_batch_converts_correctly() {
        let schema = kinesis_arrow_schema();
        let records = vec![
            make_record("seq-001", "pk-a", b"hello"),
            make_record("seq-002", "pk-b", b"world"),
        ];
        let batch = records_to_batch(&schema, &records);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);

        use arrow::array::{BinaryArray, StringArray};
        let seq = batch.column_by_name("sequence_number").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(seq.value(0), "seq-001");
        assert_eq!(seq.value(1), "seq-002");

        let data = batch.column_by_name("data").unwrap()
            .as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(data.value(0), b"hello");
        assert_eq!(data.value(1), b"world");
    }

    #[test]
    fn empty_records_produces_empty_batch() {
        let schema = kinesis_arrow_schema();
        let batch = records_to_batch(&schema, &[]);
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn config_defaults() {
        let cfg = KinesisConfig::new("my-stream", "us-east-1");
        assert_eq!(cfg.stream_name, "my-stream");
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.batch_size, 500);
        assert!(matches!(cfg.start, ShardPosition::TrimHorizon));
    }

    #[test]
    fn config_builder_pattern() {
        let cfg = KinesisConfig::new("s", "r")
            .with_shard_id("shardId-1")
            .with_start(ShardPosition::Latest)
            .with_batch_size(100);
        assert_eq!(cfg.shard_id, "shardId-1");
        assert_eq!(cfg.batch_size, 100);
        assert!(matches!(cfg.start, ShardPosition::Latest));
    }

    #[test]
    fn batch_size_clamps_to_max() {
        let cfg = KinesisConfig::new("s", "r").with_batch_size(99_999);
        assert_eq!(cfg.batch_size, 10_000);
    }

    #[test]
    fn capabilities_unbounded() {
        // Test without connecting — just verify the capabilities flag.
        let caps = ConnectorCapabilities::default().with_unbounded();
        assert!(!caps.is_bounded());
    }
}
