//! Encode in-memory [`RecordBatch`]es as `stream-kafka:` input partition descriptors.

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::RuntimeError;

fn extract_column_as_string(
    batch: &RecordBatch,
    col_idx: usize,
    row: usize,
) -> Result<String, RuntimeError> {
    let col = batch.column(col_idx);
    let col_name = batch.schema().field(col_idx).name().to_owned();
    match col.data_type() {
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                RuntimeError::transport(format!("column '{col_name}' Utf8 downcast failed"))
            })?;
            Ok(arr.value(row).to_owned())
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                RuntimeError::transport(format!("column '{col_name}' Int64 downcast failed"))
            })?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                RuntimeError::transport(format!("column '{col_name}' Int32 downcast failed"))
            })?;
            Ok(arr.value(row).to_string())
        }
        other => Err(RuntimeError::transport(format!(
            "column '{col_name}' has unsupported type {other} for stream-kafka key"
        ))),
    }
}

/// Build a `stream-kafka:` partition description for executor streaming tasks.
pub fn encode_stream_kafka_partition(
    topic: &str,
    partition: u32,
    start_offset: u64,
    batch: &RecordBatch,
    key_column: &str,
    time_column: &str,
    value_column: Option<&str>,
) -> Result<String, RuntimeError> {
    let key_idx = batch
        .schema()
        .index_of(key_column)
        .map_err(|_| RuntimeError::transport(format!("key column '{key_column}' not found")))?;
    let time_idx = batch
        .schema()
        .index_of(time_column)
        .map_err(|_| RuntimeError::transport(format!("time column '{time_column}' not found")))?;
    let value_idx = value_column
        .map(|col| {
            batch
                .schema()
                .index_of(col)
                .map_err(|_| RuntimeError::transport(format!("value column '{col}' not found")))
        })
        .transpose()?;

    let mut records = Vec::new();
    for row in 0..batch.num_rows() {
        let key = extract_column_as_string(batch, key_idx, row)?;
        let time_arr = batch
            .column(time_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                RuntimeError::transport(format!("time column '{time_column}' must be Int64"))
            })?;
        let ts = time_arr.value(row);
        let val = if let Some(vidx) = value_idx {
            batch
                .column(vidx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    RuntimeError::transport(format!(
                        "value column '{}' must be Int64",
                        value_column.unwrap_or("")
                    ))
                })?
                .value(row)
        } else {
            0
        };
        records.push(format!("key={key},ts={ts},val={val}"));
    }
    if records.is_empty() {
        return Err(RuntimeError::transport(
            "stream-kafka encoder requires at least one row",
        ));
    }
    Ok(format!(
        "stream-kafka:{topic}:{partition}:{start_offset}:{}",
        records.join("|")
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::encode_stream_kafka_partition;

    fn make_batch(keys: &[&str], times: &[i64]) -> RecordBatch {
        krishiv_common::arrow::make_test_key_ts_batch(keys.to_vec(), times.to_vec())
    }

    fn make_batch_with_value(keys: &[&str], times: &[i64], values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())) as _,
                Arc::new(Int64Array::from(times.to_vec())) as _,
                Arc::new(Int64Array::from(values.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    #[test]
    fn happy_path_single_row() {
        let batch = make_batch(&["user1"], &[1000]);
        let result =
            encode_stream_kafka_partition("events", 0, 0, &batch, "key", "ts", None).unwrap();
        assert!(result.starts_with("stream-kafka:events:0:0:"));
        assert!(result.contains("key=user1"));
        assert!(result.contains("ts=1000"));
        assert!(result.contains("val=0"));
    }

    #[test]
    fn happy_path_multiple_rows() {
        let batch = make_batch(&["a", "b", "c"], &[100, 200, 300]);
        let result =
            encode_stream_kafka_partition("topic", 1, 10, &batch, "key", "ts", None).unwrap();
        assert!(result.contains("|"));
        let records: Vec<&str> = result.split('|').collect();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn with_value_column() {
        let batch = make_batch_with_value(&["k1"], &[1000], &[42]);
        let result =
            encode_stream_kafka_partition("t", 0, 0, &batch, "key", "ts", Some("val")).unwrap();
        assert!(result.contains("val=42"));
    }

    #[test]
    fn with_value_column_multiple() {
        let batch = make_batch_with_value(&["k1", "k2"], &[1000, 2000], &[10, 20]);
        let result =
            encode_stream_kafka_partition("t", 0, 0, &batch, "key", "ts", Some("val")).unwrap();
        assert!(result.contains("val=10"));
        assert!(result.contains("val=20"));
    }

    #[test]
    fn missing_key_column_error() {
        let batch = make_batch(&["a"], &[1000]);
        let err = encode_stream_kafka_partition("t", 0, 0, &batch, "nonexistent", "ts", None)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("key column 'nonexistent' not found")
        );
    }

    #[test]
    fn missing_time_column_error() {
        let batch = make_batch(&["a"], &[1000]);
        let err = encode_stream_kafka_partition("t", 0, 0, &batch, "key", "nonexistent", None)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("time column 'nonexistent' not found")
        );
    }

    #[test]
    fn missing_value_column_error() {
        let batch = make_batch(&["a"], &[1000]);
        let err =
            encode_stream_kafka_partition("t", 0, 0, &batch, "key", "ts", Some("nonexistent"))
                .unwrap_err();
        assert!(
            err.to_string()
                .contains("value column 'nonexistent' not found")
        );
    }

    #[test]
    fn empty_batch_error() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(Vec::<&str>::new())) as _,
                Arc::new(Int64Array::from(Vec::<i64>::new())) as _,
            ],
        )
        .unwrap();
        let err = encode_stream_kafka_partition("t", 0, 0, &batch, "key", "ts", None).unwrap_err();
        assert!(
            err.to_string()
                .contains("stream-kafka encoder requires at least one row")
        );
    }

    #[test]
    fn format_contains_topic_partition_offset() {
        let batch = make_batch(&["k"], &[1000]);
        let result =
            encode_stream_kafka_partition("my-topic", 3, 42, &batch, "key", "ts", None).unwrap();
        assert_eq!(result, "stream-kafka:my-topic:3:42:key=k,ts=1000,val=0");
    }

    #[test]
    fn non_int64_time_column_error() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(StringArray::from(vec!["not-a-timestamp"])) as _,
            ],
        )
        .unwrap();
        let err = encode_stream_kafka_partition("t", 0, 0, &batch, "key", "ts", None).unwrap_err();
        assert!(err.to_string().contains("must be Int64"));
    }

    #[test]
    fn value_column_not_parseable_as_i64() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![1000])) as _,
                Arc::new(StringArray::from(vec!["not-a-number"])) as _,
            ],
        )
        .unwrap();
        let err =
            encode_stream_kafka_partition("t", 0, 0, &batch, "key", "ts", Some("val")).unwrap_err();
        assert!(err.to_string().contains("value column") && err.to_string().contains("must be Int64"));
    }
}
