//! Encode in-memory [`RecordBatch`]es as `stream-kafka:` input partition descriptors.

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use krishiv_exec::join::format_key_value;

use crate::RuntimeError;

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
        let key = format_key_value(batch, key_idx, row)
            .map_err(|e| RuntimeError::transport(e.to_string()))?;
        let time_arr = batch
            .column(time_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                RuntimeError::transport(format!("time column '{time_column}' must be Int64"))
            })?;
        let ts = time_arr.value(row);
        let val = if let Some(vidx) = value_idx {
            format_key_value(batch, vidx, row)
                .map_err(|e| RuntimeError::transport(e.to_string()))?
                .parse::<i64>()
                .unwrap_or(0)
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
