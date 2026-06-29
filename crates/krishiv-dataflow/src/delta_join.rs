#![forbid(unsafe_code)]

//! P8: Delta Join — stateless stream-stream join for append-only streams.
//!
//! Inspired by Flink 2.0's delta join implementation, this module provides
//! a stream-stream join that maintains no state. It works by matching events
//! from two streams that arrive within a time window, using only the current
//! micro-batch data (no historical state).
//!
//! # When to Use
//!
//! Delta join is optimal when:
//! - Both streams are append-only (no updates/deletes)
//! - Events from both sides arrive roughly in order
//! - A small time window is acceptable for matching
//! - State management overhead is unacceptable
//!
//! # Limitations
//!
//! - Events that arrive after the window closes are dropped
//! - No support for outer joins (only inner and left/right)
//! - Requires both streams to have a time column
//!
//! # Architecture
//!
//! ```text
//! Left Stream ──►┌─────────────────┐
//!                │  Delta Join     │──► Joined Output
//! Right Stream ──►│  (stateless)   │
//!                └─────────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use krishiv_dataflow::delta_join::{DeltaJoinSpec, DeltaJoinOperator};
//!
//! let spec = DeltaJoinSpec {
//!     left_time_col: "event_time".into(),
//!     right_time_col: "event_time".into(),
//!     left_key_col: "user_id".into(),
//!     right_key_col: "user_id".into(),
//!     window_ms: 5000, // 5 second window
//! };
//!
//! let mut operator = DeltaJoinOperator::new(spec);
//!
//! // Process left batch
//! let joined = operator.process_left(left_batch)?;
//!
//! // Process right batch
//! let joined = operator.process_right(right_batch)?;
//! ```

use std::collections::HashMap;

use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Specification for a delta join operation.
#[derive(Debug, Clone)]
pub struct DeltaJoinSpec {
    /// Column name for the time column in the left stream.
    pub left_time_col: String,
    /// Column name for the time column in the right stream.
    pub right_time_col: String,
    /// Column name for the join key in the left stream.
    pub left_key_col: String,
    /// Column name for the join key in the right stream.
    pub right_key_col: String,
    /// Time window in milliseconds for matching events.
    pub window_ms: u64,
}

/// Stateless stream-stream join operator for append-only streams.
///
/// This operator matches events from two streams that arrive within a time
/// window, without maintaining any state between micro-batches.
pub struct DeltaJoinOperator {
    spec: DeltaJoinSpec,
    /// Pending left events from the current micro-batch
    pending_left: Vec<RecordBatch>,
    /// Pending right events from the current micro-batch
    pending_right: Vec<RecordBatch>,
}

impl DeltaJoinOperator {
    /// Create a new delta join operator with the given specification.
    pub fn new(spec: DeltaJoinSpec) -> Self {
        Self {
            spec,
            pending_left: Vec::new(),
            pending_right: Vec::new(),
        }
    }

    /// Process a left stream batch and return any joined results.
    ///
    /// This method buffers the left batch and attempts to match it against
    /// any pending right batches. Since this is stateless, only the current
    /// micro-batch data is considered.
    pub fn process_left(
        &mut self,
        batch: RecordBatch,
    ) -> Result<Option<RecordBatch>, DeltaJoinError> {
        self.pending_left.push(batch);
        self.try_join()
    }

    /// Process a right stream batch and return any joined results.
    ///
    /// This method buffers the right batch and attempts to match it against
    /// any pending left batches.
    pub fn process_right(
        &mut self,
        batch: RecordBatch,
    ) -> Result<Option<RecordBatch>, DeltaJoinError> {
        self.pending_right.push(batch);
        self.try_join()
    }

    /// Attempt to join pending left and right batches.
    fn try_join(&mut self) -> Result<Option<RecordBatch>, DeltaJoinError> {
        if self.pending_left.is_empty() || self.pending_right.is_empty() {
            return Ok(None);
        }

        let mut results = Vec::new();

        // For each left batch, try to match against all right batches
        for left_batch in &self.pending_left {
            for right_batch in &self.pending_right {
                if let Some(joined) = self.join_batches(left_batch, right_batch)? {
                    results.push(joined);
                }
            }
        }

        // Clear pending batches after joining
        self.pending_left.clear();
        self.pending_right.clear();

        if results.is_empty() {
            Ok(None)
        } else {
            // Concatenate all joined results
            let schema = results
                .first()
                .ok_or_else(|| DeltaJoinError::JoinFailed("empty results".into()))?
                .schema();
            let batch_refs: Vec<&RecordBatch> = results.iter().collect();
            let concatenated = compute::concat_batches(&schema, batch_refs)
                .map_err(|e| DeltaJoinError::JoinFailed(e.to_string()))?;
            Ok(Some(concatenated))
        }
    }

    /// Join two individual batches based on the join spec.
    fn join_batches(
        &self,
        left: &RecordBatch,
        right: &RecordBatch,
    ) -> Result<Option<RecordBatch>, DeltaJoinError> {
        // Get column indices
        let left_time_idx = left
            .schema()
            .column_with_name(&self.spec.left_time_col)
            .ok_or_else(|| {
                DeltaJoinError::JoinFailed(format!(
                    "left time column '{}' not found",
                    self.spec.left_time_col
                ))
            })?
            .0;

        let right_time_idx = right
            .schema()
            .column_with_name(&self.spec.right_time_col)
            .ok_or_else(|| {
                DeltaJoinError::JoinFailed(format!(
                    "right time column '{}' not found",
                    self.spec.right_time_col
                ))
            })?
            .0;

        let left_key_idx = left
            .schema()
            .column_with_name(&self.spec.left_key_col)
            .ok_or_else(|| {
                DeltaJoinError::JoinFailed(format!(
                    "left key column '{}' not found",
                    self.spec.left_key_col
                ))
            })?
            .0;

        let right_key_idx = right
            .schema()
            .column_with_name(&self.spec.right_key_col)
            .ok_or_else(|| {
                DeltaJoinError::JoinFailed(format!(
                    "right key column '{}' not found",
                    self.spec.right_key_col
                ))
            })?
            .0;

        // Build hash map from right batch (key → row indices)
        let mut right_map: HashMap<String, Vec<usize>> = HashMap::new();
        let right_key_array = right.column(right_key_idx);
        let right_time_array = right.column(right_time_idx);

        for i in 0..right.num_rows() {
            let key = extract_string_value(right_key_array, i)?;
            let time = extract_timestamp_value(right_time_array, i)?;
            if time.is_some() {
                right_map.entry(key).or_default().push(i);
            }
        }

        // Probe with left batch
        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();

        let left_key_array = left.column(left_key_idx);
        let left_time_array = left.column(left_time_idx);

        for i in 0..left.num_rows() {
            let key = extract_string_value(left_key_array, i)?;
            let left_time = extract_timestamp_value(left_time_array, i)?;

            if let (Some(left_ts), Some(right_rows)) = (left_time, right_map.get(&key)) {
                for &j in right_rows {
                    let right_time = extract_timestamp_value(right_time_array, j)?;
                    if let Some(right_ts) = right_time {
                        let diff = (left_ts as i64 - right_ts as i64).unsigned_abs();
                        if diff <= self.spec.window_ms {
                            left_indices.push(i);
                            right_indices.push(j);
                        }
                    }
                }
            }
        }

        if left_indices.is_empty() {
            return Ok(None);
        }

        // Create output batch with left columns + right columns
        let mut output_columns: Vec<ArrayRef> = Vec::new();

        // Add all left columns
        for col_idx in 0..left.num_columns() {
            let left_array = left.column(col_idx);
            let indices = arrow::array::UInt32Array::from(
                left_indices.iter().map(|&i| i as u32).collect::<Vec<u32>>(),
            );
            let sliced = compute::take(left_array, &indices, None)
                .map_err(|e| DeltaJoinError::JoinFailed(e.to_string()))?;
            output_columns.push(sliced);
        }

        // Add all right columns
        for col_idx in 0..right.num_columns() {
            let right_array = right.column(col_idx);
            let indices = arrow::array::UInt32Array::from(
                right_indices
                    .iter()
                    .map(|&j| j as u32)
                    .collect::<Vec<u32>>(),
            );
            let sliced = compute::take(right_array, &indices, None)
                .map_err(|e| DeltaJoinError::JoinFailed(e.to_string()))?;
            output_columns.push(sliced);
        }

        // Build output schema
        let mut fields = Vec::new();
        for field in left.schema().fields() {
            fields.push(Field::new(
                format!("left_{}", field.name()),
                field.data_type().clone(),
                field.is_nullable(),
            ));
        }
        for field in right.schema().fields() {
            fields.push(Field::new(
                format!("right_{}", field.name()),
                field.data_type().clone(),
                field.is_nullable(),
            ));
        }
        let output_schema = Arc::new(Schema::new(fields));

        let output_batch = RecordBatch::try_new(output_schema, output_columns)
            .map_err(|e| DeltaJoinError::JoinFailed(e.to_string()))?;

        Ok(Some(output_batch))
    }

    /// Clear any pending state.
    pub fn clear(&mut self) {
        self.pending_left.clear();
        self.pending_right.clear();
    }

    /// Get the number of pending left events.
    pub fn pending_left_count(&self) -> usize {
        self.pending_left.iter().map(|b| b.num_rows()).sum()
    }

    /// Get the number of pending right events.
    pub fn pending_right_count(&self) -> usize {
        self.pending_right.iter().map(|b| b.num_rows()).sum()
    }
}

/// Extract a string value from an array at the given index.
fn extract_string_value(array: &ArrayRef, index: usize) -> Result<String, DeltaJoinError> {
    use arrow::array::AsArray;
    let string_array = array.as_string::<i32>();
    Ok(string_array.value(index).to_string())
}

/// Extract a timestamp value from an array at the given index.
fn extract_timestamp_value(array: &ArrayRef, index: usize) -> Result<Option<u64>, DeltaJoinError> {
    use arrow::array::AsArray;
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_primitive::<arrow::datatypes::Int64Type>();
            Ok(arr.value(index).try_into().ok())
        }
        DataType::UInt64 => {
            let arr = array.as_primitive::<arrow::datatypes::UInt64Type>();
            Ok(Some(arr.value(index)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
            let arr = array.as_primitive::<arrow::datatypes::TimestampMillisecondType>();
            Ok(arr.value(index).try_into().ok())
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
            let arr = array.as_primitive::<arrow::datatypes::TimestampSecondType>();
            Ok(arr.value(index).try_into().ok())
        }
        _ => Err(DeltaJoinError::JoinFailed(format!(
            "unsupported timestamp type: {:?}",
            array.data_type()
        ))),
    }
}

/// Errors that can occur during delta join operations.
#[derive(Debug)]
pub enum DeltaJoinError {
    /// Join operation failed.
    JoinFailed(String),
}

impl std::fmt::Display for DeltaJoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::JoinFailed(msg) => write!(f, "join failed: {msg}"),
        }
    }
}

impl std::error::Error for DeltaJoinError {}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    fn make_test_batch(time_col: &str, key_col: &str, times: &[i64], keys: &[&str]) -> RecordBatch {
        let time_array = Arc::new(Int64Array::from(times.to_vec()));
        let key_array = Arc::new(StringArray::from(keys.to_vec()));

        let schema = Arc::new(Schema::new(vec![
            Field::new(time_col, DataType::Int64, false),
            Field::new(key_col, DataType::Utf8, false),
        ]));

        RecordBatch::try_new(schema, vec![time_array, key_array]).unwrap()
    }

    #[test]
    fn delta_join_matches_events_in_window() {
        let spec = DeltaJoinSpec {
            left_time_col: "time".into(),
            right_time_col: "time".into(),
            left_key_col: "key".into(),
            right_key_col: "key".into(),
            window_ms: 1000,
        };

        let mut operator = DeltaJoinOperator::new(spec);

        let left = make_test_batch("time", "key", &[1000, 2000], &["a", "b"]);
        let right = make_test_batch("time", "key", &[1500, 2500], &["a", "b"]);

        let result = operator.process_left(left).unwrap();
        assert!(result.is_none(), "should wait for right batch");

        let result = operator.process_right(right).unwrap();
        assert!(result.is_some(), "should produce joined result");

        let joined = result.unwrap();
        assert_eq!(joined.num_rows(), 2, "should join matching events");
        assert_eq!(
            joined.num_columns(),
            4,
            "should have 2 left + 2 right columns"
        );
    }

    #[test]
    fn delta_join_drops_events_outside_window() {
        let spec = DeltaJoinSpec {
            left_time_col: "time".into(),
            right_time_col: "time".into(),
            left_key_col: "key".into(),
            right_key_col: "key".into(),
            window_ms: 100, // Very small window
        };

        let mut operator = DeltaJoinOperator::new(spec);

        let left = make_test_batch("time", "key", &[1000], &["a"]);
        let right = make_test_batch("time", "key", &[2000], &["a"]); // 1000ms apart

        operator.process_left(left).unwrap();
        let result = operator.process_right(right).unwrap();

        assert!(result.is_none(), "should not join events outside window");
    }

    #[test]
    fn delta_join_handles_empty_batches() {
        let spec = DeltaJoinSpec {
            left_time_col: "time".into(),
            right_time_col: "time".into(),
            left_key_col: "key".into(),
            right_key_col: "key".into(),
            window_ms: 1000,
        };

        let mut operator = DeltaJoinOperator::new(spec);

        let left = make_test_batch("time", "key", &[], &[]);
        let right = make_test_batch("time", "key", &[], &[]);

        operator.process_left(left).unwrap();
        let result = operator.process_right(right).unwrap();

        assert!(result.is_none(), "should not join empty batches");
    }

    #[test]
    fn delta_join_clear_resets_state() {
        let spec = DeltaJoinSpec {
            left_time_col: "time".into(),
            right_time_col: "time".into(),
            left_key_col: "key".into(),
            right_key_col: "key".into(),
            window_ms: 1000,
        };

        let mut operator = DeltaJoinOperator::new(spec);

        let left = make_test_batch("time", "key", &[1000], &["a"]);
        operator.process_left(left).unwrap();

        assert_eq!(operator.pending_left_count(), 1);

        operator.clear();

        assert_eq!(operator.pending_left_count(), 0);
    }
}
