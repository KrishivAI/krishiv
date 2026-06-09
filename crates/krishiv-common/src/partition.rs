//! Deterministic Arrow record-batch partitioning.

use std::num::NonZeroUsize;

use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, UInt64Array,
};
use arrow::compute::take_record_batch;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::hash::sha256_bytes_multi;

/// Stable domain separator for the distributed partition-key hash contract.
///
/// Changing this value remaps every key and requires an explicit partitioning
/// protocol migration.
const PARTITION_KEY_HASH_DOMAIN: &[u8] = b"krishiv.partition-key.v1\0";

/// Default target bytes per partition (128 MiB).
///
/// Used by `AutoPartitionRule` and bounded-window shard calculation to decide
/// how many partitions to create for a given data volume. Operators can override
/// this via durability profile or explicit config.
pub const TARGET_BYTES_PER_PARTITION: u64 = 128 * 1024 * 1024;

/// Return the target bytes per partition for a given data volume estimate.
///
/// When `profile` is `Some`, the profile may supply an override. When `None`,
/// the default [`TARGET_BYTES_PER_PARTITION`] (128 MiB) is returned.
#[must_use]
pub fn target_bytes_per_partition(_profile: Option<&str>) -> u64 {
    // Future: look up profile override from durability profile config.
    TARGET_BYTES_PER_PARTITION
}

/// A batch cannot be partitioned without violating keyed execution semantics.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid partitioning input: {message}")]
pub struct PartitionError {
    message: String,
}

impl PartitionError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Human-readable contract violation.
    pub fn message(&self) -> &str {
        &self.message
    }
}

fn supported_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int32 | DataType::Int64 | DataType::Float64 | DataType::Utf8 | DataType::Boolean
    )
}

fn digest_for_key(array: &dyn Array, row: usize) -> Result<[u8; 32], PartitionError> {
    let downcast_error = |expected: &str| {
        PartitionError::new(format!("declared {} key failed Arrow downcast", expected))
    };

    match array.data_type() {
        DataType::Int32 => {
            let values = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| downcast_error("Int32"))?;
            Ok(sha256_bytes_multi(&[
                PARTITION_KEY_HASH_DOMAIN,
                b"i32\0",
                &values.value(row).to_le_bytes(),
            ]))
        }
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| downcast_error("Int64"))?;
            Ok(sha256_bytes_multi(&[
                PARTITION_KEY_HASH_DOMAIN,
                b"i64\0",
                &values.value(row).to_le_bytes(),
            ]))
        }
        DataType::Float64 => {
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| downcast_error("Float64"))?;
            let value = values.value(row);
            let canonical_bits = if value.is_nan() {
                f64::NAN.to_bits()
            } else {
                value.to_bits()
            };
            Ok(sha256_bytes_multi(&[
                PARTITION_KEY_HASH_DOMAIN,
                b"f64\0",
                &canonical_bits.to_le_bytes(),
            ]))
        }
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| downcast_error("Utf8"))?;
            Ok(sha256_bytes_multi(&[
                PARTITION_KEY_HASH_DOMAIN,
                b"utf8\0",
                values.value(row).as_bytes(),
            ]))
        }
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| downcast_error("Boolean"))?;
            Ok(sha256_bytes_multi(&[
                PARTITION_KEY_HASH_DOMAIN,
                b"bool\0",
                &[u8::from(values.value(row))],
            ]))
        }
        other => Err(PartitionError::new(format!(
            "unsupported partition key type {other}; expected Int32, Int64, Float64, Utf8, or Boolean"
        ))),
    }
}

fn shard_index(
    array: &dyn Array,
    row: usize,
    shard_count: NonZeroUsize,
) -> Result<usize, PartitionError> {
    let digest = digest_for_key(array, row)?;
    let hash = u64::from_le_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ]);
    Ok((u128::from(hash) % (shard_count.get() as u128)) as usize)
}

/// Partition rows by a non-null typed key.
///
/// Every batch must contain `key_column` with the same supported data type.
/// Each input row is assigned exactly once, preserving its schema and relative
/// order within the source batch.
pub fn partition_record_batches_by_key(
    batches: &[RecordBatch],
    key_column: &str,
    shard_count: usize,
) -> Result<Vec<Vec<RecordBatch>>, PartitionError> {
    let shard_count = NonZeroUsize::new(shard_count)
        .ok_or_else(|| PartitionError::new("shard count must be greater than zero"))?;
    if key_column.trim().is_empty() {
        return Err(PartitionError::new(
            "partition key column must not be empty",
        ));
    }

    let mut shards: Vec<Vec<RecordBatch>> = (0..shard_count.get()).map(|_| Vec::new()).collect();
    let mut expected_key_type: Option<DataType> = None;
    let mut expected_schema = None;

    for (batch_idx, batch) in batches.iter().enumerate() {
        let schema = batch.schema();
        let key_idx = schema.index_of(key_column).map_err(|_| {
            PartitionError::new(format!(
                "batch {batch_idx} is missing key column '{key_column}'"
            ))
        })?;
        let key_type = schema.field(key_idx).data_type();
        if !supported_key_type(key_type) {
            return Err(PartitionError::new(format!(
                "batch {batch_idx} key column '{key_column}' has unsupported type {key_type}; \
                 expected Int32, Int64, Float64, Utf8, or Boolean"
            )));
        }
        if let Some(expected) = &expected_key_type {
            if expected != key_type {
                return Err(PartitionError::new(format!(
                    "batch {batch_idx} key column '{key_column}' changed type from \
                     {expected} to {key_type}"
                )));
            }
        } else {
            expected_key_type = Some(key_type.clone());
        }
        if let Some(expected) = &expected_schema {
            if expected != &schema {
                return Err(PartitionError::new(format!(
                    "batch {batch_idx} schema differs from batch 0"
                )));
            }
        } else {
            expected_schema = Some(schema);
        }

        let key_array = batch.column(key_idx);
        let mut row_indices: Vec<Vec<u64>> = (0..shard_count.get()).map(|_| Vec::new()).collect();
        for row in 0..batch.num_rows() {
            if key_array.is_null(row) {
                return Err(PartitionError::new(format!(
                    "batch {batch_idx} key column '{key_column}' contains null at row {row}"
                )));
            }
            let partition = shard_index(key_array.as_ref(), row, shard_count)?;
            let row_idx = u64::try_from(row).map_err(|_| {
                PartitionError::new(format!(
                    "batch {batch_idx} has more rows than the Arrow gather index can represent"
                ))
            })?;
            row_indices[partition].push(row_idx);
        }

        for (shard_idx, indices) in row_indices.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let indices = UInt64Array::from(indices);
            let partition = take_record_batch(batch, &indices).map_err(|error| {
                PartitionError::new(format!(
                    "failed to materialize batch {batch_idx} shard {shard_idx}: {error}"
                ))
            })?;
            shards[shard_idx].push(partition);
        }
    }

    Ok(shards)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray, UInt64Array};
    use arrow::datatypes::{Field, Schema};

    use super::*;

    fn batch_with_key(name: &str, key: ArrayRef) -> RecordBatch {
        let row_count = key.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new(name, key.data_type().clone(), key.null_count() > 0),
            Field::new("value", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                key,
                Arc::new(Int64Array::from_iter_values(
                    (0..row_count).map(|value| value as i64),
                )),
            ],
        )
        .unwrap()
    }

    fn row_count(shards: &[Vec<RecordBatch>]) -> usize {
        shards.iter().flatten().map(RecordBatch::num_rows).sum()
    }

    #[test]
    fn stable_hash_has_known_mapping() {
        let keys = StringArray::from(vec!["customer-42"]);
        assert_eq!(
            shard_index(&keys, 0, NonZeroUsize::new(17).unwrap()).unwrap(),
            13
        );
    }

    #[test]
    fn partitioning_is_deterministic_and_lossless() {
        let batches = vec![
            batch_with_key(
                "key",
                Arc::new(StringArray::from(vec!["same", "other"])) as ArrayRef,
            ),
            batch_with_key(
                "key",
                Arc::new(StringArray::from(vec!["third", "same"])) as ArrayRef,
            ),
        ];

        let first = partition_record_batches_by_key(&batches, "key", 7).unwrap();
        let second = partition_record_batches_by_key(&batches, "key", 7).unwrap();
        assert_eq!(row_count(&first), 4);
        assert_eq!(first, second);

        let same_shards = first
            .iter()
            .filter(|shard_batches| {
                shard_batches.iter().any(|batch| {
                    let keys = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    (0..keys.len()).any(|row| keys.value(row) == "same")
                })
            })
            .count();
        assert_eq!(same_shards, 1);
    }

    #[test]
    fn partitioning_supports_all_window_key_types() {
        let key_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![1.5, 2.5])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(BooleanArray::from(vec![true, false])),
        ];

        for key in key_arrays {
            let batch = batch_with_key("key", key);
            let shards = partition_record_batches_by_key(&[batch], "key", 3).unwrap();
            assert_eq!(row_count(&shards), 2);
        }
    }

    #[test]
    fn partitioning_rejects_invalid_contracts() {
        let batch = batch_with_key(
            "key",
            Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
        );
        assert!(partition_record_batches_by_key(std::slice::from_ref(&batch), "key", 0).is_err());
        assert!(partition_record_batches_by_key(std::slice::from_ref(&batch), " ", 2).is_err());
        assert!(
            partition_record_batches_by_key(std::slice::from_ref(&batch), "missing", 2).is_err()
        );

        let null_error = partition_record_batches_by_key(&[batch], "key", 2).unwrap_err();
        assert!(null_error.message().contains("contains null at row 1"));

        let unsupported = batch_with_key(
            "key",
            Arc::new(UInt64Array::from(vec![1_u64, 2])) as ArrayRef,
        );
        let unsupported_error =
            partition_record_batches_by_key(&[unsupported], "key", 2).unwrap_err();
        assert!(
            unsupported_error
                .message()
                .contains("unsupported type UInt64")
        );
    }

    #[test]
    fn partitioning_rejects_key_type_drift() {
        let string_batch =
            batch_with_key("key", Arc::new(StringArray::from(vec!["1"])) as ArrayRef);
        let int_batch = batch_with_key("key", Arc::new(Int64Array::from(vec![1])) as ArrayRef);

        let error =
            partition_record_batches_by_key(&[string_batch, int_batch], "key", 2).unwrap_err();
        assert!(error.message().contains("changed type from Utf8 to Int64"));
    }

    #[test]
    fn partitioning_rejects_non_key_schema_drift() {
        let first = batch_with_key("key", Arc::new(StringArray::from(vec!["a"])) as ArrayRef);
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let second = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["b"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();

        let error = partition_record_batches_by_key(&[first, second], "key", 2).unwrap_err();
        assert!(error.message().contains("schema differs from batch 0"));
    }

    #[test]
    fn partitioning_canonicalizes_nan_payloads() {
        let canonical = Float64Array::from(vec![f64::NAN]);
        let alternate = Float64Array::from(vec![f64::from_bits(0x7ff8_0000_0000_0042)]);
        let shard_count = NonZeroUsize::new(31).unwrap();

        assert_eq!(
            shard_index(&canonical, 0, shard_count).unwrap(),
            shard_index(&alternate, 0, shard_count).unwrap()
        );
    }
}
