//! Coalesce N input partition batches into fewer output batches (P2-4).

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};

/// Merge `inputs` into at most `target_partitions` batches (preserving schema).
pub fn coalesce_partition_batches(
    inputs: &[RecordBatch],
    target_partitions: usize,
) -> ExecResult<Vec<RecordBatch>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }
    let target = target_partitions.max(1);
    if inputs.len() <= target {
        return Ok(inputs.to_vec());
    }
    let schema = inputs[0].schema();
    let chunk_size = inputs.len().div_ceil(target);
    let mut outputs = Vec::with_capacity(target);
    for chunk in inputs.chunks(chunk_size) {
        if chunk.len() == 1 {
            outputs.push(chunk[0].clone());
        } else {
            let merged =
                concat_batches(&schema, chunk).map_err(|e| ExecError::Arrow(e.to_string()))?;
            outputs.push(merged);
        }
    }
    Ok(outputs)
}

/// Physical operator descriptor for coalesce stages.
#[derive(Debug, Clone)]
pub struct CoalescePartitionsOperator {
    target_partitions: usize,
}

impl CoalescePartitionsOperator {
    /// Create an operator that reduces fan-in to `target_partitions`.
    pub fn new(target_partitions: usize) -> Self {
        Self {
            target_partitions: target_partitions.max(1),
        }
    }

    /// Apply coalescing to in-memory batches.
    pub fn execute(&self, inputs: Vec<RecordBatch>) -> ExecResult<Vec<RecordBatch>> {
        coalesce_partition_batches(&inputs, self.target_partitions)
    }

    /// Target partition count stamped on the physical plan.
    pub fn target_partitions(&self) -> usize {
        self.target_partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    ids.iter().map(|i| format!("row-{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn coalesce_merges_to_target_count() {
        let inputs = vec![batch(&[1]), batch(&[2]), batch(&[3]), batch(&[4])];
        let out = coalesce_partition_batches(&inputs, 2).unwrap();
        assert_eq!(out.len(), 2);
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4);
    }

    #[test]
    fn coalesce_empty_inputs_returns_empty() {
        let out = coalesce_partition_batches(&[], 4).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn coalesce_single_input_returns_as_is() {
        let inputs = vec![batch(&[1])];
        let out = coalesce_partition_batches(&inputs, 2).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 1);
    }

    #[test]
    fn coalesce_inputs_fewer_than_target_returns_as_is() {
        let inputs = vec![batch(&[1]), batch(&[2])];
        let out = coalesce_partition_batches(&inputs, 5).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn coalesce_zero_target_treated_as_one() {
        let inputs = vec![batch(&[1]), batch(&[2]), batch(&[3])];
        let out = coalesce_partition_batches(&inputs, 0).unwrap();
        // target becomes 1, so all 3 batches are merged into 1
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 3);
    }

    #[test]
    fn coalesce_preserves_data_values() {
        let inputs = vec![batch(&[10, 20]), batch(&[30, 40])];
        let out = coalesce_partition_batches(&inputs, 1).unwrap();
        assert_eq!(out.len(), 1);
        let ids = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut vals: Vec<i32> = (0..ids.len()).map(|i| ids.value(i)).collect();
        vals.sort();
        assert_eq!(vals, vec![10, 20, 30, 40]);
    }

    #[test]
    fn coalesce_target_equals_input_count() {
        let inputs = vec![batch(&[1]), batch(&[2]), batch(&[3])];
        let out = coalesce_partition_batches(&inputs, 3).unwrap();
        assert_eq!(out.len(), 3);
    }

    // ── CoalescePartitionsOperator tests ──────────────────────────────────────

    #[test]
    fn operator_new_clamps_to_minimum_one() {
        let op = CoalescePartitionsOperator::new(0);
        assert_eq!(op.target_partitions(), 1);
    }

    #[test]
    fn operator_execute_coalesces() {
        let op = CoalescePartitionsOperator::new(2);
        let inputs = vec![batch(&[1]), batch(&[2]), batch(&[3])];
        let out = op.execute(inputs).unwrap();
        assert!(out.len() <= 2);
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn operator_debug_trait() {
        let op = CoalescePartitionsOperator::new(4);
        let dbg = format!("{:?}", op);
        assert!(dbg.contains("CoalescePartitionsOperator"));
        assert!(dbg.contains("4"));
    }
}
