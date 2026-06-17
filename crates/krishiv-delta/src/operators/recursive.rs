#![forbid(unsafe_code)]

//! Recursive (fixed-point) incremental view operator.
//!
//! Implements Datalog-style fixed-point iteration for recursive SQL views.
//! Each outer clock tick runs the view body iteratively until the output
//! delta is empty (no new changes propagate).
//!
//! Recursive views are automatically DISTINCT to prevent infinite weight
//! growth on cycles (matching Feldera's semantics).
//!
//! Safety guard: if after `max_iterations` the delta is still non-empty,
//! the operator returns `DeltaError::CycleLimitExceeded`.

use crate::delta_batch::DeltaBatch;
use crate::error::{DeltaError, DeltaResult};
use crate::operators::distinct::IncrementalDistinctOp;

/// Default iteration limit for recursive views.
pub const DEFAULT_MAX_ITERATIONS: usize = 1000;

/// A recursive step function: given the current delta batch and the accumulated
/// state, produce the next delta batch.
pub type RecursiveStepFn =
    Box<dyn FnMut(&DeltaBatch, &DeltaBatch) -> DeltaResult<DeltaBatch>>;

/// Recursive incremental view operator.
///
/// Maintains the accumulated result (`accumulated`) and runs the body function
/// iteratively until fixpoint on each call to `apply`.
pub struct RecursiveOp {
    /// All rows accumulated so far (the "stable" part of the recursive view).
    accumulated: DeltaBatch,
    distinct: IncrementalDistinctOp,
    max_iterations: usize,
}

impl RecursiveOp {
    pub fn new(empty_batch: DeltaBatch, max_iterations: usize) -> Self {
        Self {
            accumulated: empty_batch,
            distinct: IncrementalDistinctOp::new(),
            max_iterations,
        }
    }

    pub fn with_default_limit(empty_batch: DeltaBatch) -> Self {
        Self::new(empty_batch, DEFAULT_MAX_ITERATIONS)
    }

    /// Run one outer tick of the recursive computation.
    ///
    /// `step_fn` is called with `(current_delta, accumulated)` and returns the
    /// next delta. The loop runs until `next_delta` is empty or the iteration
    /// limit is reached.
    ///
    /// Returns the total change to the view's output since the last outer tick.
    pub fn apply<F>(&mut self, seed_delta: DeltaBatch, mut step_fn: F) -> DeltaResult<DeltaBatch>
    where
        F: FnMut(&DeltaBatch, &DeltaBatch) -> DeltaResult<DeltaBatch>,
    {
        let mut current_delta = seed_delta;
        let mut total_output_parts: Vec<DeltaBatch> = Vec::new();
        let mut iters = 0;

        loop {
            if current_delta.is_empty() {
                break;
            }
            if iters >= self.max_iterations {
                return Err(DeltaError::CycleLimitExceeded(iters));
            }

            // Apply DISTINCT to the current delta before passing to step_fn
            // (prevents infinite weight growth on cycles).
            let distinct_delta = self.distinct.apply(current_delta)?;

            if distinct_delta.is_empty() {
                break;
            }

            total_output_parts.push(distinct_delta.clone());

            // Update accumulated state
            let parts = if self.accumulated.is_empty() {
                vec![distinct_delta.clone()]
            } else {
                vec![self.accumulated.clone(), distinct_delta.clone()]
            };
            self.accumulated = DeltaBatch::concat(&parts)?;

            // Run one step of the recursive body
            let next_delta = step_fn(&distinct_delta, &self.accumulated)?;
            current_delta = next_delta;
            iters += 1;
        }

        if total_output_parts.is_empty() {
            return DeltaBatch::empty(self.accumulated.data_schema().clone());
        }
        DeltaBatch::concat(&total_output_parts)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn edge_batch(srcs: &[i32], dsts: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int32, false),
            Field::new("dst", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(srcs.to_vec())),
                Arc::new(Int32Array::from(dsts.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn empty_seed_produces_no_output() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int32, false),
            Field::new("dst", DataType::Int32, false),
        ]));
        let empty = DeltaBatch::empty(schema).unwrap();
        let mut op = RecursiveOp::with_default_limit(empty.clone());
        let out = op.apply(empty, |_, _| Ok(DeltaBatch::empty(
            Arc::new(Schema::new(vec![
                Field::new("src", DataType::Int32, false),
                Field::new("dst", DataType::Int32, false),
            ]))
        ).unwrap())).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn cycle_limit_returns_error() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int32, false),
            Field::new("dst", DataType::Int32, false),
        ]));
        let empty = DeltaBatch::empty(schema.clone()).unwrap();
        let mut op = RecursiveOp::new(empty, 5);

        // step_fn generates a new unseen row each call so DISTINCT always
        // emits output — genuine non-termination until the iteration cap fires.
        let seed = DeltaBatch::from_inserts(edge_batch(&[1], &[2])).unwrap();
        let call_count = std::cell::Cell::new(0i32);
        let result = op.apply(seed, |_delta, _acc| {
            let i = call_count.get();
            call_count.set(i + 1);
            let src = 100 + i;
            let dst = 200 + i;
            DeltaBatch::from_inserts(edge_batch(&[src], &[dst]))
        });
        assert!(matches!(result, Err(DeltaError::CycleLimitExceeded(_))));
    }
}
