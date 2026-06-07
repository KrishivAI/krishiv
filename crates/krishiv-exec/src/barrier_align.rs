//! Multi-input checkpoint barrier alignment (R16 S1.3).

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Errors from barrier alignment.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BarrierAlignError {
    /// Input count must be greater than zero.
    #[error("input_count must be greater than zero")]
    ZeroInputCount,
    /// Barrier alignment timed out.
    #[error(
        "checkpoint alignment timeout for epoch {epoch}: received barriers on {waited_inputs}/{expected_inputs} inputs"
    )]
    CheckpointAlignmentTimeout {
        epoch: u64,
        waited_inputs: usize,
        expected_inputs: usize,
    },
}

/// Buffers records on faster inputs until all inputs have seen the barrier epoch.
#[derive(Debug)]
pub struct BarrierAligner {
    input_count: usize,
    /// Set of input indices that have already reported a barrier for the current epoch.
    /// Using a HashSet prevents the same input from being counted more than once,
    /// which would cause premature alignment (fix for double-counting bug).
    barrier_inputs_seen: HashSet<usize>,
    current_epoch: Option<u64>,
    buffers: Vec<VecDeque<arrow::record_batch::RecordBatch>>,
    alignment_deadline: Option<Instant>,
    alignment_timeout: Duration,
}

impl BarrierAligner {
    pub fn new(input_count: usize, alignment_timeout: Duration) -> Result<Self, BarrierAlignError> {
        if input_count == 0 {
            return Err(BarrierAlignError::ZeroInputCount);
        }
        Ok(Self {
            input_count,
            barrier_inputs_seen: HashSet::new(),
            current_epoch: None,
            buffers: (0..input_count).map(|_| VecDeque::new()).collect(),
            alignment_deadline: None,
            alignment_timeout,
        })
    }

    /// Record a data batch on `input_index` while alignment is in progress.
    pub fn buffer_data(&mut self, input_index: usize, batch: arrow::record_batch::RecordBatch) {
        if let Some(buf) = self.buffers.get_mut(input_index) {
            buf.push_back(batch);
        }
    }

    /// Notify that `input_index` received barrier for `epoch`.
    ///
    /// Returns `Ok(true)` when all inputs have aligned and buffered data may be released.
    ///
    /// Each `input_index` is counted at most once per epoch — duplicate calls from
    /// the same input are idempotent and do not advance the alignment counter.
    pub fn on_barrier(
        &mut self,
        input_index: usize,
        epoch: u64,
    ) -> Result<bool, BarrierAlignError> {
        if self.current_epoch.is_none() {
            self.current_epoch = Some(epoch);
            self.barrier_inputs_seen.clear();
            self.alignment_deadline = Some(Instant::now() + self.alignment_timeout);
        }
        if self.current_epoch != Some(epoch) {
            return Ok(false);
        }
        // Insert returns false when the input_index was already present —
        // duplicates are silently ignored so one fast input cannot trigger
        // premature alignment by reporting multiple times.
        if input_index < self.input_count {
            self.barrier_inputs_seen.insert(input_index);
        }
        if self.barrier_inputs_seen.len() >= self.input_count {
            self.barrier_inputs_seen.clear();
            self.current_epoch = None;
            self.alignment_deadline = None;
            return Ok(true);
        }
        if let Some(deadline) = self.alignment_deadline
            && Instant::now() > deadline
        {
            let waited = self.barrier_inputs_seen.len();
            self.reset();
            return Err(BarrierAlignError::CheckpointAlignmentTimeout {
                epoch,
                waited_inputs: waited,
                expected_inputs: self.input_count,
            });
        }
        Ok(false)
    }

    /// Drain buffered batches for `input_index` after alignment completes.
    pub fn drain_buffer(&mut self, input_index: usize) -> Vec<arrow::record_batch::RecordBatch> {
        self.buffers
            .get_mut(input_index)
            .map(|q| q.drain(..).collect())
            .unwrap_or_default()
    }

    /// Reset alignment state so a new epoch can begin.
    /// Call this after a timeout or cancellation to unblock the aligner.
    pub fn reset(&mut self) {
        self.current_epoch = None;
        self.barrier_inputs_seen.clear();
        self.alignment_deadline = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(v: i32) -> arrow::record_batch::RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![v]))],
        )
        .unwrap()
    }

    #[test]
    fn barrier_alignment_buffers_until_all_inputs_ready() {
        let mut aligner = BarrierAligner::new(2, Duration::from_secs(5)).unwrap();
        aligner.buffer_data(0, batch(1));
        assert!(!aligner.on_barrier(0, 1).unwrap());
        aligner.buffer_data(0, batch(2));
        assert!(aligner.on_barrier(1, 1).unwrap());
        let released = aligner.drain_buffer(0);
        assert_eq!(released.len(), 2);
    }

    #[test]
    fn barrier_align_zero_input_count_returns_error() {
        let result = BarrierAligner::new(0, Duration::from_secs(5));
        assert!(result.is_err(), "input_count=0 must return Err");
    }

    #[test]
    fn barrier_align_single_input_immediately_aligned() {
        let mut aligner = BarrierAligner::new(1, Duration::from_secs(5)).unwrap();
        aligner.buffer_data(0, batch(10));
        assert!(aligner.on_barrier(0, 1).unwrap());
        let released = aligner.drain_buffer(0);
        assert_eq!(released.len(), 1);
        assert_eq!(
            released[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            10
        );
    }

    #[test]
    fn barrier_align_duplicate_input_ignored() {
        let mut aligner = BarrierAligner::new(2, Duration::from_secs(5)).unwrap();
        assert!(!aligner.on_barrier(0, 1).unwrap());
        // Duplicate from input 0 — should be ignored
        assert!(!aligner.on_barrier(0, 1).unwrap());
        // Now input 1 — should trigger alignment
        assert!(aligner.on_barrier(1, 1).unwrap());
    }

    #[test]
    fn barrier_align_different_epoch_ignored() {
        let mut aligner = BarrierAligner::new(2, Duration::from_secs(5)).unwrap();
        assert!(!aligner.on_barrier(0, 1).unwrap());
        // Wrong epoch — ignored
        assert!(!aligner.on_barrier(1, 2).unwrap());
        // Correct epoch
        assert!(aligner.on_barrier(1, 1).unwrap());
    }

    #[test]
    fn barrier_align_drain_multiple_inputs() {
        let mut aligner = BarrierAligner::new(3, Duration::from_secs(5)).unwrap();
        aligner.buffer_data(0, batch(1));
        aligner.buffer_data(1, batch(2));
        aligner.buffer_data(2, batch(3));
        aligner.on_barrier(0, 1).unwrap();
        aligner.on_barrier(1, 1).unwrap();
        assert!(aligner.on_barrier(2, 1).unwrap());
        let r0 = aligner.drain_buffer(0);
        let r1 = aligner.drain_buffer(1);
        let r2 = aligner.drain_buffer(2);
        assert_eq!(r0.len(), 1);
        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
    }

    #[test]
    fn barrier_align_out_of_range_input_index_ignored() {
        let mut aligner = BarrierAligner::new(2, Duration::from_secs(5)).unwrap();
        // input_index >= input_count is silently ignored (no panic, no alignment)
        assert!(!aligner.on_barrier(99, 1).unwrap());
        // Still need input 0 and 1
        assert!(!aligner.on_barrier(0, 1).unwrap());
        assert!(aligner.on_barrier(1, 1).unwrap());
    }

    #[test]
    fn barrier_align_epoch_resets_after_alignment() {
        let mut aligner = BarrierAligner::new(2, Duration::from_secs(5)).unwrap();
        aligner.on_barrier(0, 1).unwrap();
        aligner.on_barrier(1, 1).unwrap();
        // After alignment completes, a new epoch can start
        assert!(!aligner.on_barrier(0, 2).unwrap());
        assert!(aligner.on_barrier(1, 2).unwrap());
    }

    #[test]
    fn barrier_align_drain_empty_buffer() {
        let mut aligner = BarrierAligner::new(2, Duration::from_secs(5)).unwrap();
        let released = aligner.drain_buffer(0);
        assert!(released.is_empty());
    }

    #[test]
    fn barrier_align_invalid_input_index_buffer_data_ignored() {
        let mut aligner = BarrierAligner::new(2, Duration::from_secs(5)).unwrap();
        // buffer_data on out-of-range index should not panic
        aligner.buffer_data(99, batch(1));
        // Alignment should still work normally
        aligner.on_barrier(0, 1).unwrap();
        assert!(aligner.on_barrier(1, 1).unwrap());
    }

    #[test]
    fn barrier_align_display_trait() {
        let err = BarrierAlignError::ZeroInputCount;
        assert_eq!(format!("{err}"), "input_count must be greater than zero");

        let err = BarrierAlignError::CheckpointAlignmentTimeout {
            epoch: 42,
            waited_inputs: 1,
            expected_inputs: 3,
        };
        let msg = format!("{err}");
        assert!(msg.contains("42"));
        assert!(msg.contains("1/3"));
    }

    #[test]
    fn barrier_align_error_trait() {
        let err = BarrierAlignError::ZeroInputCount;
        let e: &dyn std::error::Error = &err;
        assert!(!e.source().is_some());
    }
}
