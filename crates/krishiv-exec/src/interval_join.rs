//! Stream-stream interval join (R16 S3.2).

use std::collections::VecDeque;

use arrow::record_batch::RecordBatch;

/// Interval join bounds relative to the opposite stream's event time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntervalJoinSpec {
    pub lower_bound_ms: i64,
    pub upper_bound_ms: i64,
}

/// Buffered event with event-time for interval matching.
#[derive(Debug, Clone)]
struct BufferedEvent {
    event_time_ms: i64,
    batch: RecordBatch,
}

/// Per-key buffers for both join sides.
#[derive(Debug)]
pub struct IntervalJoinState {
    left: VecDeque<BufferedEvent>,
    right: VecDeque<BufferedEvent>,
    spec: IntervalJoinSpec,
}

impl IntervalJoinState {
    pub fn new(spec: IntervalJoinSpec) -> Self {
        Self {
            left: VecDeque::new(),
            right: VecDeque::new(),
            spec,
        }
    }

    pub fn push_left(
        &mut self,
        event_time_ms: i64,
        batch: RecordBatch,
    ) -> Vec<(RecordBatch, RecordBatch)> {
        self.push_side(true, event_time_ms, batch)
    }

    pub fn push_right(
        &mut self,
        event_time_ms: i64,
        batch: RecordBatch,
    ) -> Vec<(RecordBatch, RecordBatch)> {
        self.push_side(false, event_time_ms, batch)
    }

    fn push_side(
        &mut self,
        is_left: bool,
        event_time_ms: i64,
        batch: RecordBatch,
    ) -> Vec<(RecordBatch, RecordBatch)> {
        let (this_buf, other_buf) = if is_left {
            (&mut self.left, &mut self.right)
        } else {
            (&mut self.right, &mut self.left)
        };
        let mut matches = Vec::new();
        for other in other_buf.iter() {
            let delta = event_time_ms - other.event_time_ms;
            if delta >= self.spec.lower_bound_ms && delta <= self.spec.upper_bound_ms {
                if is_left {
                    matches.push((batch.clone(), other.batch.clone()));
                } else {
                    matches.push((other.batch.clone(), batch.clone()));
                }
            }
        }
        this_buf.push_back(BufferedEvent {
            event_time_ms,
            batch,
        });
        matches
    }

    pub fn evict_before(&mut self, watermark_ms: i64) {
        let horizon = watermark_ms - self.spec.upper_bound_ms.max(self.spec.lower_bound_ms);
        self.left.retain(|e| e.event_time_ms >= horizon);
        self.right.retain(|e| e.event_time_ms >= horizon);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(v: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![v]))],
        )
        .unwrap()
    }

    #[test]
    fn overlapping_interval_emits_match() {
        let mut state = IntervalJoinState::new(IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
        });
        assert!(state.push_left(1000, batch(1)).is_empty());
        let matches = state.push_right(1050, batch(2));
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn non_overlapping_interval_drops() {
        let mut state = IntervalJoinState::new(IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 50,
        });
        state.push_left(1000, batch(1));
        assert!(state.push_right(2000, batch(2)).is_empty());
    }
}
