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
            // C4: Evaluate with fixed left/right orientation.
            // A left event at time L matches a right event at time R when
            // lower_bound_ms <= R - L <= upper_bound_ms.
            // For push_left: L = event_time_ms, R = other.event_time_ms, delta = R - L.
            // For push_right: R = event_time_ms, L = other.event_time_ms, delta = R - L.
            let delta = if is_left {
                other.event_time_ms - event_time_ms
            } else {
                event_time_ms - other.event_time_ms
            };
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
        // C4: Eviction horizon uses max absolute bound so asymmetric intervals
        // don't prematurely evict the longer side.
        let bound = self
            .spec
            .upper_bound_ms
            .abs()
            .max(self.spec.lower_bound_ms.abs());
        let horizon = watermark_ms - bound;
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

    #[test]
    fn evict_before_removes_old_events() {
        let mut state = IntervalJoinState::new(IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
        });
        state.push_left(1000, batch(1));
        state.push_left(2000, batch(2));
        state.push_right(1500, batch(3));
        // evict before watermark 2100: horizon = 2100 - 100 = 2000
        // left: keep 2000, remove 1000; right: keep 1500
        state.evict_before(2100);
        // left has 1 event (2000), right has 1 event (1500)
        // Push a new left at 2100 → should match right at 1500 (delta=600, within [-100,100]? No)
        let matches = state.push_left(2100, batch(4));
        assert!(matches.is_empty());
    }

    #[test]
    fn multiple_right_events_match_single_left() {
        let mut state = IntervalJoinState::new(IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
        });
        state.push_right(1000, batch(1));
        state.push_right(1050, batch(2));
        state.push_right(1100, batch(3));
        let matches = state.push_left(1050, batch(4));
        assert_eq!(matches.len(), 3); // all three right events match
    }

    #[test]
    fn symmetric_bounds_match_both_directions() {
        let mut state = IntervalJoinState::new(IntervalJoinSpec {
            lower_bound_ms: -50,
            upper_bound_ms: 50,
        });
        // left at t=1000, right at t=1030 → delta=30, within [-50,50]
        state.push_left(1000, batch(1));
        let matches = state.push_right(1030, batch(2));
        assert_eq!(matches.len(), 1);

        // left at t=2000, right at t=1970 → delta=-30, within [-50,50]
        state.push_left(2000, batch(3));
        let matches = state.push_right(1970, batch(4));
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn zero_bounds_exact_timestamp_match() {
        let mut state = IntervalJoinState::new(IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 0,
        });
        state.push_left(1000, batch(1));
        // Exact match at same timestamp
        let matches = state.push_right(1000, batch(2));
        assert_eq!(matches.len(), 1);

        // Off by 1 ms — no match
        let matches = state.push_right(1001, batch(3));
        assert!(matches.is_empty());
    }

    #[test]
    fn evict_before_empty_state_no_panic() {
        let mut state = IntervalJoinState::new(IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
        });
        state.evict_before(5000);
        // No panic, state remains empty
        assert!(state.left.is_empty());
        assert!(state.right.is_empty());
    }

    #[test]
    fn push_left_with_no_right_buffer_returns_empty() {
        let mut state = IntervalJoinState::new(IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
        });
        let matches = state.push_left(1000, batch(1));
        assert!(matches.is_empty());
    }

    #[test]
    fn push_right_with_no_left_buffer_returns_empty() {
        let mut state = IntervalJoinState::new(IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
        });
        let matches = state.push_right(1000, batch(1));
        assert!(matches.is_empty());
    }

    #[test]
    fn large_asymmetric_bounds() {
        let mut state = IntervalJoinState::new(IntervalJoinSpec {
            lower_bound_ms: -1000,
            upper_bound_ms: 100,
        });
        state.push_left(1000, batch(1));
        // Right at 100: delta = -900, within [-1000,100] → match
        let matches = state.push_right(100, batch(2));
        assert_eq!(matches.len(), 1);

        // Right at 2100: delta = 1100, > 100 → no match
        let matches = state.push_right(2100, batch(3));
        assert!(matches.is_empty());
    }
}
