//! Per-key sequential pattern matcher (R16 S2.2).

use arrow::record_batch::RecordBatch;

use crate::pattern::CompiledPattern;

/// Partial in-progress match.
#[derive(Debug, Clone)]
pub struct PartialMatch {
    pub stage_index: usize,
    pub captured_events: Vec<RecordBatch>,
    pub start_time_ms: i64,
}

/// Per-key CEP state.
#[derive(Debug, Default, Clone)]
pub struct CepKeyState {
    pub partial: Option<PartialMatch>,
    pub last_event_ms: i64,
}

/// Sequential pattern matcher for one key.
#[derive(Debug, Clone)]
pub struct SequentialPatternMatcher {
    pattern: CompiledPattern,
}

impl SequentialPatternMatcher {
    pub fn new(pattern: CompiledPattern) -> Self {
        Self { pattern }
    }

    pub fn process_event(
        &self,
        state: &mut CepKeyState,
        stage_name: &str,
        batch: RecordBatch,
        event_time_ms: i64,
    ) -> Vec<Vec<RecordBatch>> {
        state.last_event_ms = event_time_ms;
        if let Some(ref partial) = state.partial
            && event_time_ms - partial.start_time_ms > self.pattern.window_ms as i64
        {
            state.partial = None;
        }

        let stage_idx = self
            .pattern
            .stages
            .iter()
            .position(|s| s.name == stage_name);

        let Some(stage_idx) = stage_idx else {
            return Vec::new();
        };

        if state.partial.is_none() {
            if stage_idx != 0 {
                return Vec::new();
            }
            state.partial = Some(PartialMatch {
                stage_index: 0,
                captured_events: vec![batch],
                start_time_ms: event_time_ms,
            });
            if self.pattern.stages.len() == 1 {
                return self.take_complete(state);
            }
            return Vec::new();
        }

        let partial = state.partial.as_mut().unwrap();
        let expected_next = partial.stage_index + 1;
        if stage_idx != expected_next {
            return Vec::new();
        }
        partial.captured_events.push(batch);
        partial.stage_index = stage_idx;

        if partial.stage_index + 1 == self.pattern.stages.len() {
            return self.take_complete(state);
        }
        Vec::new()
    }

    fn take_complete(&self, state: &mut CepKeyState) -> Vec<Vec<RecordBatch>> {
        state
            .partial
            .take()
            .map(|p| vec![p.captured_events])
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::Pattern;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::time::Duration;

    fn batch(v: i32) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![v]))],
        )
        .unwrap()
    }

    #[test]
    fn two_stage_pattern_matches() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        assert!(matcher.process_event(&mut state, "a", batch(1), 100).is_empty());
        let done = matcher.process_event(&mut state, "b", batch(2), 200);
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].len(), 2);
    }

    #[test]
    fn expired_partial_discarded() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_millis(50))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), 0);
        assert!(matcher.process_event(&mut state, "b", batch(2), 100).is_empty());
    }
}
