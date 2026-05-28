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
    /// Wall-clock event time (ms) of the most recent event processed for this
    /// key.  Updated on every `process_event` call; useful for idle-key
    /// detection and TTL eviction by the streaming executor.
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

        if let Some(ref mut partial) = state.partial {
            let expected_next = partial.stage_index + 1;
            if stage_idx != expected_next {
                return Vec::new();
            }
            partial.captured_events.push(batch);
            partial.stage_index = stage_idx;

            if partial.stage_index + 1 == self.pattern.stages.len() {
                return self.take_complete(state);
            }
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

#[allow(dead_code)] // compiled with workspace; integration lands in streaming exec path
/// Partitioned wrapper routing events to per-key [`SequentialPatternMatcher`] instances (P3-27).
#[derive(Debug, Clone)]
pub struct PartitionedCepMatcher<K>
where
    K: std::hash::Hash + Eq + Clone,
{
    pattern: CompiledPattern,
    states: std::collections::HashMap<K, (SequentialPatternMatcher, CepKeyState)>,
}

impl<K> PartitionedCepMatcher<K>
where
    K: std::hash::Hash + Eq + Clone,
{
    pub fn new(pattern: CompiledPattern) -> Self {
        Self {
            pattern: pattern.clone(),
            states: std::collections::HashMap::new(),
        }
    }

    pub fn process_event(
        &mut self,
        key: K,
        stage_name: &str,
        batch: RecordBatch,
        event_time_ms: i64,
    ) -> Vec<Vec<RecordBatch>> {
        let entry = self.states.entry(key).or_insert_with(|| {
            (
                SequentialPatternMatcher::new(self.pattern.clone()),
                CepKeyState::default(),
            )
        });
        entry
            .0
            .process_event(&mut entry.1, stage_name, batch, event_time_ms)
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
        assert!(
            matcher
                .process_event(&mut state, "a", batch(1), 100)
                .is_empty()
        );
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
        assert!(
            matcher
                .process_event(&mut state, "b", batch(2), 100)
                .is_empty()
        );
    }

    #[test]
    fn empty_pattern_compile_rejected() {
        let result = Pattern::begin("a")
            .compile()
            .unwrap() // 1-stage is fine
            ;
        assert_eq!(result.stages.len(), 1);
    }

    #[test]
    fn single_stage_match_completes_immediately() {
        let pattern = Pattern::begin("only")
            .within(Duration::from_secs(1))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        let done = matcher.process_event(&mut state, "only", batch(42), 100);
        assert_eq!(done.len(), 1, "single-stage pattern must complete on first match");
        assert_eq!(done[0].len(), 1);
        assert!(state.partial.is_none(), "state must be cleared after completion");
    }

    #[test]
    fn boundary_event_at_exact_window_limit() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_millis(100))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), 0);
        // Exactly at the window boundary (0 + 100 = 100) — should still match.
        let done = matcher.process_event(&mut state, "b", batch(2), 100);
        assert_eq!(done.len(), 1, "event at exact window boundary must match");
    }

    #[test]
    fn boundary_event_one_ms_past_window() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_millis(100))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), 0);
        // One ms past the window — must be discarded.
        let done = matcher.process_event(&mut state, "b", batch(2), 101);
        assert!(done.is_empty(), "event past window must be discarded");
    }

    #[test]
    fn partitioned_matcher_independent_keys() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let mut pm = PartitionedCepMatcher::<String>::new(pattern);
        // Key "k1": start match
        assert!(pm
            .process_event("k1".into(), "a", batch(1), 100)
            .is_empty());
        // Key "k2": start match
        assert!(pm
            .process_event("k2".into(), "a", batch(10), 200)
            .is_empty());
        // Key "k1": complete match
        let done = pm.process_event("k1".into(), "b", batch(2), 300);
        assert_eq!(done.len(), 1);
        // Key "k2": still pending
        assert!(pm
            .process_event("k2".into(), "a", batch(11), 400)
            .is_empty());
    }

    #[test]
    fn partitioned_matcher_independent_state() {
        let pattern = Pattern::begin("x")
            .followed_by("y")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let mut pm = PartitionedCepMatcher::<i32>::new(pattern);
        pm.process_event(1, "x", batch(1), 100);
        pm.process_event(2, "x", batch(2), 200);
        // Both keys should have partial matches.
        assert!(pm.states.contains_key(&1));
        assert!(pm.states.contains_key(&2));
    }
}
