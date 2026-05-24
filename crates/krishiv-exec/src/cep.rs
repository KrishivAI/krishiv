//! CEP physical operator wrapper (R16 S2.3).

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use krishiv_cep::{CepKeyState, CompiledPattern, SequentialPatternMatcher};

/// Keyed CEP operator executing a compiled sequential pattern.
#[derive(Debug)]
pub struct CepOperator {
    matcher: SequentialPatternMatcher,
    key_column: String,
    states: HashMap<Vec<u8>, CepKeyState>,
}

impl CepOperator {
    pub fn new(pattern: CompiledPattern, key_column: impl Into<String>) -> Self {
        Self {
            matcher: SequentialPatternMatcher::new(pattern),
            key_column: key_column.into(),
            states: HashMap::new(),
        }
    }

    pub fn key_column(&self) -> &str {
        &self.key_column
    }

    /// Process a batch keyed by raw key bytes; `stage_name` identifies the pattern stage.
    pub fn process_batch(
        &mut self,
        key: Vec<u8>,
        stage_name: &str,
        batch: RecordBatch,
        event_time_ms: i64,
    ) -> Vec<Vec<RecordBatch>> {
        let state = self.states.entry(key).or_default();
        self.matcher
            .process_event(state, stage_name, batch, event_time_ms)
    }

    /// Barriers pass through without clearing CEP state (matches R16 spec).
    pub fn on_barrier(&mut self, _epoch: u64) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_cep::Pattern;
    use std::time::Duration;

    #[test]
    fn cep_operator_emits_on_match() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(1))
            .compile()
            .unwrap();
        let mut op = CepOperator::new(pattern, "k");
        use arrow::array::{Int32Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let batch = |v: i32| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![v]))],
            )
            .unwrap()
        };
        assert!(
            op.process_batch(b"k1".to_vec(), "a", batch(1), 10)
                .is_empty()
        );
        assert_eq!(op.process_batch(b"k1".to_vec(), "b", batch(2), 20).len(), 1);
    }
}
