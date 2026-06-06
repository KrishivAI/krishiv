//! CEP physical operator wrapper (R16 S2.3).

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use krishiv_cep::{CepKeyState, CompiledPattern, SequentialPatternMatcher};
use krishiv_state::{Namespace, StateBackend, StateError, StateResult};

use crate::ExecResult;

/// Keyed CEP operator executing a compiled sequential pattern.
#[derive(Debug)]
pub struct CepOperator {
    matcher: SequentialPatternMatcher,
    key_column: String,
    states: HashMap<Vec<u8>, CepKeyState>,
    last_barrier_epoch: u64,
}

impl CepOperator {
    pub fn new(pattern: CompiledPattern, key_column: impl Into<String>) -> Self {
        Self {
            matcher: SequentialPatternMatcher::new(pattern),
            key_column: key_column.into(),
            states: HashMap::new(),
            last_barrier_epoch: 0,
        }
    }

    pub fn last_barrier_epoch(&self) -> u64 {
        self.last_barrier_epoch
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

    /// Checkpoint barrier fence: record the committed epoch for restore alignment.
    pub fn on_barrier(&mut self, epoch: u64) {
        self.last_barrier_epoch = epoch;
    }

    /// Snapshot per-key partial CEP metadata to the state backend (Wave 2 durable partial state).
    pub fn persist_to_state(
        &self,
        backend: &mut dyn StateBackend,
        namespace: &Namespace,
    ) -> StateResult<()> {
        backend.clear_namespace(namespace)?;

        if !self.states.is_empty() {
            let op_id = namespace.operator_id();
            let name = namespace.state_name();
            let mut entries = Vec::with_capacity(self.states.len());
            for (key, state) in &self.states {
                let payload = serde_json::to_vec(state).map_err(|e| StateError::CorruptEntry {
                    message: e.to_string(),
                })?;
                let mut state_key = Vec::with_capacity(4 + 4 + key.len());
                state_key.extend_from_slice(b"cep:");
                state_key.extend_from_slice(&(key.len() as u32).to_le_bytes());
                state_key.extend_from_slice(key);
                entries.push((state_key, payload));
            }
            let batch_entries: Vec<(&str, &str, &[u8], &[u8])> = entries
                .iter()
                .map(|(k, v)| (op_id, name, k.as_slice(), v.as_slice()))
                .collect();
            backend.put_batch(&batch_entries)?;
        }
        backend.put(
            namespace,
            b"cep:epoch".to_vec(),
            self.last_barrier_epoch.to_le_bytes().to_vec(),
        )?;
        Ok(())
    }

    /// Restore per-key partial CEP metadata from the state backend.
    pub fn restore_from_state(
        &mut self,
        backend: &dyn StateBackend,
        namespace: &Namespace,
    ) -> StateResult<()> {
        let mut restored = HashMap::new();
        for key_bytes in backend.list_keys(namespace)? {
            if key_bytes.starts_with(b"cep:epoch") {
                continue;
            }
            if !key_bytes.starts_with(b"cep:") || key_bytes.len() < 8 {
                continue;
            }
            let key_len =
                u32::from_le_bytes([key_bytes[4], key_bytes[5], key_bytes[6], key_bytes[7]])
                    as usize;
            if key_bytes.len() < 8 + key_len {
                continue;
            }
            let key = key_bytes[8..8 + key_len].to_vec();
            let Some(payload) = backend.get(namespace, &key_bytes)? else {
                continue;
            };
            let state: CepKeyState =
                serde_json::from_slice(&payload).map_err(|e| StateError::CorruptEntry {
                    message: e.to_string(),
                })?;
            restored.insert(key, state);
        }
        self.states = restored;
        if let Some(epoch_bytes) = backend.get(namespace, b"cep:epoch")? {
            if epoch_bytes.len() >= 8 {
                self.last_barrier_epoch = u64::from_le_bytes([
                    epoch_bytes[0],
                    epoch_bytes[1],
                    epoch_bytes[2],
                    epoch_bytes[3],
                    epoch_bytes[4],
                    epoch_bytes[5],
                    epoch_bytes[6],
                    epoch_bytes[7],
                ]);
            }
        }
        Ok(())
    }

    /// JSON snapshot of per-key metadata for checkpoint operator snapshots.
    pub fn snapshot_states_json(&self) -> ExecResult<Vec<u8>> {
        let map: HashMap<String, CepKeyState> = self
            .states
            .iter()
            .map(|(k, v)| (encode_key_hex(k), v.clone()))
            .collect();
        serde_json::to_vec(&map).map_err(|e| {
            crate::ExecError::InvalidWindowConfig(format!("CEP snapshot encode failed: {e}"))
        })
    }

    /// Restore per-key metadata from a JSON snapshot.
    pub fn restore_states_json(&mut self, bytes: &[u8]) -> ExecResult<()> {
        let map: HashMap<String, CepKeyState> = serde_json::from_slice(bytes).map_err(|e| {
            crate::ExecError::InvalidWindowConfig(format!("CEP snapshot decode failed: {e}"))
        })?;
        self.states = map
            .into_iter()
            .filter_map(|(hex_key, state)| decode_key_hex(&hex_key).map(|key| (key, state)))
            .collect();
        Ok(())
    }
}

fn encode_key_hex(key: &[u8]) -> String {
    key.iter().map(|b| format!("{b:02x}")).collect()
}

fn decode_key_hex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
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

    #[test]
    fn cep_state_round_trips_through_backend() {
        use krishiv_state::{FjallStateBackend, Namespace};

        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(1))
            .compile()
            .unwrap();
        let mut op = CepOperator::new(pattern, "k");
        op.on_barrier(7);
        op.states.entry(b"k1".to_vec()).or_default().last_event_ms = 42;

        let mut backend = FjallStateBackend::ephemeral().expect("ephemeral backend");
        let ns = Namespace::new("job-1", "op-cep");
        op.persist_to_state(&mut backend, &ns).expect("persist");

        let mut restored = CepOperator::new(
            Pattern::begin("a")
                .followed_by("b")
                .within(Duration::from_secs(1))
                .compile()
                .unwrap(),
            "k",
        );
        restored.restore_from_state(&backend, &ns).expect("restore");
        assert_eq!(restored.last_barrier_epoch(), 7);
        assert_eq!(
            restored
                .states
                .get(&b"k1".to_vec())
                .map(|s| s.last_event_ms),
            Some(42)
        );
    }
}
