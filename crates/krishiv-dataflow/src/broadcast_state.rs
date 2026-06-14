#![forbid(unsafe_code)]

//! Broadcast state pattern: a broadcast stream updates shared read-only state
//! visible to all parallel tasks of a keyed operator.
//!
//! Inspired by Apache Flink's `BroadcastProcessFunction`. The broadcast side
//! (e.g. a configuration or rule stream) is forwarded to all tasks and stored
//! in a shared `broadcast_state`. The keyed side processes per-key events and
//! can read (but not write) the broadcast state.

use std::collections::HashMap;
use std::marker::PhantomData;

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::ExecResult;

// ── BroadcastStateDescriptor ──────────────────────────────────────────────────

/// Descriptor for broadcast state (shared read-only map broadcast to all tasks).
///
/// The descriptor identifies a named broadcast-state slot and the key/value
/// types stored in it.
pub struct BroadcastStateDescriptor<K, V> {
    name: String,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> BroadcastStateDescriptor<K, V> {
    /// Create a new descriptor with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }

    /// The descriptor name (used as a key in the raw broadcast-state map).
    pub fn name(&self) -> &str {
        &self.name
    }
}

// ── BroadcastContext ──────────────────────────────────────────────────────────

/// Context passed to [`BroadcastProcessFunction`] callbacks.
///
/// Provides access to broadcast state (shared across all tasks), per-key state
/// for the current keyed event, and output collection.
pub struct BroadcastContext<'a> {
    /// Current event-time watermark in milliseconds.
    pub watermark_ms: i64,
    /// Raw serialised broadcast state (one entry per descriptor name).
    pub broadcast_state: &'a mut HashMap<String, Vec<u8>>,
    /// Raw serialised per-key state for the current key.
    pub keyed_state: &'a mut Vec<u8>,
    /// Output batches emitted by this callback.
    pub output: &'a mut Vec<RecordBatch>,
}

impl<'a> BroadcastContext<'a> {
    /// Append an output record batch.
    pub fn emit(&mut self, batch: RecordBatch) {
        self.output.push(batch);
    }
}

// ── BroadcastProcessFunction trait ───────────────────────────────────────────

/// Process function for a broadcast pattern.
pub trait BroadcastProcessFunction: Send {
    /// Called for each event in the keyed (non-broadcast) stream.
    fn on_keyed_event(
        &mut self,
        key: &str,
        batch: &RecordBatch,
        row: usize,
        ctx: &mut BroadcastContext<'_>,
    ) -> ExecResult<()>;

    /// Called for each event in the broadcast stream (updates broadcast state).
    fn on_broadcast_event(
        &mut self,
        batch: &RecordBatch,
        row: usize,
        ctx: &mut BroadcastContext<'_>,
    ) -> ExecResult<()>;
}

// ── Snapshot ─────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct BroadcastSnapshot {
    keyed_state: HashMap<String, Vec<u8>>,
    broadcast_state: HashMap<String, Vec<u8>>,
    current_watermark_ms: i64,
}

// ── BroadcastProcessExecutor ──────────────────────────────────────────────────

/// Executor for a [`BroadcastProcessFunction`].
pub struct BroadcastProcessExecutor {
    func: Box<dyn BroadcastProcessFunction>,
    key_column: String,
    /// Per-key state for keyed-stream events.
    keyed_state: HashMap<String, Vec<u8>>,
    /// Shared broadcast state, keyed by descriptor name.
    broadcast_state: HashMap<String, Vec<u8>>,
    current_watermark_ms: i64,
}

impl BroadcastProcessExecutor {
    /// Create a new executor.
    pub fn new(key_column: impl Into<String>, func: Box<dyn BroadcastProcessFunction>) -> Self {
        Self {
            func,
            key_column: key_column.into(),
            keyed_state: HashMap::new(),
            broadcast_state: HashMap::new(),
            current_watermark_ms: i64::MIN,
        }
    }

    /// Process a batch from the keyed (non-broadcast) stream.
    pub fn process_keyed_batch(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        self.current_watermark_ms = self.current_watermark_ms.max(watermark_ms);

        let key_idx = batch
            .schema()
            .index_of(&self.key_column)
            .map_err(|_| crate::ExecError::ColumnNotFound(self.key_column.clone()))?;

        let mut output = Vec::new();

        for row in 0..batch.num_rows() {
            let key = crate::join::extract_agg_key(batch, key_idx, row)?;
            let key_str = key.to_string();
            let key_state = self.keyed_state.entry(key_str.clone()).or_default();

            let mut ctx = BroadcastContext {
                watermark_ms,
                broadcast_state: &mut self.broadcast_state,
                keyed_state: key_state,
                output: &mut output,
            };

            self.func.on_keyed_event(&key_str, batch, row, &mut ctx)?;
        }

        Ok(output)
    }

    /// Process a batch from the broadcast stream.
    ///
    /// This batch is forwarded to all instances; it updates the shared
    /// `broadcast_state` and does not route by key.
    pub fn process_broadcast_batch(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        self.current_watermark_ms = self.current_watermark_ms.max(watermark_ms);

        let mut output = Vec::new();
        // Use a dummy keyed state since there is no key for broadcast events.
        let mut dummy_keyed_state: Vec<u8> = Vec::new();

        for row in 0..batch.num_rows() {
            let mut ctx = BroadcastContext {
                watermark_ms,
                broadcast_state: &mut self.broadcast_state,
                keyed_state: &mut dummy_keyed_state,
                output: &mut output,
            };

            self.func.on_broadcast_event(batch, row, &mut ctx)?;
        }

        Ok(output)
    }

    /// Return a reference to the broadcast state map.
    pub fn broadcast_state(&self) -> &HashMap<String, Vec<u8>> {
        &self.broadcast_state
    }

    /// Return a reference to the per-key state map.
    pub fn keyed_state_map(&self) -> &HashMap<String, Vec<u8>> {
        &self.keyed_state
    }

    /// Serialize state to a snapshot blob.
    pub fn snapshot(&self) -> Vec<u8> {
        let snap = BroadcastSnapshot {
            keyed_state: self.keyed_state.clone(),
            broadcast_state: self.broadcast_state.clone(),
            current_watermark_ms: self.current_watermark_ms,
        };
        serde_json::to_vec(&snap).unwrap_or_default()
    }

    /// Restore state from a snapshot blob.
    pub fn restore(&mut self, bytes: &[u8]) -> ExecResult<()> {
        let snap: BroadcastSnapshot = serde_json::from_slice(bytes)
            .map_err(|e| crate::ExecError::InvalidInput(e.to_string()))?;

        for (k, v) in snap.keyed_state {
            self.keyed_state.insert(k, v);
        }
        for (k, v) in snap.broadcast_state {
            self.broadcast_state.insert(k, v);
        }
        self.current_watermark_ms = self.current_watermark_ms.max(snap.current_watermark_ms);
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn keyed_batch(keys: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "user_id",
            DataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(keys.to_vec()))]).unwrap()
    }

    fn broadcast_batch(rules: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "rule_id",
            DataType::Int32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(rules.to_vec()))]).unwrap()
    }

    /// A BroadcastProcessFunction that:
    /// - On broadcast events: stores rule IDs in broadcast_state under "rules".
    /// - On keyed events: checks broadcast_state and emits if a rule is active.
    struct RuleCheckerFn {
        matched_keys: Vec<String>,
    }

    impl RuleCheckerFn {
        fn new() -> Self {
            Self {
                matched_keys: Vec::new(),
            }
        }
    }

    impl BroadcastProcessFunction for RuleCheckerFn {
        fn on_broadcast_event(
            &mut self,
            batch: &RecordBatch,
            row: usize,
            ctx: &mut BroadcastContext<'_>,
        ) -> ExecResult<()> {
            // Read rule_id from the broadcast batch.
            let rule_col = batch
                .column_by_name("rule_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rule_id = rule_col.value(row);

            // Store rule_id list in broadcast_state.
            let mut rules: Vec<i32> = ctx
                .broadcast_state
                .get("rules")
                .and_then(|v| serde_json::from_slice(v).ok())
                .unwrap_or_default();
            rules.push(rule_id);
            ctx.broadcast_state
                .insert("rules".to_owned(), serde_json::to_vec(&rules).unwrap());
            Ok(())
        }

        fn on_keyed_event(
            &mut self,
            key: &str,
            _batch: &RecordBatch,
            _row: usize,
            ctx: &mut BroadcastContext<'_>,
        ) -> ExecResult<()> {
            // Check if there are any rules active.
            let rules: Vec<i32> = ctx
                .broadcast_state
                .get("rules")
                .and_then(|v| serde_json::from_slice(v).ok())
                .unwrap_or_default();

            if !rules.is_empty() {
                self.matched_keys.push(key.to_owned());
            }
            Ok(())
        }
    }

    #[test]
    fn broadcast_updates_shared_state() {
        let func = RuleCheckerFn::new();
        let mut exec = BroadcastProcessExecutor::new("user_id", Box::new(func));

        // Process broadcast batch (adds rules).
        let bcast = broadcast_batch(&[10, 20]);
        exec.process_broadcast_batch(&bcast, 0).unwrap();

        // Verify broadcast state was updated.
        let rules: Vec<i32> = exec
            .broadcast_state()
            .get("rules")
            .and_then(|v| serde_json::from_slice(v).ok())
            .unwrap_or_default();
        assert_eq!(rules.len(), 2);
        assert!(rules.contains(&10));
        assert!(rules.contains(&20));

        // Process keyed batch — should see rules.
        let keyed = keyed_batch(&["alice", "bob"]);
        exec.process_keyed_batch(&keyed, 0).unwrap();

        // The keyed state map should have entries for alice and bob.
        assert_eq!(exec.keyed_state_map().len(), 2);
    }

    #[test]
    fn broadcast_snapshot_restore() {
        let func = RuleCheckerFn::new();
        let mut exec = BroadcastProcessExecutor::new("user_id", Box::new(func));

        // Add broadcast state.
        let bcast = broadcast_batch(&[42]);
        exec.process_broadcast_batch(&bcast, 100).unwrap();

        // Process a keyed event so keyed_state is populated.
        let keyed = keyed_batch(&["alice"]);
        exec.process_keyed_batch(&keyed, 100).unwrap();

        let snap = exec.snapshot();

        // Restore into a new executor.
        let func2 = RuleCheckerFn::new();
        let mut exec2 = BroadcastProcessExecutor::new("user_id", Box::new(func2));
        exec2.restore(&snap).unwrap();

        // Broadcast state must be restored.
        let rules: Vec<i32> = exec2
            .broadcast_state()
            .get("rules")
            .and_then(|v| serde_json::from_slice(v).ok())
            .unwrap_or_default();
        assert_eq!(rules, vec![42]);

        // Keyed state must be restored.
        assert_eq!(exec2.keyed_state_map().len(), 1);
        assert!(exec2.keyed_state_map().contains_key("alice"));
    }
}
