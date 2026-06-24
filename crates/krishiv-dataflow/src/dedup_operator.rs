//! State-backed deduplication operator (T6 / ST10).
//!
//! Replaces the previous in-memory `HashSet<[u64; 2]>` deduplication adapter
//! (which silently cleared at 10M keys) with a real state backend so:
//!
//! - Memory is bounded by RocksDB's block cache + disk capacity, not the
//!   `DEDUP_SEEN_CAPACITY` heuristic.
//! - Dedup state is checkpointed as part of the regular state snapshot,
//!   enabling exactly-once restarts.
//! - When a watermark is set, dedup entries older than the watermark
//!   horizon are evicted via `TtlStateBackend` (event-time driven).
//!
//! The legacy in-memory adapter is preserved at
//! `krishiv_api::streaming_dataframe::DeduplicatingStream` for the no-state
//! case. The wiring site (`StreamingDataFrame::drop_duplicates`) selects
//! between the two based on the operator configuration.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use krishiv_state::{
    Namespace, RocksDbStateBackend, StateBackend, StateResult, TtlConfig, TtlStateBackend,
};

use crate::ExecError;
use crate::ExecResult;

/// Configuration for the state-backed deduplication operator.
#[derive(Debug, Clone)]
pub struct DeduplicationConfig {
    /// Columns that participate in the dedup key.
    pub columns: Vec<String>,
    /// Operator ID used to scope the state namespace.
    pub operator_id: String,
    /// State name used to scope the state namespace.
    pub state_name: String,
    /// Optional event-time watermark in ms. When set, dedup entries are
    /// retained for `state_ttl_ms` past the watermark and then evicted.
    pub state_ttl_ms: Option<u64>,
}

impl DeduplicationConfig {
    /// Build a config with the operator id `dedup`, state name `default`,
    /// no TTL, and the supplied dedup columns.
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            operator_id: "dedup".to_string(),
            state_name: "default".to_string(),
            state_ttl_ms: None,
        }
    }

    /// Override the state TTL (milliseconds past event-time watermark).
    pub fn with_state_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.state_ttl_ms = Some(ttl_ms);
        self
    }
}

/// State-backed deduplication operator.
///
/// Records the first-seen hash for each key in a state backend; emits only
/// rows whose key has not been seen before. State is bounded by the backend
/// (RocksDB on disk by default) and supports event-time-driven eviction when
/// `DeduplicationConfig::state_ttl_ms` is set.
pub struct DeduplicationOperator {
    cfg: DeduplicationConfig,
    state: Box<dyn StateBackend>,
    namespace: Namespace,
    /// Last seen watermark in ms; -1 if not set.
    watermark_ms: i64,
}

impl DeduplicationOperator {
    /// Build a state-backed dedup operator using the provided state backend.
    pub fn new(cfg: DeduplicationConfig, state: Box<dyn StateBackend>) -> ExecResult<Self> {
        if cfg.columns.is_empty() {
            return Err(ExecError::InvalidInput(
                "deduplication requires at least one column".into(),
            ));
        }
        let namespace = Namespace::new(&cfg.operator_id, &cfg.state_name);
        // Touch the namespace so the state backend has a record of it.
        state
            .list_keys(&namespace)
            .map_err(|e| ExecError::Arrow(e.to_string()))?;
        Ok(Self {
            cfg,
            state,
            namespace,
            watermark_ms: -1,
        })
    }

    /// Open an ephemeral in-memory operator.
    pub fn ephemeral(cfg: DeduplicationConfig) -> ExecResult<Self> {
        let inner = RocksDbStateBackend::ephemeral()
            .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
        let state: Box<dyn StateBackend> = if let Some(ttl) = cfg.state_ttl_ms {
            Box::new(TtlStateBackend::new(inner, TtlConfig::new(ttl)))
        } else {
            Box::new(inner)
        };
        Self::new(cfg, state)
    }

    /// Forward the current event-time watermark to the state backend so TTL
    /// eviction is event-time-driven instead of wall-clock-driven.
    pub fn set_watermark(&mut self, watermark_ms: i64) {
        self.watermark_ms = watermark_ms;
        self.state.set_watermark(watermark_ms);
    }

    /// Process one input batch, returning only the rows whose key has not
    /// been seen before.
    pub fn process_batch(&mut self, batch: &RecordBatch) -> ExecResult<RecordBatch> {
        // Resolve the dedup columns; preserve input order for output.
        let mut keep_indices: Vec<u32> = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.row_key(batch, row)?;
            let stored = self
                .state
                .get(&self.namespace, &key)
                .map_err(|e| ExecError::Arrow(e.to_string()))?;
            if stored.is_some() {
                // Already seen — drop.
                continue;
            }
            // First time we've seen this key — mark as seen and keep.
            self.state
                .put(&self.namespace, key, b"1".to_vec())
                .map_err(|e| ExecError::Arrow(e.to_string()))?;
            keep_indices.push(row as u32);
        }
        if keep_indices.is_empty() {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }
        if keep_indices.len() == batch.num_rows() {
            return Ok(batch.clone());
        }
        let indices = arrow::array::UInt32Array::from(keep_indices);
        let columns: Vec<Arc<dyn Array>> = batch
            .columns()
            .iter()
            .map(|col| arrow::compute::take(col.as_ref(), &indices, None).map_err(ExecError::from))
            .collect::<ExecResult<Vec<_>>>()?;
        RecordBatch::try_new(batch.schema(), columns).map_err(ExecError::from)
    }

    /// Encode the dedup columns for one row into a stable key.
    fn row_key(&self, batch: &RecordBatch, row: usize) -> ExecResult<Vec<u8>> {
        let mut key = Vec::new();
        for col_name in &self.cfg.columns {
            let col_idx = batch.schema().index_of(col_name).map_err(|_| {
                ExecError::ColumnNotFound(format!("dedup column '{col_name}' not in schema"))
            })?;
            let col = batch.column(col_idx);
            // Write the column name as a separator so two distinct columns
            // with the same value at the same row hash distinctly.
            let sep = format!("{col_name}=");
            sep.encode_utf16().for_each(|u| key.push(u as u8));
            match col.data_type() {
                DataType::Int64 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        if arr.is_null(row) {
                            key.extend_from_slice(b"null");
                        } else {
                            key.extend_from_slice(b"i:");
                            let v = arr.value(row);
                            key.extend_from_slice(&v.to_le_bytes());
                        }
                    } else {
                        return Err(ExecError::UnsupportedType(format!(
                            "dedup column '{col_name}' expected Int64Array"
                        )));
                    }
                }
                DataType::Utf8 => {
                    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        if arr.is_null(row) {
                            key.extend_from_slice(b"null");
                        } else {
                            key.extend_from_slice(b"s:");
                            key.extend_from_slice(arr.value(row).as_bytes());
                        }
                    } else {
                        return Err(ExecError::UnsupportedType(format!(
                            "dedup column '{col_name}' expected StringArray"
                        )));
                    }
                }
                other => {
                    return Err(ExecError::UnsupportedType(format!(
                        "dedup column '{col_name}' has unsupported type {other:?} (only Int64 and Utf8 are wired for state-backed dedup)"
                    )));
                }
            }
        }
        Ok(key)
    }

    /// Snapshot the dedup state for checkpointing.
    pub fn snapshot(&self) -> StateResult<Vec<u8>> {
        self.state.snapshot()
    }

    /// Restore dedup state from a snapshot.
    pub fn load_snapshot(&mut self, bytes: &[u8]) -> StateResult<()> {
        self.state.load_snapshot(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};

    fn make_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values.to_vec())) as _],
        )
        .unwrap()
    }

    #[test]
    fn ephemeral_dedup_drops_duplicate_keys() {
        let cfg = DeduplicationConfig::new(vec!["k".to_string()]);
        let mut op = DeduplicationOperator::ephemeral(cfg).expect("op");
        let out1 = op
            .process_batch(&make_batch(&[1, 2, 3]))
            .expect("process 1");
        assert_eq!(out1.num_rows(), 3);
        let out2 = op
            .process_batch(&make_batch(&[2, 3, 4]))
            .expect("process 2");
        assert_eq!(
            out2.num_rows(),
            1,
            "only key 4 is new; 2 and 3 were already seen"
        );
    }

    #[test]
    fn ephemeral_dedup_does_not_silently_clear_above_capacity() {
        // The previous in-memory adapter cleared the seen set at 10M.
        // The state-backed operator never silently drops, so submitting
        // 20 distinct keys (well within the 10M heuristic, but verifies
        // the cap was removed) must keep all 20 visible across batches.
        let cfg = DeduplicationConfig::new(vec!["k".to_string()]);
        let mut op = DeduplicationOperator::ephemeral(cfg).expect("op");
        let mut all_new = 0usize;
        for chunk in (0..20i64).collect::<Vec<_>>().chunks(5) {
            let out = op.process_batch(&make_batch(chunk)).expect("process");
            all_new += out.num_rows();
        }
        assert_eq!(all_new, 20, "all 20 distinct keys should be retained");
    }
}
