#![forbid(unsafe_code)]

//! Bilinear incremental join operator.
//!
//! The DBSP identity for INNER join is:
//!
//!   Δ(A ⋈ B) = (ΔA ⋈ B_trace) + (A_trace ⋈ ΔB) + (ΔA ⋈ ΔB)
//!
//! LEFT OUTER JOIN extends this with null-padded output for unmatched left rows.
//! A `right_key_group_weights` map tracks the total accumulated right-side weight
//! per key group. When this count crosses zero (empty ↔ non-empty), the operator
//! emits or retracts null-padded rows for the affected left rows. The ΔA probe
//! uses a precomputed "effective right count" (current + ΔB delta) so same-tick
//! inserts on both sides are handled correctly without spurious null rows.

use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::{Array, Int32Builder, Int64Array, Int64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::delta_batch::{DeltaBatch, WEIGHT_COLUMN};
use crate::error::{DeltaError, DeltaResult};
use crate::trace::Trace;

/// Join type for incremental joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrJoinType {
    Inner,
    /// LEFT OUTER JOIN: unmatched left rows emit null-padded output.
    LeftOuter,
}

/// Bilinear incremental join operator.
///
/// Maintains two `Trace` objects (one per join side) and performs probe-based
/// hash-join on each tick, processing only the delta input.
pub struct IncrementalJoinOp {
    left_trace: Trace,
    right_trace: Trace,
    left_key_cols: Vec<String>,
    right_key_cols: Vec<String>,
    left_schema: SchemaRef,
    output_schema: SchemaRef,
    join_type: IncrJoinType,
    /// Number of output_schema columns that come from the left side (all left columns).
    /// Used by LEFT OUTER JOIN to know where to start appending null right columns.
    left_field_count: usize,
    /// For LEFT OUTER JOIN: maps right-side join key → total accumulated right weight.
    ///
    /// When this sum crosses zero (0→positive or positive→0), the operator
    /// retracts or emits null-padded output rows for all matching left rows.
    right_key_group_weights: AHashMap<Vec<Option<String>>, i64>,
}

impl IncrementalJoinOp {
    /// Create a new incremental join operator.
    ///
    /// * `left_schema` / `right_schema` — data schemas (no `_weight`)
    /// * `left_key_cols` / `right_key_cols` — matching join key column names
    /// * `join_type` — inner join only for now
    pub fn new(
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        left_key_cols: Vec<String>,
        right_key_cols: Vec<String>,
        join_type: IncrJoinType,
    ) -> DeltaResult<Self> {
        let left_key_refs: Vec<&str> = left_key_cols.iter().map(String::as_str).collect();
        let right_key_refs: Vec<&str> = right_key_cols.iter().map(String::as_str).collect();

        let left_trace = Trace::new(left_schema.clone(), &left_key_refs)?;
        let right_trace = Trace::new(right_schema.clone(), &right_key_refs)?;

        // Output schema: all left columns + right non-key columns.
        // For LEFT OUTER JOIN the right non-key columns must be nullable (they are
        // NULL when the left row has no match on the right side).
        let mut out_fields: Vec<_> = left_schema.fields().iter().cloned().collect();
        for field in right_schema.fields().iter() {
            if !right_key_cols.contains(field.name()) {
                let f = if join_type == IncrJoinType::LeftOuter {
                    Arc::new(Field::new(field.name(), field.data_type().clone(), true))
                } else {
                    field.clone()
                };
                out_fields.push(f);
            }
        }
        let output_schema = Arc::new(Schema::new(out_fields));

        let left_field_count = left_schema.fields().len();
        Ok(Self {
            left_trace,
            right_trace,
            left_key_cols,
            right_key_cols,
            left_schema,
            output_schema,
            join_type,
            left_field_count,
            right_key_group_weights: AHashMap::new(),
        })
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// GC both traces: drop entries with timestamp < `watermark_ms`.
    ///
    /// Returns the total number of rows removed across both traces.
    pub fn gc_traces(&mut self, watermark_ms: i64) -> crate::error::DeltaResult<usize> {
        let removed_left = self.left_trace.gc_below_watermark(watermark_ms)?;
        let removed_right = self.right_trace.gc_below_watermark(watermark_ms)?;
        Ok(removed_left + removed_right)
    }

    /// Apply one tick of the bilinear join.
    ///
    /// Returns the combined output delta for both INNER and LEFT OUTER join types.
    pub fn apply(
        &mut self,
        delta_left: Option<DeltaBatch>,
        delta_right: Option<DeltaBatch>,
    ) -> DeltaResult<DeltaBatch> {
        if self.join_type == IncrJoinType::LeftOuter {
            return self.apply_left_outer(delta_left, delta_right);
        }
        // Inner join path.
        let mut output_parts: Vec<DeltaBatch> = Vec::new();

        // Step 1: ΔA ⋈ B_trace
        if let Some(dl) = &delta_left
            && !dl.is_empty()
        {
            let probe_result = self.probe_left_against_right_trace(dl)?;
            if !probe_result.is_empty() {
                output_parts.push(probe_result);
            }
        }

        // Step 2: A_trace ⋈ ΔB
        if let Some(dr) = &delta_right
            && !dr.is_empty()
        {
            let probe_result = self.probe_right_against_left_trace(dr)?;
            if !probe_result.is_empty() {
                output_parts.push(probe_result);
            }
        }

        // Step 1.5: ΔA ⋈ ΔB — same-tick cross term
        // Both deltas arrive in the same tick. Probe ΔB's keys against ΔA's
        // data to catch pairs where both sides were updated simultaneously.
        if let (Some(dl), Some(dr)) = (&delta_left, &delta_right)
            && !dl.is_empty()
            && !dr.is_empty()
        {
            let cross_result = self.join_deltas(dl, dr)?;
            if !cross_result.is_empty() {
                output_parts.push(cross_result);
            }
        }

        // Step 3: update traces AFTER probe (traces reflect state from previous ticks)
        if let Some(dl) = delta_left {
            self.left_trace.insert(dl);
        }
        if let Some(dr) = delta_right {
            self.right_trace.insert(dr);
        }

        // Step 4: combine output
        if output_parts.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }
        if output_parts.len() == 1 {
            return Ok(output_parts.remove(0));
        }
        DeltaBatch::concat(&output_parts)
    }

    // ── Internal probe methods ─────────────────────────────────────────────────

    fn join_deltas(
        &self,
        delta_left: &DeltaBatch,
        delta_right: &DeltaBatch,
    ) -> DeltaResult<DeltaBatch> {
        // ΔA ⋈ ΔB: same-tick cross term
        let left_data = delta_left.data_batch();
        let right_data = delta_right.data_batch();
        let left_weights = delta_left.weights();
        let right_weights = delta_right.weights();

        let left_key_indices = col_indices(&left_data, &self.left_key_cols)?;
        let right_key_indices = col_indices(&right_data, &self.right_key_cols)?;

        let mut out_left_rows: Vec<usize> = Vec::new();
        let mut out_right_rows: Vec<usize> = Vec::new();
        let mut out_weights: Vec<i64> = Vec::new();

        for li in 0..left_data.num_rows() {
            for ri in 0..right_data.num_rows() {
                if keys_match(
                    &left_data,
                    &left_key_indices,
                    li,
                    &right_data,
                    &right_key_indices,
                    ri,
                ) {
                    out_left_rows.push(li);
                    out_right_rows.push(ri);
                    out_weights.push(left_weights.value(li) * right_weights.value(ri));
                }
            }
        }

        if out_left_rows.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        build_join_batch(
            &left_data,
            &right_data,
            &self.right_key_cols,
            &out_left_rows,
            &out_right_rows,
            out_weights,
            &self.output_schema,
        )
    }

    fn probe_left_against_right_trace(&self, delta_left: &DeltaBatch) -> DeltaResult<DeltaBatch> {
        // For each row in delta_left, look up matching rows in right_trace.
        // Output row weight = delta_left.weight (the right_trace rows have
        // accumulated weight +1 in standard usage).
        let left_data = delta_left.data_batch();
        let left_weights = delta_left.weights();

        // Extract key values from left delta to probe right trace.
        let left_key_data = project_columns(&left_data, &self.left_key_cols)?;
        let right_matches = self.right_trace.probe_by_keys(&left_key_data)?;

        if right_matches.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        self.build_join_output_left_probe(&left_data, left_weights, &right_matches)
    }

    fn probe_right_against_left_trace(&self, delta_right: &DeltaBatch) -> DeltaResult<DeltaBatch> {
        let right_data = delta_right.data_batch();
        let right_weights = delta_right.weights();

        let right_key_data = project_columns(&right_data, &self.right_key_cols)?;
        let left_matches = self.left_trace.probe_by_keys(&right_key_data)?;

        if left_matches.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        self.build_join_output_right_probe(&left_matches, &right_data, right_weights)
    }

    /// Build output rows: for each (left_row, right_row) pair where join keys match,
    /// emit one output row with weight = left_delta.weight * right_trace.weight.
    fn build_join_output_left_probe(
        &self,
        left_data: &RecordBatch,
        left_weights: &Int64Array,
        right_matches: &DeltaBatch,
    ) -> DeltaResult<DeltaBatch> {
        let right_data = right_matches.data_batch();
        let right_weights = right_matches.weights();

        let left_key_indices = col_indices(left_data, &self.left_key_cols)?;
        let right_key_indices = col_indices(&right_data, &self.right_key_cols)?;

        let mut out_left_rows: Vec<usize> = Vec::new();
        let mut out_right_rows: Vec<usize> = Vec::new();
        let mut out_weights: Vec<i64> = Vec::new();

        for li in 0..left_data.num_rows() {
            for ri in 0..right_data.num_rows() {
                if keys_match(
                    left_data,
                    &left_key_indices,
                    li,
                    &right_data,
                    &right_key_indices,
                    ri,
                ) {
                    out_left_rows.push(li);
                    out_right_rows.push(ri);
                    out_weights.push(left_weights.value(li) * right_weights.value(ri));
                }
            }
        }

        if out_left_rows.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        build_join_batch(
            left_data,
            &right_data,
            &self.right_key_cols,
            &out_left_rows,
            &out_right_rows,
            out_weights,
            &self.output_schema,
        )
    }

    fn build_join_output_right_probe(
        &self,
        left_matches: &DeltaBatch,
        right_data: &RecordBatch,
        right_weights: &Int64Array,
    ) -> DeltaResult<DeltaBatch> {
        let left_data = left_matches.data_batch();
        let left_weights = left_matches.weights();

        let left_key_indices = col_indices(&left_data, &self.left_key_cols)?;
        let right_key_indices = col_indices(right_data, &self.right_key_cols)?;

        let mut out_left_rows: Vec<usize> = Vec::new();
        let mut out_right_rows: Vec<usize> = Vec::new();
        let mut out_weights: Vec<i64> = Vec::new();

        for li in 0..left_data.num_rows() {
            for ri in 0..right_data.num_rows() {
                if keys_match(
                    &left_data,
                    &left_key_indices,
                    li,
                    right_data,
                    &right_key_indices,
                    ri,
                ) {
                    out_left_rows.push(li);
                    out_right_rows.push(ri);
                    out_weights.push(left_weights.value(li) * right_weights.value(ri));
                }
            }
        }

        if out_left_rows.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        build_join_batch(
            &left_data,
            right_data,
            &self.right_key_cols,
            &out_left_rows,
            &out_right_rows,
            out_weights,
            &self.output_schema,
        )
    }

    // ── LEFT OUTER JOIN implementation ────────────────────────────────────────

    /// Full tick for LEFT OUTER JOIN.
    ///
    /// Processes ΔA and ΔB using the bilinear identity, extended with:
    /// - Null-padded rows for ΔA rows that have no matching right rows.
    /// - Threshold-crossing null row retractions/emissions when a key's total
    ///   right weight crosses zero.
    ///
    /// Uses a precomputed "effective right count" (current + ΔB net) for ΔA
    /// so that same-tick ΔA+ΔB arrivals on the same key produce the correct
    /// joined output without spurious null rows.
    fn apply_left_outer(
        &mut self,
        delta_left: Option<DeltaBatch>,
        delta_right: Option<DeltaBatch>,
    ) -> DeltaResult<DeltaBatch> {
        // Precompute net right weight change from ΔB for same-tick ΔA processing.
        let rw_delta = if let Some(ref dr) = delta_right {
            if !dr.is_empty() {
                let rd = dr.data_batch();
                let rw = dr.weights();
                let rki = col_indices(&rd, &self.right_key_cols)?;
                let mut m: AHashMap<Vec<Option<String>>, i64> = AHashMap::new();
                for ri in 0..rd.num_rows() {
                    *m.entry(extract_key(&rd, ri, &rki)).or_insert(0) += rw.value(ri);
                }
                m
            } else {
                AHashMap::new()
            }
        } else {
            AHashMap::new()
        };

        let mut output_parts: Vec<DeltaBatch> = Vec::new();

        // Step 1: ΔA probe (using effective right counts = current + ΔB delta).
        if let Some(ref dl) = delta_left
            && !dl.is_empty()
        {
            let result = self.probe_left_outer_against_right_trace(dl, &rw_delta)?;
            if !result.is_empty() {
                output_parts.push(result);
            }
        }

        // Step 2: A_trace ⋈ ΔB + threshold-crossing null rows.
        // Uses original right_key_group_weights (before ΔB is applied), so
        // threshold crossings are computed relative to the pre-tick state.
        if let Some(ref dr) = delta_right
            && !dr.is_empty()
        {
            let mut parts = self.probe_right_outer_against_left_trace(dr)?;
            output_parts.append(&mut parts);
        }

        // Step 1.5: ΔA ⋈ ΔB same-tick cross term.
        if let (Some(dl), Some(dr)) = (&delta_left, &delta_right)
            && !dl.is_empty()
            && !dr.is_empty()
        {
            let cross = self.join_deltas(dl, dr)?;
            if !cross.is_empty() {
                output_parts.push(cross);
            }
        }

        // Update traces AFTER all probes.
        if let Some(dl) = delta_left {
            self.left_trace.insert(dl);
        }
        if let Some(dr) = delta_right {
            self.right_trace.insert(dr);
        }

        combine_parts(output_parts, &self.output_schema)
    }

    /// ΔA probe for LEFT OUTER JOIN.
    ///
    /// Uses `effective_rw = current + rw_delta` to account for same-tick ΔB
    /// arrivals before committing to null-padded vs. joined output for ΔA rows.
    fn probe_left_outer_against_right_trace(
        &self,
        delta_left: &DeltaBatch,
        rw_delta: &AHashMap<Vec<Option<String>>, i64>,
    ) -> DeltaResult<DeltaBatch> {
        let left_data = delta_left.data_batch();
        let left_weights = delta_left.weights();
        let lki = col_indices(&left_data, &self.left_key_cols)?;

        let mut null_rows: Vec<usize> = Vec::new();
        let mut null_weights_vec: Vec<i64> = Vec::new();
        let mut matched_rows: Vec<usize> = Vec::new();

        for li in 0..left_data.num_rows() {
            let key = extract_key(&left_data, li, &lki);
            let cur_rw = self.right_key_group_weights.get(&key).copied().unwrap_or(0);
            let eff_rw = cur_rw + rw_delta.get(&key).copied().unwrap_or(0);
            if eff_rw == 0 {
                null_rows.push(li);
                null_weights_vec.push(left_weights.value(li));
            } else {
                matched_rows.push(li);
            }
        }

        let mut parts: Vec<DeltaBatch> = Vec::new();

        if !null_rows.is_empty() {
            parts.push(build_null_padded_batch(
                &left_data,
                &null_rows,
                null_weights_vec,
                &self.output_schema,
                self.left_field_count,
            )?);
        }

        if !matched_rows.is_empty() {
            let matched_delta = select_rows(delta_left, &matched_rows)?;
            let join_out = self.probe_left_against_right_trace(&matched_delta)?;
            if !join_out.is_empty() {
                parts.push(join_out);
            }
        }

        combine_parts(parts, &self.output_schema)
    }

    /// ΔB probe for LEFT OUTER JOIN.
    ///
    /// Performs standard inner-join probe (left_trace ⋈ ΔB) and additionally
    /// handles threshold crossings in `right_key_group_weights`:
    /// - 0 → positive: retract null-padded rows for matching left trace rows.
    /// - positive → 0: emit null-padded rows for matching left trace rows.
    ///
    /// Updates `right_key_group_weights` as a side-effect.
    fn probe_right_outer_against_left_trace(
        &mut self,
        delta_right: &DeltaBatch,
    ) -> DeltaResult<Vec<DeltaBatch>> {
        let right_data = delta_right.data_batch();
        let right_weights = delta_right.weights();
        let rki = col_indices(&right_data, &self.right_key_cols)?;

        // Group ΔB by key and sum weights.
        let mut delta_by_key: AHashMap<Vec<Option<String>>, i64> = AHashMap::new();
        for ri in 0..right_data.num_rows() {
            *delta_by_key
                .entry(extract_key(&right_data, ri, &rki))
                .or_insert(0) += right_weights.value(ri);
        }

        let mut null_to_matched: Vec<Vec<Option<String>>> = Vec::new();
        let mut matched_to_null: Vec<Vec<Option<String>>> = Vec::new();

        for (key, dw) in &delta_by_key {
            let old_w = self.right_key_group_weights.get(key).copied().unwrap_or(0);
            let new_w = old_w + dw;
            if new_w == 0 {
                self.right_key_group_weights.remove(key);
            } else {
                self.right_key_group_weights.insert(key.clone(), new_w);
            }
            if old_w == 0 && new_w > 0 {
                null_to_matched.push(key.clone());
            } else if old_w > 0 && new_w == 0 {
                matched_to_null.push(key.clone());
            }
        }

        let mut results: Vec<DeltaBatch> = Vec::new();

        // Standard inner-join probe always applies.
        let join_out = self.probe_right_against_left_trace(delta_right)?;
        if !join_out.is_empty() {
            results.push(join_out);
        }

        // 0→positive crossing: retract null rows for affected left trace rows.
        if !null_to_matched.is_empty()
            && let Ok(probe_batch) =
                keys_to_probe_batch(&null_to_matched, &self.left_key_cols, &self.left_schema)
            && let Ok(left_matches) = self.left_trace.probe_by_keys(&probe_batch)
            && !left_matches.is_empty()
        {
            let lm = left_matches.data_batch();
            let lmw = left_matches.weights();
            let n = lm.num_rows();
            let w: Vec<i64> = (0..n).map(|i| -lmw.value(i)).collect();
            let null_ret = build_null_padded_batch(
                &lm,
                &(0..n).collect::<Vec<_>>(),
                w,
                &self.output_schema,
                self.left_field_count,
            )?;
            if !null_ret.is_empty() {
                results.push(null_ret);
            }
        }

        // positive→0 crossing: emit null rows for affected left trace rows.
        if !matched_to_null.is_empty()
            && let Ok(probe_batch) =
                keys_to_probe_batch(&matched_to_null, &self.left_key_cols, &self.left_schema)
            && let Ok(left_matches) = self.left_trace.probe_by_keys(&probe_batch)
            && !left_matches.is_empty()
        {
            let lm = left_matches.data_batch();
            let lmw = left_matches.weights();
            let n = lm.num_rows();
            let w: Vec<i64> = (0..n).map(|i| lmw.value(i)).collect();
            let null_emit = build_null_padded_batch(
                &lm,
                &(0..n).collect::<Vec<_>>(),
                w,
                &self.output_schema,
                self.left_field_count,
            )?;
            if !null_emit.is_empty() {
                results.push(null_emit);
            }
        }

        Ok(results)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Extract key column values from a single row as `Vec<Option<String>>`.
fn extract_key(batch: &RecordBatch, row: usize, key_indices: &[usize]) -> Vec<Option<String>> {
    key_indices
        .iter()
        .map(|&i| extract_str_opt(batch.column(i).as_ref(), row))
        .collect()
}

fn extract_str_opt(arr: &dyn Array, row: usize) -> Option<String> {
    use arrow::array::{
        BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    if arr.is_null(row) {
        return None;
    }
    // Signed integers
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int16Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int8Array>() {
        return Some(a.value(row).to_string());
    }
    // Unsigned integers
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt32Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt16Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt8Array>() {
        return Some(a.value(row).to_string());
    }
    // Floats: use bit-repr for stable, injective join keys.
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row).to_bits().to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return Some((a.value(row).to_bits() as u64).to_string());
    }
    // String types
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() {
        return Some(a.value(row).to_string());
    }
    // Boolean (join on bool columns: rare but valid)
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Some((a.value(row) as u8).to_string());
    }
    // Date / Timestamp — raw integer value is the join key
    if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Date64Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Some(a.value(row).to_string());
    }
    None
}

/// Build a DeltaBatch of null-padded left rows (output for unmatched LEFT OUTER rows).
///
/// `row_indices` selects which rows from `left_data` to include.
/// Right non-key columns (positions `left_field_count..output_schema.fields().len()`)
/// are filled with Arrow null arrays.
fn build_null_padded_batch(
    left_data: &RecordBatch,
    row_indices: &[usize],
    weights: Vec<i64>,
    output_schema: &SchemaRef,
    left_field_count: usize,
) -> DeltaResult<DeltaBatch> {
    let n = row_indices.len();
    let take_indices =
        arrow::array::UInt64Array::from(row_indices.iter().map(|&r| r as u64).collect::<Vec<_>>());

    let mut cols: Vec<Arc<dyn Array>> = left_data
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c, &take_indices, None).map_err(DeltaError::Arrow))
        .collect::<DeltaResult<Vec<_>>>()?;

    for i in left_field_count..output_schema.fields().len() {
        cols.push(arrow::array::new_null_array(
            output_schema.field(i).data_type(),
            n,
        ));
    }

    cols.push(Arc::new(Int64Array::from(weights)));

    let mut full_fields: Vec<_> = output_schema.fields().iter().cloned().collect();
    full_fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
    let inner = RecordBatch::try_new(Arc::new(Schema::new(full_fields)), cols)?;
    DeltaBatch::from_weighted(inner)
}

/// Build a key probe RecordBatch from a list of `Vec<Option<String>>` keys.
///
/// Used to probe the left trace for threshold-crossing keys. Reconstructs
/// typed arrays (Int64, Int32, Utf8) from the string-encoded key values.
fn keys_to_probe_batch(
    crossing_keys: &[Vec<Option<String>>],
    key_col_names: &[String],
    full_schema: &SchemaRef,
) -> DeltaResult<RecordBatch> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
    let mut fields: Vec<Arc<Field>> = Vec::new();

    for (col_pos, col_name) in key_col_names.iter().enumerate() {
        let field = full_schema
            .field_with_name(col_name)
            .map_err(|_| DeltaError::ColumnNotFound(col_name.clone()))?;

        let arr: Arc<dyn Array> = match field.data_type() {
            DataType::Int64 => {
                let mut b = Int64Builder::new();
                for k in crossing_keys {
                    match k
                        .get(col_pos)
                        .and_then(|v| v.as_ref())
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Int32 => {
                let mut b = Int32Builder::new();
                for k in crossing_keys {
                    match k
                        .get(col_pos)
                        .and_then(|v| v.as_ref())
                        .and_then(|s| s.parse::<i32>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            _ => {
                let mut b = StringBuilder::new();
                for k in crossing_keys {
                    match k.get(col_pos).and_then(|v| v.as_deref()) {
                        Some(s) => b.append_value(s),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(arr);
        fields.push(Arc::new(field.clone()));
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(DeltaError::Arrow)
}

/// Select specific rows from a DeltaBatch (preserving weights).
fn select_rows(delta: &DeltaBatch, indices: &[usize]) -> DeltaResult<DeltaBatch> {
    let data = delta.data_batch();
    let weights = delta.weights();
    let take_indices =
        arrow::array::UInt64Array::from(indices.iter().map(|&r| r as u64).collect::<Vec<_>>());

    let mut cols: Vec<Arc<dyn Array>> = data
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c, &take_indices, None).map_err(DeltaError::Arrow))
        .collect::<DeltaResult<Vec<_>>>()?;

    cols.push(
        arrow::compute::take(weights as &dyn Array, &take_indices, None)
            .map_err(DeltaError::Arrow)?,
    );

    let mut fields: Vec<_> = data.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
    let inner = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?;
    DeltaBatch::from_weighted(inner)
}

/// Combine output parts into a single DeltaBatch, or return empty if none.
fn combine_parts(mut parts: Vec<DeltaBatch>, output_schema: &SchemaRef) -> DeltaResult<DeltaBatch> {
    match parts.len() {
        0 => DeltaBatch::empty(output_schema.clone()),
        1 => Ok(parts.remove(0)),
        _ => DeltaBatch::concat(&parts),
    }
}

fn col_indices(batch: &RecordBatch, cols: &[String]) -> DeltaResult<Vec<usize>> {
    cols.iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map_err(|_| DeltaError::ColumnNotFound(name.clone()))
        })
        .collect()
}

fn project_columns(batch: &RecordBatch, col_names: &[String]) -> DeltaResult<RecordBatch> {
    let indices = col_indices(batch, col_names)?;
    let fields: Vec<_> = indices
        .iter()
        .map(|&i| Arc::new(batch.schema().field(i).clone()))
        .collect();
    let cols: Vec<Arc<dyn Array>> = indices.iter().map(|&i| batch.column(i).clone()).collect();
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
}

fn keys_match(
    left: &RecordBatch,
    left_indices: &[usize],
    li: usize,
    right: &RecordBatch,
    right_indices: &[usize],
    ri: usize,
) -> bool {
    left_indices
        .iter()
        .zip(right_indices.iter())
        .all(|(&lk, &rk)| {
            let la = left.column(lk);
            let ra = right.column(rk);
            scalar_eq(la, li, ra, ri)
        })
}

fn scalar_eq(a: &dyn Array, ai: usize, b: &dyn Array, bi: usize) -> bool {
    use arrow::array::{Int32Array, Int64Array, StringArray};
    if a.is_null(ai) && b.is_null(bi) {
        return true;
    }
    if a.is_null(ai) || b.is_null(bi) {
        return false;
    }
    if let (Some(av), Some(bv)) = (
        a.as_any().downcast_ref::<Int64Array>(),
        b.as_any().downcast_ref::<Int64Array>(),
    ) {
        return av.value(ai) == bv.value(bi);
    }
    if let (Some(av), Some(bv)) = (
        a.as_any().downcast_ref::<Int32Array>(),
        b.as_any().downcast_ref::<Int32Array>(),
    ) {
        return av.value(ai) == bv.value(bi);
    }
    if let (Some(av), Some(bv)) = (
        a.as_any().downcast_ref::<StringArray>(),
        b.as_any().downcast_ref::<StringArray>(),
    ) {
        return av.value(ai) == bv.value(bi);
    }
    false
}

fn build_join_batch(
    left_data: &RecordBatch,
    right_data: &RecordBatch,
    right_key_cols: &[String],
    left_rows: &[usize],
    right_rows: &[usize],
    weights: Vec<i64>,
    output_schema: &SchemaRef,
) -> DeltaResult<DeltaBatch> {
    let left_indices =
        arrow::array::UInt64Array::from(left_rows.iter().map(|&r| r as u64).collect::<Vec<_>>());
    let right_indices =
        arrow::array::UInt64Array::from(right_rows.iter().map(|&r| r as u64).collect::<Vec<_>>());

    let left_cols: Vec<Arc<dyn Array>> = left_data
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c, &left_indices, None).map_err(DeltaError::Arrow))
        .collect::<DeltaResult<Vec<_>>>()?;

    let right_non_key_cols: Vec<Arc<dyn Array>> = right_data
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !right_key_cols.contains(f.name()))
        .map(|(i, _)| {
            arrow::compute::take(right_data.column(i), &right_indices, None)
                .map_err(DeltaError::Arrow)
        })
        .collect::<DeltaResult<Vec<_>>>()?;

    let mut all_cols: Vec<Arc<dyn Array>> = left_cols;
    all_cols.extend(right_non_key_cols);
    all_cols.push(Arc::new(Int64Array::from(weights)));

    // Build the full schema (output_schema + _weight).
    let mut full_fields: Vec<_> = output_schema.fields().iter().cloned().collect();
    full_fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
    let full_schema = Arc::new(Schema::new(full_fields));

    let inner = RecordBatch::try_new(full_schema, all_cols)?;
    DeltaBatch::from_weighted(inner)
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn orders_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
        ]))
    }

    fn customers_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn orders_batch(order_ids: &[i32], cust_ids: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            orders_schema(),
            vec![
                Arc::new(Int32Array::from(order_ids.to_vec())),
                Arc::new(Int32Array::from(cust_ids.to_vec())),
            ],
        )
        .unwrap()
    }

    fn customers_batch(cust_ids: &[i32], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            customers_schema(),
            vec![
                Arc::new(Int32Array::from(cust_ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn join_delta_left_against_trace_right() {
        let mut op = IncrementalJoinOp::new(
            orders_schema(),
            customers_schema(),
            vec!["customer_id".into()],
            vec!["customer_id".into()],
            IncrJoinType::Inner,
        )
        .unwrap();

        // Tick 1: insert right (customers) only
        let c = DeltaBatch::from_inserts(customers_batch(&[1, 2], &["Alice", "Bob"])).unwrap();
        let out1 = op.apply(None, Some(c)).unwrap();
        assert!(out1.is_empty(), "no left delta → no output yet");

        // Tick 2: insert left (orders) — should join with right trace
        let o = DeltaBatch::from_inserts(orders_batch(&[100, 101], &[1, 2])).unwrap();
        let out2 = op.apply(Some(o), None).unwrap();
        assert_eq!(
            out2.num_rows(),
            2,
            "two orders should join with two customers"
        );
        assert!(out2.weights().iter().all(|w| w == Some(1)));
    }

    #[test]
    fn join_retraction_propagates_negative_weight() {
        let mut op = IncrementalJoinOp::new(
            orders_schema(),
            customers_schema(),
            vec!["customer_id".into()],
            vec!["customer_id".into()],
            IncrJoinType::Inner,
        )
        .unwrap();

        // Build up traces first
        let c = DeltaBatch::from_inserts(customers_batch(&[1], &["Alice"])).unwrap();
        op.apply(None, Some(c)).unwrap();
        let o = DeltaBatch::from_inserts(orders_batch(&[100], &[1])).unwrap();
        op.apply(Some(o), None).unwrap();

        // Delete a customer → should produce retraction in output
        let del_c = DeltaBatch::from_deletes(customers_batch(&[1], &["Alice"])).unwrap();
        let out = op.apply(None, Some(del_c)).unwrap();
        assert!(!out.is_empty());
        assert_eq!(out.weights().value(0), -1);
    }

    fn left_outer_op() -> IncrementalJoinOp {
        IncrementalJoinOp::new(
            orders_schema(),
            customers_schema(),
            vec!["customer_id".into()],
            vec!["customer_id".into()],
            IncrJoinType::LeftOuter,
        )
        .unwrap()
    }

    #[test]
    fn left_outer_unmatched_left_emits_null_row() {
        // Insert order for cust 99 (no customer exists). Expect null-padded output.
        let mut op = left_outer_op();
        let order = DeltaBatch::from_inserts(orders_batch(&[1], &[99])).unwrap();
        let out = op.apply(Some(order), None).unwrap();
        assert!(!out.is_empty(), "expected null-padded row");
        let data = out.data_batch();
        // 'name' column (right non-key) should be null
        let name_col = data.column_by_name("name").expect("name column");
        assert!(
            name_col.is_null(0),
            "name should be null for unmatched left"
        );
        assert_eq!(out.weights().value(0), 1);
    }

    #[test]
    fn left_outer_matched_left_emits_joined_row() {
        // Insert customer first, then insert matching order.
        let mut op = left_outer_op();
        let cust = DeltaBatch::from_inserts(customers_batch(&[5], &["Bob"])).unwrap();
        op.apply(None, Some(cust)).unwrap();

        let order = DeltaBatch::from_inserts(orders_batch(&[10], &[5])).unwrap();
        let out = op.apply(Some(order), None).unwrap();
        assert!(!out.is_empty(), "expected joined row");
        let data = out.data_batch();
        let name_col = data.column_by_name("name").expect("name column");
        assert!(
            !name_col.is_null(0),
            "name should not be null when match exists"
        );
    }

    #[test]
    fn left_outer_right_arrives_retracts_null_row() {
        // Order inserted first (no customer) → null row. Then customer arrives → null retracted, join emitted.
        let mut op = left_outer_op();
        let order = DeltaBatch::from_inserts(orders_batch(&[20], &[7])).unwrap();
        op.apply(Some(order), None).unwrap();

        let cust = DeltaBatch::from_inserts(customers_batch(&[7], &["Carol"])).unwrap();
        let out = op.apply(None, Some(cust)).unwrap();
        assert!(!out.is_empty());

        // Should see a null-retraction (weight -1) and a join-insertion (weight +1).
        let mut has_null_retract = false;
        let mut has_join_insert = false;
        let data = out.data_batch();
        let name_col = data.column_by_name("name").expect("name column");
        for i in 0..out.data_batch().num_rows() {
            let w = out.weights().value(i);
            let is_null = name_col.is_null(i);
            if is_null && w == -1 {
                has_null_retract = true;
            }
            if !is_null && w == 1 {
                has_join_insert = true;
            }
        }
        assert!(has_null_retract, "expected null row retraction");
        assert!(has_join_insert, "expected joined row insertion");
    }

    #[test]
    fn left_outer_right_retracted_emits_null_row() {
        // Customer and matching order both present. Retract customer → join retracted, null emitted.
        let mut op = left_outer_op();
        let cust = DeltaBatch::from_inserts(customers_batch(&[3], &["Dave"])).unwrap();
        let order = DeltaBatch::from_inserts(orders_batch(&[30], &[3])).unwrap();
        op.apply(Some(order), Some(cust)).unwrap();

        let del_cust = DeltaBatch::from_deletes(customers_batch(&[3], &["Dave"])).unwrap();
        let out = op.apply(None, Some(del_cust)).unwrap();
        assert!(!out.is_empty());

        let data = out.data_batch();
        let name_col = data.column_by_name("name").expect("name column");
        let mut has_null_emit = false;
        let mut has_join_retract = false;
        for i in 0..data.num_rows() {
            let w = out.weights().value(i);
            let is_null = name_col.is_null(i);
            if is_null && w == 1 {
                has_null_emit = true;
            }
            if !is_null && w == -1 {
                has_join_retract = true;
            }
        }
        assert!(
            has_null_emit,
            "expected null row emission after right retraction"
        );
        assert!(has_join_retract, "expected join retraction");
    }

    #[test]
    fn left_outer_same_tick_left_and_right_no_null_row() {
        // Both order and matching customer arrive in the same tick. Should produce
        // only the joined row — no null row emitted and immediately retracted.
        let mut op = left_outer_op();
        let order = DeltaBatch::from_inserts(orders_batch(&[40], &[8])).unwrap();
        let cust = DeltaBatch::from_inserts(customers_batch(&[8], &["Eve"])).unwrap();
        let out = op.apply(Some(order), Some(cust)).unwrap();

        let data = out.data_batch();
        let name_col = data.column_by_name("name").expect("name column");
        for i in 0..data.num_rows() {
            assert!(
                !name_col.is_null(i),
                "no null rows expected when right arrives same tick"
            );
        }
        // Net: exactly one joined row with weight +1
        let pos: Vec<_> = (0..data.num_rows())
            .filter(|&i| out.weights().value(i) > 0)
            .collect();
        assert_eq!(pos.len(), 1);
    }
}
