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
use crate::operators::key_util::scalar_to_key;
use crate::trace::Trace;

/// Join type for incremental joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrJoinType {
    Inner,
    /// LEFT OUTER JOIN: unmatched left rows emit null-padded output.
    LeftOuter,
    /// SEMI-1: emit each left row ONCE while its key has at least one right
    /// match — no pair multiplication, LEFT COLUMNS only. The right side is a
    /// membership test, which is exactly what a decorrelated IN/EXISTS asks.
    LeftSemi,
    /// SEMI-1: the complement — each left row while its key has NO right
    /// match (decorrelated NOT IN / NOT EXISTS).
    LeftAnti,
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
    /// The right side's data schema, kept for SEMI-3's pair batches.
    right_schema: SchemaRef,
    /// SEMI-3: a non-equi MEMBERSHIP condition for semi/anti joins — `EXISTS
    /// (… WHERE key match AND l2.l_suppkey != l1.l_suppkey)`. Membership is
    /// then per LEFT ROW, not per key: a left row is a member iff the summed
    /// weight of key-matching right rows PASSING this predicate is positive
    /// (semi) or zero (anti). Evaluated over pair batches whose columns are
    /// left fields ++ right fields, positionally — the caller compiled the
    /// expression against that layout. Plan-compiled, never checkpointed.
    membership_residual: Option<MembershipResidual>,
}

/// SEMI-3: evaluates the membership predicate over a (left ++ right) pair
/// batch, returning the boolean mask. NULL means "does not pass", exactly
/// SQL's EXISTS semantics.
pub type MembershipResidual =
    Arc<dyn Fn(&RecordBatch) -> DeltaResult<arrow::array::BooleanArray> + Send + Sync>;

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
        Self::new_with_lateness(
            left_schema,
            right_schema,
            left_key_cols,
            right_key_cols,
            join_type,
            None,
        )
    }

    /// Create a new incremental join operator with an optional lateness column
    /// for watermark-driven GC of the join traces.
    ///
    /// IVM-3: without calling `with_lateness_column`, `gc_below_watermark` is a
    /// universal no-op and join traces grow unbounded.
    pub fn new_with_lateness(
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        left_key_cols: Vec<String>,
        right_key_cols: Vec<String>,
        join_type: IncrJoinType,
        lateness_col: Option<&str>,
    ) -> DeltaResult<Self> {
        let left_key_refs: Vec<&str> = left_key_cols.iter().map(String::as_str).collect();
        let right_key_refs: Vec<&str> = right_key_cols.iter().map(String::as_str).collect();

        let mut left_trace = Trace::new(left_schema.clone(), &left_key_refs)?;
        let mut right_trace = Trace::new(right_schema.clone(), &right_key_refs)?;
        if let Some(col) = lateness_col {
            left_trace = left_trace.with_lateness_column(col)?;
            right_trace = right_trace.with_lateness_column(col)?;
        }

        // Output schema: all left columns + right non-key columns — except
        // SEMI/ANTI (SEMI-1), whose relation is the LEFT columns alone: the
        // right side is a membership test, not a data source.
        // For LEFT OUTER JOIN the right non-key columns must be nullable (they
        // are NULL when the left row has no match on the right side).
        let mut out_fields: Vec<_> = left_schema.fields().iter().cloned().collect();
        if !matches!(join_type, IncrJoinType::LeftSemi | IncrJoinType::LeftAnti) {
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
            right_schema,
            membership_residual: None,
        })
    }

    /// SEMI-3: install the non-equi membership predicate (semi/anti only —
    /// the caller guarantees the join type). See [`MembershipResidual`].
    pub fn set_membership_residual(&mut self, residual: MembershipResidual) {
        self.membership_residual = Some(residual);
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
        if matches!(
            self.join_type,
            IncrJoinType::LeftSemi | IncrJoinType::LeftAnti
        ) {
            if self.membership_residual.is_some() {
                return self.apply_left_semi_anti_residual(delta_left, delta_right);
            }
            return self.apply_left_semi_anti(delta_left, delta_right);
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

        // IVM-AUD-PERF-2: this was `for li { for ri { keys_match } }` — a
        // nested loop over BOTH deltas, so a tick feeding 5k rows to each side
        // ran 25 million comparisons and took ~6 s, against ~25 ms for full
        // recompute of the same query. The three NEXMark two-source joins
        // (q3, q8, q20) were 200-300x SLOWER incrementally than recomputing
        // from scratch, which is not a trade-off: an O(delta) plan that loses
        // to O(state) recompute is a defect. Measured shape before the fix —
        // seed held fixed, delta scaled 1k/2k/5k -> 220 ms/687 ms/8040 ms:
        // quadratic in the delta, i.e. the loop, not accumulated state.
        //
        // Hashing the right delta once and probing it per left row makes the
        // term O(|dL| + |dR|). The key is the SAME `Vec<Option<String>>`
        // encoding `extract_key` builds from `scalar_to_key`, which reproduces
        // `scalar_eq` exactly, including its deliberately non-SQL rule that a
        // NULL key MATCHES a NULL key (`None == None`). Emission order is
        // unchanged — left-major with right rows ascending — because the probe
        // walks left rows in order and each bucket keeps its rows in insertion
        // (ascending) order.
        let (out_left_rows, out_right_rows, out_weights) = pair_rows_by_key(
            &left_data,
            &left_key_indices,
            left_weights,
            &right_data,
            &right_key_indices,
            right_weights,
        )?;

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

        // IVM-AUD-PERF-2: hash-indexed pairing. This site is where the cost
        // scaled with ACCUMULATED STATE: `right_data` is the set of trace rows
        // whose keys matched this delta, so the old nested loop was
        // O(delta x matched-trace-rows).
        let (out_left_rows, out_right_rows, out_weights) = pair_rows_by_key(
            left_data,
            &left_key_indices,
            left_weights,
            &right_data,
            &right_key_indices,
            right_weights,
        )?;

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

        // IVM-AUD-PERF-2: hash-indexed pairing; the mirror of the left probe,
        // where `left_data` is the matched trace side.
        let (out_left_rows, out_right_rows, out_weights) = pair_rows_by_key(
            &left_data,
            &left_key_indices,
            left_weights,
            right_data,
            &right_key_indices,
            right_weights,
        )?;

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
    /// SEMI-1: LEFT SEMI / LEFT ANTI. Rides the LEFT OUTER machinery's
    /// per-key right-weight crossings: a left row belongs to the SEMI
    /// relation while its key's right count is positive and to the ANTI
    /// relation while it is zero, each with the row's own weight — never a
    /// pair product. ΔA rows evaluate against EFFECTIVE counts (current +
    /// this tick's ΔB net) and ΔB crossings probe the pre-tick left trace,
    /// the same same-tick discipline `apply_left_outer` uses, so the two
    /// halves never double-count one transition.
    fn apply_left_semi_anti(
        &mut self,
        delta_left: Option<DeltaBatch>,
        delta_right: Option<DeltaBatch>,
    ) -> DeltaResult<DeltaBatch> {
        let semi = self.join_type == IncrJoinType::LeftSemi;
        // Net ΔB weight per key, for effective-count ΔA evaluation.
        let rw_delta = if let Some(ref dr) = delta_right {
            if !dr.is_empty() {
                let rd = dr.data_batch();
                let rw = dr.weights();
                let rki = col_indices(&rd, &self.right_key_cols)?;
                let mut m: AHashMap<Vec<Option<String>>, i64> = AHashMap::new();
                for ri in 0..rd.num_rows() {
                    *m.entry(extract_key(&rd, ri, &rki)?).or_insert(0) += rw.value(ri);
                }
                m
            } else {
                AHashMap::new()
            }
        } else {
            AHashMap::new()
        };

        let mut parts: Vec<DeltaBatch> = Vec::new();

        // ΔA: a left row enters the relation iff the mode's membership test
        // holds under the effective right count.
        if let Some(ref dl) = delta_left
            && !dl.is_empty()
        {
            let left_data = dl.data_batch();
            let lki = col_indices(&left_data, &self.left_key_cols)?;
            let mut keep: Vec<usize> = Vec::new();
            for li in 0..left_data.num_rows() {
                let key = extract_key(&left_data, li, &lki)?;
                let cur = self.right_key_group_weights.get(&key).copied().unwrap_or(0);
                let eff = cur + rw_delta.get(&key).copied().unwrap_or(0);
                let member = if semi { eff > 0 } else { eff == 0 };
                if member {
                    keep.push(li);
                }
            }
            if !keep.is_empty() {
                parts.push(select_rows(dl, &keep)?);
            }
        }

        // ΔB: key crossings flip membership for every left-trace row of the
        // crossed key. SEMI gains on 0→positive and loses on positive→0;
        // ANTI is the mirror image.
        if let Some(ref dr) = delta_right
            && !dr.is_empty()
        {
            let right_data = dr.data_batch();
            let right_weights = dr.weights();
            let rki = col_indices(&right_data, &self.right_key_cols)?;
            let mut delta_by_key: AHashMap<Vec<Option<String>>, i64> = AHashMap::new();
            for ri in 0..right_data.num_rows() {
                *delta_by_key
                    .entry(extract_key(&right_data, ri, &rki)?)
                    .or_insert(0) += right_weights.value(ri);
            }
            let mut gained: Vec<Vec<Option<String>>> = Vec::new();
            let mut lost: Vec<Vec<Option<String>>> = Vec::new();
            for (key, dw) in &delta_by_key {
                let old_w = self.right_key_group_weights.get(key).copied().unwrap_or(0);
                let new_w = old_w + dw;
                if new_w == 0 {
                    self.right_key_group_weights.remove(key);
                } else {
                    self.right_key_group_weights.insert(key.clone(), new_w);
                }
                let (crossed_up, crossed_down) = (old_w == 0 && new_w > 0, old_w > 0 && new_w == 0);
                match (semi, crossed_up, crossed_down) {
                    (true, true, _) | (false, _, true) => gained.push(key.clone()),
                    (true, _, true) | (false, true, _) => lost.push(key.clone()),
                    _ => {}
                }
            }
            for (keys, sign) in [(&gained, 1i64), (&lost, -1i64)] {
                if keys.is_empty() {
                    continue;
                }
                let probe_batch =
                    keys_to_probe_batch(keys, &self.left_key_cols, &self.left_schema)?;
                let left_matches = self.left_trace.probe_by_keys(&probe_batch)?;
                if left_matches.is_empty() {
                    continue;
                }
                let lm = left_matches.data_batch();
                let lmw = left_matches.weights();
                let n = lm.num_rows();
                let signed: Vec<i64> = (0..n).map(|i| sign * lmw.value(i)).collect();
                let mut cols: Vec<arrow::array::ArrayRef> = lm.columns().to_vec();
                cols.push(Arc::new(Int64Array::from(signed)));
                let mut fields: Vec<_> = lm.schema().fields().iter().cloned().collect();
                fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
                let inner = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?;
                parts.push(DeltaBatch::from_weighted(inner)?);
            }
        }

        // Update traces AFTER all probes.
        if let Some(dl) = delta_left {
            self.left_trace.insert(dl);
        }
        if let Some(dr) = delta_right {
            self.right_trace.insert(dr);
        }

        combine_parts(parts, &self.output_schema)
    }

    /// SEMI-3: summed weight of `rights` rows passing the membership
    /// predicate against left row `li` — the left row replicated across a
    /// (left ++ right) pair batch, the mask applied to the rights' weights.
    fn residual_pass_weight(
        &self,
        left_data: &RecordBatch,
        li: usize,
        rights: &DeltaBatch,
    ) -> DeltaResult<i64> {
        let n = rights.data_batch().num_rows();
        if n == 0 {
            return Ok(0);
        }
        let Some(residual) = &self.membership_residual else {
            return Err(DeltaError::Operator(
                "membership residual evaluated without one installed".into(),
            ));
        };
        let idx = arrow::array::UInt64Array::from(vec![li as u64; n]);
        let mut cols: Vec<Arc<dyn Array>> = Vec::new();
        let mut fields: Vec<Arc<Field>> = Vec::new();
        for (c, f) in left_data
            .columns()
            .iter()
            .zip(left_data.schema().fields().iter())
        {
            cols.push(arrow::compute::take(c, &idx, None)?);
            fields.push(f.clone());
        }
        let rd = rights.data_batch();
        for (c, f) in rd.columns().iter().zip(rd.schema().fields().iter()) {
            cols.push(c.clone());
            fields.push(f.clone());
        }
        let pair = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?;
        let mask = residual(&pair)?;
        let rw = rights.weights();
        let mut total = 0i64;
        for i in 0..n {
            if mask.is_valid(i) && mask.value(i) {
                total += rw.value(i);
            }
        }
        Ok(total)
    }
}

/// Partition probe results into per-key groups.
///
/// Shared by both the ΔA and ΔB branches of the residual semi/anti path so the
/// two cannot drift into grouping differently — they must agree, because ΔA's
/// post-tick membership and ΔB's pre-tick membership are two readings of the
/// same right state.
fn group_matches_by_key(
    matches: &DeltaBatch,
    key_cols: &[String],
) -> DeltaResult<AHashMap<Vec<Option<String>>, DeltaBatch>> {
    let data = matches.data_batch();
    let ki = col_indices(&data, key_cols)?;
    let mut rows_by_key: AHashMap<Vec<Option<String>>, Vec<usize>> = AHashMap::new();
    for ri in 0..data.num_rows() {
        rows_by_key
            .entry(extract_key(&data, ri, &ki)?)
            .or_default()
            .push(ri);
    }
    rows_by_key
        .into_iter()
        .map(|(k, rows)| select_rows(matches, &rows).map(|b| (k, b)))
        .collect()
}

impl IncrementalJoinOp {
    /// SEMI-3: semi/anti with a non-equi membership condition. Membership is
    /// per LEFT ROW — `∃ right: key match AND residual(left, right)` — so
    /// the per-key crossing shortcut does not apply; each affected left row
    /// re-evaluates against its key group, which is bounded by key fanout.
    /// ΔA rows evaluate against the POST-tick right state (trace + ΔB);
    /// ΔB-driven flips probe the PRE-insert left trace, so same-tick ΔA rows
    /// are never counted twice — the same discipline as the keyed path.
    fn apply_left_semi_anti_residual(
        &mut self,
        delta_left: Option<DeltaBatch>,
        delta_right: Option<DeltaBatch>,
    ) -> DeltaResult<DeltaBatch> {
        let semi = self.join_type == IncrJoinType::LeftSemi;
        let mut parts: Vec<DeltaBatch> = Vec::new();

        // ΔB rows grouped by key, for both ΔA's post-state and the flips.
        let mut dr_by_key: AHashMap<Vec<Option<String>>, Vec<usize>> = AHashMap::new();
        if let Some(ref dr) = delta_right
            && !dr.is_empty()
        {
            let rd = dr.data_batch();
            let rki = col_indices(&rd, &self.right_key_cols)?;
            for ri in 0..rd.num_rows() {
                dr_by_key
                    .entry(extract_key(&rd, ri, &rki)?)
                    .or_default()
                    .push(ri);
            }
        }
        let dr_rows_for = |key: &Vec<Option<String>>| -> DeltaResult<Option<DeltaBatch>> {
            match (&delta_right, dr_by_key.get(key)) {
                (Some(dr), Some(rows)) => Ok(Some(select_rows(dr, rows)?)),
                _ => Ok(None),
            }
        };

        // ΔA: membership under the post-tick right state.
        if let Some(ref dl) = delta_left
            && !dl.is_empty()
        {
            let ld = dl.data_batch();
            let lw = dl.weights();
            let lki = col_indices(&ld, &self.left_key_cols)?;
            // IVM-AUD-PERF-5: probe the trace ONCE for all delta keys, not once
            // per delta row. `probe_by_keys` is O(THE ENTIRE TRACE) — it scans
            // every level and batch building a match mask — and it already
            // accepts a BATCH of keys, so calling it per row turned an O(state)
            // operation into O(delta x state): 5,000 delta rows against a 1M-row
            // trace is 5 billion row-scans per tick. Measured on TPC-H q21
            // before this: 252 ms at seed 200k, 7.9 s at 800k, ~10 s at 1M.
            //
            // The fix is to use the seam as designed. Probe once with every
            // distinct key, then group the matched rows by their key so each
            // left row still evaluates its residual against exactly the rows it
            // would have seen. The per-key match sets are identical; only the
            // number of trace scans changes.
            let left_keys: Vec<Vec<Option<String>>> = (0..ld.num_rows())
                .map(|li| extract_key(&ld, li, &lki))
                .collect::<DeltaResult<Vec<_>>>()?;
            let mut distinct: Vec<Vec<Option<String>>> = left_keys.clone();
            distinct.sort();
            distinct.dedup();
            let matches_by_key: AHashMap<Vec<Option<String>>, DeltaBatch> = if distinct.is_empty() {
                AHashMap::new()
            } else {
                let probe =
                    keys_to_probe_batch(&distinct, &self.right_key_cols, &self.right_schema)?;
                let all = self.right_trace.probe_by_keys(&probe)?;
                group_matches_by_key(&all, &self.right_key_cols)?
            };
            let empty_right = DeltaBatch::empty(self.right_schema.clone())?;

            let mut keep: Vec<usize> = Vec::new();
            for (li, key) in left_keys.iter().enumerate() {
                let trace_rights = matches_by_key.get(key).unwrap_or(&empty_right);
                let mut w = self.residual_pass_weight(&ld, li, trace_rights)?;
                if let Some(db) = dr_rows_for(key)? {
                    w += self.residual_pass_weight(&ld, li, &db)?;
                }
                let member = if semi { w > 0 } else { w == 0 };
                if member {
                    keep.push(li);
                }
            }
            let _ = lw;
            if !keep.is_empty() {
                parts.push(select_rows(dl, &keep)?);
            }
        }

        // ΔB: per affected key, each PRE-insert left-trace row's membership
        // may flip between the pre-tick and post-tick right states.
        //
        // IVM-AUD-PERF-7: probe each trace ONCE for every affected key, not
        // twice per key. This is the transformation §66 applied to ΔA, applied
        // to the branch that entry deliberately left alone. `probe_by_keys` is
        // O(the batches it must consider) even with the PERF-6 key index, so
        // 1,250 distinct right-delta keys meant 2,500 probes per tick where two
        // suffice. Measured on TPC-H q21 at seed 800k, PERF-6 alone left the
        // call count untouched at 390,280 per run — the index made each probe
        // cheap without making this loop ask fewer times.
        //
        // Probing {k1,k2} and partitioning the result by key gives the same
        // per-key groups as probing k1 and k2 separately: `probe_by_keys`
        // filters by key-set membership and preserves trace order, so each
        // group is row-for-row what the per-key probe returned.
        let affected: Vec<Vec<Option<String>>> = dr_by_key.keys().cloned().collect();
        let (left_by_key, right_by_key) = if affected.is_empty() {
            (AHashMap::new(), AHashMap::new())
        } else {
            let lprobe = keys_to_probe_batch(&affected, &self.left_key_cols, &self.left_schema)?;
            let all_left = self.left_trace.probe_by_keys(&lprobe)?;
            let left_by_key = group_matches_by_key(&all_left, &self.left_key_cols)?;
            // Only a key with left-trace rows can produce a flip. The per-key
            // loop got this narrowing for free — `if left_matches.is_empty()
            // { continue }` ran BEFORE it touched the right trace — and a naive
            // batching loses it, probing and grouping right-side rows for keys
            // that are about to be skipped. Measured: without this, q21 at seed
            // 400k ran at 0.83x of the per-key version even while 800k gained.
            let live: Vec<Vec<Option<String>>> = affected
                .iter()
                .filter(|k| left_by_key.contains_key(*k))
                .cloned()
                .collect();
            let right_by_key = if live.is_empty() {
                AHashMap::new()
            } else {
                let rprobe = keys_to_probe_batch(&live, &self.right_key_cols, &self.right_schema)?;
                let all_right = self.right_trace.probe_by_keys(&rprobe)?;
                group_matches_by_key(&all_right, &self.right_key_cols)?
            };
            (left_by_key, right_by_key)
        };
        // A key with no rows in the right TRACE must still evaluate its
        // residual against an EMPTY right side. Skipping it would make an
        // anti-join's `pre_w == 0 => member` silently never fire, so a row that
        // should be retracted stays in the relation forever.
        let empty_right_trace = DeltaBatch::empty(self.right_schema.clone())?;

        for key in &affected {
            let Some(left_matches) = left_by_key.get(key) else {
                continue;
            };
            if left_matches.is_empty() {
                continue;
            }
            let trace_rights = right_by_key.get(key).unwrap_or(&empty_right_trace);
            let db = dr_rows_for(key)?;
            let lm = left_matches.data_batch();
            let lmw = left_matches.weights();
            let mut flip_idx: Vec<usize> = Vec::new();
            let mut flip_sign: Vec<i64> = Vec::new();
            for li in 0..lm.num_rows() {
                let pre_w = self.residual_pass_weight(&lm, li, trace_rights)?;
                let delta_w = match &db {
                    Some(d) => self.residual_pass_weight(&lm, li, d)?,
                    None => 0,
                };
                let post_w = pre_w + delta_w;
                let (pre_m, post_m) = if semi {
                    (pre_w > 0, post_w > 0)
                } else {
                    (pre_w == 0, post_w == 0)
                };
                match (pre_m, post_m) {
                    (false, true) => {
                        flip_idx.push(li);
                        flip_sign.push(1);
                    }
                    (true, false) => {
                        flip_idx.push(li);
                        flip_sign.push(-1);
                    }
                    _ => {}
                }
            }
            if flip_idx.is_empty() {
                continue;
            }
            let take_idx = arrow::array::UInt64Array::from(
                flip_idx.iter().map(|&i| i as u64).collect::<Vec<_>>(),
            );
            let mut cols: Vec<Arc<dyn Array>> = Vec::new();
            for c in lm.columns() {
                cols.push(arrow::compute::take(c, &take_idx, None)?);
            }
            let signed: Vec<i64> = flip_idx
                .iter()
                .zip(flip_sign.iter())
                .map(|(&i, &s)| s * lmw.value(i))
                .collect();
            cols.push(Arc::new(Int64Array::from(signed)));
            let mut fields: Vec<_> = lm.schema().fields().iter().cloned().collect();
            fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
            let inner = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?;
            parts.push(DeltaBatch::from_weighted(inner)?);
        }

        // Keep the per-key bookkeeping identical to the keyed path, so a
        // checkpoint restored into either path sees the same state.
        if let Some(ref dr) = delta_right
            && !dr.is_empty()
        {
            let rd = dr.data_batch();
            let rw = dr.weights();
            let rki = col_indices(&rd, &self.right_key_cols)?;
            for ri in 0..rd.num_rows() {
                let key = extract_key(&rd, ri, &rki)?;
                let e = self.right_key_group_weights.entry(key).or_insert(0);
                *e += rw.value(ri);
                if *e == 0 {
                    // Entry removal mirrors the keyed path.
                }
            }
            self.right_key_group_weights.retain(|_, w| *w != 0);
        }

        // Update traces AFTER all probes.
        if let Some(dl) = delta_left {
            self.left_trace.insert(dl);
        }
        if let Some(dr) = delta_right {
            self.right_trace.insert(dr);
        }

        combine_parts(parts, &self.output_schema)
    }

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
                    *m.entry(extract_key(&rd, ri, &rki)?).or_insert(0) += rw.value(ri);
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
            let key = extract_key(&left_data, li, &lki)?;
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
                .entry(extract_key(&right_data, ri, &rki)?)
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

    // ── Checkpoint serialization ─────────────────────────────────────────────

    /// Serialize the operator's accumulated state losslessly: both traces (as
    /// weighted Z-sets) plus the LEFT OUTER `right_key_group_weights` map.
    ///
    /// Format: `u8 version (1) || u64 len || left trace || u64 len || right
    /// trace || u32 n_groups || (u32 n_parts || (u8 present || u32 len ||
    /// utf8)* || i64 weight)*`. Structural shape (schemas, key columns, join
    /// type) is rebuilt from the view SQL, so only the running state transfers
    /// — same contract as the aggregate/distinct operators.
    pub fn state_bytes(&self) -> DeltaResult<Vec<u8>> {
        let mut out = Vec::new();
        out.push(1u8);
        for trace in [&self.left_trace, &self.right_trace] {
            let bytes = trace.state_bytes()?;
            out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            out.extend_from_slice(&bytes);
        }
        let n_groups = u32::try_from(self.right_key_group_weights.len())
            .map_err(|_| DeltaError::Serialization("join group count overflows u32".into()))?;
        out.extend_from_slice(&n_groups.to_le_bytes());
        for (key, weight) in &self.right_key_group_weights {
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            for part in key {
                match part {
                    Some(s) => {
                        out.push(1u8);
                        out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                        out.extend_from_slice(s.as_bytes());
                    }
                    None => out.push(0u8),
                }
            }
            out.extend_from_slice(&weight.to_le_bytes());
        }
        Ok(out)
    }

    /// Restore state produced by [`state_bytes`](Self::state_bytes) into an
    /// operator rebuilt with the same structural shape. Row multiplicities in
    /// the traces are preserved exactly (seeding from a materialized snapshot
    /// cannot do this — a snapshot is a set).
    pub fn restore_state_bytes(&mut self, bytes: &[u8]) -> DeltaResult<()> {
        let truncated = || DeltaError::Serialization("join state truncated".into());
        let mut pos = 0usize;
        let version = *bytes.first().ok_or_else(truncated)?;
        if version != 1 {
            return Err(DeltaError::Serialization(format!(
                "unsupported join state version {version}"
            )));
        }
        pos += 1;
        let read_section = |pos: &mut usize| -> DeltaResult<&[u8]> {
            let raw = bytes.get(*pos..*pos + 8).ok_or_else(truncated)?;
            *pos += 8;
            let len = u64::from_le_bytes(raw.try_into().map_err(|_| truncated())?) as usize;
            let section = bytes.get(*pos..*pos + len).ok_or_else(truncated)?;
            *pos += len;
            Ok(section)
        };
        let left_section = read_section(&mut pos)?;
        let right_section = read_section(&mut pos)?;

        let read_u32 = |bytes: &[u8], pos: &mut usize| -> DeltaResult<u32> {
            let raw = bytes.get(*pos..*pos + 4).ok_or_else(truncated)?;
            *pos += 4;
            Ok(u32::from_le_bytes(raw.try_into().map_err(|_| truncated())?))
        };
        let n_groups = read_u32(bytes, &mut pos)? as usize;
        let mut groups: AHashMap<Vec<Option<String>>, i64> = AHashMap::with_capacity(n_groups);
        for _ in 0..n_groups {
            let n_parts = read_u32(bytes, &mut pos)? as usize;
            let mut key: Vec<Option<String>> = Vec::with_capacity(n_parts);
            for _ in 0..n_parts {
                let present = *bytes.get(pos).ok_or_else(truncated)?;
                pos += 1;
                if present == 1 {
                    let len = read_u32(bytes, &mut pos)? as usize;
                    let raw = bytes.get(pos..pos + len).ok_or_else(truncated)?;
                    pos += len;
                    key.push(Some(
                        std::str::from_utf8(raw)
                            .map_err(|e| DeltaError::Serialization(e.to_string()))?
                            .to_string(),
                    ));
                } else {
                    key.push(None);
                }
            }
            let raw = bytes.get(pos..pos + 8).ok_or_else(truncated)?;
            pos += 8;
            groups.insert(
                key,
                i64::from_le_bytes(raw.try_into().map_err(|_| truncated())?),
            );
        }

        // Decode everything before mutating anything: a corrupt right section
        // must not leave a half-restored operator for the seeding fallback to
        // pile deltas onto.
        let left_batches = crate::trace::Trace::decode_state(left_section)?;
        let right_batches = crate::trace::Trace::decode_state(right_section)?;
        self.left_trace.replace_batches(left_batches);
        self.right_trace.replace_batches(right_batches);
        self.right_key_group_weights = groups;
        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Pair rows from two batches on their key columns, hash-indexed.
///
/// IVM-AUD-PERF-2: all three delta-join pairing sites — the same-tick cross
/// term and both trace-probe output builders — ran `for li { for ri {
/// keys_match } }`. The cross term was quadratic in the DELTA (5k x 5k = 25M
/// comparisons, ~6 s per tick); the two probe builders were worse in kind,
/// quadratic in delta x MATCHED TRACE ROWS, so their cost grew with
/// accumulated state — the very thing an O(delta) plan exists to avoid.
///
/// The key is the `Vec<Option<String>>` encoding `extract_key` builds from
/// `scalar_to_key`, which reproduces `scalar_eq` exactly, INCLUDING its
/// deliberately non-SQL rule that a NULL key matches a NULL key (`None ==
/// None`). Emission order is unchanged — left-major, right rows ascending —
/// because left rows are walked in order and each bucket holds its rows in
/// insertion order.
fn pair_rows_by_key(
    left: &RecordBatch,
    left_key_indices: &[usize],
    left_weights: &Int64Array,
    right: &RecordBatch,
    right_key_indices: &[usize],
    right_weights: &Int64Array,
) -> DeltaResult<(Vec<usize>, Vec<usize>, Vec<i64>)> {
    let mut index: AHashMap<Vec<Option<String>>, Vec<usize>> =
        AHashMap::with_capacity(right.num_rows());
    for ri in 0..right.num_rows() {
        index
            .entry(extract_key(right, ri, right_key_indices)?)
            .or_default()
            .push(ri);
    }
    let mut out_left: Vec<usize> = Vec::new();
    let mut out_right: Vec<usize> = Vec::new();
    let mut out_weights: Vec<i64> = Vec::new();
    for li in 0..left.num_rows() {
        let key = extract_key(left, li, left_key_indices)?;
        let Some(matches) = index.get(&key) else {
            continue;
        };
        for &ri in matches {
            out_left.push(li);
            out_right.push(ri);
            out_weights.push(left_weights.value(li) * right_weights.value(ri));
        }
    }
    Ok((out_left, out_right, out_weights))
}

/// Extract key column values from a single row as `Vec<Option<String>>`.
fn extract_key(
    batch: &RecordBatch,
    row: usize,
    key_indices: &[usize],
) -> DeltaResult<Vec<Option<String>>> {
    key_indices
        .iter()
        .map(|&i| scalar_to_key(batch.column(i).as_ref(), row))
        .collect()
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
    // KEYLESS-1: a keyless join projects ZERO key columns, and a zero-column
    // batch must carry its row count explicitly — every row's key is the
    // empty tuple, one group.
    let opts =
        arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        cols,
        &opts,
    )?)
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

    // ── ΔA ⋈ ΔB cross term (IVM-AUD-PERF-2) ───────────────────────────────

    /// The same-tick cross term was a NESTED LOOP over both deltas: with 5k
    /// rows on each side it ran 25 million `keys_match` calls and took ~6
    /// seconds per tick, against ~25 ms for full recompute of the same query.
    /// Measured shape, not guessed: holding the seed fixed and scaling only
    /// the delta gave 220 ms / 687 ms / 8040 ms at 1k / 2k / 5k rows —
    /// quadratic in the delta, which is the loop and not accumulated state.
    ///
    /// Revert-proof: restore the `for li { for ri { keys_match } }` loop and
    /// this test runs for ~9 seconds and fails its budget.
    #[test]
    fn the_same_tick_cross_term_is_not_quadratic_in_the_delta() {
        const N: i32 = 6_000;
        let mut op = IncrementalJoinOp::new(
            orders_schema(),
            customers_schema(),
            vec!["customer_id".into()],
            vec!["customer_id".into()],
            IncrJoinType::Inner,
        )
        .unwrap();

        let ids: Vec<i32> = (0..N).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("c{i}")).collect();
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let left = DeltaBatch::from_inserts(orders_batch(&ids, &ids)).unwrap();
        let right = DeltaBatch::from_inserts(customers_batch(&ids, &name_refs)).unwrap();

        let t0 = std::time::Instant::now();
        let out = op.apply(Some(left), Some(right)).unwrap();
        let elapsed = t0.elapsed();

        // Each key appears once on each side, so the cross term is exactly N
        // rows — the work is in FINDING them, which is the point.
        assert_eq!(out.num_rows(), N as usize, "every key must match once");
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "cross term took {elapsed:?} for {N}x{N}; the nested loop is back"
        );
    }

    /// The rewrite must preserve `scalar_eq`'s exact matching rules, and one
    /// of them is deliberately NOT SQL: a NULL key matches a NULL key here
    /// (`scalar_eq` returns true for two nulls). Changing that under cover of
    /// a performance fix would be a silent semantics change, so it is pinned.
    #[test]
    fn the_cross_term_still_matches_null_keys_to_null_keys() {
        let left_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, true),
        ]));
        let right_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, false),
        ]));
        let mut op = IncrementalJoinOp::new(
            left_schema.clone(),
            right_schema.clone(),
            vec!["customer_id".into()],
            vec!["customer_id".into()],
            IncrJoinType::Inner,
        )
        .unwrap();
        let left = RecordBatch::try_new(
            left_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![None, Some(7)])),
            ],
        )
        .unwrap();
        let right = RecordBatch::try_new(
            right_schema,
            vec![
                Arc::new(Int32Array::from(vec![None, Some(7)])),
                Arc::new(StringArray::from(vec!["null-keyed", "seven"])),
            ],
        )
        .unwrap();

        let out = op
            .apply(
                Some(DeltaBatch::from_inserts(left).unwrap()),
                Some(DeltaBatch::from_inserts(right).unwrap()),
            )
            .unwrap();
        assert_eq!(
            out.num_rows(),
            2,
            "null-to-null and 7-to-7 must both match (current scalar_eq contract)"
        );
    }

    /// Multiplicity and emission order are part of the contract too: weights
    /// multiply, a key present k times on the left and m times on the right
    /// produces k*m rows, and rows come out left-major with right rows in
    /// ascending order — the nested loop's order, which downstream
    /// consolidation and every existing expectation were written against.
    #[test]
    fn the_cross_term_preserves_multiplicity_and_emission_order() {
        let mut op = IncrementalJoinOp::new(
            orders_schema(),
            customers_schema(),
            vec!["customer_id".into()],
            vec!["customer_id".into()],
            IncrJoinType::Inner,
        )
        .unwrap();

        // Left: orders 10,11 on key 5; order 12 on key 6.
        // Right: two customers on key 5, one on key 6.
        let left = orders_batch(&[10, 11, 12], &[5, 5, 6]);
        let right = customers_batch(&[5, 5, 6], &["a", "b", "c"]);
        let out = op
            .apply(
                Some(DeltaBatch::from_inserts(left).unwrap()),
                Some(DeltaBatch::from_deletes(right).unwrap()),
            )
            .unwrap();

        // 2 left x 2 right on key 5, plus 1x1 on key 6 = 5 rows.
        assert_eq!(out.num_rows(), 5);
        // Retraction on one side flips every product weight to -1.
        assert!(
            out.weights().iter().all(|w| w == Some(-1)),
            "weights must be the product of the two sides"
        );
        let data = out.data_batch();
        let order_ids = data
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = data
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let seen: Vec<(i32, &str)> = (0..out.num_rows())
            .map(|r| (order_ids.value(r), names.value(r)))
            .collect();
        assert_eq!(
            seen,
            vec![(10, "a"), (10, "b"), (11, "a"), (11, "b"), (12, "c")],
            "left-major, right-ascending emission order"
        );
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

    /// #160: checkpoint/restore round-trips the traces losslessly — a restored
    /// operator behaves identically to the original on the next delta,
    /// including duplicate-row multiplicity that snapshot seeding (a set)
    /// cannot reconstruct.
    #[test]
    fn inner_join_state_round_trip_preserves_multiplicity() {
        let build = || {
            IncrementalJoinOp::new(
                orders_schema(),
                customers_schema(),
                vec!["customer_id".into()],
                vec!["customer_id".into()],
                IncrJoinType::Inner,
            )
            .unwrap()
        };
        let mut original = build();
        // Customer 1 twice (weight 2 in the right trace) + one order.
        let c = DeltaBatch::from_inserts(customers_batch(&[1, 1], &["Alice", "Alice"])).unwrap();
        original.apply(None, Some(c)).unwrap();
        let o = DeltaBatch::from_inserts(orders_batch(&[100], &[1])).unwrap();
        original.apply(Some(o), None).unwrap();

        let state = original.state_bytes().unwrap();
        let mut restored = build();
        restored.restore_state_bytes(&state).unwrap();

        // Retract ONE duplicate customer on both ops: the output must retract
        // the join pair exactly once (net weight −1), which requires the trace
        // to still know the row had weight 2.
        let del = DeltaBatch::from_deletes(customers_batch(&[1], &["Alice"])).unwrap();
        let out_orig = original.apply(None, Some(del.clone())).unwrap();
        let out_rest = restored.apply(None, Some(del)).unwrap();
        let net = |d: &DeltaBatch| -> i64 { d.weights().iter().flatten().sum() };
        assert_eq!(net(&out_orig), -1, "original retracts one pair");
        assert_eq!(
            net(&out_rest),
            net(&out_orig),
            "restored operator must match the original exactly"
        );
        // And the next probe still finds the surviving duplicate.
        let o2 = DeltaBatch::from_inserts(orders_batch(&[101], &[1])).unwrap();
        let out2 = restored.apply(Some(o2), None).unwrap();
        assert_eq!(net(&out2), 1, "one customer copy remains in the trace");
    }

    /// #160: LEFT OUTER state includes `right_key_group_weights` — after a
    /// restore, emptying a right key group must emit null-padded rows exactly
    /// as the original operator would.
    #[test]
    fn left_outer_state_round_trip_preserves_group_weights() {
        let mut original = left_outer_op();
        let c = DeltaBatch::from_inserts(customers_batch(&[1], &["Alice"])).unwrap();
        original.apply(None, Some(c)).unwrap();
        let o = DeltaBatch::from_inserts(orders_batch(&[100], &[1])).unwrap();
        original.apply(Some(o), None).unwrap();

        let state = original.state_bytes().unwrap();
        let mut restored = left_outer_op();
        restored.restore_state_bytes(&state).unwrap();

        // Deleting the only customer must retract the join pair AND emit the
        // null-padded left row (positive→0 crossing) — this depends on the
        // restored group-weight map, not just the traces.
        let del = DeltaBatch::from_deletes(customers_batch(&[1], &["Alice"])).unwrap();
        let out = restored.apply(None, Some(del)).unwrap();
        let data = out.data_batch();
        let name_col = data.column_by_name("name").expect("name column");
        let mut retracted_pair = false;
        let mut emitted_null = false;
        for i in 0..data.num_rows() {
            let w = out.weights().value(i);
            if name_col.is_null(i) && w > 0 {
                emitted_null = true;
            }
            if !name_col.is_null(i) && w < 0 {
                retracted_pair = true;
            }
        }
        assert!(retracted_pair, "restored op must retract the joined row");
        assert!(
            emitted_null,
            "restored op must emit the null-padded row on the positive→0 crossing"
        );
    }

    /// Regression (crate-13 audit, A-class): the old `scalar_eq` returned `false` for
    /// every key type outside Int64/Int32/Utf8, so a join keyed on e.g.
    /// Utf8View silently emitted no rows from the delta-probe paths.
    #[test]
    fn join_on_utf8view_keys_matches() {
        use arrow::array::StringViewArray;
        let left_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8View, false),
            Field::new("lv", DataType::Int32, false),
        ]));
        let right_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8View, false),
            Field::new("rv", DataType::Int32, false),
        ]));
        let mut op = IncrementalJoinOp::new(
            left_schema.clone(),
            right_schema.clone(),
            vec!["k".into()],
            vec!["k".into()],
            IncrJoinType::Inner,
        )
        .unwrap();
        let left = RecordBatch::try_new(
            left_schema,
            vec![
                Arc::new(StringViewArray::from(vec!["a"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let right = RecordBatch::try_new(
            right_schema,
            vec![
                Arc::new(StringViewArray::from(vec!["a"])),
                Arc::new(Int32Array::from(vec![2])),
            ],
        )
        .unwrap();
        // Same-tick cross term exercises the shared key encoding directly
        // (IVM-AUD-PERF-2 replaced `keys_match`/`scalar_eq` with it).
        let out = op
            .apply(
                Some(DeltaBatch::from_inserts(left).unwrap()),
                Some(DeltaBatch::from_inserts(right).unwrap()),
            )
            .unwrap();
        assert_eq!(
            out.num_rows(),
            1,
            "Utf8View-keyed join must match on the same-tick cross term"
        );
    }

    /// A truncated payload must fail cleanly without mutating the operator.
    #[test]
    fn join_state_restore_rejects_truncated_payload() {
        let mut op = left_outer_op();
        let c = DeltaBatch::from_inserts(customers_batch(&[1], &["Alice"])).unwrap();
        op.apply(None, Some(c)).unwrap();
        let mut state = op.state_bytes().unwrap();
        state.truncate(state.len() - 3);
        let mut fresh = left_outer_op();
        assert!(fresh.restore_state_bytes(&state).is_err());
        // The failed restore left the fresh op untouched: an order for
        // customer 1 finds no match and emits a null-padded row.
        let o = DeltaBatch::from_inserts(orders_batch(&[100], &[1])).unwrap();
        let out = fresh.apply(Some(o), None).unwrap();
        let data = out.data_batch();
        let name_col = data.column_by_name("name").expect("name column");
        assert_eq!(data.num_rows(), 1);
        assert!(name_col.is_null(0), "fresh op state must be empty");
    }

    /// SEMI-1: a left row appears ONCE while its key has any right match —
    /// two right matches must not double it — and the crossing directions
    /// (first match arrives / last match leaves) flip membership for the
    /// whole left trace of that key.
    #[test]
    fn left_semi_membership_without_pair_multiplication() {
        let mut op = IncrementalJoinOp::new_with_lateness(
            orders_schema(),
            customers_schema(),
            vec!["customer_id".into()],
            vec!["customer_id".into()],
            IncrJoinType::LeftSemi,
            None,
        )
        .unwrap();
        // Left columns only.
        assert_eq!(
            op.output_schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            vec!["order_id".to_string(), "customer_id".to_string()],
        );

        // Tick 1: two orders for customer 1, no customers yet -> nothing.
        let d = op
            .apply(
                Some(DeltaBatch::from_inserts(orders_batch(&[10, 11], &[1, 1])).unwrap()),
                None,
            )
            .unwrap();
        assert!(d.is_empty(), "no membership before any right match");

        // Tick 2: customer 1 arrives TWICE (two copies) -> both orders enter
        // ONCE each, not four pair rows.
        let d = op
            .apply(
                None,
                Some(DeltaBatch::from_inserts(customers_batch(&[1, 1], &["a", "a"])).unwrap()),
            )
            .unwrap();
        let pos = d.filter_positive().unwrap();
        assert_eq!(pos.num_rows(), 2, "one membership row per left row");

        // Tick 3: retract ONE copy of customer 1 -> still a match, no change.
        let d = op
            .apply(
                None,
                Some(DeltaBatch::from_deletes(customers_batch(&[1], &["a"])).unwrap()),
            )
            .unwrap();
        assert!(d.is_empty(), "one of two copies leaving crosses nothing");

        // Tick 4: retract the LAST copy -> both orders leave the relation.
        let d = op
            .apply(
                None,
                Some(DeltaBatch::from_deletes(customers_batch(&[1], &["a"])).unwrap()),
            )
            .unwrap();
        let (ins, ret): (i64, i64) = {
            let w = d.weights();
            let mut i = 0;
            let mut r = 0;
            for k in 0..w.len() {
                if w.value(k) > 0 {
                    i += w.value(k);
                } else {
                    r -= w.value(k);
                }
            }
            (i, r)
        };
        assert_eq!((ins, ret), (0, 2), "last copy leaving retracts both orders");
    }

    // ── SEMI-3 residual membership (IVM-AUD-PERF-7) ───────────────────────

    /// An anti-join whose membership carries a non-equi residual: a right row
    /// only counts if its name differs from the left row's label. This is the
    /// shape TPC-H q21 takes (`l2.l_suppkey <> l1.l_suppkey`), reduced to two
    /// columns.
    fn anti_with_residual() -> IncrementalJoinOp {
        let mut op = IncrementalJoinOp::new(
            orders_schema(),
            customers_schema(),
            vec!["customer_id".into()],
            vec!["customer_id".into()],
            IncrJoinType::LeftAnti,
        )
        .unwrap();
        // Pair batch is (order_id, customer_id, customer_id, name); the right
        // row counts when its name is not "self".
        op.set_membership_residual(Arc::new(|pair: &RecordBatch| {
            let names = pair
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DeltaError::Operator("residual: name column".into()))?;
            Ok((0..names.len())
                .map(|i| Some(names.value(i) != "self"))
                .collect())
        }));
        op
    }

    /// IVM-AUD-PERF-7, and the reason the batched ΔB probe needs an
    /// `empty_right_trace` fallback rather than a `continue`.
    ///
    /// A key whose right TRACE holds nothing still has to evaluate its residual
    /// against an empty right side, because an anti-join reads `pre_w == 0` as
    /// "this row IS in the relation". Skip the key and the flip to non-member
    /// never fires, so a row that should be retracted stays in the anti
    /// relation forever — a silent wrong answer, not an error.
    ///
    /// Revert-proof: replace `right_by_key.get(key).unwrap_or(&empty_right_trace)`
    /// with a `let ... else { continue }` and the retraction below disappears.
    #[test]
    fn a_key_absent_from_the_right_trace_still_evaluates_its_residual() {
        let mut op = anti_with_residual();

        // Tick 1: two orders for customer 1, no customers at all. Unmatched, so
        // both are IN the anti relation.
        let d = op
            .apply(
                Some(DeltaBatch::from_inserts(orders_batch(&[10, 11], &[1, 1])).unwrap()),
                None,
            )
            .unwrap();
        assert_eq!(
            d.filter_positive().unwrap().num_rows(),
            2,
            "unmatched orders enter the anti relation"
        );

        // Tick 2: customer 1 arrives and PASSES the residual. The right trace
        // still holds nothing for key 1 — this is the first right row ever, and
        // traces are updated only after all probes — so ΔB must fall back to an
        // empty right side to see pre_w = 0 (member) flip to post_w > 0.
        let d = op
            .apply(
                None,
                Some(DeltaBatch::from_inserts(customers_batch(&[1], &["other"])).unwrap()),
            )
            .unwrap();
        let w = d.weights();
        let total: i64 = (0..w.len()).map(|i| w.value(i)).sum();
        assert_eq!(
            total, -2,
            "gaining a residual-passing match must retract BOTH anti rows; \
             got {total} (0 means the key was skipped entirely)"
        );
    }

    /// A right row that FAILS the residual must not change membership — the
    /// guard that stops the test above from passing for the wrong reason (any
    /// right row at all retracting the anti rows).
    #[test]
    fn a_right_row_failing_the_residual_does_not_flip_membership() {
        let mut op = anti_with_residual();
        op.apply(
            Some(DeltaBatch::from_inserts(orders_batch(&[10, 11], &[1, 1])).unwrap()),
            None,
        )
        .unwrap();
        let d = op
            .apply(
                None,
                Some(DeltaBatch::from_inserts(customers_batch(&[1], &["self"])).unwrap()),
            )
            .unwrap();
        assert!(
            d.is_empty(),
            "a match that fails the residual is not a match"
        );
    }

    /// IVM-AUD-PERF-7. The ΔB branch probed BOTH traces once per distinct
    /// right-delta key: 2 x K probes per tick. IVM-AUD-PERF-6 made each probe
    /// cheap but left K untouched (measured: 390,280 calls per q21 run before
    /// and after), so the count is its own defect and needs its own assertion.
    ///
    /// Revert-proof: restore the per-key `for key in dr_by_key.keys()` probes
    /// and this reports ~400 calls against a budget of 8.
    #[test]
    fn delta_b_probes_the_traces_once_not_once_per_key() {
        const KEYS: i32 = 200;
        let mut op = anti_with_residual();

        let ids: Vec<i32> = (0..KEYS).collect();
        op.apply(
            Some(DeltaBatch::from_inserts(orders_batch(&ids, &ids)).unwrap()),
            None,
        )
        .unwrap();

        // Only a right delta, so ΔA does not run and every probe counted here
        // belongs to ΔB.
        let names: Vec<&str> = (0..KEYS).map(|_| "other").collect();
        crate::trace::reset_probe_counters();
        let d = op
            .apply(
                None,
                Some(DeltaBatch::from_inserts(customers_batch(&ids, &names)).unwrap()),
            )
            .unwrap();
        let calls = crate::trace::probe_calls();

        let w = d.weights();
        let total: i64 = (0..w.len()).map(|i| w.value(i)).sum();
        assert_eq!(total, -(KEYS as i64), "every anti row must be retracted");
        assert!(
            calls <= 8,
            "ΔB issued {calls} trace probes for {KEYS} distinct keys; \
             it should probe each trace once (pre-fix this was {})",
            2 * KEYS
        );
    }

    /// SEMI-1: ANTI is the mirror — rows live in the relation while UNMATCHED.
    #[test]
    fn left_anti_is_the_mirror_image() {
        let mut op = IncrementalJoinOp::new_with_lateness(
            orders_schema(),
            customers_schema(),
            vec!["customer_id".into()],
            vec!["customer_id".into()],
            IncrJoinType::LeftAnti,
            None,
        )
        .unwrap();
        // Orders for customers 1 and 2; only customer 1 exists.
        let d = op
            .apply(
                Some(DeltaBatch::from_inserts(orders_batch(&[10, 20], &[1, 2])).unwrap()),
                Some(DeltaBatch::from_inserts(customers_batch(&[1], &["a"])).unwrap()),
            )
            .unwrap();
        let pos = d.filter_positive().unwrap();
        assert_eq!(pos.num_rows(), 1, "only the unmatched order is ANTI");

        // Customer 2 arrives: order 20 LEAVES the anti relation.
        let d = op
            .apply(
                None,
                Some(DeltaBatch::from_inserts(customers_batch(&[2], &["b"])).unwrap()),
            )
            .unwrap();
        let w = d.weights();
        assert_eq!(w.len(), 1);
        assert_eq!(w.value(0), -1, "gaining a match retracts the anti row");

        // Customer 1 leaves: order 10 ENTERS the anti relation.
        let d = op
            .apply(
                None,
                Some(DeltaBatch::from_deletes(customers_batch(&[1], &["a"])).unwrap()),
            )
            .unwrap();
        let w = d.weights();
        assert_eq!(w.len(), 1);
        assert_eq!(w.value(0), 1, "losing the last match admits the anti row");
    }
}
