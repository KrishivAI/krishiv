#![forbid(unsafe_code)]

//! Stateful incremental aggregate operators.
//!
//! Supports SUM, COUNT, AVG with correct retraction handling.
//! For each delta row (row, weight):
//!   1. Compute old aggregate for the row's group → emit retraction (-1)
//!   2. Apply delta to running state
//!   3. Compute new aggregate for the row's group → emit insertion (+1)
//!
//! Each aggregation expression has its own state so a `[Count, Sum]` spec
//! does not double-count or cross-contaminate (Sum's `sum` and Count's
//! `count` are distinct fields).

use std::collections::BTreeMap;
use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, RecordBatch};
use arrow::compute;
use arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, Field, Schema, SchemaRef,
};
use arrow::row::{RowConverter, SortField};

use crate::delta_batch::{DeltaBatch, WEIGHT_COLUMN};
use crate::error::{DeltaError, DeltaResult};

// ── Aggregation specification ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Aggregation {
    Sum {
        input_col: String,
        output_col: String,
    },
    Count {
        output_col: String,
        /// When `Some`, only non-null values of this column are counted
        /// (SQL `COUNT(col)` excludes nulls).  When `None`, counts all rows
        /// (SQL `COUNT(*)`).
        input_col: Option<String>,
    },
    Avg {
        input_col: String,
        output_col: String,
    },
    Min {
        input_col: String,
        output_col: String,
    },
    Max {
        input_col: String,
        output_col: String,
    },
    /// CDIST-1: `COUNT(DISTINCT col)`. Shares MIN/MAX's per-group value
    /// multiset (value → cumulative Z-weight), so the marginal state is one
    /// counter: the number of values whose weight is positive, maintained on
    /// zero-crossings. A retraction that removes the LAST copy of a value
    /// decrements the count; removing one of several copies does not — which
    /// is exactly the question CORE-22 said a plain retraction cannot answer
    /// without per-value multiplicity. This is that multiplicity.
    CountDistinct {
        input_col: String,
        output_col: String,
    },
}

impl Aggregation {
    pub fn output_col(&self) -> &str {
        match self {
            Self::Sum { output_col, .. }
            | Self::Count { output_col, .. }
            | Self::CountDistinct { output_col, .. }
            | Self::Avg { output_col, .. }
            | Self::Min { output_col, .. }
            | Self::Max { output_col, .. } => output_col,
        }
    }

    fn input_col(&self) -> Option<&str> {
        match self {
            Self::Sum { input_col, .. }
            | Self::Avg { input_col, .. }
            | Self::Min { input_col, .. }
            | Self::Max { input_col, .. }
            | Self::CountDistinct { input_col, .. } => Some(input_col),
            Self::Count { input_col, .. } => input_col.as_deref(),
        }
    }
}

// ── Numeric kind (AUD-3) ────────────────────────────────────────────────────────

/// The accumulation strategy for a numeric aggregate input, decided **once**
/// from the column's Arrow type — not by per-row string sniffing.
///
/// AUD-3: the old code parsed each value's *string form* and picked i64-vs-f64
/// by whether `parse::<i64>()` happened to succeed. Rust renders `10.0_f64` as
/// `"10"`, so a float column would latch the integer AVG path on its whole
/// values and silently corrupt AVG over mixed values like `[10.0, 10.5]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumKind {
    Int,
    Float,
    /// Fixed-point input (`Decimal128(precision, scale)`) accumulated exactly in
    /// `i128` at the column's own scale — never routed through `f64`.
    ///
    /// IVM-AUD-DEC-1: money is the reason. TPC-H's reference answers are exact
    /// decimal, and an engine that maintains a view perfectly but accumulates
    /// its totals in binary floating point still returns a wrong number. There
    /// is no incremental-maintenance fix for a lossy accumulator.
    Decimal {
        precision: u8,
        scale: i8,
    },
}

/// Map an Arrow type to its numeric accumulation kind, or `None` for a type
/// this operator cannot aggregate numerically (String/Bool/temporal/…). A
/// `None` here makes `IncrementalAggOp::new` error, so the view falls back to
/// DiffBased full recompute rather than producing a silently-wrong `0.0`.
fn num_kind(dt: &DataType) -> Option<NumKind> {
    use DataType::*;
    match dt {
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Some(NumKind::Int),
        Float16 | Float32 | Float64 => Some(NumKind::Float),
        // DEC-1. `Decimal256` is deliberately absent: its accumulator would be
        // `i256`, a different arithmetic, and claiming support by narrowing it
        // to `i128` is the exact class of silent truncation this exists to
        // remove. It reads as `None` and the view falls back to DiffBased.
        Decimal128(p, s) => Some(NumKind::Decimal {
            precision: *p,
            scale: *s,
        }),
        _ => None,
    }
}

/// SQL result type of `SUM(Decimal128(p, s))`: DataFusion widens the precision
/// by 10 and keeps the scale. Mirrored here rather than inferred, because the
/// operator's emitted type has to equal the planner's declared type exactly —
/// IVM-AUD-SCHEMA-1's guard compares `Decimal128` precision and scale, and a
/// near-miss fails the view closed instead of publishing a mistyped relation.
fn decimal_sum_type(precision: u8, scale: i8) -> DataType {
    DataType::Decimal128(
        precision.saturating_add(10).min(DECIMAL128_MAX_PRECISION),
        scale,
    )
}

/// SQL result type of `AVG(Decimal128(p, s))`: precision and scale both +4,
/// each capped at the `Decimal128` maximum.
fn decimal_avg_type(precision: u8, scale: i8) -> DataType {
    DataType::Decimal128(
        precision.saturating_add(4).min(DECIMAL128_MAX_PRECISION),
        scale.saturating_add(4).min(DECIMAL128_MAX_SCALE),
    )
}

/// `acc + v * weight`, or `None` on overflow.
///
/// DEC-1: the old SUM used `saturating_add(saturating_mul(..))`, which clamps
/// at `i64::MAX` and publishes the clamp as if it were the total — a wrong
/// answer with no error attached. Every exact accumulator now reports
/// overflow to its caller, which fails the tick.
fn accumulate_i64(acc: i64, v: i64, weight: i64) -> Option<i64> {
    v.checked_mul(weight).and_then(|d| acc.checked_add(d))
}

/// `acc + v * weight` in `i128` (DEC-1), or `None` on overflow.
fn accumulate_i128(acc: i128, v: i128, weight: i64) -> Option<i128> {
    v.checked_mul(weight as i128)
        .and_then(|d| acc.checked_add(d))
}

/// A typed aggregate output value, so integer aggregates stay exact (no f64
/// round-trip that loses precision above 2^53) and emit the correct Arrow type.
#[derive(Debug, Clone, Copy)]
enum AggScalar {
    I64(i64),
    F64(f64),
    /// DEC-1: an exact fixed-point value as its **unscaled** `i128`, carrying
    /// the scale of the field it will be written into — never a scaled `f64`.
    Dec(i128),
}

// ── Per-aggregation state ──────────────────────────────────────────────────────

/// Ordered f64 wrapper for MIN/MAX BTreeMap keys.
///
/// `f64` does not implement `Ord` (NaN). `total_cmp` is used so NaN sorts
/// consistently (after all finite values), keeping the BTreeMap invariants valid.
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdF64(f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl Default for OrdF64 {
    fn default() -> Self {
        Self(0.0)
    }
}

/// Multiset key for MIN/MAX.
///
/// DEC-1: the old key was `OrdF64` for **every** input type, so an `Int64` or
/// decimal value was rounded to `f64` before being used as a map key. Past
/// 2^53 that is not a rounding error, it is a *collision*: two distinct values
/// land on one entry, their weights merge, and retracting one of them deletes
/// both — MIN then returns a value that is still in the relation's true
/// multiset only by luck. Integer-valued inputs now key on `i128` and keep
/// their identity.
///
/// A given aggregation's `NumKind` is fixed at construction, so the two
/// variants never coexist in one map; the derived `Ord` (which would compare
/// variant tags first) is therefore never asked to order across them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MinMaxKey {
    Int(i128),
    Float(OrdF64),
}

/// Separate running state for ONE aggregation expression.
/// A group's full state is `Vec<AggState>` indexed by position in `aggregations`.
///
/// `sum` is used by SUM. `avg_sum_i64` + `avg_count_i64` are used by AVG:
/// for integer-typed input columns they accumulate exactly in i64, emitting
/// the quotient as f64 only at output time. For float-typed inputs the caller
/// sets `avg_is_integer = false` and falls back to f64 accumulation in `sum`.
#[derive(Debug, Default, Clone)]
struct AggState {
    /// Weighted sum for SUM / AVG over **float** inputs (f64 accumulation).
    sum: f64,
    /// Weighted sum for SUM over **integer** inputs (exact i64 accumulation,
    /// AUD-3 — avoids the >2^53 precision loss of the old f64-only path).
    sum_i64: i64,
    /// DEC-1: weighted sum for SUM over **`Decimal128`** inputs, held as an
    /// unscaled `i128` at the input column's own scale. Exact, and the scale is
    /// invariant under addition, so no rescaling happens on the tick path.
    sum_i128: i128,
    /// Row count for COUNT. Also used as the non-null input count for AVG
    /// when inputs are float (avg_is_integer == false). NOT a group-liveness
    /// signal — see `rows`.
    count: i64,
    /// COUNTNULL-1: the group's TRUE row weight, accumulated BEFORE any
    /// null-exclusion. `count` used to double as empty-group detection, but
    /// `COUNT(col)` and SUM/MIN/MAX skip NULL inputs without counting them —
    /// so a group whose rows were all NULL in the aggregated column read as
    /// empty and was GC'd, deleting a group SQL says exists (`COUNT(col) =
    /// 0` over a LEFT JOIN's padding, q13's exact shape). Liveness reads
    /// THIS field, which no null-skip can starve.
    rows: i64,
    /// Integer-precision weighted sum for AVG over integer-typed inputs.
    avg_sum_i64: i64,
    /// DEC-1: exact weighted sum for AVG over `Decimal128` inputs. Separate
    /// from `sum_i128` so `[SUM(x), AVG(x)]` in one spec cannot cross-feed.
    avg_sum_i128: i128,
    /// Non-null input count for AVG (separately tracked from `count` so
    /// COUNT and AVG can coexist in a multi-aggregation spec).
    avg_count_i64: i64,
    /// True when the AVG input is an integer column — use i64 accumulation.
    avg_is_integer: bool,
    /// DEC-1: set when an exact accumulator overflowed. Sticky — once a group's
    /// running total cannot be represented, every later delta for it errors too,
    /// so the view stays visibly broken rather than quietly resuming from a
    /// number that was never the sum.
    overflow: bool,
    /// For MIN/MAX: multiset of (value → cumulative weight), keyed exactly for
    /// the column's kind (DEC-1) rather than through `f64` for all of them.
    min_max_set: BTreeMap<MinMaxKey, i64>,
    /// CDIST-1: how many `min_max_set` values currently have POSITIVE weight —
    /// maintained on zero-crossings so `COUNT(DISTINCT)` emits in O(1).
    /// Derived, not serialized: restore recomputes it from the multiset.
    distinct_pos: i64,
}

/// One row's typed aggregate input value (AUD-7 / audit §5c): read directly
/// from the Arrow array — no per-row stringify + re-parse. `Null` is a SQL
/// NULL (or a value the safe cast could not represent, e.g. a `UInt64` above
/// `i64::MAX`, which arrow's safe cast nulls out); `None` means the
/// aggregation has no input column (`COUNT(*)`).
#[derive(Debug, Clone, Copy)]
enum AggInput {
    None,
    Null,
    I64(i64),
    F64(f64),
    /// DEC-1: the unscaled `i128` of a `Decimal128` cell, at the column's scale.
    I128(i128),
}

impl AggState {
    /// Apply one row's delta. `kind` is `Some` for numeric aggregates
    /// (SUM/AVG/MIN/MAX) and `None` for COUNT, decided from the column's Arrow
    /// type in `IncrementalAggOp::new`. The value arrives typed (AUD-7): the
    /// old string round-trip — and its `.unwrap_or(0.0)` silent-zero bug on
    /// unparseable values — no longer exists.
    fn apply_delta_for_agg(
        &mut self,
        agg: &Aggregation,
        kind: Option<NumKind>,
        value: AggInput,
        weight: i64,
    ) -> DeltaResult<()> {
        // COUNTNULL-1: the row is part of the group whatever its value — the
        // per-aggregation null-exclusions below must not starve liveness.
        self.rows += weight;
        // DEC-1: a poisoned accumulator never silently resumes.
        if self.overflow {
            return Err(DeltaError::Operator(format!(
                "aggregate '{}' overflowed its exact accumulator on an earlier delta; the \
                 running total is unrepresentable and the view will not publish one",
                agg.output_col()
            )));
        }
        match agg {
            Aggregation::Sum { .. } => {
                match value {
                    // SQL: null inputs are excluded from SUM.
                    AggInput::Null | AggInput::None => return Ok(()),
                    AggInput::I64(v) => {
                        self.sum_i64 =
                            self.overflowing(accumulate_i64(self.sum_i64, v, weight), agg, "SUM")?;
                    }
                    AggInput::I128(v) => {
                        self.sum_i128 = self.overflowing(
                            accumulate_i128(self.sum_i128, v, weight),
                            agg,
                            "SUM",
                        )?;
                    }
                    AggInput::F64(v) => self.sum += v * weight as f64,
                }
                self.count += weight;
            }
            Aggregation::Count { input_col, .. } => {
                // IVM-6: COUNT(col) excludes nulls; COUNT(*) counts all rows.
                if input_col.is_some() && matches!(value, AggInput::Null) {
                    return Ok(());
                }
                self.count += weight;
            }
            Aggregation::Avg { .. } => {
                // AUD-3: strategy is fixed by the column's declared type.
                // Integer inputs accumulate exactly in i64; float in f64.
                match value {
                    // SQL: null inputs are excluded from AVG.
                    AggInput::Null | AggInput::None => return Ok(()),
                    AggInput::I64(v) => {
                        self.avg_is_integer = true;
                        self.avg_sum_i64 = self.overflowing(
                            accumulate_i64(self.avg_sum_i64, v, weight),
                            agg,
                            "AVG",
                        )?;
                    }
                    AggInput::I128(v) => {
                        // DEC-1: the decimal path is selected by `kind` at
                        // output time, so no `avg_is_integer` flag is set here.
                        self.avg_sum_i128 = self.overflowing(
                            accumulate_i128(self.avg_sum_i128, v, weight),
                            agg,
                            "AVG",
                        )?;
                    }
                    AggInput::F64(v) => {
                        self.avg_is_integer = false;
                        self.sum += v * weight as f64;
                    }
                }
                self.avg_count_i64 += weight;
                self.count += weight;
            }
            Aggregation::Min { .. }
            | Aggregation::Max { .. }
            | Aggregation::CountDistinct { .. } => {
                let key = match value {
                    // SQL: null inputs do not affect MIN/MAX or COUNT(DISTINCT).
                    AggInput::Null | AggInput::None => return Ok(()),
                    // DEC-1: integer-valued inputs key exactly. The old code
                    // widened them to f64 here, which collided above 2^53.
                    AggInput::I64(v) => MinMaxKey::Int(v as i128),
                    AggInput::I128(v) => MinMaxKey::Int(v),
                    AggInput::F64(v) => MinMaxKey::Float(OrdF64(v)),
                };
                let _ = kind; // ordering strategy is value-driven now
                let entry = self.min_max_set.entry(key).or_insert(0);
                let was_positive = *entry > 0;
                *entry += weight;
                let is_positive = *entry > 0;
                // CDIST-1: count zero-crossings, not rows — retracting one of
                // several copies of a value must not change the distinct count.
                match (was_positive, is_positive) {
                    (false, true) => self.distinct_pos += 1,
                    (true, false) => self.distinct_pos -= 1,
                    _ => {}
                }
                if *entry == 0 {
                    self.min_max_set.remove(&key);
                }
                self.count += weight;
            }
        }
        Ok(())
    }

    /// Unwrap an exact accumulation, poisoning this state and failing the tick
    /// if it overflowed (DEC-1).
    fn overflowing<T>(&mut self, v: Option<T>, agg: &Aggregation, what: &str) -> DeltaResult<T> {
        match v {
            Some(v) => Ok(v),
            None => {
                self.overflow = true;
                Err(DeltaError::Operator(format!(
                    "{what} for '{}' overflowed its exact accumulator; the incremental view \
                     fails closed rather than publishing a saturated total",
                    agg.output_col()
                )))
            }
        }
    }

    fn current_value(&self, agg: &Aggregation, kind: Option<NumKind>) -> Option<AggScalar> {
        match agg {
            // SQL: SUM over zero (non-null) inputs is NULL, not 0. A SUM
            // state's `count` advances only for non-null inputs (the Null arm
            // returns before it), so it is exactly the "has anything been
            // summed" bit — and it returns to 0 when every contribution is
            // retracted, where SQL again says NULL. Emitting 0 instead was
            // invisible to every value assertion that read the column through
            // `Int64Array::value()` (NULL slots read as 0); the decomposed-q6
            // text comparison against full recompute is what surfaced it.
            Aggregation::Sum { .. } if self.count == 0 => None,
            Aggregation::Sum { .. } => match kind {
                Some(NumKind::Int) => Some(AggScalar::I64(self.sum_i64)),
                // DEC-1: SUM keeps the input's scale, so the running `i128` is
                // already the output's unscaled value — no conversion at all.
                Some(NumKind::Decimal { .. }) => Some(AggScalar::Dec(self.sum_i128)),
                _ => Some(AggScalar::F64(self.sum)),
            },
            Aggregation::Count { .. } => Some(AggScalar::I64(self.count)),
            // COUNT semantics: 0 over empty input, never NULL.
            Aggregation::CountDistinct { .. } => Some(AggScalar::I64(self.distinct_pos)),
            Aggregation::Avg { .. } => {
                if self.avg_count_i64 == 0 {
                    None
                } else if let Some(NumKind::Decimal { precision, scale }) = kind {
                    // DEC-1: AVG over a decimal stays decimal. DataFusion
                    // rescales the sum to the result scale and then divides,
                    // truncating toward zero — reproduced exactly here, because
                    // "the same answer to within a cent" is not the same answer.
                    let DataType::Decimal128(_, out_scale) = decimal_avg_type(precision, scale)
                    else {
                        return None;
                    };
                    let lift = (out_scale - scale).max(0) as u32;
                    10i128
                        .checked_pow(lift)
                        .and_then(|mul| self.avg_sum_i128.checked_mul(mul))
                        .map(|scaled| AggScalar::Dec(scaled / self.avg_count_i64 as i128))
                } else if self.avg_is_integer {
                    Some(AggScalar::F64(
                        self.avg_sum_i64 as f64 / self.avg_count_i64 as f64,
                    ))
                } else {
                    Some(AggScalar::F64(self.sum / self.avg_count_i64 as f64))
                }
            }
            Aggregation::Min { .. } => self.min_max_set.keys().next().map(|k| scalar_of(*k, kind)),
            Aggregation::Max { .. } => self
                .min_max_set
                .keys()
                .next_back()
                .map(|k| scalar_of(*k, kind)),
        }
    }
}

/// Wrap a min/max key in the correct typed scalar for its column kind.
///
/// The key variant and the kind always agree (both are decided by the column's
/// Arrow type); the cross cases are written out anyway so a future kind cannot
/// reach a `_ =>` arm and silently emit the wrong representation.
fn scalar_of(key: MinMaxKey, kind: Option<NumKind>) -> AggScalar {
    match (kind, key) {
        (Some(NumKind::Decimal { .. }), MinMaxKey::Int(v)) => AggScalar::Dec(v),
        (Some(NumKind::Int), MinMaxKey::Int(v)) => AggScalar::I64(v as i64),
        (Some(NumKind::Int), MinMaxKey::Float(v)) => AggScalar::I64(v.0 as i64),
        (_, MinMaxKey::Int(v)) => AggScalar::F64(v as f64),
        (_, MinMaxKey::Float(v)) => AggScalar::F64(v.0),
    }
}

/// `group_key → per-aggregation running state`.
///
/// AUD-7: keys are arrow **row-format** bytes — a single opaque, order-preserving
/// encoding of the group-by columns produced by the op's shared [`RowConverter`],
/// replacing the old `Vec<Option<String>>` that allocated a `String` for every
/// group column of every delta row. `Box<[u8]>` keeps the key heap-compact.
type GroupStateMap = AHashMap<Box<[u8]>, Vec<AggState>>;

/// The row-format key of the single implicit group a `GROUP BY`-less aggregate
/// owns (IVM-AUD-GLOBAL-1). Empty, because there are no group columns to encode.
const GLOBAL_KEY: &[u8] = &[];

/// Before-snapshot map used within a single `apply` tick: `group_key → state as
/// it was before the tick's deltas` (`None` = the group did not exist yet).
type TouchedMap = AHashMap<Box<[u8]>, Option<Vec<AggState>>>;

/// AUD-7: per-aggregation typed column reader. Casts an aggregation's input
/// column to its accumulation array **once per delta batch** (Int64 / Float64),
/// so [`IncrementalAggOp::apply`] reads a typed value per row with no per-row
/// stringify + re-parse (the root of the old `.unwrap_or(0.0)` silent-zero bug).
enum ValueReader {
    /// `COUNT(*)` — no input column; every row contributes.
    NoInput,
    /// Aggregation references a column absent from the delta schema → every row
    /// reads as SQL NULL (excluded from SUM/AVG/MIN/MAX; not counted).
    Missing,
    /// `COUNT(col)`: only nullness matters (`col` may be non-numeric).
    NullMask(ArrayRef),
    /// Numeric input accumulated as i64 (integer-typed column).
    Int(Int64Array),
    /// Numeric input accumulated as f64 (float-typed column).
    Float(Float64Array),
    /// DEC-1: fixed-point input read as its unscaled `i128` at the declared
    /// scale — the delta is cast to that scale once per batch, so every row
    /// contributes at one common scale and addition stays exact.
    Decimal(Decimal128Array),
}

impl ValueReader {
    fn build(data: &RecordBatch, agg: &Aggregation, kind: Option<NumKind>) -> DeltaResult<Self> {
        let Some(name) = agg.input_col() else {
            return Ok(ValueReader::NoInput); // COUNT(*)
        };
        let idx = match data.schema().index_of(name) {
            Ok(i) => i,
            Err(_) => return Ok(ValueReader::Missing),
        };
        let col = data.column(idx);
        match kind {
            // COUNT(col): keep the original array, we only probe its null mask.
            None => Ok(ValueReader::NullMask(col.clone())),
            // Cast once per batch, ERRORING on overflow (UINT-1). arrow's
            // default safe cast nulls out values that don't fit (a UInt64
            // above i64::MAX), and a NULL is silently EXCLUDED from
            // SUM/MIN/MAX — a wrong answer nobody sees. With unsigned
            // declared outputs now accepted, that path is reachable, so it
            // fails loudly instead (the sticky-overflow discipline of DEC-1).
            Some(NumKind::Int) => {
                let arr = compute::cast_with_options(
                    col,
                    &DataType::Int64,
                    &compute::CastOptions {
                        safe: false,
                        ..Default::default()
                    },
                )?;
                let arr = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DeltaError::Operator("int64 cast produced wrong type".into()))?
                    .clone();
                Ok(ValueReader::Int(arr))
            }
            Some(NumKind::Float) => {
                let arr = compute::cast(col, &DataType::Float64)?;
                let arr = arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| DeltaError::Operator("float64 cast produced wrong type".into()))?
                    .clone();
                Ok(ValueReader::Float(arr))
            }
            Some(NumKind::Decimal { precision, scale }) => {
                let want = DataType::Decimal128(precision, scale);
                let arr = if col.data_type() == &want {
                    col.clone()
                } else {
                    compute::cast(col, &want)?
                };
                let arr = arr
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        DeltaError::Operator("decimal128 cast produced wrong type".into())
                    })?
                    .clone();
                Ok(ValueReader::Decimal(arr))
            }
        }
    }

    fn value(&self, row: usize) -> AggInput {
        match self {
            ValueReader::NoInput => AggInput::None,
            ValueReader::Missing => AggInput::Null,
            ValueReader::NullMask(a) => {
                if a.is_null(row) {
                    AggInput::Null
                } else {
                    // COUNT(col) ignores the magnitude; any non-null marker works.
                    AggInput::I64(0)
                }
            }
            ValueReader::Int(a) => {
                if a.is_null(row) {
                    AggInput::Null
                } else {
                    AggInput::I64(a.value(row))
                }
            }
            ValueReader::Float(a) => {
                if a.is_null(row) {
                    AggInput::Null
                } else {
                    AggInput::F64(a.value(row))
                }
            }
            ValueReader::Decimal(a) => {
                if a.is_null(row) {
                    AggInput::Null
                } else {
                    AggInput::I128(a.value(row))
                }
            }
        }
    }
}

// ── IncrementalAggOp ──────────────────────────────────────────────────────────

/// Stateful incremental aggregate operator.
pub struct IncrementalAggOp {
    group_by: Vec<String>,
    aggregations: Vec<Aggregation>,
    /// AUD-3: per-aggregation numeric kind, decided once from the input schema.
    /// `None` for COUNT (no numeric input to accumulate).
    input_kinds: Vec<Option<NumKind>>,
    /// DEC-1: the SQL result type of each aggregate column *before* the view's
    /// declared schema is applied. `build_output_batch` needs it to know what
    /// an `AggScalar::Dec` is scaled by when the declared column is some other
    /// type, and `new_with_output_schema` needs it to refuse a decimal
    /// declaration that disagrees with the one the values actually carry.
    agg_natural_types: Vec<DataType>,
    output_schema: SchemaRef,
    /// AUD-7: shared row-format encoder for group-by keys, built once from the
    /// group columns' declared types and reused across every tick. Reuse is what
    /// keeps a value's encoding stable when a group column is dictionary-encoded
    /// — a per-tick converter would re-intern and could drift, splitting one
    /// logical group across two keys.
    group_converter: RowConverter,
    /// Declared arrow types of the group-by columns, in order. Used to rebuild
    /// the converter after a restore and to name the reconstructed group columns.
    group_field_types: Vec<DataType>,
    /// state[group_key] → per-aggregation running state (one entry per aggregation)
    state: GroupStateMap,
}

impl IncrementalAggOp {
    pub fn new(
        input_schema: &SchemaRef,
        group_by: Vec<String>,
        aggregations: Vec<Aggregation>,
    ) -> DeltaResult<Self> {
        // Validate group-by columns exist
        for col in &group_by {
            input_schema
                .field_with_name(col)
                .map_err(|_| DeltaError::ColumnNotFound(col.clone()))?;
        }

        // Validate input columns for each agg and decide its numeric kind once
        // from the schema (AUD-3). SUM/AVG/MIN/MAX over a non-numeric column
        // return an error so the caller falls back to DiffBased full recompute
        // rather than the old silent `0.0`.
        let mut input_kinds: Vec<Option<NumKind>> = Vec::with_capacity(aggregations.len());
        for agg in &aggregations {
            let kind = match agg {
                // COUNT only needs a null check; no numeric accumulation.
                Aggregation::Count { input_col, .. } => {
                    if let Some(col) = input_col {
                        input_schema
                            .field_with_name(col)
                            .map_err(|_| DeltaError::ColumnNotFound(col.clone()))?;
                    }
                    None
                }
                Aggregation::Sum { input_col, .. }
                | Aggregation::Avg { input_col, .. }
                | Aggregation::Min { input_col, .. }
                | Aggregation::Max { input_col, .. }
                | Aggregation::CountDistinct { input_col, .. } => {
                    let field = input_schema
                        .field_with_name(input_col)
                        .map_err(|_| DeltaError::ColumnNotFound(input_col.clone()))?;
                    let k = num_kind(field.data_type()).ok_or_else(|| {
                        DeltaError::Operator(format!(
                            "aggregate '{}' over non-numeric column '{}' ({:?}) is not \
                             supported by the incremental operator; the view falls back to \
                             DiffBased full recompute",
                            agg.output_col(),
                            input_col,
                            field.data_type()
                        ))
                    })?;
                    Some(k)
                }
            };
            input_kinds.push(kind);
        }

        // Build output schema: group-by columns + aggregate output columns.
        let mut out_fields: Vec<_> = group_by
            .iter()
            .map(|name| {
                input_schema
                    .field_with_name(name)
                    .map(|f| Arc::new(f.clone()))
                    .map_err(|_| DeltaError::ColumnNotFound(name.clone()))
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        let mut agg_natural_types: Vec<DataType> = Vec::with_capacity(aggregations.len());
        for (agg, kind) in aggregations.iter().zip(input_kinds.iter()) {
            // AUD-3: match SQL output types. COUNT → Int64; AVG → Float64 except
            // over decimals; SUM/MIN/MAX preserve the input's family
            // (SUM(Int)→Int64, SUM(Decimal(p,s))→Decimal(p+10,s), …).
            let output_type = match agg {
                Aggregation::Count { .. } | Aggregation::CountDistinct { .. } => DataType::Int64,
                Aggregation::Avg { .. } => match kind {
                    Some(NumKind::Decimal { precision, scale }) => {
                        decimal_avg_type(*precision, *scale)
                    }
                    _ => DataType::Float64,
                },
                Aggregation::Sum { .. } => match kind {
                    Some(NumKind::Int) => DataType::Int64,
                    Some(NumKind::Decimal { precision, scale }) => {
                        decimal_sum_type(*precision, *scale)
                    }
                    _ => DataType::Float64,
                },
                // MIN/MAX return a value drawn from the column, so the type is
                // the column's own — no widening.
                Aggregation::Min { .. } | Aggregation::Max { .. } => match kind {
                    Some(NumKind::Int) => DataType::Int64,
                    Some(NumKind::Decimal { precision, scale }) => {
                        DataType::Decimal128(*precision, *scale)
                    }
                    _ => DataType::Float64,
                },
            };
            agg_natural_types.push(output_type.clone());
            out_fields.push(Arc::new(Field::new(agg.output_col(), output_type, true)));
        }

        let output_schema = Arc::new(Schema::new(out_fields));

        // AUD-7: build the shared row-format encoder for the group-by columns
        // from their declared source types (validated as present above).
        let group_field_types: Vec<DataType> = group_by
            .iter()
            .map(|name| {
                input_schema
                    .field_with_name(name)
                    .map(|f| f.data_type().clone())
                    .map_err(|_| DeltaError::ColumnNotFound(name.clone()))
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        let group_converter = RowConverter::new(
            group_field_types
                .iter()
                .map(|dt| SortField::new(dt.clone()))
                .collect(),
        )
        .map_err(DeltaError::Arrow)?;

        Ok(Self {
            group_by,
            aggregations,
            input_kinds,
            agg_natural_types,
            output_schema,
            group_converter,
            group_field_types,
            state: GroupStateMap::default(),
        })
    }

    /// Like [`new`](Self::new) but adopts the view's **declared** output column
    /// types for the aggregate columns (by name), preserving the operator's
    /// canonical column order (group-by columns first, then aggregates).
    ///
    /// AUD-3: `SUM(Int64)` is SQL-typed `Int64`, but a view may legitimately
    /// declare its output column as `Float64` (or vice-versa). The operator
    /// honors that declaration so the materialized snapshot matches the
    /// registered contract that downstream plans and the DiffBased baseline
    /// diff against — instead of the old behavior of always emitting `Float64`.
    /// Declared aggregate columns must be `Int64`/`Float64`, or the *exact*
    /// `Decimal128(p, s)` the aggregate naturally produces; anything else errors
    /// so the planner falls back to DiffBased. DEC-1: a decimal declaration that
    /// differs in scale is refused rather than rescaled, because rescaling an
    /// already-accumulated total is where fixed-point silently loses digits —
    /// and a wrong scale is a wrong number by a factor of ten, not a rounding.
    pub fn new_with_output_schema(
        input_schema: &SchemaRef,
        group_by: Vec<String>,
        aggregations: Vec<Aggregation>,
        declared: &SchemaRef,
    ) -> DeltaResult<Self> {
        let mut op = Self::new(input_schema, group_by, aggregations)?;
        let n_group = op.group_by.len();
        let mut fields: Vec<Arc<Field>> = op.output_schema.fields().iter().cloned().collect();
        for (i, agg) in op.aggregations.iter().enumerate() {
            if let Ok(df) = declared.field_with_name(agg.output_col()) {
                let natural = op.agg_natural_types.get(i);
                let accept = match df.data_type() {
                    DataType::Int64 | DataType::Float64 => true,
                    // UINT-1: DataFusion types MAX/SUM over an unsigned column
                    // as unsigned; the operator accumulates in i64 and emits a
                    // checked cast back (negative or oversized totals error
                    // loudly rather than wrapping).
                    DataType::UInt64 => natural == Some(&DataType::Int64),
                    dec @ DataType::Decimal128(_, _) => natural == Some(dec),
                    _ => false,
                };
                if !accept {
                    return Err(DeltaError::Operator(format!(
                        "declared output column '{}' has type {:?} but the incremental \
                         aggregate produces {:?}; it emits Int64/Float64 or that exact \
                         Decimal128 — view falls back to DiffBased",
                        agg.output_col(),
                        df.data_type(),
                        natural
                    )));
                }
                if let Some(slot) = fields.get_mut(n_group + i) {
                    *slot = Arc::new(Field::new(agg.output_col(), df.data_type().clone(), true));
                }
            }
        }
        op.output_schema = Arc::new(Schema::new(fields));
        Ok(op)
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Evict aggregate groups whose event time is below `watermark`.
    ///
    /// Note: the current data model does not carry a per-group event time on
    /// `IncrementalAggOp::state` (groups are keyed by value, not by a typed
    /// timestamp). Until that schema is added, the operator is a no-op here.
    /// The interface exists so the `ViewPlan::Aggregate` arm of
    /// `gc_watermark` is reached; the eviction is wired to no-op pending
    /// schema work. A long-running incremental aggregate over an unbounded
    /// source should add a `TUMBLE/HOP/SESSION` window or filter on
    /// `event_time_col` in the view body so the SQL engine can prune older
    /// partitions.
    /// Watermark GC is **not applicable** to this operator, and returning 0
    /// is the honest answer rather than a stub (IVM-AUD-CORE-6).
    ///
    /// A `GROUP BY k` accumulator holds one entry per key with no event-time
    /// dimension: a SUM does not retain the timestamps of the rows folded into
    /// it, so no watermark can prove that a key's state is complete and
    /// evictable. State here is bounded by key cardinality, not by time — the
    /// same property a streaming aggregation without windows has.
    ///
    /// Only operators that retain the contributing ROWS (the join traces) can
    /// be watermark-GC'd; the audit's finding was not that this returns 0, but
    /// that the docs claimed LATENESS pruned "aggregate state" when it never
    /// could.
    pub fn gc_watermark(&mut self, _watermark: i64) -> crate::DeltaResult<usize> {
        Ok(0)
    }

    /// Apply one tick of incremental aggregation.
    ///
    /// For each row in `delta`:
    /// 1. Look up the group's current state (per-aggregation).
    /// 2. Emit retraction of old aggregate output (if group was non-empty).
    /// 3. Apply delta weight to each aggregation's state independently.
    /// 4. Emit insertion of new aggregate output (if group is now non-empty).
    pub fn apply(&mut self, delta: DeltaBatch) -> DeltaResult<DeltaBatch> {
        if delta.is_empty() {
            // IVM-AUD-GLOBAL-1: an empty delta normally means nothing changed,
            // and emitting nothing is right. The exception is a global
            // aggregate that has not published yet: its row exists from the
            // first tick regardless of whether any input row survived the
            // view's WHERE clause, and a filter that admits nothing is exactly
            // when the caller most needs to see the zero.
            if self.group_by.is_empty() && !self.state.contains_key(GLOBAL_KEY) {
                return self.emit_initial_global_row();
            }
            return DeltaBatch::empty(self.output_schema.clone());
        }

        let data = delta.data_batch();
        let weights = delta.weights();

        let group_col_indices: Vec<usize> = self
            .group_by
            .iter()
            .map(|name| {
                data.schema()
                    .index_of(name)
                    .map_err(|_| DeltaError::ColumnNotFound(name.clone()))
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        // AUD-7: encode every group-by column to a single row-format key in one
        // pass (no per-cell String alloc). A global aggregate (no GROUP BY) has
        // one implicit group keyed by the empty byte string.
        let group_rows = if group_col_indices.is_empty() {
            None
        } else {
            let group_arrays: Vec<ArrayRef> = group_col_indices
                .iter()
                .map(|&idx| data.column(idx).clone())
                .collect();
            Some(
                self.group_converter
                    .convert_columns(&group_arrays)
                    .map_err(DeltaError::Arrow)?,
            )
        };

        // AUD-7: cast each aggregation's input column to its typed accumulation
        // array once for the whole batch, replacing the per-row stringify+parse.
        let value_readers: Vec<ValueReader> = self
            .aggregations
            .iter()
            .zip(self.input_kinds.iter())
            .map(|(agg, kind)| ValueReader::build(&data, agg, *kind))
            .collect::<DeltaResult<Vec<_>>>()?;

        // Track which groups were touched and their before-tick state.
        let mut touched: TouchedMap = AHashMap::new();

        for row in 0..data.num_rows() {
            let key: Box<[u8]> = match &group_rows {
                Some(rows) => rows.row(row).as_ref().into(),
                None => Box::<[u8]>::default(),
            };

            // Record state before this row's delta (once per group per tick).
            if !touched.contains_key(&key) {
                let before = self.state.get(&key).cloned();
                touched.insert(key.clone(), before);
            }

            let w = weights.value(row);

            // Apply delta to each aggregation's state independently. Each
            // aggregation has its own AggState, so [Count, Sum] does not
            // double-count and Sum + Min do not cross-contaminate.
            let group_state = self
                .state
                .entry(key.clone())
                .or_insert_with(|| vec![AggState::default(); self.aggregations.len()]);

            // Ensure the state vector matches the aggregation count (handles a
            // new aggregation added after state was created).
            if group_state.len() < self.aggregations.len() {
                group_state.resize(self.aggregations.len(), AggState::default());
            }

            for (((state, agg), kind), reader) in group_state
                .iter_mut()
                .zip(self.aggregations.iter())
                .zip(self.input_kinds.iter())
                .zip(value_readers.iter())
            {
                state.apply_delta_for_agg(agg, *kind, reader.value(row), w)?;
            }

            // GC empty groups: a group is empty when ALL its per-agg states are.
            // IVM-AUD-GLOBAL-1: except the implicit group of a `GROUP BY`-less
            // aggregate, which is not a group that happens to have no rows — it
            // is the single row the query always returns. Dropping it published
            // nothing where SQL says `count(*) = 0`.
            if !self.group_by.is_empty()
                && let Some(states) = self.state.get(&key)
                && states.iter().all(|s| s.rows == 0)
            {
                self.state.remove(&key);
            }
        }

        let global = self.group_by.is_empty();

        // Build output: retract old agg + insert new agg for each touched group.
        let mut out_keys: Vec<Box<[u8]>> = Vec::new();
        let mut out_weights: Vec<i64> = Vec::new();
        let mut agg_values: Vec<Vec<Option<AggScalar>>> = Vec::new();

        for (key, before_states) in &touched {
            // A keyed group exists exactly while some aggregation still counts
            // rows for it. The implicit group of a `GROUP BY`-less aggregate is
            // not like that (IVM-AUD-GLOBAL-1): it exists from the first tick
            // to the last, and reads zero in between. Retracting it when its
            // count hits zero — which is what "does any aggregation still have
            // rows" answers — publishes no rows where SQL publishes `0`.
            // Whether the implicit row was already out there is read from the
            // STATE, not from a flag: a flag is not part of `state_bytes`, so a
            // restored operator would call itself unpublished and emit the `+1`
            // half of its update without the matching `-1` — the mirror then
            // counts the group twice.
            let has_before = if global {
                before_states.is_some()
            } else {
                before_states
                    .as_ref()
                    .map(|s| s.iter().any(|a| a.rows != 0))
                    .unwrap_or(false)
            };
            let has_after = if global {
                true
            } else {
                self.state
                    .get(key)
                    .map(|s| s.iter().any(|a| a.rows != 0))
                    .unwrap_or(false)
            };

            if has_before && let Some(states) = before_states.as_ref() {
                let vals = compute_agg_values(states, &self.aggregations, &self.input_kinds);
                out_keys.push(key.clone());
                out_weights.push(-1);
                agg_values.push(vals);
            }
            if has_after
                && let Some(after_states) = self.state.get(key).or(if global {
                    self.state.get(GLOBAL_KEY)
                } else {
                    None
                })
            {
                let vals = compute_agg_values(after_states, &self.aggregations, &self.input_kinds);
                out_keys.push(key.clone());
                out_weights.push(1);
                agg_values.push(vals);
            }
        }

        if out_keys.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        self.build_output_batch(&out_keys, &out_weights, &agg_values)
    }

    /// Serialize the per-group accumulator state to a self-contained blob.
    ///
    /// This is the piece of an incremental view that a full flow checkpoint
    /// cannot reconstruct from the materialized source or view snapshots: the
    /// source snapshot is a *set* (multiplicity is dropped by `filter_positive`)
    /// and the view snapshot loses the multiset MIN/MAX and the SUM/COUNT split
    /// AVG needs. Persisting the accumulator directly is the only lossless way
    /// to restore an incremental aggregate across a coordinator restart (G6/F4).
    ///
    /// Format **v2** (AUD-7): group keys are now opaque arrow row-format bytes,
    /// which are not stable across arrow encoding changes, so the group *values*
    /// are serialized as a portable Arrow IPC batch of the group columns instead
    /// of raw key bytes. Layout (little-endian):
    ///   `MAGIC "AGGS2" || u8 has_group_cols || u32 n_groups ||
    ///    [ u32 ipc_len || ipc(group columns) ]  (only if has_group_cols && n>0) ||
    ///    (u32 n_states || (state)*){n_groups}`
    /// States are written in the same order as the IPC batch rows. A blob that
    /// does not begin with `MAGIC` fails [`restore_state_bytes`], so an
    /// incompatible/older blob falls back (loudly) to seed-from-snapshots.
    pub fn state_bytes(&self) -> Vec<u8> {
        let entries: Vec<(&[u8], &Vec<AggState>)> =
            self.state.iter().map(|(k, v)| (&k[..], v)).collect();
        let has_group_cols = !self.group_field_types.is_empty();

        // Reconstruct group key columns (portable IPC) when there are group
        // columns AND at least one live group. If reconstruction fails, emit an
        // empty blob so restore falls back to seed-from-snapshots rather than
        // installing wrong state.
        let group_ipc: Option<Vec<u8>> = if has_group_cols && !entries.is_empty() {
            match self
                .group_columns_batch(entries.iter().map(|(k, _)| *k))
                .and_then(|b| encode_batch_ipc(&b))
            {
                Ok(ipc) => Some(ipc),
                Err(_) => {
                    let mut out = Vec::new();
                    out.extend_from_slice(AGG_STATE_MAGIC_V4);
                    out.push(1u8);
                    out.extend_from_slice(&0u32.to_le_bytes());
                    return out;
                }
            }
        } else {
            None
        };

        let mut out = Vec::new();
        out.extend_from_slice(AGG_STATE_MAGIC_V4);
        out.push(has_group_cols as u8);
        out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        if let Some(ipc) = &group_ipc {
            out.extend_from_slice(&(ipc.len() as u32).to_le_bytes());
            out.extend_from_slice(ipc);
        }
        for (_key, states) in &entries {
            out.extend_from_slice(&(states.len() as u32).to_le_bytes());
            for st in *states {
                st.write_bytes(&mut out);
            }
        }
        out
    }

    /// Replace the accumulator state with one previously produced by
    /// [`state_bytes`](Self::state_bytes). The group-by / aggregation shape is
    /// taken from `self` (rebuilt from the view SQL), so only the running
    /// values are transferred. An unrecognized (non-v3) blob errors so the
    /// caller can fall back to seed-from-snapshots.
    pub fn restore_state_bytes(&mut self, bytes: &[u8]) -> DeltaResult<()> {
        let v4 = bytes.starts_with(AGG_STATE_MAGIC_V4);
        if !v4 && !bytes.starts_with(AGG_STATE_MAGIC_V3) {
            return Err(DeltaError::Operator(
                "aggregate state blob is not format v3 (AUD-7, DEC-1); restore falls back to \
                 seed-from-snapshots"
                    .into(),
            ));
        }
        let mut pos = AGG_STATE_MAGIC_V3.len();
        let has_group_cols = read_u8(bytes, &mut pos)? == 1;
        let n_groups = read_u32(bytes, &mut pos)? as usize;

        // Rebuild the per-group row keys.
        let keys: Vec<Box<[u8]>> = if !has_group_cols {
            // Global aggregate: 0 or 1 group with the empty key.
            (0..n_groups).map(|_| Box::<[u8]>::default()).collect()
        } else if n_groups == 0 {
            Vec::new()
        } else {
            let ipc_len = read_u32(bytes, &mut pos)? as usize;
            let ipc = bytes
                .get(pos..pos + ipc_len)
                .ok_or_else(|| DeltaError::Operator("agg state truncated (group ipc)".into()))?;
            pos += ipc_len;
            let batch = decode_batch_ipc(ipc)?;
            let rows = self
                .group_converter
                .convert_columns(batch.columns())
                .map_err(DeltaError::Arrow)?;
            (0..batch.num_rows())
                .map(|i| rows.row(i).as_ref().into())
                .collect()
        };

        if keys.len() != n_groups {
            return Err(DeltaError::Operator(
                "agg state group-count mismatch on restore".into(),
            ));
        }

        let mut state: GroupStateMap = AHashMap::with_capacity(n_groups);
        for key in keys {
            let n_states = read_u32(bytes, &mut pos)? as usize;
            let mut states: Vec<AggState> = Vec::with_capacity(n_states);
            for _ in 0..n_states {
                states.push(AggState::read_bytes(bytes, &mut pos, v4)?);
            }
            state.insert(key, states);
        }
        self.state = state;
        Ok(())
    }

    /// Rebuild the group-by columns as a `RecordBatch` from a sequence of
    /// row-format keys, using the shared converter (AUD-7). Shared by
    /// `state_bytes` (for portable serialization) and `build_output_batch`
    /// (for emitting the group columns natively, no string cast).
    fn group_columns_batch<'a>(
        &self,
        keys: impl Iterator<Item = &'a [u8]>,
    ) -> DeltaResult<RecordBatch> {
        let parser = self.group_converter.parser();
        let rows: Vec<_> = keys.map(|k| parser.parse(k)).collect();
        let arrays = self
            .group_converter
            .convert_rows(rows)
            .map_err(DeltaError::Arrow)?;
        let fields: Vec<Field> = self
            .group_by
            .iter()
            .zip(self.group_field_types.iter())
            .map(|(name, dt)| Field::new(name, dt.clone(), true))
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(DeltaError::from)
    }

    /// AUD-7: build the retract/insert output batch, rebuilding the group-by
    /// columns natively from row-format keys (no `String`→cast round trip) and
    /// emitting aggregate columns in the declared output types.
    /// The `+1` row a `GROUP BY`-less aggregate owes from its very first tick
    /// (IVM-AUD-GLOBAL-1): `COUNT` reads 0, every other aggregate reads NULL,
    /// which is what SQL returns for an aggregate over no rows.
    fn emit_initial_global_row(&mut self) -> DeltaResult<DeltaBatch> {
        let states = vec![AggState::default(); self.aggregations.len()];
        let values = compute_agg_values(&states, &self.aggregations, &self.input_kinds);
        self.state.insert(GLOBAL_KEY.into(), states);
        self.build_output_batch(&[GLOBAL_KEY.into()], &[1], &[values])
    }

    fn build_output_batch(
        &self,
        group_keys: &[Box<[u8]>],
        weights: &[i64],
        agg_values: &[Vec<Option<AggScalar>>],
    ) -> DeltaResult<DeltaBatch> {
        let n_group = self.group_by.len();

        let mut cols: Vec<ArrayRef> = if n_group == 0 {
            Vec::new()
        } else {
            let batch = self.group_columns_batch(group_keys.iter().map(|k| &k[..]))?;
            // Cast a group column only if the declared output type differs from
            // the source column type (rare; `new_with_output_schema` never
            // re-types group columns, but a view may declare a widened type).
            batch
                .columns()
                .iter()
                .enumerate()
                .map(|(gi, arr)| {
                    let target = self.output_schema.field(gi).data_type();
                    if arr.data_type() == target {
                        Ok(arr.clone())
                    } else {
                        compute::cast(arr, target).map_err(DeltaError::from)
                    }
                })
                .collect::<DeltaResult<Vec<_>>>()?
        };

        // Aggregate columns, typed to the declared output schema (AUD-3):
        // integer SUM/MIN/MAX/COUNT emit Int64 exactly; decimal aggregates emit
        // Decimal128 at their own scale (DEC-1); everything else emits Float64.
        for ai in 0..self.aggregations.len() {
            let target = self.output_schema.field(n_group + ai).data_type();
            // The scale the running `i128` is expressed in — the natural output
            // type's, which is what `current_value` produced regardless of what
            // the view declared.
            let scale = match self.agg_natural_types.get(ai) {
                Some(DataType::Decimal128(_, s)) => *s,
                _ => 0,
            };
            let col: ArrayRef = match target {
                DataType::Int64 => {
                    let vals: Int64Array = agg_values
                        .iter()
                        .map(|row| {
                            row.get(ai).copied().flatten().map(|s| match s {
                                AggScalar::I64(v) => v,
                                AggScalar::F64(v) => v as i64,
                                AggScalar::Dec(v) => descale_to_i64(v, scale),
                            })
                        })
                        .collect();
                    Arc::new(vals)
                }
                // UINT-1: an unsigned declared output — accumulate in i64 as
                // usual, then checked-cast back. `safe: false` makes a
                // negative or oversized total an ERROR, never a wrap or NULL.
                DataType::UInt64 => {
                    let vals: Int64Array = agg_values
                        .iter()
                        .map(|row| {
                            row.get(ai).copied().flatten().map(|s| match s {
                                AggScalar::I64(v) => v,
                                AggScalar::F64(v) => v as i64,
                                AggScalar::Dec(v) => descale_to_i64(v, scale),
                            })
                        })
                        .collect();
                    compute::cast_with_options(
                        &(Arc::new(vals) as ArrayRef),
                        target,
                        &compute::CastOptions {
                            safe: false,
                            ..Default::default()
                        },
                    )?
                }
                DataType::Decimal128(p, s) => {
                    // `with_precision_and_scale` validates every value against
                    // the declared precision and errors if one does not fit —
                    // the last fail-closed gate before a truncated total could
                    // reach a snapshot.
                    let vals: Vec<Option<i128>> = agg_values
                        .iter()
                        .map(|row| {
                            row.get(ai).copied().flatten().map(|sc| match sc {
                                AggScalar::Dec(v) => v,
                                AggScalar::I64(v) => v as i128,
                                AggScalar::F64(v) => (v * 10f64.powi(*s as i32)) as i128,
                            })
                        })
                        .collect();
                    let arr = Decimal128Array::from(vals).with_precision_and_scale(*p, *s)?;
                    Arc::new(arr)
                }
                _ => {
                    let vals: Float64Array = agg_values
                        .iter()
                        .map(|row| {
                            row.get(ai).copied().flatten().map(|s| match s {
                                AggScalar::I64(v) => v as f64,
                                AggScalar::F64(v) => v,
                                AggScalar::Dec(v) => descale_to_f64(v, scale),
                            })
                        })
                        .collect();
                    Arc::new(vals)
                }
            };
            cols.push(col);
        }

        // Weight column.
        cols.push(Arc::new(Int64Array::from(weights.to_vec())));

        let mut full_fields: Vec<_> = self.output_schema.fields().iter().cloned().collect();
        full_fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
        let full_schema = Arc::new(Schema::new(full_fields));

        let inner = RecordBatch::try_new(full_schema, cols)?;
        DeltaBatch::from_weighted(inner)
    }
}

/// Magic prefix for the version-2 aggregate-state blob (AUD-7).
/// DEC-1: bumped from `AGGS2` because the per-aggregation state gained exact
/// `i128` accumulators and a typed min/max key. A v2 blob is *parseable* as v3
/// byte-wise garbage, so the magic is the only thing standing between an old
/// checkpoint and a plausible-looking wrong total — restore rejects it and the
/// view reseeds from snapshots, which is slower and correct.
const AGG_STATE_MAGIC_V3: &[u8; 5] = b"AGGS3";
/// COUNTNULL-1 added `rows` to every serialized [`AggState`]. A V3 blob
/// restores with `rows := count` — exactly the assumption the pre-fix code
/// baked in, and the best information a V3 checkpoint carries.
const AGG_STATE_MAGIC_V4: &[u8; 5] = b"AGGS4";

/// Serialize a `RecordBatch` to a bare Arrow IPC stream (no magic — this is an
/// internal, length-framed payload inside the aggregate-state blob).
/// Truncate an unscaled fixed-point value to a whole number (DEC-1). Only
/// reached when a view declares an integer column for a decimal aggregate,
/// which is the view's own stated contract — SQL's own `CAST(dec AS BIGINT)`
/// truncates toward zero, and so does this.
fn descale_to_i64(unscaled: i128, scale: i8) -> i64 {
    let v = match scale.cmp(&0) {
        std::cmp::Ordering::Greater => 10i128
            .checked_pow(scale as u32)
            .map(|d| unscaled / d)
            .unwrap_or(0),
        std::cmp::Ordering::Less => 10i128
            .checked_pow(scale.unsigned_abs() as u32)
            .and_then(|m| unscaled.checked_mul(m))
            .unwrap_or(unscaled),
        std::cmp::Ordering::Equal => unscaled,
    };
    v.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// Render an unscaled fixed-point value as `f64` (DEC-1). Lossy by nature —
/// only reached when the view itself declares a float column.
fn descale_to_f64(unscaled: i128, scale: i8) -> f64 {
    unscaled as f64 / 10f64.powi(scale as i32)
}

fn encode_batch_ipc(batch: &RecordBatch) -> DeltaResult<Vec<u8>> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &batch.schema())?;
        w.write(batch)?;
        w.finish()?;
    }
    Ok(buf)
}

/// Inverse of [`encode_batch_ipc`].
fn decode_batch_ipc(bytes: &[u8]) -> DeltaResult<RecordBatch> {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    reader
        .next()
        .ok_or_else(|| DeltaError::Operator("empty group-columns IPC stream".into()))?
        .map_err(DeltaError::from)
}

fn read_u8(bytes: &[u8], pos: &mut usize) -> DeltaResult<u8> {
    let b = *bytes
        .get(*pos)
        .ok_or_else(|| DeltaError::Operator("agg state truncated".into()))?;
    *pos += 1;
    Ok(b)
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> DeltaResult<u32> {
    let raw = bytes
        .get(*pos..*pos + 4)
        .ok_or_else(|| DeltaError::Operator("agg state truncated".into()))?;
    *pos += 4;
    Ok(u32::from_le_bytes(raw.try_into().unwrap_or([0; 4])))
}

fn read_i64(bytes: &[u8], pos: &mut usize) -> DeltaResult<i64> {
    let raw = bytes
        .get(*pos..*pos + 8)
        .ok_or_else(|| DeltaError::Operator("agg state truncated".into()))?;
    *pos += 8;
    Ok(i64::from_le_bytes(raw.try_into().unwrap_or([0; 8])))
}

fn read_f64(bytes: &[u8], pos: &mut usize) -> DeltaResult<f64> {
    let raw = bytes
        .get(*pos..*pos + 8)
        .ok_or_else(|| DeltaError::Operator("agg state truncated".into()))?;
    *pos += 8;
    Ok(f64::from_le_bytes(raw.try_into().unwrap_or([0; 8])))
}

fn read_i128(bytes: &[u8], pos: &mut usize) -> DeltaResult<i128> {
    let raw = bytes
        .get(*pos..*pos + 16)
        .ok_or_else(|| DeltaError::Operator("agg state truncated".into()))?;
    *pos += 16;
    Ok(i128::from_le_bytes(raw.try_into().unwrap_or([0; 16])))
}

impl AggState {
    fn write_bytes(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.sum.to_le_bytes());
        out.extend_from_slice(&self.sum_i64.to_le_bytes());
        out.extend_from_slice(&self.sum_i128.to_le_bytes());
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&self.rows.to_le_bytes());
        out.extend_from_slice(&self.avg_sum_i64.to_le_bytes());
        out.extend_from_slice(&self.avg_sum_i128.to_le_bytes());
        out.extend_from_slice(&self.avg_count_i64.to_le_bytes());
        out.push(self.avg_is_integer as u8);
        out.push(self.overflow as u8);
        out.extend_from_slice(&(self.min_max_set.len() as u32).to_le_bytes());
        for (k, w) in &self.min_max_set {
            // DEC-1: the key is tagged, so an exact i128 key round-trips as an
            // i128 instead of being flattened back through f64 on restore —
            // which would reintroduce the collision the typed key removed.
            match k {
                MinMaxKey::Int(v) => {
                    out.push(0u8);
                    out.extend_from_slice(&v.to_le_bytes());
                }
                MinMaxKey::Float(v) => {
                    out.push(1u8);
                    out.extend_from_slice(&v.0.to_le_bytes());
                }
            }
            out.extend_from_slice(&w.to_le_bytes());
        }
    }

    fn read_bytes(bytes: &[u8], pos: &mut usize, v4: bool) -> DeltaResult<Self> {
        let sum = read_f64(bytes, pos)?;
        let sum_i64 = read_i64(bytes, pos)?;
        let sum_i128 = read_i128(bytes, pos)?;
        let count = read_i64(bytes, pos)?;
        // COUNTNULL-1 (V4): the true row weight. A V3 blob predates the
        // field; `count` is the assumption its writer baked in.
        let rows = if v4 { read_i64(bytes, pos)? } else { count };
        let avg_sum_i64 = read_i64(bytes, pos)?;
        let avg_sum_i128 = read_i128(bytes, pos)?;
        let avg_count_i64 = read_i64(bytes, pos)?;
        let avg_is_integer = read_u8(bytes, pos)? == 1;
        let overflow = read_u8(bytes, pos)? == 1;
        let n_minmax = read_u32(bytes, pos)? as usize;
        let mut min_max_set: BTreeMap<MinMaxKey, i64> = BTreeMap::new();
        for _ in 0..n_minmax {
            let key = match read_u8(bytes, pos)? {
                0 => MinMaxKey::Int(read_i128(bytes, pos)?),
                1 => MinMaxKey::Float(OrdF64(read_f64(bytes, pos)?)),
                other => {
                    return Err(DeltaError::Operator(format!(
                        "agg state has unknown min/max key tag {other}"
                    )));
                }
            };
            let w = read_i64(bytes, pos)?;
            min_max_set.insert(key, w);
        }
        // CDIST-1: derived, so older AGGS3 blobs restore without a format
        // bump — the multiset is the ground truth and this is its summary.
        let distinct_pos = min_max_set.values().filter(|w| **w > 0).count() as i64;
        Ok(Self {
            sum,
            rows,
            sum_i64,
            sum_i128,
            count,
            avg_sum_i64,
            avg_sum_i128,
            avg_count_i64,
            avg_is_integer,
            overflow,
            min_max_set,
            distinct_pos,
        })
    }
}

fn compute_agg_values(
    states: &[AggState],
    aggregations: &[Aggregation],
    input_kinds: &[Option<NumKind>],
) -> Vec<Option<AggScalar>> {
    states
        .iter()
        .zip(aggregations.iter())
        .zip(input_kinds.iter())
        .map(|((state, agg), kind)| state.current_value(agg, *kind))
        .collect()
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Read the single positive (weight `+1`) row's `col` value as f64 from an
    /// aggregate output whose touched group ended non-empty. AUD-7 tests assert
    /// on the emitted batch instead of reaching into the (now opaque) row keys.
    fn positive_f64(out: &DeltaBatch, col: &str) -> Option<f64> {
        let pos = out.filter_positive().ok()?;
        if pos.num_rows() == 0 {
            return None;
        }
        let arr = pos.column_by_name(col)?;
        if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
            Some(a.value(0))
        } else {
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(0) as f64)
        }
    }

    fn order_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }

    fn order_batch(cids: &[&str], amounts: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            order_schema(),
            vec![
                Arc::new(StringArray::from(cids.to_vec())) as Arc<dyn Array>,
                Arc::new(Float64Array::from(amounts.to_vec())) as Arc<dyn Array>,
            ],
        )
        .unwrap()
    }

    #[test]
    fn sum_increases_on_insert() {
        let mut op = IncrementalAggOp::new(
            &order_schema(),
            vec!["customer_id".into()],
            vec![Aggregation::Sum {
                input_col: "amount".into(),
                output_col: "total".into(),
            }],
        )
        .unwrap();

        let delta = DeltaBatch::from_inserts(order_batch(&["c1"], &[100.0])).unwrap();
        let out = op.apply(delta).unwrap();
        // Should have one insertion of sum=100
        assert!(!out.is_empty());
        let positive = out.filter_positive().unwrap();
        assert_eq!(positive.num_rows(), 1);
    }

    #[test]
    fn sum_retracts_on_delete() {
        let mut op = IncrementalAggOp::new(
            &order_schema(),
            vec!["customer_id".into()],
            vec![Aggregation::Sum {
                input_col: "amount".into(),
                output_col: "total".into(),
            }],
        )
        .unwrap();

        // First insert
        let d1 = DeltaBatch::from_inserts(order_batch(&["c1"], &[100.0])).unwrap();
        op.apply(d1).unwrap();

        // Then delete → should emit retraction of sum=100 and insertion of sum=0 (empty group GC'd)
        let d2 = DeltaBatch::from_deletes(order_batch(&["c1"], &[100.0])).unwrap();
        let out = op.apply(d2).unwrap();
        assert!(!out.is_empty());
        // Retraction should appear
        let retractions = out.filter_negative().unwrap();
        assert_eq!(retractions.num_rows(), 1);
    }

    #[test]
    fn count_increments_correctly() {
        let mut op = IncrementalAggOp::new(
            &order_schema(),
            vec!["customer_id".into()],
            vec![Aggregation::Count {
                output_col: "cnt".into(),
                input_col: None,
            }],
        )
        .unwrap();

        let d1 = DeltaBatch::from_inserts(order_batch(&["c1", "c1"], &[10.0, 20.0])).unwrap();
        let out = op.apply(d1).unwrap();
        // Count for c1 should be 2 (single group → one positive output row).
        assert_eq!(positive_f64(&out, "cnt"), Some(2.0));
    }

    #[test]
    fn min_float_retract_current_min_substitutes_next() {
        // Insert 3.5, 1.2, 2.7 for key "g". Min = 1.2.
        // Retract 1.2. Min must become 2.7 (not 0.0, which the old i64 parse would give).
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Float64, false),
        ]));
        let mut op = IncrementalAggOp::new(
            &schema,
            vec!["k".into()],
            vec![Aggregation::Min {
                input_col: "v".into(),
                output_col: "min_v".into(),
            }],
        )
        .unwrap();

        let insert = DeltaBatch::from_inserts(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["g", "g", "g"])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![3.5, 1.2, 2.7])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let out = op.apply(insert).unwrap();

        // Current min for "g" should be 1.2 (the positive output row).
        let min_val = positive_f64(&out, "min_v");
        assert!(
            (min_val.unwrap_or(f64::NAN) - 1.2).abs() < 1e-9,
            "min before retraction should be 1.2, got {min_val:?}"
        );

        // Retract 1.2
        let retract = DeltaBatch::from_deletes(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["g"])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![1.2])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let out = op.apply(retract).unwrap();

        // Min should now be 2.7, not 0.0 (the new positive output row).
        let min_after = positive_f64(&out, "min_v");
        assert!(
            (min_after.unwrap_or(f64::NAN) - 2.7).abs() < 1e-9,
            "min after retracting 1.2 should be 2.7, got {min_after:?}"
        );
    }

    #[test]
    fn max_float_retract_current_max_substitutes_next() {
        // Insert 3.5, 1.2, 2.7 for key "g". Max = 3.5.
        // Retract 3.5. Max must become 2.7.
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Float64, false),
        ]));
        let mut op = IncrementalAggOp::new(
            &schema,
            vec!["k".into()],
            vec![Aggregation::Max {
                input_col: "v".into(),
                output_col: "max_v".into(),
            }],
        )
        .unwrap();

        let insert = DeltaBatch::from_inserts(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["g", "g", "g"])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![3.5, 1.2, 2.7])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        op.apply(insert).unwrap();

        // Retract 3.5
        let retract = DeltaBatch::from_deletes(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["g"])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![3.5])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let out = op.apply(retract).unwrap();

        let max_after = positive_f64(&out, "max_v");
        assert!(
            (max_after.unwrap_or(f64::NAN) - 2.7).abs() < 1e-9,
            "max after retracting 3.5 should be 2.7, got {max_after:?}"
        );
    }

    /// AUD-3: AVG over a **float** column with mixed integer-looking and
    /// fractional values must not latch the i64 path. `[10.0, 10.5]` averages to
    /// 10.25; the old string-sniffing code sent `10.0` (rendered `"10"`) to the
    /// i64 accumulator and `10.5` to the f64 one, then divided one accumulator by
    /// the combined count — a wrong result.
    #[test]
    fn avg_over_float_column_with_integral_values_is_exact() {
        let mut op = IncrementalAggOp::new(
            &order_schema(),
            vec!["customer_id".into()],
            vec![Aggregation::Avg {
                input_col: "amount".into(),
                output_col: "avg_amt".into(),
            }],
        )
        .unwrap();
        let out = op
            .apply(DeltaBatch::from_inserts(order_batch(&["c1", "c1"], &[10.0, 10.5])).unwrap())
            .unwrap();
        let avg = positive_f64(&out, "avg_amt");
        assert!(
            (avg.unwrap_or(f64::NAN) - 10.25).abs() < 1e-9,
            "avg should be 10.25, got {avg:?}"
        );
    }

    /// AUD-3: SUM over an integer column emits an Int64 output column (SQL
    /// semantics: `SUM(Int64) → Int64`), not a lossy Float64.
    #[test]
    fn sum_over_integer_column_emits_int64() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let mut op = IncrementalAggOp::new(
            &schema,
            vec!["k".into()],
            vec![Aggregation::Sum {
                input_col: "v".into(),
                output_col: "total".into(),
            }],
        )
        .unwrap();
        assert_eq!(
            op.output_schema().field(1).data_type(),
            &DataType::Int64,
            "SUM over Int64 must be typed Int64"
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])) as Arc<dyn Array>,
                Arc::new(Int64Array::from(vec![3_000_000_000_i64, 4_000_000_000_i64]))
                    as Arc<dyn Array>,
            ],
        )
        .unwrap();
        let out = op.apply(DeltaBatch::from_inserts(batch).unwrap()).unwrap();
        let data = out.filter_positive().unwrap();
        let total = data
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("total column must be Int64")
            .value(0);
        assert_eq!(total, 7_000_000_000_i64);
    }

    /// AUD-3: an aggregate over a non-numeric column errors from `new`, so the
    /// planner falls back to DiffBased instead of producing silent zeros.
    #[test]
    fn sum_over_non_numeric_column_errors() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let err = IncrementalAggOp::new(
            &schema,
            vec!["k".into()],
            vec![Aggregation::Sum {
                input_col: "label".into(),
                output_col: "total".into(),
            }],
        );
        assert!(
            err.is_err(),
            "SUM over Utf8 must error (→ DiffBased fallback)"
        );
    }

    /// `state_bytes` → `restore_state_bytes` transfers the accumulator
    /// losslessly, *including* the multiset multiplicity of genuinely-identical
    /// rows — the exact property the materialized source snapshot loses. A fresh
    /// op restored from the bytes then emits the same retract+insert on the next
    /// delta as the original would (G6/F4 lossless restore).
    #[test]
    fn state_bytes_round_trip_preserves_multiset() {
        let group = vec!["customer_id".to_string()];
        let sum = vec![Aggregation::Sum {
            input_col: "amount".into(),
            output_col: "total".into(),
        }];
        let mut op = IncrementalAggOp::new(&order_schema(), group.clone(), sum.clone()).unwrap();
        // Two *identical* rows (c1, 5.0) — a set-based snapshot would collapse
        // these; the accumulator must remember both (sum = 10, count = 2).
        op.apply(DeltaBatch::from_inserts(order_batch(&["c1", "c1"], &[5.0, 5.0])).unwrap())
            .unwrap();

        // Serialize, then restore into a brand-new empty operator.
        let bytes = op.state_bytes();
        let mut restored = IncrementalAggOp::new(&order_schema(), group, sum).unwrap();
        restored.restore_state_bytes(&bytes).unwrap();

        // Retract ONE of the two identical rows on the restored op. If the
        // multiset was preserved, c1 remains present with sum=5 → the op emits
        // retract(total=10) + insert(total=5). If multiplicity had been lost
        // (count=1), the group would vanish → retract(total=5) + nothing.
        let out = restored
            .apply(DeltaBatch::from_deletes(order_batch(&["c1"], &[5.0])).unwrap())
            .unwrap();
        let data = out.data_batch();
        let weights = out.weights();
        let totals = data
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut retract_10 = false;
        let mut insert_5 = false;
        for i in 0..data.num_rows() {
            let w = weights.value(i);
            let t = totals.value(i);
            if w < 0 && (t - 10.0).abs() < 1e-9 {
                retract_10 = true;
            }
            if w > 0 && (t - 5.0).abs() < 1e-9 {
                insert_5 = true;
            }
        }
        assert!(
            retract_10 && insert_5,
            "restored op must retract total=10 and insert total=5 \
             (multiset multiplicity preserved); got {out:?}"
        );
    }
}
