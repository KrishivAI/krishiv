#![forbid(unsafe_code)]

//! `DeltaBatch` — the core incremental data type.
//!
//! A `DeltaBatch` is an Arrow `RecordBatch` where the last column is always
//! `_weight: Int64`. Positive weight = insertion (+1), negative = retraction (-1).
//! Weights may be any i64 for multisets, but in practice are ±1 or ±N.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int64Array, RecordBatch};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::error::{DeltaError, DeltaResult};

/// Name of the synthetic weight column appended to every `DeltaBatch`.
pub const WEIGHT_COLUMN: &str = "_weight";

/// Magic prefix written by the versioned serializer (format version 1).
///
/// Legacy bytes (produced before versioning) start with Arrow IPC continuation
/// markers (`0xFF 0xFF 0xFF 0xFF` or a small positive `i32` size), never with
/// `b"DLT1"`. The deserializer uses this to auto-detect the format.
const DELTA_BATCH_MAGIC: &[u8; 4] = b"DLT1";

/// The integer weight type: positive = insert, negative = retract.
pub type Weight = i64;

/// A weighted multiset of row changes over an Arrow schema.
///
/// Invariant: the last column of `inner` is always named `_weight` with type `Int64`.
#[derive(Debug, Clone)]
pub struct DeltaBatch {
    /// Full RecordBatch including the `_weight` column as the last column.
    inner: RecordBatch,
    /// Schema of the *data* columns only (without `_weight`).
    data_schema: SchemaRef,
}

impl DeltaBatch {
    // ── Constructors ──────────────────────────────────────────────────────────

    /// Build a `DeltaBatch` where every row has weight `+1` (pure insertions).
    pub fn from_inserts(batch: RecordBatch) -> DeltaResult<Self> {
        Self::with_uniform_weight(batch, 1)
    }

    /// Build a `DeltaBatch` where every row has weight `-1` (pure deletions).
    pub fn from_deletes(batch: RecordBatch) -> DeltaResult<Self> {
        Self::with_uniform_weight(batch, -1)
    }

    /// Build a `DeltaBatch` encoding an update: `before` rows get weight `-1`,
    /// `after` rows get weight `+1`. Schemas must match.
    pub fn from_update(before: &RecordBatch, after: &RecordBatch) -> DeltaResult<Self> {
        if before.schema() != after.schema() {
            return Err(DeltaError::SchemaMismatch(
                "before/after schemas differ in DeltaBatch::from_update".into(),
            ));
        }
        let data_schema = before.schema();
        let retractions = Self::with_uniform_weight(before.clone(), -1)?;
        let insertions = Self::with_uniform_weight(after.clone(), 1)?;
        Self::concat(&[retractions, insertions]).map(|mut cb| {
            cb.data_schema = data_schema;
            cb
        })
    }

    /// Build a `DeltaBatch` from a CDC change event by dispatching on the
    /// before/after pair:
    ///
    /// - INSERT `(None, Some(after))`   → all `+1` (via [`from_inserts`](Self::from_inserts))
    /// - DELETE `(Some(before), None)`  → all `-1` (via [`from_deletes`](Self::from_deletes))
    /// - UPDATE `(Some(before), Some(after))` → retract+insert (via [`from_update`](Self::from_update))
    /// - no-op  `(None, None)`          → `Ok(None)` (nothing to feed)
    ///
    /// Returning `Option` means callers skip `feed` entirely on the no-op case
    /// without needing a schema to build an empty batch.
    pub fn from_cdc(
        before: Option<RecordBatch>,
        after: Option<RecordBatch>,
    ) -> DeltaResult<Option<Self>> {
        Ok(match (before, after) {
            (None, Some(after)) => Some(Self::from_inserts(after)?),
            (Some(before), None) => Some(Self::from_deletes(before)?),
            (Some(before), Some(after)) => Some(Self::from_update(&before, &after)?),
            (None, None) => None,
        })
    }

    /// Construct directly from a batch that already has a `_weight` column.
    pub fn from_weighted(inner: RecordBatch) -> DeltaResult<Self> {
        let ncols = inner.num_columns();
        if ncols == 0 {
            return Err(DeltaError::SchemaMismatch(
                "DeltaBatch requires at least one column (_weight)".into(),
            ));
        }
        let schema = inner.schema();
        let weight_field = schema.field(ncols - 1);
        if weight_field.name() != WEIGHT_COLUMN {
            return Err(DeltaError::SchemaMismatch(format!(
                "last column must be '{WEIGHT_COLUMN}', got '{}'",
                weight_field.name()
            )));
        }
        if *weight_field.data_type() != DataType::Int64 {
            return Err(DeltaError::SchemaMismatch(
                "_weight column must be Int64".into(),
            ));
        }
        let data_schema = Arc::new(Schema::new(
            inner
                .schema()
                .fields()
                .iter()
                .take(ncols - 1)
                .cloned()
                .collect::<Vec<_>>(),
        ));
        Ok(Self { inner, data_schema })
    }

    /// Create an empty `DeltaBatch` with the given data schema.
    pub fn empty(data_schema: SchemaRef) -> DeltaResult<Self> {
        let mut fields: Vec<_> = data_schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
        let full_schema = Arc::new(Schema::new(fields));
        let mut columns: Vec<Arc<dyn Array>> = data_schema
            .fields()
            .iter()
            .map(|f| arrow::array::new_empty_array(f.data_type()))
            .collect();
        columns.push(Arc::new(Int64Array::from(Vec::<i64>::new())));
        let inner = RecordBatch::try_new(full_schema, columns)?;
        Ok(Self { inner, data_schema })
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    /// Number of rows in this batch.
    pub fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// Returns `true` if all rows have weight >= 0 (no retractions).
    /// Insert-only workloads use this for an O(delta) fast path in `apply_delta`.
    pub fn is_insert_only(&self) -> bool {
        self.weights().iter().all(|w| w.unwrap_or(0) >= 0)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.num_rows() == 0
    }

    /// Schema of the data columns (excluding `_weight`).
    pub fn data_schema(&self) -> &SchemaRef {
        &self.data_schema
    }

    /// The weight column as an `Int64Array`.
    pub fn weights(&self) -> &Int64Array {
        static EMPTY: std::sync::OnceLock<Int64Array> = std::sync::OnceLock::new();
        self.inner
            .column(self.inner.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| EMPTY.get_or_init(|| Int64Array::from(Vec::<i64>::new())))
    }

    /// The full inner `RecordBatch` including the `_weight` column.
    pub fn inner(&self) -> &RecordBatch {
        &self.inner
    }

    /// A view of the data columns only (no `_weight` column).
    pub fn data_batch(&self) -> RecordBatch {
        let ncols = self.inner.num_columns() - 1;
        RecordBatch::try_new(
            self.data_schema.clone(),
            self.inner
                .columns()
                .iter()
                .take(ncols)
                .cloned()
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| RecordBatch::new_empty(self.data_schema.clone()))
    }

    // ── Filtering by weight sign ───────────────────────────────────────────────

    /// Returns a plain `RecordBatch` of rows with weight > 0 (insertions).
    /// The `_weight` column is stripped.
    pub fn filter_positive(&self) -> DeltaResult<RecordBatch> {
        let weights = self.weights();
        let mask: BooleanArray = weights.iter().map(|w| Some(w.unwrap_or(0) > 0)).collect();
        let data = self.data_batch();
        Ok(filter_record_batch(&data, &mask)?)
    }

    /// Returns a plain `RecordBatch` of rows with weight < 0 (retractions).
    /// The `_weight` column is stripped.
    pub fn filter_negative(&self) -> DeltaResult<RecordBatch> {
        let weights = self.weights();
        let mask: BooleanArray = weights.iter().map(|w| Some(w.unwrap_or(0) < 0)).collect();
        let data = self.data_batch();
        Ok(filter_record_batch(&data, &mask)?)
    }

    // ── Z-set algebra ─────────────────────────────────────────────────────────

    /// Negate all weights (insert ↔ retract).
    pub fn negate(&self) -> DeltaResult<Self> {
        let weights = self.weights();
        let negated: Int64Array = weights.iter().map(|w| w.map(|v| -v)).collect();
        let mut cols: Vec<Arc<dyn Array>> = self
            .inner
            .columns()
            .iter()
            .take(self.inner.num_columns() - 1)
            .cloned()
            .collect::<Vec<_>>();
        cols.push(Arc::new(negated));
        let inner = RecordBatch::try_new(self.inner.schema(), cols)?;
        Ok(Self {
            inner,
            data_schema: self.data_schema.clone(),
        })
    }

    /// Concatenate multiple `DeltaBatch`es with identical data schemas.
    /// Does NOT consolidate — use `consolidate()` afterwards if needed.
    pub fn concat(batches: &[DeltaBatch]) -> DeltaResult<Self> {
        let first = batches
            .first()
            .ok_or_else(|| DeltaError::Operator("cannot concat empty slice".into()))?;
        let schema = first.inner.schema();
        let data_schema = first.data_schema.clone();
        let inners: Vec<&RecordBatch> = batches.iter().map(|b| &b.inner).collect();
        let inner = concat_batches(&schema, inners)?;
        Ok(Self { inner, data_schema })
    }

    /// Clamp every weight to its sign: `+1`, `-1`, or `0`.
    ///
    /// Call this before feeding batches with arbitrary integer weights into
    /// operators that assume unit weights (e.g. the dedup filter). Zero-weight
    /// rows should be removed afterwards with [`drop_zeros`](Self::drop_zeros).
    pub fn normalize_weights(&self) -> DeltaResult<Self> {
        let weights = self.weights();
        let clamped: Int64Array = weights.iter().map(|w| w.map(|v| v.signum())).collect();
        let mut cols: Vec<Arc<dyn Array>> = self
            .inner
            .columns()
            .iter()
            .take(self.inner.num_columns() - 1)
            .cloned()
            .collect::<Vec<_>>();
        cols.push(Arc::new(clamped));
        let inner = RecordBatch::try_new(self.inner.schema(), cols)?;
        Ok(Self {
            inner,
            data_schema: self.data_schema.clone(),
        })
    }

    /// Panic in debug builds if the internal invariant is violated.
    ///
    /// Invariant: last column is `_weight: Int64`.
    /// This is enforced at construction; call this at the entry to hot paths
    /// during development (`#[cfg(debug_assertions)]` guards the call site).
    #[cfg(debug_assertions)]
    pub fn validate(&self) {
        let ncols = self.inner.num_columns();
        let schema = self.inner.schema();
        let field = schema.field(ncols - 1);
        assert_eq!(
            field.name(),
            WEIGHT_COLUMN,
            "DeltaBatch invariant violated: last column is '{}'",
            field.name()
        );
        assert_eq!(
            *field.data_type(),
            arrow::datatypes::DataType::Int64,
            "DeltaBatch invariant violated: _weight must be Int64"
        );
    }

    /// Remove rows with weight == 0 from this batch.
    pub fn drop_zeros(&self) -> DeltaResult<Self> {
        let weights = self.weights();
        let mask: BooleanArray = weights.iter().map(|w| Some(w.unwrap_or(0) != 0)).collect();
        let inner = filter_record_batch(&self.inner, &mask)?;
        Ok(Self {
            inner,
            data_schema: self.data_schema.clone(),
        })
    }

    /// Apply a boolean mask to this batch (keeps rows where mask is true).
    pub fn filter_mask(&self, mask: &BooleanArray) -> DeltaResult<Self> {
        let inner = filter_record_batch(&self.inner, mask)?;
        Ok(Self {
            inner,
            data_schema: self.data_schema.clone(),
        })
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    fn with_uniform_weight(batch: RecordBatch, w: Weight) -> DeltaResult<Self> {
        let data_schema = batch.schema();
        let nrows = batch.num_rows();
        let weights: Int64Array = std::iter::repeat_n(Some(w), nrows).collect();

        let mut fields: Vec<_> = data_schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
        let full_schema = Arc::new(Schema::new(fields));

        let mut cols: Vec<Arc<dyn Array>> = batch.columns().to_vec();
        cols.push(Arc::new(weights));

        let inner = RecordBatch::try_new(full_schema, cols)?;
        Ok(Self { inner, data_schema })
    }
}

// ── Display ───────────────────────────────────────────────────────────────────

impl std::fmt::Display for DeltaBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DeltaBatch({} rows, schema: {})",
            self.num_rows(),
            self.data_schema
                .fields()
                .iter()
                .map(|fi| fi.name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

// ── Conversions ────────────────────────────────────────────────────────────────

impl TryFrom<RecordBatch> for DeltaBatch {
    type Error = DeltaError;

    /// Wrap a plain `RecordBatch` as a `DeltaBatch` with all rows as insertions
    /// (weight +1). Fails if the schema cannot be cloned into the weighted form.
    fn try_from(batch: RecordBatch) -> DeltaResult<Self> {
        Self::from_inserts(batch)
    }
}

impl std::convert::TryFrom<Vec<RecordBatch>> for DeltaBatch {
    type Error = DeltaError;

    /// Concatenate multiple batches and wrap as a single `DeltaBatch`.
    fn try_from(batches: Vec<RecordBatch>) -> DeltaResult<Self> {
        let combined = if batches.len() == 1 {
            batches
                .into_iter()
                .next()
                .ok_or_else(|| DeltaError::Operator("empty batch list".into()))?
        } else {
            let schema = batches
                .first()
                .map(|b| b.schema())
                .ok_or_else(|| DeltaError::Operator("empty batch list".into()))?;
            arrow::compute::concat_batches(&schema, &batches)
                .map_err(|e| DeltaError::Operator(format!("concat failed: {e}")))?
        };
        Self::from_inserts(combined)
    }
}

// ── Consolidation (sort by key columns, sum weights, drop zeros) ──────────────
//
// This is the core Z-set normalization step. It is implemented in
// `operators/consolidate.rs` because it requires knowing which columns are
// key columns. The `DeltaBatch::drop_zeros()` helper removes only the trivial
// zero-weight rows.

// ── Arrow IPC serialization (for Trace persistence) ───────────────────────────

/// Serialize a `DeltaBatch` to versioned bytes.
///
/// Format: `DELTA_BATCH_MAGIC (4 bytes) || Arrow IPC stream bytes`.
/// The magic prefix lets `deserialize_delta_batch` auto-detect legacy vs.
/// versioned payloads so existing checkpoint files remain readable.
pub fn serialize_delta_batch(batch: &DeltaBatch) -> DeltaResult<Vec<u8>> {
    use arrow::ipc::writer::StreamWriter;
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &batch.inner.schema())?;
        writer.write(&batch.inner)?;
        writer.finish()?;
    }
    let mut buf = Vec::with_capacity(DELTA_BATCH_MAGIC.len() + ipc.len());
    buf.extend_from_slice(DELTA_BATCH_MAGIC);
    buf.extend_from_slice(&ipc);
    Ok(buf)
}

/// Deserialize a `DeltaBatch` from bytes produced by `serialize_delta_batch`.
///
/// Handles both versioned payloads (prefixed with `DELTA_BATCH_MAGIC`) and
/// legacy Arrow IPC streams written before versioning was introduced.
pub fn deserialize_delta_batch(bytes: &[u8]) -> DeltaResult<DeltaBatch> {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    // Strip magic prefix if present (versioned format); otherwise treat as legacy.
    let ipc_bytes = if bytes.starts_with(DELTA_BATCH_MAGIC) {
        bytes.get(DELTA_BATCH_MAGIC.len()..).unwrap_or(bytes)
    } else {
        bytes
    };
    let cursor = Cursor::new(ipc_bytes);
    let mut reader = StreamReader::try_new(cursor, None)?;
    let batch = reader
        .next()
        .ok_or_else(|| DeltaError::Serialization("empty IPC stream".into()))?
        .map_err(|e| DeltaError::Serialization(e.to_string()))?;
    DeltaBatch::from_weighted(batch)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn small_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    #[test]
    fn from_inserts_all_positive() {
        let cb = DeltaBatch::from_inserts(small_batch(&[1, 2, 3])).unwrap();
        assert_eq!(cb.num_rows(), 3);
        let w = cb.weights();
        assert!(w.iter().all(|v| v == Some(1)));
    }

    #[test]
    fn from_deletes_all_negative() {
        let cb = DeltaBatch::from_deletes(small_batch(&[1, 2])).unwrap();
        let w = cb.weights();
        assert!(w.iter().all(|v| v == Some(-1)));
    }

    #[test]
    fn negate_flips_weights() {
        let cb = DeltaBatch::from_inserts(small_batch(&[1])).unwrap();
        let neg = cb.negate().unwrap();
        assert_eq!(neg.weights().value(0), -1);
    }

    #[test]
    fn filter_positive_strips_weight_col() {
        let cb = DeltaBatch::from_inserts(small_batch(&[1, 2])).unwrap();
        let pos = cb.filter_positive().unwrap();
        assert_eq!(pos.num_rows(), 2);
        // _weight column should not be present
        assert!(pos.schema().field_with_name(WEIGHT_COLUMN).is_err());
    }

    #[test]
    fn filter_negative_on_inserts_is_empty() {
        let cb = DeltaBatch::from_inserts(small_batch(&[1, 2])).unwrap();
        let neg = cb.filter_negative().unwrap();
        assert_eq!(neg.num_rows(), 0);
    }

    #[test]
    fn from_update_has_correct_row_count() {
        let before = small_batch(&[1]);
        let after = small_batch(&[2]);
        let cb = DeltaBatch::from_update(&before, &after).unwrap();
        assert_eq!(cb.num_rows(), 2);
        let w = cb.weights();
        assert_eq!(w.value(0), -1); // before retracted
        assert_eq!(w.value(1), 1); // after inserted
    }

    #[test]
    fn empty_batch_has_zero_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let cb = DeltaBatch::empty(schema).unwrap();
        assert!(cb.is_empty());
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let cb = DeltaBatch::from_inserts(small_batch(&[10, 20])).unwrap();
        let bytes = serialize_delta_batch(&cb).unwrap();
        let restored = deserialize_delta_batch(&bytes).unwrap();
        assert_eq!(restored.num_rows(), 2);
        assert_eq!(restored.weights().value(0), 1);
    }

    #[test]
    fn serialize_has_magic_prefix() {
        let cb = DeltaBatch::from_inserts(small_batch(&[1])).unwrap();
        let bytes = serialize_delta_batch(&cb).unwrap();
        assert!(
            bytes.starts_with(b"DLT1"),
            "versioned bytes must start with DLT1 magic"
        );
    }

    #[test]
    fn deserialize_legacy_ipc_without_magic() {
        use arrow::ipc::writer::StreamWriter;
        // Produce a raw Arrow IPC stream (no magic prefix — legacy format).
        let cb = DeltaBatch::from_inserts(small_batch(&[5])).unwrap();
        let mut legacy = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut legacy, &cb.inner().schema()).unwrap();
            w.write(cb.inner()).unwrap();
            w.finish().unwrap();
        }
        let restored = deserialize_delta_batch(&legacy).unwrap();
        assert_eq!(restored.num_rows(), 1);
        assert_eq!(restored.weights().value(0), 1);
    }

    #[test]
    fn from_cdc_dispatches_all_arms() {
        // INSERT: (None, Some)
        let ins = DeltaBatch::from_cdc(None, Some(small_batch(&[1, 2])))
            .unwrap()
            .unwrap();
        assert_eq!(ins.num_rows(), 2);
        assert!(ins.weights().iter().all(|w| w == Some(1)));

        // DELETE: (Some, None)
        let del = DeltaBatch::from_cdc(Some(small_batch(&[3])), None)
            .unwrap()
            .unwrap();
        assert_eq!(del.num_rows(), 1);
        assert_eq!(del.weights().value(0), -1);

        // UPDATE: (Some, Some) → retract before, insert after
        let upd = DeltaBatch::from_cdc(Some(small_batch(&[4])), Some(small_batch(&[5])))
            .unwrap()
            .unwrap();
        assert_eq!(upd.num_rows(), 2);
        assert_eq!(upd.weights().value(0), -1);
        assert_eq!(upd.weights().value(1), 1);

        // NO-OP: (None, None) → None
        assert!(DeltaBatch::from_cdc(None, None).unwrap().is_none());
    }

    #[test]
    fn normalize_weights_clamps_to_sign() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let inner = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![5i64, -3, 0, 1])),
            ],
        )
        .unwrap();
        let cb = DeltaBatch::from_weighted(inner).unwrap();
        let normed = cb.normalize_weights().unwrap();
        let w: Vec<i64> = normed.weights().iter().map(|v| v.unwrap()).collect();
        assert_eq!(w, vec![1, -1, 0, 1]);
    }

    #[test]
    fn drop_zeros_removes_zero_weight_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let inner = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![1i64, 0, -1])),
            ],
        )
        .unwrap();
        let cb = DeltaBatch::from_weighted(inner).unwrap();
        let dropped = cb.drop_zeros().unwrap();
        assert_eq!(dropped.num_rows(), 2);
    }
}
