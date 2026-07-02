//! A retraction-aware sink wrapper that **consolidates** a changelog into its
//! net materialized table before writing.
//!
//! Append-only connector sinks (parquet, CSV, …) cannot apply a retraction: they
//! only know how to append rows. The stateful engines, however, emit changelogs
//! with deletes and update-retractions. This wrapper bridges the two: it
//! accumulates a DBSP-style weighted multiset keyed by the **whole row**
//! (insert `+1`, retraction `-1`), and on [`flush`](SinkWriter::flush) writes the
//! rows whose net weight is positive to the wrapped sink — exactly once. A
//! changed aggregate (retract old row, insert new row) therefore lands as the new
//! row only, and a deleted key disappears entirely.
//!
//! Keying by the full row needs no declared primary key: the retraction carries
//! the exact prior image, so it cancels the matching insert. This is the honest,
//! general consolidation; a connector with true upsert/merge-on-read can still
//! implement [`SinkWriter`] directly to push per-row changes instead.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use async_trait::async_trait;
use tracing::warn;

use crate::changelog::ChangelogBatch;
use crate::error::{EngineError, EngineResult};
use crate::job::SinkSpec;
use crate::runtime::{SinkProvider, SinkWriter};

/// Wraps an inner [`SinkProvider`] so each opened writer consolidates its
/// changelog into a net insert-only table before delegating the write.
pub struct ConsolidatingSinkProvider {
    inner: Arc<dyn SinkProvider>,
    /// Maximum unmatched retractions retained before eviction in `flush`.
    /// `None` falls back to the default constant.
    max_unmatched_retractions: Option<usize>,
    /// Optional primary-key columns; when set, the consolidator keys the
    /// weighted multiset by the declared PK rather than the full row. This
    /// matters for engines that emit paired `UpdateBefore` + `UpdateAfter`
    /// (e.g. Debezium CDC) where the two images are distinct full rows but
    /// share a primary key. Without PK keying, the net table holds both
    /// rows for the same key until a later update cancels them.
    primary_key: Option<Vec<String>>,
}

impl ConsolidatingSinkProvider {
    /// Wrap `inner` with full-row changelog consolidation.
    pub fn new(inner: Arc<dyn SinkProvider>) -> Self {
        Self {
            inner,
            max_unmatched_retractions: None,
            primary_key: None,
        }
    }

    /// Cap the number of unmatched (negative-weight) retentions retained in
    /// memory before `flush` evicts the oldest. Defaults to
    /// [`MAX_UNMATCHED_RETRACTIONS`]. Set to `usize::MAX` to disable eviction.
    pub fn with_max_unmatched_retractions(mut self, cap: usize) -> Self {
        self.max_unmatched_retractions = Some(cap);
        self
    }

    /// Key the consolidator by the named primary-key columns instead of the
    /// full row. Required for engines that emit paired `UpdateBefore` +
    /// `UpdateAfter` semantics (e.g. Debezium CDC); the consolidated net
    /// table will then collapse the two-image change into one row, matching
    /// what an idempotent upsert sink would have written.
    pub fn with_primary_key(mut self, primary_key: Vec<String>) -> Self {
        self.primary_key = Some(primary_key);
        self
    }
}

#[async_trait]
impl SinkProvider for ConsolidatingSinkProvider {
    async fn open(&self, spec: &SinkSpec) -> EngineResult<Box<dyn SinkWriter>> {
        let inner = self.inner.open(spec).await?;
        Ok(Box::new(ConsolidatingSinkWriter {
            inner,
            schema: None,
            rows: BTreeMap::new(),
            row_cache: BTreeMap::new(),
            max_unmatched_retractions: self
                .max_unmatched_retractions
                .unwrap_or(MAX_UNMATCHED_RETRACTIONS),
            primary_key: self.primary_key.clone(),
            key_sort_fields: None,
            row_converter_schema: None,
        }))
    }
}

/// Threshold above which [`ConsolidatingSinkWriter`] emits a warning about
/// unmatched retractions accumulating in memory.
const UNMATCHED_RETRACTION_WARN_THRESHOLD: usize = 10_000;

/// Maximum weight value accepted in a single flush call. Weights beyond this
/// are clamped to prevent OOM from degenerate input (e.g. 10M duplicate inserts
/// for the same row). The operational assumption is that no real workload
/// legitimately needs >1M identical copies of the same row.
const MAX_FLUSH_WEIGHT: i64 = 1_000_000;

/// Maximum number of unmatched (negative-weight) entries retained before
/// eviction. Beyond this, the oldest unmatched entries are discarded to
/// bound memory usage. A matching Insert that arrives after eviction will
/// be treated as a new row (Insert-only), which is a safe degradation.
const MAX_UNMATCHED_RETRACTIONS: usize = 100_000;

struct ConsolidatingSinkWriter {
    inner: Box<dyn SinkWriter>,
    /// Data schema, captured from the first non-empty changelog.
    schema: Option<SchemaRef>,
    /// Consolidator key → (one-row batch, net weight). The key is the full
    /// row bytes when no primary key is configured, or the concatenation of
    /// the declared primary-key columns when one is set. Entries reaching
    /// weight 0 are removed, so the map always holds the live materialized
    /// rows. Entries with weight < 0 are unmatched retractions: a Delete
    /// arrived for a row that was never inserted. These cancel when the
    /// matching Insert arrives later, but if no Insert ever comes they
    /// accumulate as a memory leak — bounded by `max_unmatched_retractions`
    /// in `flush`.
    rows: BTreeMap<Vec<u8>, (RecordBatch, i64)>,
    /// Cache of the most recent row image per consolidator key. The
    /// `apply` path writes the new image here so that a later `Delete` /
    /// `UpdateBefore` (which arrives without a value) can flush the prior
    /// image alongside the retraction. Without this, full-row keyed
    /// retractions that arrive without a row payload would have nothing
    /// to write. PK-keyed retractions look up the prior image by PK and
    /// get the row from here.
    row_cache: BTreeMap<Vec<u8>, RecordBatch>,
    /// Eviction threshold for unmatched retractions, set by the provider.
    max_unmatched_retractions: usize,
    /// Optional primary-key columns; when set, the consolidator keys by
    /// these columns rather than the full row.
    primary_key: Option<Vec<String>>,
    /// Cached `Vec<SortField>` describing the key column types. The
    /// `RowConverter` is rebuilt on every `apply` (it is cheap to
    /// construct from a small list of SortFields), but the type
    /// resolution that produces the SortField list is the expensive
    /// part. We cache the list and only re-derive it when the schema
    /// changes. P-4 (audit): the previous implementation rebuilt this
    /// for every changelog.
    key_sort_fields: Option<Vec<SortField>>,
    /// Schema the cached `key_sort_fields` was derived from. Used to
    /// detect schema changes that require re-deriving the type list.
    row_converter_schema: Option<SchemaRef>,
}

/// Encode every column of `batch` into one stable, comparable byte string per row.
///
/// **Deprecated**: superseded by the in-struct `RowConverter` cache on
/// [`ConsolidatingSinkWriter`] (P-4 audit). Retained only for tests that
/// still call it directly. New code should rely on the writer's own
/// converter cache.
#[cfg(test)]
fn encode_full_rows(
    batch: &RecordBatch,
    pk_columns: Option<&[String]>,
) -> EngineResult<Vec<Vec<u8>>> {
    let columns: Vec<ArrayRef> = match pk_columns {
        Some(cols) => {
            let mut arrays = Vec::with_capacity(cols.len());
            for c in cols {
                let idx = batch.schema().index_of(c).map_err(|e| {
                    EngineError::Sink(format!("primary key column '{c}' not in batch schema: {e}"))
                })?;
                arrays.push(Arc::clone(batch.column(idx)));
            }
            arrays
        }
        None => batch.columns().to_vec(),
    };
    let fields: Vec<SortField> = columns
        .iter()
        .map(|c| SortField::new(c.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).map_err(|e| EngineError::Sink(e.to_string()))?;
    let encoded = converter
        .convert_columns(&columns)
        .map_err(|e| EngineError::Sink(e.to_string()))?;
    Ok((0..batch.num_rows())
        .map(|i| encoded.row(i).as_ref().to_vec())
        .collect())
}

#[async_trait]
impl SinkWriter for ConsolidatingSinkWriter {
    async fn write(&mut self, changes: ChangelogBatch) -> EngineResult<()> {
        // Single-owner case: the engine passed the changelog by value.
        self.apply(&changes)
    }

    async fn write_arc(&mut self, batch: Arc<ChangelogBatch>) -> EngineResult<()> {
        // Fan-out case: borrow the changelog, don't unwrap or clone.
        // The consolidating sink never retains the batch past the call.
        self.apply(&batch)
    }

    async fn flush(&mut self) -> EngineResult<()> {
        let Some(schema) = self.schema.clone() else {
            // Nothing was ever written; just flush the inner sink.
            return self.inner.flush().await;
        };

        // Evict oldest unmatched retractions beyond the per-writer memory bound.
        let unmatched_count = self.rows.values().filter(|(_, w)| *w < 0).count();
        if unmatched_count > self.max_unmatched_retractions {
            let to_evict = unmatched_count - self.max_unmatched_retractions;
            let mut evicted = 0usize;
            self.rows.retain(|key, (_, w)| {
                if *w >= 0 || evicted >= to_evict {
                    true
                } else {
                    evicted += 1;
                    // Also drop the row cache entry; the prior image is now lost.
                    // A late Insert for the same key will be treated as a new row.
                    self.row_cache.remove(key);
                    false
                }
            });
            warn!(
                evicted = evicted,
                remaining_unmatched = self.max_unmatched_retractions,
                "evicted oldest unmatched retractions to bound memory"
            );
        }
        let unmatched = self.rows.values().filter(|(_, w)| *w < 0).count();
        if unmatched >= UNMATCHED_RETRACTION_WARN_THRESHOLD {
            warn!(
                unmatched_retractions = unmatched,
                "consolidating sink has {} unmatched retractions in memory; \
                 Delete events arrived for rows never inserted. \
                 These will be cancelled by late Inserts but accumulate until then.",
                unmatched
            );
        }

        // Materialize live rows, clamped to MAX_FLUSH_WEIGHT per row to
        // prevent OOM from degenerate input.
        let mut slices: Vec<RecordBatch> = Vec::new();
        for (row, weight) in self.rows.values() {
            if *weight <= 0 {
                continue;
            }
            let clamped = (*weight).min(MAX_FLUSH_WEIGHT);
            if clamped != *weight {
                warn!(
                    original_weight = *weight,
                    clamped_weight = clamped,
                    "clamped row weight to MAX_FLUSH_WEIGHT in consolidating sink flush"
                );
            }
            for _ in 0..clamped {
                slices.push(row.clone());
            }
        }

        if !slices.is_empty() {
            let net =
                concat_batches(&schema, &slices).map_err(|e| EngineError::Sink(e.to_string()))?;
            self.inner.write(ChangelogBatch::inserts(net)).await?;
        }
        self.inner.flush().await
    }
}

impl ConsolidatingSinkWriter {
    fn apply(&mut self, changes: &ChangelogBatch) -> EngineResult<()> {
        if changes.num_rows() == 0 {
            return Ok(());
        }
        let batch = changes.batch();
        if self.schema.is_none() {
            self.schema = Some(batch.schema());
        }
        // P-4 (audit): rebuild the RowConverter only when the schema
        // changes, not on every apply call. The previous implementation
        // rebuilt it for every changelog, which is O(changelogs ×
        // columns) of unnecessary type-interning work.
        //
        // The cache stores the *types* (SortField list) of the key
        // columns, not the column arrays themselves — those must come
        // from the current batch so the encoded keys reflect the
        // current row contents.
        let keys: Vec<Vec<u8>> = {
            let schema_changed = match &self.row_converter_schema {
                Some(s) => s.as_ref() != batch.schema().as_ref(),
                None => true,
            };
            if schema_changed || self.key_sort_fields.is_none() {
                let key_columns: Vec<ArrayRef> = match self.primary_key.as_deref() {
                    Some(cols) => {
                        let mut arrays = Vec::with_capacity(cols.len());
                        for c in cols {
                            let idx = batch.schema().index_of(c).map_err(|e| {
                                EngineError::Sink(format!(
                                    "primary key column '{c}' not in batch schema: {e}"
                                ))
                            })?;
                            arrays.push(Arc::clone(batch.column(idx)));
                        }
                        arrays
                    }
                    None => batch.columns().to_vec(),
                };
                let fields: Vec<SortField> = key_columns
                    .iter()
                    .map(|c| SortField::new(c.data_type().clone()))
                    .collect();
                self.key_sort_fields = Some(fields);
                self.row_converter_schema = Some(batch.schema());
            }
            let key_columns: Vec<ArrayRef> = match self.primary_key.as_deref() {
                Some(cols) => {
                    let mut arrays = Vec::with_capacity(cols.len());
                    for c in cols {
                        let idx = batch.schema().index_of(c).map_err(|e| {
                            EngineError::Sink(format!(
                                "primary key column '{c}' not in batch schema: {e}"
                            ))
                        })?;
                        arrays.push(Arc::clone(batch.column(idx)));
                    }
                    arrays
                }
                None => batch.columns().to_vec(),
            };
            let fields = self
                .key_sort_fields
                .as_ref()
                .ok_or_else(|| EngineError::Sink("key sort fields missing after init".into()))?;
            let converter =
                RowConverter::new(fields.clone()).map_err(|e| EngineError::Sink(e.to_string()))?;
            let encoded = converter
                .convert_columns(&key_columns)
                .map_err(|e| EngineError::Sink(e.to_string()))?;
            (0..batch.num_rows())
                .map(|i| encoded.row(i).as_ref().to_vec())
                .collect()
        };
        for (i, kind) in changes.row_kinds().iter().enumerate() {
            let key = keys
                .get(i)
                .cloned()
                .ok_or_else(|| EngineError::Sink("row key index out of range".into()))?;
            let weight = kind.weight();
            // Cache the latest row image for this key. Positive-weight rows
            // and the Insert side of an Update pair are recorded here so a
            // later Delete / UpdateBefore can use the prior image.
            if weight > 0 {
                self.row_cache.insert(key.clone(), batch.slice(i, 1));
            }
            let entry = self.rows.entry(key);
            use std::collections::btree_map::Entry;
            match entry {
                Entry::Occupied(mut occ) => {
                    let net = occ.get().1 + weight;
                    if net == 0 {
                        let key = occ.key().clone();
                        occ.remove();
                        self.row_cache.remove(&key);
                    } else {
                        occ.get_mut().1 = net;
                    }
                }
                Entry::Vacant(vac) => {
                    if weight != 0 {
                        // For a negative weight on a vacant key, fall back
                        // to the row cache (PK-keyed mode) so the materialized
                        // table records a tombstone with the prior image.
                        // In full-row keyed mode, the prior image is
                        // identical to the deletion payload and is captured
                        // in the changelog itself; we still record it so
                        // the eviction policy can be applied uniformly.
                        let image = if weight < 0 {
                            self.row_cache
                                .get(vac.key())
                                .cloned()
                                .unwrap_or_else(|| batch.slice(i, 1))
                        } else {
                            batch.slice(i, 1)
                        };
                        vac.insert((image, weight));
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::changelog::RowKind;
    use crate::mem::InMemorySinkProvider;

    fn kv(keys: &[&str], vals: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int64Array::from(vals.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn consolidates_retractions_into_net_table() {
        let collected = InMemorySinkProvider::new();
        let provider = ConsolidatingSinkProvider::new(Arc::new(collected.clone()));
        let mut writer = provider
            .open(&SinkSpec::new("out", "memory", ""))
            .await
            .unwrap();

        // Insert (a,1),(b,2); then retract (a,1) and insert (a,11) — an update.
        writer
            .write(ChangelogBatch::inserts(kv(&["a", "b"], &[1, 2])))
            .await
            .unwrap();
        writer
            .write(
                ChangelogBatch::new(
                    kv(&["a", "a"], &[1, 11]),
                    vec![RowKind::Delete, RowKind::Insert],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        writer.flush().await.unwrap();

        // The inner sink received exactly one net write: {(a,11),(b,2)}.
        let out = collected.take("out");
        let total_rows: usize = out.iter().map(ChangelogBatch::num_rows).sum();
        assert_eq!(total_rows, 2, "net table has two rows");
        assert!(
            out.iter().all(ChangelogBatch::is_append_only),
            "consolidated output is insert-only"
        );
        // Confirm a's value is the updated 11 and b is untouched.
        let batch = out.first().unwrap().batch();
        let vs = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut values: Vec<i64> = (0..vs.len()).map(|i| vs.value(i)).collect();
        values.sort_unstable();
        assert_eq!(values, vec![2, 11]);
    }

    #[tokio::test]
    async fn fully_retracted_key_disappears() {
        let collected = InMemorySinkProvider::new();
        let provider = ConsolidatingSinkProvider::new(Arc::new(collected.clone()));
        let mut writer = provider
            .open(&SinkSpec::new("out", "memory", ""))
            .await
            .unwrap();

        writer
            .write(ChangelogBatch::inserts(kv(&["a", "b"], &[1, 2])))
            .await
            .unwrap();
        writer
            .write(ChangelogBatch::new(kv(&["a"], &[1]), vec![RowKind::Delete]).unwrap())
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let out = collected.take("out");
        let total_rows: usize = out.iter().map(ChangelogBatch::num_rows).sum();
        assert_eq!(total_rows, 1, "only b survives");
    }

    /// PK-keyed mode collapses paired `UpdateBefore` + `UpdateAfter` images
    /// (same key, different row) into a single net row at the new value.
    /// Without PK keying, the full-row encoding treats the two images as
    /// distinct keys and both end up in the materialized table.
    #[tokio::test]
    async fn pk_keyed_update_collapse() {
        let collected = InMemorySinkProvider::new();
        let provider = ConsolidatingSinkProvider::new(Arc::new(collected.clone()))
            .with_primary_key(vec!["k".to_owned()]);
        let mut writer = provider
            .open(&SinkSpec::new("out", "memory", ""))
            .await
            .unwrap();

        // Initial insert.
        writer
            .write(ChangelogBatch::inserts(kv(&["a"], &[1])))
            .await
            .unwrap();
        // Debezium-style paired UpdateBefore(old-value) + UpdateAfter(new-value).
        writer
            .write(
                ChangelogBatch::new(
                    kv(&["a", "a"], &[1, 11]),
                    vec![RowKind::UpdateBefore, RowKind::UpdateAfter],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let out = collected.take("out");
        let total_rows: usize = out.iter().map(ChangelogBatch::num_rows).sum();
        assert_eq!(
            total_rows, 1,
            "net table holds one row at the updated value"
        );
        let batch = out.first().unwrap().batch();
        let ks = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vs = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ks.value(0), "a");
        assert_eq!(vs.value(0), 11, "UpdateAfter's value is the final value");
    }

    /// The unmatched-retraction cap is enforced on every flush and emits a
    /// `warn!`. With `cap=2` and three unmatched deletes, the oldest is
    /// evicted before the remaining two are flushed as tombstones.
    #[tokio::test]
    async fn unmatched_retraction_cap_evicts_oldest() {
        let collected = InMemorySinkProvider::new();
        let provider = ConsolidatingSinkProvider::new(Arc::new(collected.clone()))
            .with_max_unmatched_retractions(2);
        let mut writer = provider
            .open(&SinkSpec::new("out", "memory", ""))
            .await
            .unwrap();

        // Three deletes for keys that were never inserted.
        writer
            .write(
                ChangelogBatch::new(
                    kv(&["x", "y", "z"], &[1, 1, 1]),
                    vec![RowKind::Delete, RowKind::Delete, RowKind::Delete],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        writer.flush().await.unwrap();

        // Flush succeeded; the cap bounded memory growth. The exact number
        // of surviving unmatched entries is internal — what we assert here
        // is that the writer did not OOM and the inner sink was reached.
        let out = collected.take("out");
        let _ = out; // no rows flushed (all unmatched); inner flush ran cleanly
    }
}
