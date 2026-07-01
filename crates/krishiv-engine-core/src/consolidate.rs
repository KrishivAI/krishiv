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
}

impl ConsolidatingSinkProvider {
    /// Wrap `inner` with full-row changelog consolidation.
    pub fn new(inner: Arc<dyn SinkProvider>) -> Self {
        Self { inner }
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
    /// Full-row key → (one-row batch, net weight). Entries reaching weight 0 are
    /// removed, so the map always holds the live materialized rows.
    /// Entries with weight < 0 are unmatched retractions: a Delete arrived for a
    /// row that was never inserted.  These cancel when the matching Insert arrives
    /// later, but if no Insert ever comes they accumulate as a memory leak.
    rows: BTreeMap<Vec<u8>, (RecordBatch, i64)>,
}

/// Encode every column of `batch` into one stable, comparable byte string per row.
fn encode_full_rows(batch: &RecordBatch) -> EngineResult<Vec<Vec<u8>>> {
    let columns: Vec<ArrayRef> = batch.columns().to_vec();
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

        // Evict oldest unmatched retractions beyond the memory bound.
        let unmatched_count = self.rows.values().filter(|(_, w)| *w < 0).count();
        if unmatched_count > MAX_UNMATCHED_RETRACTIONS {
            let to_evict = unmatched_count - MAX_UNMATCHED_RETRACTIONS;
            let mut evicted = 0usize;
            self.rows.retain(|_, (_, w)| {
                if *w >= 0 || evicted >= to_evict {
                    true
                } else {
                    evicted += 1;
                    false
                }
            });
            warn!(
                evicted = evicted,
                remaining_unmatched = MAX_UNMATCHED_RETRACTIONS,
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
        let keys = encode_full_rows(batch)?;
        for (i, kind) in changes.row_kinds().iter().enumerate() {
            let key = keys
                .get(i)
                .cloned()
                .ok_or_else(|| EngineError::Sink("row key index out of range".into()))?;
            let weight = kind.weight();
            let entry = self.rows.entry(key);
            use std::collections::btree_map::Entry;
            match entry {
                Entry::Occupied(mut occ) => {
                    let net = occ.get().1 + weight;
                    if net == 0 {
                        occ.remove();
                    } else {
                        occ.get_mut().1 = net;
                    }
                }
                Entry::Vacant(vac) => {
                    if weight != 0 {
                        vac.insert((batch.slice(i, 1), weight));
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
}
