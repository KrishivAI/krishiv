//! A primary-key-keyed **upsert** sink wrapper: it *applies* a changelog by key
//! before writing, rather than appending it.
//!
//! Where [`ConsolidatingSinkProvider`](crate::ConsolidatingSinkProvider) keys by
//! the **whole row** (so an update must carry the exact prior image to cancel the
//! old insert), this wrapper keys by a declared **primary key**: an
//! insert/`UpdateAfter` writes (replaces) the keyed row, a delete/`UpdateBefore`
//! removes it. That is the merge-on-read / upsert-connector contract (Iceberg
//! MOR, upsert-Kafka, JDBC `MERGE`): per-row upserts and deletes land by key
//! without the source having to emit the prior row image.
//!
//! On [`flush`](SinkWriter::flush) the net materialized table — one row per live
//! key, in ascending key order — is written once to the wrapped sink.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use async_trait::async_trait;

use crate::changelog::ChangelogBatch;
use crate::error::{EngineError, EngineResult};
use crate::job::SinkSpec;
use crate::runtime::{SinkProvider, SinkWriter};

/// Wraps an inner [`SinkProvider`] so each opened writer applies its changelog by
/// the sink's [`primary_key`](SinkSpec::primary_key) and writes the net keyed
/// table on flush.
pub struct UpsertSinkProvider {
    inner: Arc<dyn SinkProvider>,
}

impl UpsertSinkProvider {
    /// Wrap `inner` with primary-key upsert application.
    pub fn new(inner: Arc<dyn SinkProvider>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl SinkProvider for UpsertSinkProvider {
    async fn open(&self, spec: &SinkSpec) -> EngineResult<Box<dyn SinkWriter>> {
        let key_columns = spec.primary_key.clone().ok_or_else(|| {
            EngineError::Sink(
                "upsert sink requires a primary key (SinkSpec::with_primary_key)".into(),
            )
        })?;
        if key_columns.is_empty() {
            return Err(EngineError::Sink(
                "upsert primary key must name at least one column".into(),
            ));
        }
        let inner = self.inner.open(spec).await?;
        Ok(Box::new(UpsertSinkWriter {
            inner,
            key_columns,
            schema: None,
            rows: BTreeMap::new(),
        }))
    }
}

struct UpsertSinkWriter {
    inner: Box<dyn SinkWriter>,
    key_columns: Vec<String>,
    /// Data schema, captured from the first non-empty changelog.
    schema: Option<SchemaRef>,
    /// Primary-key bytes → the current row for that key (last write wins).
    rows: BTreeMap<Vec<u8>, RecordBatch>,
}

/// Encode each row's primary-key columns (looked up by name) into one stable,
/// comparable byte string per row.
fn encode_keys_by_name(batch: &RecordBatch, names: &[String]) -> EngineResult<Vec<Vec<u8>>> {
    let mut key_arrays: Vec<ArrayRef> = Vec::with_capacity(names.len());
    for name in names {
        let col = batch.column_by_name(name).ok_or_else(|| {
            EngineError::Sink(format!(
                "upsert primary-key column '{name}' not found in the output schema"
            ))
        })?;
        key_arrays.push(col.clone());
    }
    let fields: Vec<SortField> = key_arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).map_err(|e| EngineError::Sink(e.to_string()))?;
    let rows = converter
        .convert_columns(&key_arrays)
        .map_err(|e| EngineError::Sink(e.to_string()))?;
    Ok((0..batch.num_rows())
        .map(|i| rows.row(i).as_ref().to_vec())
        .collect())
}

#[async_trait]
impl SinkWriter for UpsertSinkWriter {
    async fn write(&mut self, changes: ChangelogBatch) -> EngineResult<()> {
        if changes.num_rows() == 0 {
            return Ok(());
        }
        let batch = changes.batch();
        if self.schema.is_none() {
            self.schema = Some(batch.schema());
        }
        let keys = encode_keys_by_name(batch, &self.key_columns)?;
        let kinds = changes.row_kinds();

        // Retractions first, then additions: an update encoded as (retract old,
        // insert new) on the same key resolves to the new row regardless of the
        // row order within the changelog.
        for (i, kind) in kinds.iter().enumerate() {
            if kind.is_retraction()
                && let Some(key) = keys.get(i)
            {
                self.rows.remove(key);
            }
        }
        for (i, kind) in kinds.iter().enumerate() {
            if !kind.is_retraction() {
                let key = keys
                    .get(i)
                    .cloned()
                    .ok_or_else(|| EngineError::Sink("upsert row key index out of range".into()))?;
                self.rows.insert(key, batch.slice(i, 1));
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> EngineResult<()> {
        let Some(schema) = self.schema.clone() else {
            return self.inner.flush().await;
        };
        if !self.rows.is_empty() {
            let net = concat_batches(&schema, self.rows.values())
                .map_err(|e| EngineError::Sink(e.to_string()))?;
            self.inner.write(ChangelogBatch::inserts(net)).await?;
        }
        self.inner.flush().await
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
    async fn upsert_replaces_by_key_without_prior_image() {
        let collected = InMemorySinkProvider::new();
        let provider = UpsertSinkProvider::new(Arc::new(collected.clone()));
        let spec = SinkSpec::new("out", "memory", "").with_primary_key(["k"]);
        let mut writer = provider.open(&spec).await.unwrap();

        // Insert (a,1),(b,2); then a bare upsert of a→11 (UpdateAfter, NO prior
        // image) and a delete of b by key.
        writer
            .write(ChangelogBatch::inserts(kv(&["a", "b"], &[1, 2])))
            .await
            .unwrap();
        writer
            .write(ChangelogBatch::new(kv(&["a"], &[11]), vec![RowKind::UpdateAfter]).unwrap())
            .await
            .unwrap();
        writer
            .write(ChangelogBatch::new(kv(&["b"], &[2]), vec![RowKind::Delete]).unwrap())
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let out = collected.take("out");
        let total_rows: usize = out.iter().map(ChangelogBatch::num_rows).sum();
        assert_eq!(total_rows, 1, "only a survives, b was deleted by key");
        let batch = out.first().unwrap().batch();
        let v = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 11, "a was upserted to 11 by key, no prior image needed");
    }

    #[tokio::test]
    async fn open_without_primary_key_errors() {
        let collected = InMemorySinkProvider::new();
        let provider = UpsertSinkProvider::new(Arc::new(collected));
        let err = provider
            .open(&SinkSpec::new("out", "memory", ""))
            .await
            .err()
            .expect("opening an upsert sink without a primary key must fail");
        assert!(err.to_string().contains("primary key"));
    }
}
