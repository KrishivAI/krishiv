//! Hybrid batch+stream feature store (R17 Sprint 6).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;

use crate::{ConnectorError, ConnectorResult};

/// One materialized feature row.
#[derive(Debug, Clone)]
pub struct FeatureRow {
    pub entity_key: BTreeMap<String, String>,
    pub feature_values: BTreeMap<String, f64>,
    pub created_at_ms: u64,
    pub ttl_ms: Option<u64>,
}

/// Parquet-backed feature store with point-in-time lookup.
#[derive(Debug)]
pub struct FeatureStoreSink {
    root: PathBuf,
    table: String,
    live: RwLock<Vec<FeatureRow>>,
    live_index: RwLock<BTreeMap<String, Vec<usize>>>,
    next_fragment_id: RwLock<u64>,
}

impl FeatureStoreSink {
    /// Open a feature store at `root/table`.
    pub fn open(root: impl AsRef<Path>, table: impl Into<String>) -> ConnectorResult<Self> {
        let root = root.as_ref().to_path_buf();
        let table = table.into();
        std::fs::create_dir_all(&root).map_err(ConnectorError::Io)?;
        std::fs::create_dir_all(Self::fragments_dir(&root, &table)).map_err(ConnectorError::Io)?;
        let sink = Self {
            root,
            table,
            live: RwLock::new(Vec::new()),
            live_index: RwLock::new(BTreeMap::new()),
            next_fragment_id: RwLock::new(0),
        };
        sink.reload_from_fragments()?;
        Ok(sink)
    }

    fn fragments_dir(root: &Path, table: &str) -> PathBuf {
        root.join(format!("{table}_fragments"))
    }

    fn fragments_dir_for(&self) -> PathBuf {
        Self::fragments_dir(&self.root, &self.table)
    }

    fn reload_from_fragments(&self) -> ConnectorResult<()> {
        let dir = self.fragments_dir_for();
        let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
            .map_err(ConnectorError::Io)?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
            .collect();
        paths.sort();

        let mut rows = Vec::new();
        let mut max_fragment = 0_u64;
        for path in paths {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                && let Some(id) = stem.strip_prefix("batch-")
                && let Ok(fragment_id) = id.parse::<u64>()
            {
                max_fragment = max_fragment.max(fragment_id);
            }
            rows.extend(read_feature_rows_from_parquet(&path)?);
        }

        if let Ok(mut guard) = self.live.write() {
            *guard = rows;
        }
        if let Ok(live_guard) = self.live.read() {
            let mut index = BTreeMap::new();
            for (idx, row) in live_guard.iter().enumerate() {
                let entity_id = row
                    .entity_key
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("|");
                index.entry(entity_id).or_insert_with(Vec::new).push(idx);
            }
            if let Ok(mut index_guard) = self.live_index.write() {
                *index_guard = index;
            }
        }
        if let Ok(mut guard) = self.next_fragment_id.write() {
            *guard = max_fragment.saturating_add(1);
        }
        Ok(())
    }

    /// Append feature rows (batch backfill or streaming).
    pub fn append(&self, rows: &[FeatureRow]) -> ConnectorResult<()> {
        {
            let mut guard = self
                .live
                .write()
                .map_err(|e| ConnectorError::Parquet(format!("feature store lock: {e}")))?;
            let start_idx = guard.len();
            guard.extend(rows.iter().cloned());
            let mut index_guard = self
                .live_index
                .write()
                .map_err(|e| ConnectorError::Parquet(format!("feature store lock: {e}")))?;
            for (offset, row) in rows.iter().enumerate() {
                let entity_id = row
                    .entity_key
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("|");
                index_guard
                    .entry(entity_id)
                    .or_insert_with(Vec::new)
                    .push(start_idx + offset);
            }
        }
        self.flush_parquet(rows)
    }

    fn flush_parquet(&self, rows: &[FeatureRow]) -> ConnectorResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut entity_ids = Vec::new();
        let mut feature_names = Vec::new();
        let mut values = Vec::new();
        let mut created = Vec::new();
        for row in rows {
            for (fname, val) in &row.feature_values {
                entity_ids.push(
                    row.entity_key
                        .values()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join("|"),
                );
                feature_names.push(fname.clone());
                values.push(*val);
                created.push(row.created_at_ms as i64);
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("feature_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("created_at_ms", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(entity_ids)),
                Arc::new(StringArray::from(feature_names)),
                Arc::new(Float64Array::from(values)),
                Arc::new(Int64Array::from(created)),
            ],
        )
        .map_err(|e| ConnectorError::Parquet(e.to_string()))?;

        let fragment_id = {
            let mut guard = self
                .next_fragment_id
                .write()
                .map_err(|e| ConnectorError::Parquet(format!("feature store lock: {e}")))?;
            let id = *guard;
            *guard += 1;
            id
        };
        let fragment_path = self
            .fragments_dir_for()
            .join(format!("batch-{fragment_id}.parquet"));
        let file = std::fs::File::create(&fragment_path).map_err(ConnectorError::Io)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| ConnectorError::Parquet(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| ConnectorError::Parquet(e.to_string()))?;
        writer
            .close()
            .map_err(|e| ConnectorError::Parquet(e.to_string()))?;
        Ok(())
    }

    /// Point-in-time lookup: latest row with `created_at_ms <= timestamp_ms`.
    pub fn lookup(
        &self,
        entity_key: &BTreeMap<String, String>,
        timestamp_ms: u64,
    ) -> ConnectorResult<BTreeMap<String, f64>> {
        let entity_id = entity_key.values().cloned().collect::<Vec<_>>().join("|");
        let now = timestamp_ms;
        let guard = self
            .live
            .read()
            .map_err(|e| ConnectorError::Parquet(format!("feature store lock: {e}")))?;
        let index_guard = self
            .live_index
            .read()
            .map_err(|e| ConnectorError::Parquet(format!("feature store lock: {e}")))?;
        let mut best: BTreeMap<String, (u64, f64)> = BTreeMap::new();
        let empty = Vec::new();
        let row_indices = index_guard.get(&entity_id).unwrap_or(&empty);
        for &idx in row_indices {
            let row = &guard[idx];
            if row.created_at_ms > timestamp_ms {
                continue;
            }
            if let Some(ttl) = row.ttl_ms
                && now.saturating_sub(row.created_at_ms) > ttl
            {
                continue;
            }
            for (k, v) in &row.feature_values {
                best.entry(k.clone())
                    .and_modify(|(ts, val)| {
                        if row.created_at_ms >= *ts {
                            *ts = row.created_at_ms;
                            *val = *v;
                        }
                    })
                    .or_insert((row.created_at_ms, *v));
            }
        }
        Ok(best.into_iter().map(|(k, (_, v))| (k, v)).collect())
    }

    /// Number of on-disk parquet fragments (for tests).
    pub fn fragment_count(&self) -> ConnectorResult<usize> {
        let dir = self.fragments_dir_for();
        Ok(std::fs::read_dir(&dir)
            .map_err(ConnectorError::Io)?
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
            .count())
    }
}

fn read_feature_rows_from_parquet(path: &Path) -> ConnectorResult<Vec<FeatureRow>> {
    let file = std::fs::File::open(path).map_err(ConnectorError::Io)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| ConnectorError::Parquet(e.to_string()))?
        .build()
        .map_err(|e| ConnectorError::Parquet(e.to_string()))?;

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| ConnectorError::Parquet(e.to_string()))?;
        let schema = batch.schema();
        let entity_idx = schema
            .index_of("entity_id")
            .map_err(|e| ConnectorError::Parquet(format!("missing entity_id column: {e}")))?;
        let feature_idx = schema
            .index_of("feature_name")
            .map_err(|e| ConnectorError::Parquet(format!("missing feature_name column: {e}")))?;
        let value_idx = schema
            .index_of("value")
            .map_err(|e| ConnectorError::Parquet(format!("missing value column: {e}")))?;
        let created_idx = schema
            .index_of("created_at_ms")
            .map_err(|e| ConnectorError::Parquet(format!("missing created_at_ms column: {e}")))?;

        let entities = batch
            .column(entity_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ConnectorError::Parquet("entity_id must be Utf8".into()))?;
        let features = batch
            .column(feature_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ConnectorError::Parquet("feature_name must be Utf8".into()))?;
        let values = batch
            .column(value_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| ConnectorError::Parquet("value must be Float64".into()))?;
        let created = batch
            .column(created_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| ConnectorError::Parquet("created_at_ms must be Int64".into()))?;

        for row_idx in 0..batch.num_rows() {
            let entity_id = entities.value(row_idx);
            let mut entity_key = BTreeMap::new();
            entity_key.insert("entity_id".into(), entity_id.to_string());
            let mut feature_values = BTreeMap::new();
            feature_values.insert(features.value(row_idx).to_string(), values.value(row_idx));
            rows.push(FeatureRow {
                entity_key,
                feature_values,
                created_at_ms: created.value(row_idx) as u64,
                ttl_ms: None,
            });
        }
    }
    Ok(rows)
}

/// **Testing only**: In-memory implementation for unit tests. Not for production use.
///
/// In-memory Kafka-style streaming source for feature updates (tests).
#[derive(Debug, Default)]
pub struct InMemoryFeatureStream {
    events: RwLock<Vec<FeatureRow>>,
}

impl InMemoryFeatureStream {
    /// Create an empty stream.
    pub fn new() -> Self {
        Self::default()
    }

    /// Push live feature updates.
    pub fn push(&self, rows: Vec<FeatureRow>) {
        if let Ok(mut g) = self.events.write() {
            g.extend(rows);
        }
    }

    /// Drain pending events.
    pub fn drain(&self) -> Vec<FeatureRow> {
        self.events
            .write()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_store_backfill_point_in_time() {
        let dir = tempfile::tempdir().unwrap();
        let store = FeatureStoreSink::open(dir.path(), "users").unwrap();
        let mut key = BTreeMap::new();
        key.insert("user_id".into(), "u1".into());
        store
            .append(&[FeatureRow {
                entity_key: key.clone(),
                feature_values: BTreeMap::from([("age".into(), 30.0)]),
                created_at_ms: 1000,
                ttl_ms: None,
            }])
            .unwrap();
        store
            .append(&[FeatureRow {
                entity_key: key.clone(),
                feature_values: BTreeMap::from([("age".into(), 31.0)]),
                created_at_ms: 2000,
                ttl_ms: None,
            }])
            .unwrap();
        let at_1500 = store.lookup(&key, 1500).unwrap();
        assert_eq!(at_1500.get("age"), Some(&30.0));
        let at_2500 = store.lookup(&key, 2500).unwrap();
        assert_eq!(at_2500.get("age"), Some(&31.0));
    }

    #[test]
    fn feature_store_streaming_updates() {
        let dir = tempfile::tempdir().unwrap();
        let store = FeatureStoreSink::open(dir.path(), "events").unwrap();
        let stream = InMemoryFeatureStream::new();
        let mut key = BTreeMap::new();
        key.insert("id".into(), "1".into());
        stream.push(vec![FeatureRow {
            entity_key: key.clone(),
            feature_values: BTreeMap::from([("score".into(), 0.5)]),
            created_at_ms: 500,
            ttl_ms: Some(60_000),
        }]);
        store.append(&stream.drain()).unwrap();
        let v = store.lookup(&key, 1000).unwrap();
        assert_eq!(v.get("score"), Some(&0.5));
    }

    #[test]
    fn feature_store_parquet_append_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut key = BTreeMap::new();
        key.insert("user_id".into(), "u1".into());

        {
            let store = FeatureStoreSink::open(dir.path(), "users").unwrap();
            for batch in 0..3 {
                store
                    .append(&[FeatureRow {
                        entity_key: key.clone(),
                        feature_values: BTreeMap::from([(format!("f{batch}"), batch as f64)]),
                        created_at_ms: 1000 + batch as u64,
                        ttl_ms: None,
                    }])
                    .unwrap();
            }
            assert_eq!(store.fragment_count().unwrap(), 3);
        }

        let reloaded = FeatureStoreSink::open(dir.path(), "users").unwrap();
        assert_eq!(reloaded.fragment_count().unwrap(), 3);
        for batch in 0..3 {
            let values = reloaded.lookup(&key, 2000).unwrap();
            assert_eq!(values.get(&format!("f{batch}")), Some(&(batch as f64)));
        }
    }
}
