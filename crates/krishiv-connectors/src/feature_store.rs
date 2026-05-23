//! Hybrid batch+stream feature store (R17 Sprint 6).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
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
}

impl FeatureStoreSink {
    /// Open a feature store at `root/table`.
    pub fn open(root: impl AsRef<Path>, table: impl Into<String>) -> ConnectorResult<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root).map_err(ConnectorError::Io)?;
        Ok(Self {
            root,
            table: table.into(),
            live: RwLock::new(Vec::new()),
        })
    }

    fn parquet_path(&self) -> PathBuf {
        self.root.join(format!("{}.parquet", self.table))
    }

    /// Append feature rows (batch backfill or streaming).
    pub fn append(&self, rows: &[FeatureRow]) -> ConnectorResult<()> {
        {
            let mut guard = self.live.write().map_err(|e| {
                ConnectorError::Parquet(format!("feature store lock: {e}"))
            })?;
            guard.extend(rows.iter().cloned());
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
        let path = self.parquet_path();
        let file = std::fs::File::create(&path).map_err(ConnectorError::Io)?;
        let props = WriterProperties::builder().build();
        let mut writer =
            ArrowWriter::try_new(file, schema, Some(props)).map_err(|e| {
                ConnectorError::Parquet(e.to_string())
            })?;
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
        let guard = self.live.read().map_err(|e| {
            ConnectorError::Parquet(format!("feature store lock: {e}"))
        })?;
        let mut best: BTreeMap<String, (u64, f64)> = BTreeMap::new();
        for row in guard.iter() {
            let row_id = row.entity_key.values().cloned().collect::<Vec<_>>().join("|");
            if row_id != entity_id || row.created_at_ms > timestamp_ms {
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
}

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
        self.events.write().map(|mut g| std::mem::take(&mut *g)).unwrap_or_default()
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
}
