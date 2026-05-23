//! Live table physical operators (R14 S1.2).

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use krishiv_lakehouse::{DeltaOp, DeltaStore, MemoryDeltaStore};

use crate::schema_normalize::SchemaNormalizeOperator;
use crate::ExecError;

/// Physical operator that writes normalized CDC batches into a delta log.
pub struct CreateLiveTableExec {
    pub table_name: String,
    pub query: String,
    store: Arc<dyn DeltaStore>,
    normalizer: SchemaNormalizeOperator,
}

impl CreateLiveTableExec {
    pub fn new(
        table_name: impl Into<String>,
        query: impl Into<String>,
        target_schema: Arc<arrow::datatypes::Schema>,
        store: Option<Arc<dyn DeltaStore>>,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            query: query.into(),
            store: store.unwrap_or_else(|| Arc::new(MemoryDeltaStore::new())),
            normalizer: SchemaNormalizeOperator::new(target_schema),
        }
    }

    pub fn ingest(&self, batch: &RecordBatch, op: DeltaOp) -> Result<(), ExecError> {
        let normalized = self.normalizer.normalize(batch)?;
        self.store
            .append(normalized, op)
            .map_err(|e| ExecError::Arrow(e.to_string()))
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn delta_store(&self) -> &dyn DeltaStore {
        self.store.as_ref()
    }
}

/// Compaction operator: merge delta log into base table and truncate log.
pub struct RefreshLiveTableExec {
    pub table_name: String,
    store: Arc<dyn DeltaStore>,
}

impl RefreshLiveTableExec {
    pub fn new(table_name: impl Into<String>, store: Arc<dyn DeltaStore>) -> Self {
        Self {
            table_name: table_name.into(),
            store,
        }
    }

    pub fn compact(&self) -> Result<usize, ExecError> {
        let entries = self
            .store
            .scan()
            .map_err(|e| ExecError::Arrow(e.to_string()))?;
        let count = entries.len();
        self.store
            .truncate()
            .map_err(|e| ExecError::Arrow(e.to_string()))?;
        Ok(count)
    }
}

/// Change feed over a live table delta log.
#[derive(Debug, Clone)]
pub struct ChangeFeed {
    pub op: DeltaOp,
    pub batch: RecordBatch,
}

impl ChangeFeed {
    pub fn from_store(store: &dyn DeltaStore) -> Result<Vec<Self>, ExecError> {
        let entries = store.scan().map_err(|e| ExecError::Arrow(e.to_string()))?;
        Ok(entries
            .into_iter()
            .map(|e| ChangeFeed {
                op: e.op,
                batch: e.batch,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_lakehouse::DeltaOp;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(v: i64) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    #[test]
    fn live_table_ingest_and_change_feed() {
        let exec = CreateLiveTableExec::new("orders", "SELECT * FROM orders", schema(), None);
        exec.ingest(&batch(1), DeltaOp::Insert).unwrap();
        exec.ingest(&batch(2), DeltaOp::Update).unwrap();
        let feed = ChangeFeed::from_store(exec.delta_store()).unwrap();
        assert_eq!(feed.len(), 2);
        assert_eq!(feed[0].op, DeltaOp::Insert);
    }

    #[test]
    fn refresh_truncates_delta_log() {
        let store: Arc<dyn DeltaStore> = Arc::new(MemoryDeltaStore::new());
        let create = CreateLiveTableExec::new("t", "SELECT 1", schema(), Some(store.clone()));
        create.ingest(&batch(1), DeltaOp::Insert).unwrap();
        let refresh = RefreshLiveTableExec::new("t", store.clone());
        assert_eq!(refresh.compact().unwrap(), 1);
        assert_eq!(store.len().unwrap(), 0);
    }
}
