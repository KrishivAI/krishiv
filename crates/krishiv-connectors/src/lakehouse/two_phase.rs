//! Iceberg two-phase commit with Kafka offset metadata (R14 S4.2).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use super::{LakehouseError, LakehouseTable, MemoryLakehouseTable};

pub const KAFKA_OFFSETS_SUMMARY_KEY: &str = "krishiv.kafka.committed_offsets";

/// Staged snapshot pending commit.
#[derive(Debug, Clone)]
pub struct StagedSnapshot {
    pub snapshot_id: i64,
    pub batches: Vec<RecordBatch>,
}

/// Two-phase commit protocol for Iceberg tables.
#[async_trait]
pub trait IcebergTwoPhaseCommit: Send + Sync {
    async fn prepare(&self, batches: Vec<RecordBatch>) -> Result<StagedSnapshot, LakehouseError>;
    async fn commit(
        &self,
        staged: StagedSnapshot,
        kafka_offsets: BTreeMap<String, i64>,
    ) -> Result<i64, LakehouseError>;
    async fn abort(&self, staged: StagedSnapshot) -> Result<(), LakehouseError>;
}

/// Memory-backed two-phase commit for tests and embedded pipelines.
pub struct MemoryIcebergTwoPhaseCommit {
    table: Arc<MemoryLakehouseTable>,
    staged: tokio::sync::Mutex<Vec<StagedSnapshot>>,
    committed_offsets: tokio::sync::Mutex<BTreeMap<String, i64>>,
}

impl MemoryIcebergTwoPhaseCommit {
    pub fn new(table: Arc<MemoryLakehouseTable>) -> Self {
        Self {
            table,
            staged: tokio::sync::Mutex::new(Vec::new()),
            committed_offsets: tokio::sync::Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn committed_kafka_offsets(&self) -> BTreeMap<String, i64> {
        self.committed_offsets.lock().await.clone()
    }
}

#[async_trait]
impl IcebergTwoPhaseCommit for MemoryIcebergTwoPhaseCommit {
    async fn prepare(&self, batches: Vec<RecordBatch>) -> Result<StagedSnapshot, LakehouseError> {
        let snap = self.table.current_snapshot_id().await?.unwrap_or(0) + 1;
        let staged = StagedSnapshot {
            snapshot_id: snap,
            batches,
        };
        self.staged.lock().await.push(staged.clone());
        Ok(staged)
    }

    async fn commit(
        &self,
        staged: StagedSnapshot,
        kafka_offsets: BTreeMap<String, i64>,
    ) -> Result<i64, LakehouseError> {
        self.table.append(staged.batches).await?;
        *self.committed_offsets.lock().await = kafka_offsets;
        self.staged
            .lock()
            .await
            .retain(|s| s.snapshot_id != staged.snapshot_id);
        self.table
            .current_snapshot_id()
            .await?
            .ok_or_else(|| LakehouseError::Concurrency {
                message: "snapshot missing after commit".to_string(),
            })
    }

    async fn abort(&self, staged: StagedSnapshot) -> Result<(), LakehouseError> {
        self.staged
            .lock()
            .await
            .retain(|s| s.snapshot_id != staged.snapshot_id);
        Ok(())
    }
}

pub fn kafka_offsets_json(offsets: &BTreeMap<String, i64>) -> String {
    serde_json::to_string(offsets).unwrap_or_else(|_| "{}".to_string())
}

pub fn parse_kafka_offsets_json(json: &str) -> BTreeMap<String, i64> {
    serde_json::from_str(json).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::lakehouse::{IcebergTableRef, MemoryLakehouseTable, SchemaField, SchemaVersion};

    use super::*;

    fn table() -> Arc<MemoryLakehouseTable> {
        let schema = SchemaVersion {
            schema_id: 1,
            fields: vec![SchemaField {
                id: 1,
                name: "id".to_string(),
                required: true,
                data_type: "long".to_string(),
            }],
        };
        Arc::new(MemoryLakehouseTable::new(
            IcebergTableRef::new("cat", "ns", "orders"),
            schema,
        ))
    }

    fn batch(v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    #[tokio::test]
    async fn two_phase_commit_prepare_commit() {
        let tpc = MemoryIcebergTwoPhaseCommit::new(table());
        let staged = tpc.prepare(vec![batch(1)]).await.unwrap();
        let mut offsets = BTreeMap::new();
        offsets.insert("orders-0".to_string(), 42);
        let snap = tpc.commit(staged, offsets.clone()).await.unwrap();
        assert!(snap >= 1);
        assert_eq!(tpc.committed_kafka_offsets().await, offsets);
    }

    #[tokio::test]
    async fn two_phase_abort_discards_staged() {
        let tpc = MemoryIcebergTwoPhaseCommit::new(table());
        let staged = tpc.prepare(vec![batch(2)]).await.unwrap();
        tpc.abort(staged).await.unwrap();
        let snap = tpc.table.current_snapshot_id().await.unwrap();
        assert!(snap.is_none() || snap == Some(0));
    }
}
