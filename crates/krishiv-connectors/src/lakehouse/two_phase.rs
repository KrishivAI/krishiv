//! Iceberg two-phase commit with Kafka offset metadata (R14 S4.2).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

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
    committed: tokio::sync::Mutex<BTreeMap<i64, i64>>,
    next_stage_id: AtomicI64,
}

impl MemoryIcebergTwoPhaseCommit {
    pub fn new(table: Arc<MemoryLakehouseTable>) -> Self {
        Self {
            table,
            staged: tokio::sync::Mutex::new(Vec::new()),
            committed_offsets: tokio::sync::Mutex::new(BTreeMap::new()),
            committed: tokio::sync::Mutex::new(BTreeMap::new()),
            next_stage_id: AtomicI64::new(1),
        }
    }

    pub async fn committed_kafka_offsets(&self) -> BTreeMap<String, i64> {
        self.committed_offsets.lock().await.clone()
    }
}

#[async_trait]
impl IcebergTwoPhaseCommit for MemoryIcebergTwoPhaseCommit {
    async fn prepare(&self, batches: Vec<RecordBatch>) -> Result<StagedSnapshot, LakehouseError> {
        let staged = StagedSnapshot {
            snapshot_id: self.next_stage_id.fetch_add(1, Ordering::Relaxed),
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
        if let Some(snapshot_id) = self
            .committed
            .lock()
            .await
            .get(&staged.snapshot_id)
            .copied()
        {
            return Ok(snapshot_id);
        }
        let mut pending = self.staged.lock().await;
        if !pending
            .iter()
            .any(|candidate| candidate.snapshot_id == staged.snapshot_id)
        {
            return Err(LakehouseError::Concurrency {
                message: format!(
                    "staged snapshot {} is unknown or aborted",
                    staged.snapshot_id
                ),
            });
        }
        self.table.append(staged.batches).await?;
        let snapshot_id =
            self.table
                .current_snapshot_id()
                .await?
                .ok_or_else(|| LakehouseError::Concurrency {
                    message: "snapshot missing after commit".to_string(),
                })?;
        *self.committed_offsets.lock().await = kafka_offsets;
        pending.retain(|candidate| candidate.snapshot_id != staged.snapshot_id);
        self.committed
            .lock()
            .await
            .insert(staged.snapshot_id, snapshot_id);
        Ok(snapshot_id)
    }

    async fn abort(&self, staged: StagedSnapshot) -> Result<(), LakehouseError> {
        self.staged
            .lock()
            .await
            .retain(|s| s.snapshot_id != staged.snapshot_id);
        Ok(())
    }
}

/// Coordinator-owned commit aggregation for one distributed write epoch.
///
/// Task outputs remain invisible until all expected task attempts have staged
/// successfully. The coordinator then prepares and commits one combined
/// Iceberg snapshot. Aborting an epoch drops every staged task output.
pub struct DistributedIcebergCommitCoordinator {
    committer: Arc<dyn IcebergTwoPhaseCommit>,
    expected_tasks: usize,
    epochs: tokio::sync::Mutex<BTreeMap<u64, BTreeMap<u32, Vec<RecordBatch>>>>,
    committed_epochs: tokio::sync::Mutex<BTreeMap<u64, i64>>,
    commit_lock: tokio::sync::Mutex<()>,
}

impl DistributedIcebergCommitCoordinator {
    pub fn new(
        committer: Arc<dyn IcebergTwoPhaseCommit>,
        expected_tasks: usize,
    ) -> Result<Self, LakehouseError> {
        if expected_tasks == 0 {
            return Err(LakehouseError::Concurrency {
                message: "distributed commit requires at least one task".into(),
            });
        }
        Ok(Self {
            committer,
            expected_tasks,
            epochs: tokio::sync::Mutex::new(BTreeMap::new()),
            committed_epochs: tokio::sync::Mutex::new(BTreeMap::new()),
            commit_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub async fn stage_task(
        &self,
        epoch: u64,
        task_id: u32,
        batches: Vec<RecordBatch>,
    ) -> Result<(), LakehouseError> {
        if self.committed_epochs.lock().await.contains_key(&epoch) {
            return Err(LakehouseError::Concurrency {
                message: format!("epoch {epoch} is already committed"),
            });
        }
        let mut epochs = self.epochs.lock().await;
        let tasks = epochs.entry(epoch).or_default();
        if tasks.insert(task_id, batches).is_some() {
            return Err(LakehouseError::Concurrency {
                message: format!("duplicate task {task_id} for epoch {epoch}"),
            });
        }
        Ok(())
    }

    pub async fn commit_epoch(
        &self,
        epoch: u64,
        offsets: BTreeMap<String, i64>,
    ) -> Result<i64, LakehouseError> {
        // Serialize epoch publication so concurrent retries observe the same
        // committed snapshot instead of racing the staged-task removal.
        let _commit_guard = self.commit_lock.lock().await;
        if let Some(snapshot) = self.committed_epochs.lock().await.get(&epoch).copied() {
            return Ok(snapshot);
        }
        let tasks = {
            let mut epochs = self.epochs.lock().await;
            let tasks = epochs
                .get(&epoch)
                .ok_or_else(|| LakehouseError::Concurrency {
                    message: format!("epoch {epoch} has no staged tasks"),
                })?;
            if tasks.len() != self.expected_tasks {
                return Err(LakehouseError::Concurrency {
                    message: format!(
                        "epoch {epoch} has {} of {} required tasks",
                        tasks.len(),
                        self.expected_tasks
                    ),
                });
            }
            epochs
                .remove(&epoch)
                .ok_or_else(|| LakehouseError::Concurrency {
                    message: format!("epoch {epoch} disappeared during commit"),
                })?
        };
        let batches = tasks.values().flatten().cloned().collect::<Vec<_>>();
        let staged = match self.committer.prepare(batches).await {
            Ok(staged) => staged,
            Err(error) => {
                self.epochs.lock().await.insert(epoch, tasks);
                return Err(error);
            }
        };
        match self.committer.commit(staged.clone(), offsets).await {
            Ok(snapshot) => {
                self.committed_epochs.lock().await.insert(epoch, snapshot);
                Ok(snapshot)
            }
            Err(error) => {
                let abort_result = self.committer.abort(staged).await;
                self.epochs.lock().await.insert(epoch, tasks);
                match abort_result {
                    Ok(()) => Err(error),
                    Err(abort_error) => Err(LakehouseError::Concurrency {
                        message: format!(
                            "commit for epoch {epoch} failed ({error}); abort of staged snapshot \
                             failed ({abort_error}); staged data may require recovery"
                        ),
                    }),
                }
            }
        }
    }

    pub async fn abort_epoch(&self, epoch: u64) -> Result<(), LakehouseError> {
        if self.committed_epochs.lock().await.contains_key(&epoch) {
            return Err(LakehouseError::Concurrency {
                message: format!("cannot abort committed epoch {epoch}"),
            });
        }
        self.epochs.lock().await.remove(&epoch);
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
    use async_trait::async_trait;

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
    async fn distributed_commit_is_atomic_and_idempotent() {
        let table = table();
        let committer = Arc::new(MemoryIcebergTwoPhaseCommit::new(table.clone()));
        let coordinator = Arc::new(DistributedIcebergCommitCoordinator::new(committer, 2).unwrap());
        coordinator.stage_task(7, 0, vec![batch(1)]).await.unwrap();
        assert!(coordinator.commit_epoch(7, BTreeMap::new()).await.is_err());
        assert!(table.current_snapshot_id().await.unwrap().is_none());
        coordinator.stage_task(7, 1, vec![batch(2)]).await.unwrap();
        let first_attempt = Arc::clone(&coordinator);
        let concurrent_retry = Arc::clone(&coordinator);
        let (first, retry) = tokio::join!(
            first_attempt.commit_epoch(7, BTreeMap::new()),
            concurrent_retry.commit_epoch(7, BTreeMap::new())
        );
        assert_eq!(first.unwrap(), retry.unwrap());
        let rows: usize = table
            .scan(&Default::default())
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn two_phase_abort_discards_staged() {
        let tpc = MemoryIcebergTwoPhaseCommit::new(table());
        let staged = tpc.prepare(vec![batch(2)]).await.unwrap();
        tpc.abort(staged).await.unwrap();
        let snap = tpc.table.current_snapshot_id().await.unwrap();
        assert!(snap.is_none() || snap == Some(0));
    }

    struct FailingCommitter;

    #[async_trait]
    impl IcebergTwoPhaseCommit for FailingCommitter {
        async fn prepare(
            &self,
            batches: Vec<RecordBatch>,
        ) -> Result<StagedSnapshot, LakehouseError> {
            Ok(StagedSnapshot {
                snapshot_id: 11,
                batches,
            })
        }

        async fn commit(
            &self,
            _staged: StagedSnapshot,
            _kafka_offsets: BTreeMap<String, i64>,
        ) -> Result<i64, LakehouseError> {
            Err(LakehouseError::Iceberg("commit down".into()))
        }

        async fn abort(&self, _staged: StagedSnapshot) -> Result<(), LakehouseError> {
            Err(LakehouseError::Io("abort down".into()))
        }
    }

    #[tokio::test]
    async fn distributed_commit_reports_abort_failure_after_commit_failure() {
        let coordinator =
            DistributedIcebergCommitCoordinator::new(Arc::new(FailingCommitter), 1).unwrap();
        coordinator.stage_task(3, 0, vec![batch(3)]).await.unwrap();

        let error = coordinator
            .commit_epoch(3, BTreeMap::new())
            .await
            .expect_err("commit plus abort failure must be returned");
        let message = error.to_string();
        assert!(message.contains("commit down"));
        assert!(message.contains("abort of staged snapshot"));
        assert!(message.contains("abort down"));
    }
}
