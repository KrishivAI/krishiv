//! Key-partitioned incremental flow — the IVM side of unified auto-partitioning.
//!
//! [`PartitionedIncrementalFlow`] shards an [`IncrementalFlow`](crate::IncrementalFlow)
//! across `N` partitions by a key column. Each shard is an independent flow
//! holding the same views; feeds are routed by the **shared keyed hash**
//! (`krishiv_common::partition`, SHA-256 — the same family streaming key groups
//! use), so every key's rows land in exactly one shard.
//!
//! This is correct for views whose output for a key depends only on rows with
//! that key — per-key aggregates (`GROUP BY <key>`), filters, projections, and
//! equi-joins on the shard key. The pipeline driver enables it only when it can
//! prove that shape; everything else runs on a single flow. Shards step in
//! parallel, removing the single-core ceiling on keyed incremental views.

use arrow::record_batch::RecordBatch;
use krishiv_common::partition::partition_record_batches_by_key;
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};

use crate::error::{IvmError, IvmResult};
use crate::flow::{IncrementalFlow, StepSummary};

/// An [`IncrementalFlow`] sharded by a key column across `N` partitions.
pub struct PartitionedIncrementalFlow {
    shards: Vec<IncrementalFlow>,
    key_column: String,
}

impl PartitionedIncrementalFlow {
    /// Create a partitioned flow with `num_shards` shards keyed on `key_column`.
    pub fn new(num_shards: usize, key_column: impl Into<String>) -> Self {
        let n = num_shards.max(1);
        Self {
            shards: (0..n).map(|_| IncrementalFlow::new()).collect(),
            key_column: key_column.into(),
        }
    }

    /// Number of shards.
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// The key column rows are sharded by.
    pub fn key_column(&self) -> &str {
        &self.key_column
    }

    /// Register a view on every shard.
    pub fn register_view(&self, spec: IncrementalViewSpec) -> IvmResult<()> {
        for shard in &self.shards {
            shard.register_view(spec.clone())?;
        }
        Ok(())
    }

    /// Feed a delta, routing each row to its shard by the key column.
    pub fn feed(&self, source: &str, delta: DeltaBatch) -> IvmResult<()> {
        if self.shards.len() == 1 {
            return self.shards[0].feed(source, delta);
        }
        // Split the weighted inner batch by the key column using the shared
        // keyed partitioner (`take` preserves the trailing `_weight` column).
        let inner = delta.inner().clone();
        let routed = partition_record_batches_by_key(&[inner], &self.key_column, self.shards.len())
            .map_err(|e| IvmError::execution(e.to_string()))?;
        for (shard_idx, batches) in routed.into_iter().enumerate() {
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let shard_delta =
                    DeltaBatch::from_weighted(batch).map_err(|e| IvmError::execution(e.to_string()))?;
                self.shards[shard_idx].feed(source, shard_delta)?;
            }
        }
        Ok(())
    }

    /// Advance every shard one tick, in parallel.
    pub async fn step_datafusion(&self) -> IvmResult<StepSummary> {
        let results =
            futures::future::try_join_all(self.shards.iter().map(|s| s.step_datafusion())).await?;
        let mut merged = StepSummary::default();
        for r in results {
            merged.active_views = merged.active_views.max(r.active_views);
            merged.total_output_rows += r.total_output_rows;
        }
        Ok(merged)
    }

    /// Read a view's snapshot, concatenating the per-shard partial results.
    ///
    /// For a `GROUP BY <key>` view sharded by `<key>`, each group lives entirely
    /// in one shard, so concatenation is the complete, correct result with no
    /// cross-shard merge.
    pub fn snapshot(&self, view: &str) -> IvmResult<Option<RecordBatch>> {
        let mut parts: Vec<RecordBatch> = Vec::new();
        for shard in &self.shards {
            if let Some(snap) = shard.snapshot(view)? {
                if snap.num_rows() > 0 {
                    parts.push(snap);
                }
            }
        }
        if parts.is_empty() {
            return Ok(None);
        }
        let schema = parts[0].schema();
        let merged = arrow::compute::concat_batches(&schema, &parts)
            .map_err(|e| IvmError::execution(e.to_string()))?;
        Ok(Some(merged))
    }

    /// Current tick count (shards advance together).
    pub fn tick(&self) -> IvmResult<u64> {
        self.shards.first().map(|s| s.tick()).unwrap_or(Ok(0))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn orders(regions: &[&str], amounts: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(regions.to_vec())),
                Arc::new(Int64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn revenue_spec() -> IncrementalViewSpec {
        IncrementalViewSpec {
            name: "revenue".into(),
            body_sql: "SELECT region, SUM(amount) AS total FROM orders GROUP BY region".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("total", DataType::Float64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        }
    }

    /// A 3-shard partitioned flow yields the same per-region totals as 1 shard.
    #[tokio::test]
    async fn partitioned_group_by_matches_single_flow() {
        let data = orders(
            &["US", "EU", "US", "APAC", "EU", "US"],
            &[100, 50, 25, 10, 75, 5],
        );

        // Reference: single flow.
        let single = PartitionedIncrementalFlow::new(1, "region");
        single.register_view(revenue_spec()).unwrap();
        single
            .feed("orders", DeltaBatch::from_inserts(data.clone()).unwrap())
            .unwrap();
        single.step_datafusion().await.unwrap();
        let ref_snap = single.snapshot("revenue").unwrap().unwrap();

        // Partitioned: 3 shards by region.
        let part = PartitionedIncrementalFlow::new(3, "region");
        assert_eq!(part.num_shards(), 3);
        part.register_view(revenue_spec()).unwrap();
        part.feed("orders", DeltaBatch::from_inserts(data).unwrap())
            .unwrap();
        part.step_datafusion().await.unwrap();
        let part_snap = part.snapshot("revenue").unwrap().unwrap();

        // Same total rows (one per region) and same grand total.
        assert_eq!(ref_snap.num_rows(), part_snap.num_rows());
        let grand = |b: &RecordBatch| -> f64 {
            b.column(1)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap_or(0.0))
                .sum()
        };
        assert_eq!(grand(&ref_snap), grand(&part_snap));
        assert_eq!(grand(&part_snap), 265.0); // 100+50+25+10+75+5
    }
}
