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

use std::collections::HashMap;
use std::sync::Mutex;

use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use krishiv_common::partition::{partition_record_batches_by_key, recommend_buckets_default};
use krishiv_delta::{
    DeltaBatch, IncrementalViewSpec, deserialize_delta_batch, differentiate, serialize_delta_batch,
};

use crate::error::{IvmError, IvmResult};
use crate::flow::{IncrementalFlow, StepSummary};
use crate::plan::partition_key_for_view;

/// An [`IncrementalFlow`] sharded by a key column across `N` partitions.
pub struct PartitionedIncrementalFlow {
    shards: Vec<IncrementalFlow>,
    key_column: String,
    /// Per-source previous snapshot for [`feed_snapshot`](Self::feed_snapshot).
    /// Held at the partitioned level so differentiation happens once, before
    /// routing — see that method for why. Participates in checkpoint/restore.
    streaming_prev: Mutex<HashMap<String, RecordBatch>>,
}

impl PartitionedIncrementalFlow {
    /// Create a partitioned flow with `num_shards` shards keyed on `key_column`.
    pub fn new(num_shards: usize, key_column: impl Into<String>) -> Self {
        let n = num_shards.max(1);
        Self {
            shards: (0..n).map(|_| IncrementalFlow::new()).collect(),
            key_column: key_column.into(),
            streaming_prev: Mutex::new(HashMap::new()),
        }
    }

    /// Auto-size shard count for `total_bytes` of expected input, capped at
    /// `max_shards`. Delegates to the unified sizing brain
    /// ([`recommend_buckets`](krishiv_common::partition::recommend_buckets),
    /// AP-1) so batch, streaming, and IVM all agree on bytes-per-shard.
    pub fn recommended_shards(total_bytes: u64, max_shards: usize) -> usize {
        let cap = u32::try_from(max_shards.max(1)).unwrap_or(u32::MAX);
        recommend_buckets_default(total_bytes, 1, cap) as usize
    }

    /// Build a partitioned flow for a view, automatically — the zero-config IVM
    /// entry point.
    ///
    /// Inspects `spec.body_sql` with [`partition_key_for_view`]: if the view is a
    /// single-column `GROUP BY` aggregate, it shards by that key, sized by
    /// `recommended_shards(total_bytes_hint, max_shards)`; otherwise it falls
    /// back to a single flow. Either way the returned flow has the view
    /// registered and is ready to `feed`.
    pub async fn auto_for_view(
        ctx: &SessionContext,
        spec: IncrementalViewSpec,
        total_bytes_hint: u64,
        max_shards: usize,
    ) -> IvmResult<Self> {
        let flow = match partition_key_for_view(ctx, &spec.body_sql).await {
            Some(key) => {
                let shards = Self::recommended_shards(total_bytes_hint, max_shards);
                Self::new(shards, key)
            }
            None => Self::new(1, String::new()),
        };
        flow.register_view(spec)?;
        Ok(flow)
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

    /// Enable delta-checkpoint accumulation on every shard.
    pub fn enable_delta_checkpoints(&self) -> IvmResult<()> {
        for shard in &self.shards {
            shard.enable_delta_checkpoints()?;
        }
        Ok(())
    }

    /// Enable content-addressed input dedup on every shard.
    pub fn enable_input_dedup(&self) -> IvmResult<()> {
        for shard in &self.shards {
            shard.enable_input_dedup()?;
        }
        Ok(())
    }

    /// Feed a delta, routing each row to its shard by the key column.
    pub fn feed(&self, source: &str, delta: DeltaBatch) -> IvmResult<()> {
        if self.shards.len() == 1 {
            return self
                .shards
                .first()
                .ok_or_else(|| IvmError::execution("no shards".to_string()))?
                .feed(source, delta);
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
                let shard_delta = DeltaBatch::from_weighted(batch)
                    .map_err(|e| IvmError::execution(e.to_string()))?;
                self.shards
                    .get(shard_idx)
                    .ok_or_else(|| IvmError::execution(format!("shard {shard_idx} out of range")))?
                    .feed(source, shard_delta)?;
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

    /// Feed a full streaming snapshot, partitioned-correctly.
    ///
    /// Unlike `feed`, this does **not** route the raw snapshot to per-shard
    /// `feed_snapshot` — that would break the drain case (a key whose rows all
    /// disappear produces an empty per-shard sub-snapshot, which the shard-level
    /// diff treats as "no new data" rather than "retract all"). Instead it
    /// differentiates the whole snapshot **once** at this level (owning
    /// `streaming_prev`), then routes the resulting delta — insertions *and*
    /// retractions, each carrying its key — to shards via `feed`. Retractions
    /// route to the same shard their insertions did, so drains are correct.
    pub fn feed_snapshot(&self, source: &str, batches: &[RecordBatch]) -> IvmResult<()> {
        let non_empty: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
        // A truly empty call (no batches) is a no-op; a 0-row batch with schema
        // would be a drain-to-empty, but the HTTP bridge never sends those.
        if non_empty.is_empty() {
            return Ok(());
        }
        let first = non_empty
            .first()
            .ok_or_else(|| IvmError::execution("empty".to_string()))?;
        let schema = first.schema();
        let new_snapshot = if non_empty.len() == 1 {
            (*first).clone()
        } else {
            arrow::compute::concat_batches(&schema, non_empty.iter().copied())
                .map_err(|e| IvmError::execution(e.to_string()))?
        };

        let delta = {
            let mut prev = self.streaming_prev.lock().map_err(|_| lock_err())?;
            let d = differentiate(&schema, prev.get(source), &new_snapshot)
                .map_err(|e| IvmError::execution(e.to_string()))?;
            prev.insert(source.to_string(), new_snapshot);
            d
        };
        if delta.is_empty() {
            return Ok(());
        }
        self.feed(source, delta)
    }

    /// Drop a view from every shard. Returns `true` if it existed on any shard.
    pub fn drop_view(&self, name: &str) -> IvmResult<bool> {
        let mut dropped = false;
        for shard in &self.shards {
            dropped |= shard.drop_view(name)?;
        }
        Ok(dropped)
    }

    /// Read a view's materialized snapshot, concatenating per-shard partials.
    ///
    /// For a `GROUP BY <key>` view sharded by `<key>`, each group lives entirely
    /// in one shard, so concatenation is the complete, correct result with no
    /// cross-shard merge.
    pub fn snapshot(&self, view: &str) -> IvmResult<Option<RecordBatch>> {
        self.concat_per_shard(|s| s.snapshot(view))
    }

    pub fn view_spec(&self, view: &str) -> IvmResult<Option<IncrementalViewSpec>> {
        // All shards carry the same spec; read from shard 0.
        self.shards
            .first()
            .map(|s| s.view_spec(view))
            .transpose()
            .map(|o| o.flatten())
    }

    /// Read a source/view snapshot from the per-source map (the surface the
    /// coordinator's `/snap` endpoint reads), concatenating per-shard partials.
    pub fn source_snapshot(&self, name: &str) -> IvmResult<Option<RecordBatch>> {
        self.concat_per_shard(|s| s.source_snapshot(name))
    }

    /// Concatenate a per-shard `Option<RecordBatch>` getter into one batch.
    fn concat_per_shard(
        &self,
        get: impl Fn(&IncrementalFlow) -> IvmResult<Option<RecordBatch>>,
    ) -> IvmResult<Option<RecordBatch>> {
        let mut parts: Vec<RecordBatch> = Vec::new();
        for shard in &self.shards {
            if let Some(b) = get(shard)?
                && b.num_rows() > 0
            {
                parts.push(b);
            }
        }
        if parts.is_empty() {
            return Ok(None);
        }
        let schema = parts
            .first()
            .ok_or_else(|| IvmError::execution("empty parts".to_string()))?
            .schema();
        let merged = arrow::compute::concat_batches(&schema, &parts)
            .map_err(|e| IvmError::execution(e.to_string()))?;
        Ok(Some(merged))
    }

    /// Current tick count (shards advance together).
    pub fn tick(&self) -> IvmResult<u64> {
        self.shards.first().map(|s| s.tick()).unwrap_or(Ok(0))
    }

    /// Peek a view's latest output delta, merging the per-shard deltas.
    ///
    /// Each shard emits the portion of the view output for the keys it owns;
    /// concatenating the shards' current values gives the combined latest delta.
    /// (For exact materialized state prefer [`snapshot`](Self::snapshot) — output
    /// deltas are tick-relative, so a quiet shard may report an older tick.)
    pub fn view_output_peek(&self, view: &str) -> IvmResult<Option<DeltaBatch>> {
        let mut parts: Vec<DeltaBatch> = Vec::new();
        for shard in &self.shards {
            if let Some(d) = shard.view_output_peek(view)?
                && !d.is_empty()
            {
                parts.push(d);
            }
        }
        if parts.is_empty() {
            return Ok(None);
        }
        let merged = DeltaBatch::concat(&parts).map_err(|e| IvmError::execution(e.to_string()))?;
        Ok(Some(merged))
    }

    /// Spawn a vector-view background task on **every shard**, all writing to the
    /// same shared sink.
    ///
    /// For a `GROUP BY <key>` view sharded by `<key>`, each id (the group key)
    /// lives in exactly one shard, so the shards push disjoint id sets to the
    /// shared sink with no cross-shard conflict. Returns one join handle per
    /// shard; drop them to stop the tasks.
    pub fn spawn_vector_views(
        &self,
        spec: crate::vector_sink::VectorViewSpec,
    ) -> IvmResult<Vec<tokio::task::JoinHandle<()>>> {
        let mut handles = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            // One spec per shard, sharing the same sink (Arc clone).
            let shard_spec = crate::vector_sink::VectorViewSpec {
                view_name: spec.view_name.clone(),
                id_column: spec.id_column.clone(),
                vector_column: spec.vector_column.clone(),
                sink: spec.sink.clone(),
            };
            handles.push(crate::vector_sink::spawn_vector_view(shard, shard_spec)?);
        }
        Ok(handles)
    }

    // ── Checkpoint / restore ──────────────────────────────────────────────────
    //
    // Format: `u32 num_shards || (u32 len || shard_checkpoint)* || streaming_prev`
    // where streaming_prev = `u32 count || (u32 name_len||name || u32 len||ipc)*`.
    // Restore validates the shard count matches the live flow (the registry
    // re-creates the flow with its key/shape from the registered view first).

    /// Full checkpoint: every shard's source snapshots plus the streaming-prev
    /// map, framed with the shard count for restore-time validation.
    pub fn checkpoint(&self) -> IvmResult<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.shards.len() as u32).to_le_bytes());
        for shard in &self.shards {
            let bytes = shard.checkpoint()?;
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&bytes);
        }
        self.write_streaming_prev(&mut out)?;
        Ok(out)
    }

    /// Restore from [`checkpoint`](Self::checkpoint) bytes.
    pub fn restore(&self, bytes: &[u8]) -> IvmResult<()> {
        let mut pos = 0usize;
        let n = read_u32(bytes, &mut pos)? as usize;
        if n != self.shards.len() {
            return Err(IvmError::execution(format!(
                "checkpoint shard count {n} != live shard count {}",
                self.shards.len()
            )));
        }
        for shard in &self.shards {
            let len = read_u32(bytes, &mut pos)? as usize;
            let chunk = bytes.get(pos..pos + len).ok_or_else(slice_err)?;
            pos += len;
            shard.restore(chunk)?;
        }
        self.read_streaming_prev(bytes, &mut pos)?;
        Ok(())
    }

    /// Delta checkpoint: every shard's accumulated deltas, shard-count framed.
    pub fn checkpoint_delta(&self) -> IvmResult<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.shards.len() as u32).to_le_bytes());
        for shard in &self.shards {
            let bytes = shard.checkpoint_delta()?;
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&bytes);
        }
        Ok(out)
    }

    /// Restore from [`checkpoint_delta`](Self::checkpoint_delta) bytes.
    pub fn restore_delta(&self, bytes: &[u8]) -> IvmResult<()> {
        let mut pos = 0usize;
        let n = read_u32(bytes, &mut pos)? as usize;
        if n != self.shards.len() {
            return Err(IvmError::execution(format!(
                "delta checkpoint shard count {n} != live shard count {}",
                self.shards.len()
            )));
        }
        for shard in &self.shards {
            let len = read_u32(bytes, &mut pos)? as usize;
            let chunk = bytes.get(pos..pos + len).ok_or_else(slice_err)?;
            pos += len;
            shard.restore_delta(chunk)?;
        }
        Ok(())
    }

    fn write_streaming_prev(&self, out: &mut Vec<u8>) -> IvmResult<()> {
        let prev = self.streaming_prev.lock().map_err(|_| lock_err())?;
        out.extend_from_slice(&(prev.len() as u32).to_le_bytes());
        for (name, snap) in prev.iter() {
            let delta = DeltaBatch::from_inserts(snap.clone())
                .map_err(|e| IvmError::execution(e.to_string()))?;
            let ipc =
                serialize_delta_batch(&delta).map_err(|e| IvmError::execution(e.to_string()))?;
            out.extend_from_slice(&(name.len() as u32).to_le_bytes());
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(&(ipc.len() as u32).to_le_bytes());
            out.extend_from_slice(&ipc);
        }
        Ok(())
    }

    fn read_streaming_prev(&self, bytes: &[u8], pos: &mut usize) -> IvmResult<()> {
        let count = read_u32(bytes, pos)? as usize;
        let mut map: HashMap<String, RecordBatch> = HashMap::with_capacity(count);
        for _ in 0..count {
            let name_len = read_u32(bytes, pos)? as usize;
            let name = std::str::from_utf8(bytes.get(*pos..*pos + name_len).ok_or_else(slice_err)?)
                .map_err(|e| IvmError::execution(e.to_string()))?
                .to_string();
            *pos += name_len;
            let data_len = read_u32(bytes, pos)? as usize;
            let data = bytes.get(*pos..*pos + data_len).ok_or_else(slice_err)?;
            *pos += data_len;
            let delta =
                deserialize_delta_batch(data).map_err(|e| IvmError::execution(e.to_string()))?;
            let snap = delta
                .filter_positive()
                .map_err(|e| IvmError::execution(e.to_string()))?;
            map.insert(name, snap);
        }
        *self.streaming_prev.lock().map_err(|_| lock_err())? = map;
        Ok(())
    }
}

fn lock_err() -> IvmError {
    IvmError::execution("partitioned flow lock poisoned")
}

fn slice_err() -> IvmError {
    IvmError::execution("checkpoint byte slice out of bounds")
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> IvmResult<u32> {
    let raw = bytes.get(*pos..*pos + 4).ok_or_else(slice_err)?;
    *pos += 4;
    let arr: [u8; 4] = raw.try_into().map_err(|_| slice_err())?;
    Ok(u32::from_le_bytes(arr))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;

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

    /// `recommended_shards` reuses the AP-1 sizing brain: ~128 MiB/shard, ≥1,
    /// clamped to `max_shards`.
    #[test]
    fn recommended_shards_sizes_and_clamps() {
        // Tiny input → 1 shard.
        assert_eq!(PartitionedIncrementalFlow::recommended_shards(1_024, 8), 1);
        // ~5 * 128 MiB → 5 shards, under the cap.
        let five = 5 * 128 * 1024 * 1024;
        assert_eq!(PartitionedIncrementalFlow::recommended_shards(five, 16), 5);
        // Same bytes, capped at 3.
        assert_eq!(PartitionedIncrementalFlow::recommended_shards(five, 3), 3);
    }

    /// Register an empty `orders` table so view SQL is planner-resolvable.
    fn ctx_with_orders() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let table = MemTable::try_new(schema, vec![vec![]]).unwrap();
        ctx.register_table("orders", Arc::new(table)).unwrap();
        ctx
    }

    /// `auto_for_view` shards a single-column GROUP BY by its key, sized by bytes.
    #[tokio::test]
    async fn auto_for_view_shards_single_group_by() {
        let ctx = ctx_with_orders();
        let two = 2 * 128 * 1024 * 1024;
        let flow = PartitionedIncrementalFlow::auto_for_view(&ctx, revenue_spec(), two, 8)
            .await
            .unwrap();
        assert_eq!(flow.key_column(), "region");
        assert_eq!(flow.num_shards(), 2);
    }

    /// A non-aggregate (pass-through) view is not shardable → single flow.
    #[tokio::test]
    async fn auto_for_view_falls_back_to_single_flow() {
        let ctx = ctx_with_orders();
        let spec = IncrementalViewSpec {
            name: "passthrough".into(),
            body_sql: "SELECT region, amount FROM orders".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("amount", DataType::Int64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        };
        let huge = 100 * 128 * 1024 * 1024;
        let flow = PartitionedIncrementalFlow::auto_for_view(&ctx, spec, huge, 8)
            .await
            .unwrap();
        assert_eq!(flow.num_shards(), 1);
    }

    /// A two-column GROUP BY is not safely single-key shardable → single flow.
    #[tokio::test]
    async fn auto_for_view_multi_key_group_by_not_sharded() {
        let ctx = ctx_with_orders();
        let key = partition_key_for_view(
            &ctx,
            "SELECT region, amount, COUNT(*) AS n FROM orders GROUP BY region, amount",
        )
        .await;
        assert_eq!(key, None);
    }

    /// Schema-free key detection mirrors the planner-based rule.
    #[test]
    fn partition_key_from_sql_detects_single_group_by() {
        use crate::partition_key_from_sql;
        assert_eq!(
            partition_key_from_sql("SELECT region, SUM(amount) FROM orders GROUP BY region")
                .as_deref(),
            Some("region")
        );
        // Qualified column → bare name.
        assert_eq!(
            partition_key_from_sql(
                "SELECT o.region, SUM(o.amount) FROM orders o GROUP BY o.region"
            )
            .as_deref(),
            Some("region")
        );
        // Multi-column GROUP BY, no GROUP BY, and garbage all decline.
        assert_eq!(
            partition_key_from_sql("SELECT region, amount FROM orders GROUP BY region, amount"),
            None
        );
        assert_eq!(
            partition_key_from_sql("SELECT region, amount FROM orders"),
            None
        );
        assert_eq!(partition_key_from_sql("not valid sql"), None);
    }

    /// Exhaustive shape coverage for the schema-free key detector.
    #[test]
    fn partition_key_from_sql_shape_coverage() {
        let key = crate::partition_key_from_sql;

        // Accepts: GROUP BY survives HAVING / ORDER BY / LIMIT / WHERE.
        assert_eq!(
            key("SELECT region, SUM(amount) t FROM orders WHERE amount > 0 \
                 GROUP BY region HAVING SUM(amount) > 10 ORDER BY t LIMIT 5")
            .as_deref(),
            Some("region")
        );
        // Case-insensitive keywords; column name preserved verbatim.
        assert_eq!(
            key("select Region, count(*) from orders group by Region").as_deref(),
            Some("Region")
        );

        // Rejects: multi-statement, set ops, CTEs (outer body isn't a Select),
        // GROUP BY an expression, GROUP BY ALL/ROLLUP, no GROUP BY, empty.
        assert_eq!(key("SELECT 1; SELECT 2"), None);
        assert_eq!(
            key("SELECT region FROM a GROUP BY region UNION SELECT region FROM b GROUP BY region"),
            None
        );
        assert_eq!(
            key("WITH t AS (SELECT region FROM orders GROUP BY region) SELECT * FROM t"),
            None
        );
        assert_eq!(
            key("SELECT date_trunc('day', ts) d, COUNT(*) FROM e GROUP BY date_trunc('day', ts)"),
            None
        );
        assert_eq!(key("SELECT COUNT(*) FROM orders"), None);
        assert_eq!(key(""), None);
        assert_eq!(key("   "), None);
    }

    /// Checkpoint a partitioned flow, restore into a fresh one of the same shape,
    /// and confirm the materialized snapshot survives the round-trip.
    #[tokio::test]
    async fn checkpoint_restore_round_trips_across_shards() {
        let data = orders(&["US", "EU", "US", "APAC"], &[100, 50, 25, 10]);
        let src = PartitionedIncrementalFlow::new(3, "region");
        src.register_view(revenue_spec()).unwrap();
        src.feed("orders", DeltaBatch::from_inserts(data).unwrap())
            .unwrap();
        src.step_datafusion().await.unwrap();
        // checkpoint() persists the fed source state (sharded across flows);
        // concatenated it is the full `orders` snapshot.
        let before = src.source_snapshot("orders").unwrap().unwrap();
        assert_eq!(before.num_rows(), 4);

        let bytes = src.checkpoint().unwrap();

        // Fresh flow of the same shape (registry re-creates this from the view).
        let restored = PartitionedIncrementalFlow::new(3, "region");
        restored.register_view(revenue_spec()).unwrap();
        restored.restore(&bytes).unwrap();
        let after = restored.source_snapshot("orders").unwrap().unwrap();

        // Every source row survives the round-trip across all shards.
        assert_eq!(before.num_rows(), after.num_rows());
    }

    /// Restoring a checkpoint with a mismatched shard count is rejected.
    #[test]
    fn restore_rejects_shard_count_mismatch() {
        let src = PartitionedIncrementalFlow::new(3, "region");
        let bytes = src.checkpoint().unwrap();
        let wrong = PartitionedIncrementalFlow::new(2, "region");
        assert!(wrong.restore(&bytes).is_err());
    }

    /// `feed_snapshot` drains correctly: when a key's rows vanish from the
    /// snapshot, the retraction routes to its shard and the group disappears.
    #[tokio::test]
    async fn feed_snapshot_drains_vanished_keys() {
        let part = PartitionedIncrementalFlow::new(3, "region");
        part.register_view(revenue_spec()).unwrap();

        // Tick 1: US + EU present.
        part.feed_snapshot("orders", &[orders(&["US", "EU"], &[100, 50])])
            .unwrap();
        part.step_datafusion().await.unwrap();
        let snap1 = part.snapshot("revenue").unwrap().unwrap();
        assert_eq!(snap1.num_rows(), 2);

        // Tick 2: EU gone, US changed. Snapshot is now just US.
        part.feed_snapshot("orders", &[orders(&["US"], &[200])])
            .unwrap();
        part.step_datafusion().await.unwrap();
        let snap2 = part.snapshot("revenue").unwrap().unwrap();
        // EU's group must have been retracted from its shard.
        assert_eq!(snap2.num_rows(), 1);
    }

    // ── Constructor / sizing edge cases ───────────────────────────────────────

    #[test]
    fn new_clamps_zero_shards_to_one() {
        let f = PartitionedIncrementalFlow::new(0, "region");
        assert_eq!(f.num_shards(), 1);
        assert_eq!(f.key_column(), "region");
    }

    #[test]
    fn recommended_shards_edge_caps() {
        // Zero max_shards is coerced to 1.
        assert_eq!(
            PartitionedIncrementalFlow::recommended_shards(1 << 40, 0),
            1
        );
        // Zero bytes → 1 shard regardless of cap.
        assert_eq!(PartitionedIncrementalFlow::recommended_shards(0, 16), 1);
        // Saturates at the cap, never overflows.
        assert_eq!(
            PartitionedIncrementalFlow::recommended_shards(u64::MAX, 4),
            4
        );
    }

    // ── feed() routing edge cases ─────────────────────────────────────────────

    #[test]
    fn feed_empty_delta_is_noop() {
        let part = PartitionedIncrementalFlow::new(3, "region");
        part.register_view(revenue_spec()).unwrap();
        let empty = DeltaBatch::from_inserts(orders(&[], &[])).unwrap();
        // No rows to route → no panic, no error.
        part.feed("orders", empty).unwrap();
        assert_eq!(part.tick().unwrap(), 0);
    }

    #[test]
    fn feed_missing_key_column_errors_when_sharded() {
        // A delta whose batch lacks the shard key column → routing error, not panic.
        let part = PartitionedIncrementalFlow::new(3, "region");
        part.register_view(revenue_spec()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let delta = DeltaBatch::from_inserts(batch).unwrap();
        assert!(part.feed("orders", delta).is_err());
    }

    #[test]
    fn feed_null_key_errors_when_sharded() {
        let part = PartitionedIncrementalFlow::new(3, "region");
        part.register_view(revenue_spec()).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("US"), None])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let delta = DeltaBatch::from_inserts(batch).unwrap();
        assert!(part.feed("orders", delta).is_err());
    }

    #[test]
    fn single_shard_feed_tolerates_absent_key_column() {
        // With one shard there is no routing, so a missing key column is fine —
        // this is the auto-rule's non-shardable fallback path.
        let part = PartitionedIncrementalFlow::new(1, "");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        part.feed("orders", DeltaBatch::from_inserts(batch).unwrap())
            .unwrap();
    }

    #[tokio::test]
    async fn more_shards_than_keys_leaves_some_empty() {
        // 16 shards, 2 distinct keys → most shards empty, result still correct.
        let part = PartitionedIncrementalFlow::new(16, "region");
        part.register_view(revenue_spec()).unwrap();
        part.feed(
            "orders",
            DeltaBatch::from_inserts(orders(&["US", "EU", "US"], &[10, 20, 30])).unwrap(),
        )
        .unwrap();
        part.step_datafusion().await.unwrap();
        let snap = part.snapshot("revenue").unwrap().unwrap();
        assert_eq!(snap.num_rows(), 2); // US, EU
    }

    // ── snapshot edge cases ───────────────────────────────────────────────────

    #[test]
    fn snapshot_unregistered_view_errors() {
        let part = PartitionedIncrementalFlow::new(3, "region");
        assert!(part.snapshot("nonexistent").is_err());
    }

    #[test]
    fn snapshot_before_any_step_is_none() {
        let part = PartitionedIncrementalFlow::new(3, "region");
        part.register_view(revenue_spec()).unwrap();
        assert!(part.snapshot("revenue").unwrap().is_none());
    }

    // ── checkpoint / restore edge cases ───────────────────────────────────────

    #[test]
    fn checkpoint_restore_empty_flow_round_trips() {
        let src = PartitionedIncrementalFlow::new(4, "region");
        src.register_view(revenue_spec()).unwrap();
        let bytes = src.checkpoint().unwrap();
        let dst = PartitionedIncrementalFlow::new(4, "region");
        dst.register_view(revenue_spec()).unwrap();
        dst.restore(&bytes).unwrap();
        assert!(dst.source_snapshot("orders").unwrap().is_none());
    }

    #[test]
    fn restore_truncated_bytes_errors_not_panics() {
        let dst = PartitionedIncrementalFlow::new(3, "region");
        assert!(dst.restore(&[]).is_err());
        assert!(dst.restore(&[1, 2]).is_err()); // shorter than a u32 header
        assert!(dst.restore_delta(&[0, 0]).is_err());
    }

    #[tokio::test]
    async fn delta_checkpoint_round_trips_across_shards() {
        let src = PartitionedIncrementalFlow::new(3, "region");
        src.enable_delta_checkpoints().unwrap();
        src.register_view(revenue_spec()).unwrap();
        src.feed(
            "orders",
            DeltaBatch::from_inserts(orders(&["US", "EU", "US"], &[1, 2, 3])).unwrap(),
        )
        .unwrap();
        src.step_datafusion().await.unwrap();
        let full = src.checkpoint().unwrap();
        let delta = src.checkpoint_delta().unwrap();

        // Full + delta restore into a fresh flow of the same shape.
        let dst = PartitionedIncrementalFlow::new(3, "region");
        dst.enable_delta_checkpoints().unwrap();
        dst.register_view(revenue_spec()).unwrap();
        dst.restore(&full).unwrap();
        dst.restore_delta(&delta).unwrap();
        // Round-trip does not panic and source rows are present.
        assert_eq!(
            dst.source_snapshot("orders").unwrap().unwrap().num_rows(),
            3
        );
    }

    #[test]
    fn checkpoint_delta_without_enable_is_empty_frame() {
        let src = PartitionedIncrementalFlow::new(3, "region");
        src.register_view(revenue_spec()).unwrap();
        let delta = src.checkpoint_delta().unwrap();
        // Restoring it is a no-op (per-shard count=0), never an error.
        let dst = PartitionedIncrementalFlow::new(3, "region");
        dst.register_view(revenue_spec()).unwrap();
        dst.restore_delta(&delta).unwrap();
    }

    // ── feed_snapshot edge cases ──────────────────────────────────────────────

    #[tokio::test]
    async fn feed_snapshot_identical_twice_is_stable() {
        let part = PartitionedIncrementalFlow::new(3, "region");
        part.register_view(revenue_spec()).unwrap();
        let snap = orders(&["US", "EU"], &[100, 50]);
        part.feed_snapshot("orders", std::slice::from_ref(&snap))
            .unwrap();
        part.step_datafusion().await.unwrap();
        let first = part.snapshot("revenue").unwrap().unwrap();
        // Identical snapshot again → empty delta → no change.
        part.feed_snapshot("orders", &[snap]).unwrap();
        part.step_datafusion().await.unwrap();
        let second = part.snapshot("revenue").unwrap().unwrap();
        assert_eq!(first.num_rows(), second.num_rows());
        assert_eq!(second.num_rows(), 2);
    }

    #[test]
    fn feed_snapshot_empty_batches_is_noop() {
        let part = PartitionedIncrementalFlow::new(3, "region");
        part.register_view(revenue_spec()).unwrap();
        part.feed_snapshot("orders", &[]).unwrap();
        part.feed_snapshot("orders", &[orders(&[], &[])]).unwrap();
        assert_eq!(part.tick().unwrap(), 0);
    }

    // ── output-watch peek + vector-view fan-out (partitioned endpoints) ────────

    #[test]
    fn view_output_peek_before_step_is_none() {
        let part = PartitionedIncrementalFlow::new(3, "region");
        part.register_view(revenue_spec()).unwrap();
        assert!(part.view_output_peek("revenue").unwrap().is_none());
    }

    #[tokio::test]
    async fn view_output_peek_merges_shard_deltas() {
        let part = PartitionedIncrementalFlow::new(3, "region");
        part.register_view(revenue_spec()).unwrap();
        part.feed(
            "orders",
            DeltaBatch::from_inserts(orders(&["US", "EU", "US"], &[10, 20, 30])).unwrap(),
        )
        .unwrap();
        part.step_datafusion().await.unwrap();
        // Two groups (US, EU) emitted, possibly from different shards → merged.
        let peek = part.view_output_peek("revenue").unwrap().unwrap();
        assert_eq!(peek.num_rows(), 2);
    }

    #[tokio::test]
    async fn spawn_vector_views_one_task_per_shard() {
        use crate::vector_sink::VectorViewSpec;
        use crate::vector_sink::testing::InMemoryVectorSink;

        let part = PartitionedIncrementalFlow::new(4, "region");
        part.register_view(revenue_spec()).unwrap();
        let spec = VectorViewSpec {
            view_name: "revenue".into(),
            id_column: "region".into(),
            vector_column: "v".into(),
            sink: InMemoryVectorSink::new(),
        };
        let handles = part.spawn_vector_views(spec).unwrap();
        assert_eq!(handles.len(), 4); // one background task per shard
        for h in handles {
            h.abort();
        }
    }

    #[tokio::test]
    async fn spawn_vector_views_errors_for_unregistered_view() {
        use crate::vector_sink::VectorViewSpec;
        use crate::vector_sink::testing::InMemoryVectorSink;

        let part = PartitionedIncrementalFlow::new(2, "region");
        let spec = VectorViewSpec {
            view_name: "missing".into(),
            id_column: "region".into(),
            vector_column: "v".into(),
            sink: InMemoryVectorSink::new(),
        };
        assert!(part.spawn_vector_views(spec).is_err());
    }
}
