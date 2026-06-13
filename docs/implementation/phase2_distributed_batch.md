# Phase 2: Make Distributed Batch Unbreakable

Goal: reliably run large batch SQL jobs under memory pressure, executor loss,
and coordinator restart. This is the tracking list for Phase 2 of the engine
roadmap. Each item lists its current state in the codebase so the list stays
grounded; update the checkboxes and state notes as work lands.

Legend: `[x]` done, `[~]` partial, `[ ]` not started.

## 2.1 Production memory manager

- [x] Per-task memory accounting primitive
      (`krishiv-common/src/memory_budget.rs`, `MemoryBudget`).
- [x] Per-task limit on the wire
      (`ExecutorTaskAssignment::memory_limit_bytes`, `krishiv-proto`).
- [x] Wire a bounded DataFusion memory pool into `SqlEngine` so all
      DataFusion operators (sort, hash join, aggregation) run under a real
      limit instead of unbounded (`SqlEngine::new_with_memory_limit`,
      `FairSpillPool` + default disk manager in `build_local`).
- [x] Pass the task `MemoryBudget` limit into the per-task engines built in
      `krishiv-executor/src/fragment/{batch,streaming}.rs`.
- [x] Env-configurable default query memory limit
      (`KRISHIV_QUERY_MEMORY_LIMIT_BYTES`).
- [ ] Executor-level (whole-process) memory limit shared across concurrent
      task slots, not just per-task limits.
- [ ] Memory metrics per operator surfaced through `krishiv-metrics`.
- [ ] Admission control in the coordinator based on memory estimates.

## 2.2 Spillable joins, sorts, and aggregations

- [x] DataFusion-executed fragments spill via the bounded pool + disk
      manager (covered by 2.1 wiring; DataFusion sort/agg/join spill when the
      pool is exhausted).
- [~] Krishiv-native dataflow operators: `HashJoin` enforces budget but
      errors with `ResourceExhausted` instead of spilling
      (`krishiv-dataflow/src/join.rs`); `CepOperator` evicts LRU keys.
- [ ] Spill path for `krishiv-dataflow` sort (`sort.rs` is in-memory only).
- [ ] Budget support + spill for `LocalAggregator`
      (`krishiv-dataflow/src/aggregate.rs`).
- [ ] Spill metrics (bytes spilled, spill file count) per task.

## 2.3 Distributed file/table writes

- [~] Per-partition object-store parquet sink exists
      (`OBJECT_PARQUET_SINK` fragments in `krishiv-executor/src/fragment/`),
      but partitions commit independently — no atomic publication.
- [ ] Replace client-side collect-then-write in
      `krishiv-api/src/dataframe.rs` (`write_parquet`/`write_csv`/
      `write_json`) with a distributed sink stage for distributed sessions.
- [ ] Temp-file + coordinator-commit protocol (write to staging paths,
      atomic rename/manifest publish on job success, cleanup on abort).
- [ ] Write modes: append, overwrite, error-if-exists, ignore.
- [ ] Partitioned writes (`PARTITION BY` columns → directory layout).
- [ ] Idempotent recovery: re-run of a failed write must not duplicate or
      corrupt output.

## 2.4 Shuffle retry and corruption handling

- [x] BLAKE3 content hashes written and validated on read
      (`krishiv-shuffle/src/disk_store.rs`, `object_store.rs`,
      `tiered_store.rs`).
- [x] Shuffle fetch retry with exponential backoff for transient transport
      failures (`FlightShuffleClient::fetch_with_retry`,
      `KRISHIV_SHUFFLE_FETCH_RETRIES` / `KRISHIV_SHUFFLE_FETCH_RETRY_BASE_MS`),
      used by `read_shuffle_flight_partitions` in `krishiv-executor`.
- [ ] Missing-partition recovery: when a fetch fails with NotFound, the
      consumer task fails; the scheduler should re-run the producing stage
      (upstream recompute) instead of only retrying the consumer task.
- [ ] Disk-full behavior for local shuffle (clear error + task failure,
      not process abort).
- [ ] Fetch concurrency limits / object-store request throttling.

## 2.5 Executor loss recovery

- [x] Heartbeat timeout marks executors Lost
      (`krishiv-scheduler/src/heartbeat.rs`).
- [x] Running tasks on a lost executor reset to Pending and reassigned,
      capped at `MAX_EXECUTOR_LOSSES_BEFORE_FAIL = 5`
      (`coordinator/executor_ops.rs::reset_running_tasks_for_lost_executor`).
- [ ] Invalidate shuffle outputs produced by a lost executor and re-run the
      producing stage proactively (today consumers discover the loss only
      when their fetch fails).
- [ ] Duplicate task attempt fencing: late completion of a superseded
      attempt must not overwrite the winning attempt's output.

## 2.6 Coordinator restart recovery

- [x] Jobs, executors, and checkpoint state restored from `MetadataStore`
      on restart (`coordinator/recovery.rs::recover_from_store`).
- [x] Re-attach grace period so executors reconnect without being evicted.
- [ ] Post-restart shuffle availability audit: verify upstream partitions
      still exist before resuming downstream stages.
- [ ] Failure-injection test loop: kill coordinator at randomized points in
      batch jobs and assert convergence (extend `krishiv-common` chaos suite).

## 2.7 Statistics and cost-based optimization

- [~] Table row counts collected at registration and stamped onto scan
      nodes for `BroadcastAutoRule` (`krishiv-sql` `table_row_counts`).
- [~] `StaticCostModel` exists with hardcoded per-operator coefficients;
      no production cost model (`krishiv-plan/src/optimizer.rs`).
- [ ] Column-level statistics (min/max/null-count/NDV) from Parquet
      metadata at registration.
- [ ] Cardinality estimation for filter selectivity and join output size.
- [ ] Join reordering based on estimated cardinalities.

## 2.8 Skew handling

- [x] Hot-key detection during shuffle write (`HeavyHittersTracker`,
      SpaceSaving; reports flow to the coordinator).
- [x] `ThresholdSkewRule::detect_hot_partitions` flags skewed partitions
      from runtime stats.
- [ ] Mitigation: split skewed partitions into sub-partitions (salting or
      range-splitting) and replicate the small join side accordingly.
      Detection currently only logs; `skew_repartition_overrides` is never
      populated.

## 2.9 Adaptive query execution

- [x] AQE rule framework (`AqeOptimizer`, `RuntimeStats`,
      `AutoPartitionRule`, `CoalesceRule`, `StreamingAqeGuard`).
- [~] Applied at plan-submission time only; no mid-job re-optimization
      between stages when fresh `RuntimeStats` arrive.
- [ ] Stage-boundary re-optimization: after each shuffle-producing stage
      completes, re-run guarded AQE rules with observed partition sizes
      before scheduling the consumer stage.
- [ ] Runtime broadcast-join demotion/promotion from observed sizes.

## 2.10 TPC-H benchmarks at increasing scales

- [~] TPC-H Q1 and Q6 at SF10 via criterion
      (`krishiv-bench/benches/tpch_sf10.rs`, needs
      `KRISHIV_TPCH_DATA_DIR`); Nexmark Q1/Q2/Q5/Q8 for streaming.
- [ ] Remaining TPC-H queries (start with join-heavy Q3, Q5, Q10, Q18).
- [ ] Distributed-mode benchmark runs (multi-executor), not only embedded.
- [ ] Scale ladder (SF1 → SF10 → SF100) with published results per release.

## Suggested execution order

1. ~~Memory pool wiring + per-task limits (2.1/2.2 DataFusion path)~~ — done.
2. ~~Shuffle fetch retry (2.4)~~ — done.
3. Upstream-stage recompute on missing shuffle partitions (2.4/2.5) — the
   single biggest reliability gap left.
4. Distributed write commit protocol (2.3).
5. Stage-boundary AQE re-optimization (2.9), then skew mitigation (2.8).
6. Column statistics + cardinality estimation (2.7).
7. Broaden TPC-H coverage and add a distributed harness (2.10).
