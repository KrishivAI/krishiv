# Engine Production-Readiness Audit — 2026-07-10

Code-grounded audit of the engine across components, execution flow
(source → sink), the three compute engines (batch / delta-batch / streaming),
the three placements (embedded / single-node / distributed), and every API
surface (SQL, Rust, Python, Flight SQL, gateway, MCP, connectors). Every
claim cites code, not docs. This audit is the evidence base for the
platform plan's **Track 6 (phases 51–62): engine production readiness** —
the arc that takes the engine from "certified single-path" to a credible
Spark/Flink alternative for community adoption.

Builds on (does not repeat) the 2026-07-09 core-component audit in
`status.md` (AUD-1..10): AUD-1..4 + AUD-10 are fixed; **AUD-6, AUD-7
(aggregate keys), AUD-8, AUD-9 remain open and are re-confirmed in code
this pass.**

## 1. What the engine is today (verified shape)

- **Workspace**: 25 crates, ~260k LOC Rust, `#![forbid(unsafe_code)]`
  across core crates, 4 TODO/FIXME markers total outside tests —
  exceptional hygiene.
- **Three-engine spine** (`krishiv-engine-core/src/lib.rs`): Batch
  (Spark-style bounded SQL), Incremental (DBSP/Feldera-style IVM),
  Streaming (Flink-style event-time + keyed state) — each compiles to a
  `CompiledJob` run by a `ComputeEngine` over placement-injected
  `EngineRuntime` services. Engine × placement × API surface are three
  independent axes; `krishiv-api/src/{conformance,mode_conformance}.rs`
  test the contract.
- **Placements**: embedded (`krishiv-runtime/src/in_process.rs`),
  single-node daemon, distributed (coordinator daemon + executor CLI +
  optional shuffle/flight services). `just check-{embedded,single-node,
  distributed,k8s,full}` builds each.
- **Certified today**: exactly-once Kafka → continuous TUMBLE → Iceberg
  upsert through a mid-commit kill (G8, 2026-07-10, prod k3s) — one
  combination, honestly labeled via registry-published delivery metadata.

## 2. Distributed batch: the single-task ceiling (the #1 gap)

**Finding: a distributed batch SQL job is one stage with one task.**
`submit_batch_sql_job_inner` (`krishiv-scheduler/src/batch_sql.rs:~240`)
builds exactly `stage-sql` / `task-sql` with fragment `sql: <query>`. The
whole query executes on **one executor**; DataFusion parallelizes across
that node's cores, but adding executors adds zero speed to a single
query. "Distributed" batch SQL is remote execution, not scale-out.

Adjacent evidence:

- **The multi-stage path exists but is unreachable from SQL.**
  `job_spec_from_exchange_stages` (`krishiv-scheduler/src/job/scheduler.rs:691`)
  splits a `PhysicalPlan` at `NodeOp::Exchange` boundaries into
  ShuffleMap/Result stages — but tasks are created **one per plan node**,
  not per data partition, and the SQL→plan translation
  (`df_plan_to_krishiv_nodes`, `krishiv-sql/src/lib.rs:2672`) lowers most
  operators to `NodeOp::Other { description }` (display strings) and
  `DfPlan::Repartition` to `Partitioning::Unpartitioned`
  (`lib.rs:2796-2804`). The executor's batch dispatcher has **zero
  `NodeOp::` handling** (`krishiv-executor/src/fragment/batch.rs`) — the
  translated plan drives the optimizer (`BroadcastAutoRule`) and EXPLAIN,
  not execution.
- **Task fragments are strings.** `TypedTaskFragment` wraps
  `body: String` (`krishiv-plan/src/task_fragment.rs`); bodies are `sql:`,
  `stream:loop:`, `delta:step:{job}|{deltas_b64}|{specs_b64}|{state_b64}`.
  Inline tables ride as base64 Arrow IPC in the assignment
  (`BatchSqlInlineTable`). There is no partition-addressed,
  proto-encoded physical plan fragment — the prerequisite for
  partition-parallel task generation.
- **The shuffle crate is production-shaped but under-consumed.**
  `krishiv-shuffle` has hash/range partitioners, sort-shuffle writer +
  index, disk/object-store/tiered stores, an ESS binary, push-shuffle
  (T12), spillable buffers under `UnifiedMemoryManager`, and Arrow-IPC
  Flight transport. Its consumers are the in-memory shuffle fragments and
  hand-built plans — never a SQL query.

**SOTA direction** (phase 52): Ballista proves the architecture on the
same DataFusion base — protobuf plan fragments, stage-per-exchange,
task-per-partition, shuffle-service reads; its published TPC-H SF100
result is 2.9× single-node DataFusion. Iceberg split planning gives the
scan-side parallelism for free (file/split → task), and locality inputs.

## 2b. Batch execution flow, per-task overhead, and the Sail lesson (eighth pass, 2026-07-10)

End-to-end trace of a coordinated batch query (submit →
`submit_batch_sql_job` → single `task-sql` assignment → executor
`fragment/batch.rs` → results back through the coordinator), checked
against Sail (lakehq/sail) — a DataFusion-based Spark replacement
publishing ~4× overall / up to ~8× per-query (TPC-H-derived, 100 GB:
Q19 ~7.3×, Q6 ~6.2×) vs Spark with **zero shuffle spill** (Spark wrote
110 GB) and 22 GB brief peak vs 54 GB resident. Sail's wins come from
being Rust/Arrow/DataFusion-native — which Krishiv already is — so none
of that gap separates us; what matters is that Sail keeps data
**streaming end-to-end with no per-query setup tax**, and Krishiv's
distributed path currently violates that three ways:

- **Per-task engine setup.** Every task builds a fresh
  `SqlEngine`/SessionContext, re-registers UDFs, and re-registers the
  Iceberg REST catalog (`fragment/batch.rs:193-203`). This is exactly
  the SessionContext-per-tick overhead #102 exposed for IVM (fixed by
  G14 per-flow reuse) — batch still pays it per task, and Phase 52's
  N-tasks-per-query multiplies it by N, including per-task catalog
  metadata HTTP round-trips. Fix: executor-resident engine/session
  reuse (job-scoped or LRU) + a shared catalog client with cached table
  metadata.
- **Eager input materialization.** Only the local-parquet path registers
  a file-backed provider; shuffle-Flight, object-parquet, connector, and
  registry partitions are all **fully read into `Vec<RecordBatch>` and
  registered as MemTables before the query starts**
  (`fragment/batch.rs:212-265`). That defeats pipelined execution,
  discards predicate/projection pushdown into those scans, and holds
  entire inputs in RAM — the anti-Sail pattern (their zero-spill number
  is streaming Arrow exchange). Fix: streaming `TableProvider`s
  (shuffle reader as a `RecordBatchStream`; pushdown-capable providers
  for object/connector partitions). Phase 52's proto plan fragments
  make this the natural shape: the fragment carries the scan node, the
  executor builds a stream, never a MemTable.
- **Sink writes collect the whole result.** The object-parquet sink path
  calls `collect_with_stats()` — "Sink writes need the full batch set
  for partition splitting" (`fragment/batch.rs:283-291`) — so a large
  CTAS/INSERT holds its entire output in executor memory. The inline
  path is already streamed (execute_stream → spool decision, #156); the
  sink path must be too — the Phase 52 partition-aware fanout writer
  consumes the stream batch-by-batch.

What is already right: inline results stream through the spool decision
on the executor (#156), job-completion waits use Notify (no busy poll),
and per-task memory limits arm DataFusion spill. The remaining
coordinator-memory hop for inline results is the control-plane-only
invariant (§10), already owned by Phase 52.

Yardstick implication (Phase 51): publish an **engine-overhead
microbenchmark** — the same query via raw DataFusion vs embedded
session vs coordinated single-task — so the scheduling/serialization
tax is a tracked number with a budget, the way Sail publishes theirs
against Spark.

## 3. Scheduler: sound skeleton, algorithms parked in `cfg(test)`

Production placement is `SlotAwareScheduler` — greedy most-free-slots
(`krishiv-scheduler/src/job/scheduler.rs:173-233`). What exists but is
**not wired**:

- `LocalityScheduler` (`scheduler.rs:245`, `#[cfg(test)]`): node-local
  preference with greedy fallback; rack tier reserved. Tested, no
  production caller. `ExecutorPlacement::with_locality` is
  `#[expect(dead_code)]`.
- `FairScheduler` (`scheduler.rs:338`, `#[cfg(test)]`): namespace pools —
  and its weight/min-share math is dead code inside its own loop
  (`let _ = min_share…; let _ = total_weight` at `scheduler.rs:439-440`).
- `key_group_range_for_task` + `MAX_KEY_GROUPS = 32_768`
  (`scheduler.rs:15-32`, `#[cfg(test)]`): Flink-style key-group ranges —
  the foundation for parallel keyed streaming — computed nowhere live.

What IS live and good: SC10 resource-profile executor filtering, SC11
cascade circuit breaker + IMM-1 per-executor failure threshold, bounded
assignment fan-out (128 concurrent RPCs, env-tunable), per-endpoint
channel coalescing (#43/#44), round-robin delivery interleaving,
admission `QueueManager` with namespace quota snapshots, one-shot hot-key
`skew_repartition_overrides`, R7.2 adaptive governance types
(hot-key-split / repartition / source-throttle / slow-sink).

Missing entirely: delay scheduling, speculative execution for stragglers,
priority/preemption, task-level retry budgets distinct from stage retry
(P1.24 exists).

## 4. Distributed streaming: one task per job, cycles over RPC

- `prepare_continuous_input_cycle`
  (`krishiv-scheduler/src/coordinator/task_assignment.rs:285`) **requires
  exactly one `stream:loop` task per job** and fences one in-flight cycle.
  Streaming parallelism per job = 1 executor. Input arrives as
  coordinator-pushed partitions in the assignment payload; output drains
  back through the coordinator. There is no executor↔executor streaming
  data plane and no per-channel credit-based flow control (the
  `krishiv-common/backpressure` credits gate **source admission**, not
  network channels).
- **Barrier subsystem: fully built, zero live consumers** (G5 register,
  re-confirmed): `CheckpointCoordinator::try_tick` + 2s
  `barrier_dispatch_loop` run in production;
  `ExecutorTaskRunner::drain_pending_barriers`' only caller is a CLI debug
  command. Chandy-Lamport aligned two-input join
  (`execute_window_join_aligned`, `aligned_join.rs`) is unit-tested with
  no caller; live two-input joins rebuild the operator from scratch every
  cycle (`fragment/streaming.rs` → plain `execute_window_join`) — a
  continuous join loses state at every cycle boundary today.
- Checkpoints are **full-state per cycle**: the executor ships the whole
  operator snapshot each completed cycle; the coordinator persists it
  every cycle. `krishiv-state` has `incremental_checkpoint.rs`,
  `checkpoint/rescaling.rs`, `dfs_backend.rs`, `savepoint.rs`,
  `migration.rs`, TTL — the only reference to incremental checkpoints
  outside the crate is the MCP info endpoint. Built, not wired.

**SOTA direction** (phases 55–56): key-group-sharded continuous jobs;
wire the existing barrier pipeline as the checkpoint driver; Flink-2.0
style disaggregated state (DFS-primary + local cache — `dfs_backend.rs`
is the seed) with incremental checkpoints; savepoint compatibility
windows; unaligned checkpoints later (roadmap already names them).

## 4b. Streaming execution flow: the latency gap vs Flink/Arroyo (ninth pass, 2026-07-10)

Traced end-to-end on the production path: Kafka → platformd
`kafka_bridge` → HTTP `continuous-push` → coordinator task assignment
(gRPC) → executor cycle → per-cycle Iceberg commit → coordinator inline
result store → HTTP `continuous-drain` poll.

**The distributed path is stop-and-wait micro-batch through the control
plane; end-to-end latency is seconds by construction:**

- Nothing in the engine drives cycles. `should_consider_for_launch`
  explicitly excludes streaming jobs (`job_coordinator.rs:208` — a
  deliberate fix for the Phase-20 double-cycle race), so every cycle —
  including for engine-native registry sources — exists only because an
  external client POSTed `continuous-push`. The production driver is the
  platform bridge, which lingers up to **2 s** per cycle
  (`MAX_BATCH_LINGER`, platform `kafka_bridge/imp.rs:45`).
- Each cycle pays the full task-assignment machinery: coordinator write
  lock, launch, gRPC dispatch, dispatch-response bookkeeping
  (`api_continuous_push`, `continuous_stream_http.rs:510-596`); input
  rides base64 JSON over HTTP.
- Strict stop-and-wait fencing: one cycle in flight AND undrained output
  each 409 the next push (`prepare_continuous_input_cycle`,
  `task_assignment.rs:285-306`). Output sits in coordinator memory until
  the same external client polls `continuous-drain` — a consumer that
  stops draining halts the stream (the known bridge-wedge class).
- O(state) twice per cycle on the hot path: the full operator
  `snapshot()` ships in the task output and is persisted by the
  coordinator every cycle (§4), and a **fresh ephemeral RocksDB is
  built and loaded from those same bytes for queryable state each
  cycle** (`fragment/streaming.rs:594-604`).
- One Iceberg commit per cycle (`commit_cycle`,
  `fragment/streaming.rs:740`): catalog commit latency lands on the
  cycle path, and snapshot/small-file growth is per-cycle (feeds the
  §7c maintenance gap).

**Liveness/latency bugs (new this pass):**

- **The distributed loop has no idle tick.**
  `ContinuousWindowExecutor::tick()` (ST-4) has exactly one caller — the
  **embedded** loop (`krishiv-api/engines.rs:1185`). In distributed
  mode windows fire only when the *next* push advances the watermark: a
  topic that goes quiet never emits its final windows, and session
  windows never close on inactivity. Latency bug and a correctness/
  liveness bug in one.
- **`KRISHIV_STREAM_EARLY_FIRE_MS` is a silent no-op**:
  `emit_open_windows_speculative` returns `None` for the production
  state-backed operators by design (stub, `continuous.rs:589-614`) — a
  documented knob that does nothing. Wire-or-delete.
- Platform-side: the bridge **drops oldest buffered messages** at its
  hard cap (`imp.rs:196-204`) — at-most-once under backpressure until
  the certified feeder protocol lands (platform task #171).

**The low-latency architecture already exists in this codebase —
embedded placement only.** `run_streaming_continuous`
(`krishiv-api/engines.rs:1139`) is the Arroyo-shaped loop: long-running
task that owns its source, notify-driven wakeup (µs-class when the
source implements `data_notify()`), 50 µs idle floor / 5 ms fallback
tick, ST-4 idle tick, S-3 background checkpoints off the hot path, and
a `StreamProfile` latency-vs-throughput knob. The distributed
`stream:loop` path uses none of it — it replaced the loop with
coordinator round-trips.

**SOTA reference.** Flink: pipelined per-record dataflow; the latency
dial is the network buffer timeout (100 ms default), not a job restart;
a per-key timer service fires windows with no input required;
credit-based flow control; checkpoints incremental and off the hot
path. Arroyo (Rust/Arrow/DataFusion — Krishiv's own base): millisecond
latency; workers exchange Arrow batches over TCP; `BATCH_SIZE` /
`BATCH_LINGER_MS` is the explicit latency/throughput dial; Chandy-
Lamport checkpoints; the controller is control-plane-only. Krishiv's
embedded loop is architecturally equivalent to Arroyo's model; the
distributed cycle model is not, and no knob can fix it — the driver has
to move from external HTTP pushes to a source-owning executor loop.

**Direction (Phase 55, folded in as the low-latency-loop task):**
promote the embedded loop to the distributed runtime — the stream:loop
task launches once and runs, owns its source splits, wakes on data;
checkpoints happen at barrier epochs only; sink commits happen at epoch
boundaries on a configured interval; results stream to consumers
instead of parking in the coordinator; push/drain remains only as an
ingest/egress API, never the execution driver.

## 5. Delta-batch / IVM: correct now, doesn't scale yet

Re-confirmed open (post-AUD fixes):

- **AUD-6**: distributed tick ships full state both directions, base64 in
  a string fragment; `MAX_IVM_OFFLOAD_STATE_BYTES = 16 MiB`
  (`ivm_http.rs:387`) silently falls back to central compute — distributed
  IVM never scales past 16 MiB of state; coordinator persists the full
  snapshot every cycle.
- **AUD-7 (remainder)**: `GroupStateMap = AHashMap<Vec<Option<String>>, …>`
  (`krishiv-delta/src/operators/aggregate.rs:299`) — group keys are still
  string tuples (join **traces** got real state in #160; aggregate keys
  and trace probe masks remain string-based).
- **AUD-8**: insert-only snapshot fast path still `concat_batches(prev,
  delta)` (`operators/stream.rs:151`) — O(n) copy per tick, unbounded
  memory; **`register_lateness` still has zero callers**, so lateness GC
  (whose mask AUD-2 fixed) has never actually run.
- **AUD-9**: O(Δ) plan coverage = bare Aggregate / 2-way equi-Join /
  Distinct / WHERE-filtered aggregate; HAVING, agg-over-join, UNION,
  subqueries → DiffBased full recompute + full-output stringify-diff.
- Standing benchmark verdict (#102/G14): even after ctx-reuse, full
  recompute wins below ~23M rows. The IVM engine is a differentiator on
  paper it cannot yet cash at production scale.

## 5b. Security & durability P0s (external review 2026-07-10, code-verified)

An external engine review surfaced end-to-end security and durability
gaps this audit's first passes did not cover (they assumed the Phase-27/P0
security posture was adequate). Verified against the tree:

- **SEC-1 (real): HTTP auth bypass.** `coordinator_http_router`
  (`krishiv-scheduler/src/coordinator_daemon.rs:527-531`) layers the
  bearer middleware only on `protected`, then merges `ivm_routes` and
  `qs_routes` as siblings with no auth — unauthenticated IVM submission
  and raw queryable-state reads.
- **SEC-2 (real, asymmetry): Flight SQL authz.** Statement paths
  fail-closed (`service.rs:371/418/626`); `do_put_prepared_statement_update`
  (`:730`) and `do_action_fallback` (`:1150`) authenticate without the
  policy default-deny. (The review's "normal queries fail with no policy"
  is the intended fail-closed behavior, not a bug.)
- **DUR-1 (real, batch-sink path): false success.** Publish deferred
  outside the write lock (`grpc.rs`), failure only in-memory
  (`coordinator/mod.rs:1724`) → restart can persist `Succeeded` for
  unpublished output. Needs a persisted `Committing` state.
- **DUR-2 (real): prepared sink txns not in durable checkpoints.**
  `EpochTransactionLog.prepared` is in-memory (`two_phase.rs`);
  `CheckpointMetadata`/`CheckpointAckRequest` carry offsets, no
  participant/txn refs — offsets can restore past uncommitted output.
  General-path form of the honest G8 caveat (#171).
- **STALE — do not action as written:** the review's "G7 is an unsafe
  uncommitted draft that drops/recreates the table" predates the tree.
  G7 (`b2dec7b`) + CONN-3 (`9dd1fdf`) are committed; drop/recreate was
  replaced by the crash-safe `overwrite_commit`
  (`iceberg_native.rs:~297`, atomic version-hint flip); the Kafka→Iceberg
  kill-loop it demanded is the certified G8.

→ **Phase 63 (Track 6 GATE 0, P0)** — runs first, gates GA, precedes
Phase 31. Tasks SEC-1/SEC-2/DUR-1/DUR-2.

## 5c. Delta-batch execution flow: tick mechanics + the Feldera/RisingWave lessons (tenth pass, 2026-07-10)

Traced a tick end-to-end: `feed` → pending deltas → `step_datafusion`
(5-phase: drain/snapshot → dirty-bit toposort → plan-or-DiffBased per
view → apply → publish) and the distributed `delta:step` offload
(`fragment/ivm.rs`).

**The foundation is genuinely DBSP-shaped — better than §5's summary
implies:**

- `DeltaBatch` is a real weighted Z-set (Int64 weights, negate/concat/
  normalize/consolidate); `Trace` is an 8-level spine with cascade
  merge + consolidation (O(log N) amortized, `trace.rs:12`) — the same
  data structures Feldera's DBSP runtime uses.
- The DiffBased differ is **not** stringify-based (stale impression):
  `differentiate` uses Arrow `RowConverter` canonical rows
  (`operators/stream.rs:63`). It does allocate a `Vec<u8>` per row per
  side per tick (`.to_vec()` on every row key) — borrowable, worth
  fixing inside the AUD-7 row-format work.
- Central ticks reuse a cached per-flow `SessionContext` (G14) that is
  spill-configured by default (`tick_ctx` +
  `spill::spill_session_context`, `flow.rs:715-720`); dirty-bit
  scheduling skips clean views.

**New bugs (code-verified):**

- **Silent numeric coercion in aggregates**: the accumulator parses the
  stringified input value with `.unwrap_or(0.0)`
  (`operators/aggregate.rs:235`) — an unparseable/non-numeric value
  contributes **0 to SUM/AVG instead of erroring**. Wrong answers, no
  diagnostic.
- **AUD-7 is understated**: not only group keys — **aggregate input
  values** are stringified and re-parsed per row per tick
  (`input_val_str.parse::<f64>()`), and MIN/MAX keep an in-RAM
  multiset of every distinct value per group (`min_max_set`,
  BTreeMap<OrdF64, i64>) — correct retraction semantics, unbounded
  memory, no state backend, invisible to any budget.
- **The offloaded tick rebuilds the world**: the executor builds a
  transient flow per tick — re-registers views, `restore_full`
  (O(state) in), **fresh SessionContext per tick**
  (`fragment/ivm.rs:209` calls `step_datafusion()` on a brand-new flow,
  so the per-flow ctx cache never hits — the exact overhead G14
  removed centrally), **recompiles every view plan per tick**
  (`build_view_plan`, transient `view_plans` starts empty), and ships
  **every view's full materialized output back** (`flow.snapshot` per
  view → `encode_batch_map`) even when the plan executed O(Δ). Wire +
  compute cost is O(state + output) per tick regardless of delta size.
  AUD-6's executor-resident refactor kills all four at once — the exit
  criterion must include "executor returns deltas, not snapshots."
- **No O(Δ) view-on-view cascade**: a downstream view of an
  incremental view always executes DiffBased over the upstream's
  **full output** (the incremental branch registers only the previous
  snapshot for downstream consumers, `flow.rs:940-958`); output deltas
  never propagate down the view DAG. Every extra DAG level costs
  O(state), so composed views — the thing pipelines produce — defeat
  the engine's own differentiator.
- `enable_disk_spill` has zero callers outside the crate (the tick ctx
  is spill-configured anyway) — wire-or-delete the knob (Phase 51).

**SOTA reference.** Feldera (DBSP — the formal basis this crate already
declares): incrementalizes *arbitrary* SQL programs including deeply
nested view hierarchies — delta cascades are free by construction
because the whole program is one circuit; spills state to NVMe for
larger-than-RAM operation; millions of events/s single-node.
RisingWave: **shared arrangements** — indexes are materialized views
shared by all downstream operators, so N views over the same join key
keep one copy of state; **delta joins** — Δ(A⋈B⋈C) evaluated as a
union of N lookup terms against shared indexes, no per-join state
duplication; state lives in Hummock (disaggregated LSM) with two-phase
aggregation. Krishiv's per-view private traces + DiffBased cascades
are the opposite of both on exactly these axes.

**Direction (folded into Phases 57/64):** AUD-7 grows to cover value
stringification + the 0.0 coercion bug + differentiate allocations;
AUD-9 grows the delta cascade (view-on-view O(Δ)) with shared upstream
traces; AUD-6's exit adds deltas-not-snapshots; MIN/MAX multisets move
behind the Phase-56 state/arbiter seam; Phase 64 evaluates the
delta-join form for multi-way joins at shard scale.

## 6. Fault tolerance & HA

- Coordinator HA: etcd leader election exists behind `feature = "etcd"`
  (`coordinator_daemon.rs:110`, `etcd_lease.rs`, `leadership.rs`);
  default mode is `single`. Prod runs a single coordinator; failover is
  restart + `store.rs` NDJSON event-log/snapshot recovery (rotation at
  64 MiB, bounded ring). RocksDB/etcd metadata backends exist.
- Executor loss: heartbeat lease → task requeue works (G6 chaos gate 50×
  in CI; DIST-1 loss-counter reset; #81 cycle-boundary race fixed).
  No shuffle-output loss recovery (lost map output ⇒ job failure, not
  stage regeneration) — meaningful once phase 52 makes multi-stage real.
- No speculative re-execution; no completed-job history service (roadmap
  names it); chaos gate stuck at N-small (platform task #98,
  infra-blocked).

## 7. Memory, spill, and large results (healthy)

`krishiv-common/memory_budget` (`MemoryBudget`) feeds batch fragments
(per-task FairSpillPool limit + optional process-wide budget), dataflow
windows/CEP, and shuffle spillables; executor result spool + chunked
fetch (#156) removed the OOM path for large results; CTAS/DML no longer
stream full results over Flight SQL (#162). G2's mechanism is verified;
the TPC-H-at-RAM benchmark remains blocked on hardware headroom. Spill
mechanics need *benchmark proof*, not rework — but the *accounting* is
per-subsystem islands, not one arbiter (see §7c).

## 7b. Partitioning across the three engines (sixth pass, 2026-07-10)

- **Shuffle-layer partitioners are rich and mostly parked** (the §2/§3
  pattern again): `HashPartitioner`, `SaltedHashPartitioner` (hot-bucket
  splitting with correct scoping — never keyed streaming),
  `RangePartitioner` with a reservoir-sampling boundary builder (E2.4,
  for GlobalSort/SortMergeJoin), and a full `Partitioning` enum
  (Unpartitioned/Hash/RoundRobin/Broadcast/Range) in `krishiv-plan` —
  but SQL lowers `DfPlan::Repartition` → `Unpartitioned` (§2), so none
  of it is reachable from a query. Phase 52 wires it.
- **"Dynamic partitioning" today = the R7.2 hot-key path**:
  `HeavyHittersTracker` heat reports → source throttle + a one-shot
  `skew_repartition_overrides.insert(job, executor_count)`
  (`executor_ops.rs:233`) that switches the *whole job* to round-robin
  over all executors — batch only, correctly skipped for streaming
  (keyed-state pinning, documented in code). Inversion worth noting: the
  blunt round-robin override is wired while the purpose-built
  `SaltedHashPartitioner` is not. Phase 54 (skew-split on the salted
  partitioner, coalescing, runtime filters) is the fix.
- **The engine can read partitioned Iceberg tables but can never write
  one — the biggest gap found this pass.** Every data file written
  carries `.partition(Struct::empty()).partition_spec_id(0)`
  (`iceberg_native.rs:175`); `PARTITIONED BY` has zero hits in the tree;
  there is no partition-aware fanout writer. Everything Krishiv
  materializes (durable CTAS/G17, the G7/G8 streaming sink, live tables)
  is unpartitioned files — scan pruning (#158) only ever benefits
  externally-written tables, and pruning/compaction degrade with table
  size. → Phase 52 (partitioned table writes task), Phase 60
  (`PARTITIONED BY` grammar), Phase 55 (streaming sink adopts the
  writer), Phase 54 (dynamic partition pruning once tables can be
  partitioned).
- **Streaming source partitions**: single-node picks up new Kafka
  partitions via consumer-group rebalance (C6, `kafka.rs:1116`); the
  coordinator-fed distributed path has no dynamic split discovery —
  a topic that grows partitions needs a restart. → Phase 55.
- **IVM has no partitioning dimension at all** — and Phase 57
  deliberately keeps one executor per IVM job (executor-resident state
  is the step that pays). Key-group-sharded IVM (DBSP multi-worker) is
  **Phase 64** — post-GA, demand-triggered, reusing Phase 55's
  key-group/exchange/barrier machinery. Note: source-table partition
  pruning and (post-#191) partitioned output tables work for IVM
  regardless — this bullet is about sharding the *computation*.

## 7c. Shuffle lifecycle, memory arbitration, watermarks, table maintenance (seventh pass, 2026-07-10)

Component sweep beyond partitioning. What is genuinely wired (no plan
change needed): shuffle write compression (`shuffle_svc.rs:107` —
`LocalDiskShuffleStore::with_compression(Lz4)`), shuffle orphan cleanup
(`orphan.rs` driven from `coordinator_daemon.rs`), and checkpoint epoch
GC (`coordinator/checkpoint_ops.rs:547` → `delete_epoch`). Gaps:

- **Table maintenance is a toy, and nothing schedules it — the headline
  of this pass.** The three procedures exist and are SQL-reachable
  (`CALL system.expire_snapshots|remove_orphan_files|compact_data_files`,
  `krishiv-sql/lib.rs:2345`), but `compact_data_files`
  (`lakehouse/maintenance.rs:339`) reads **every data file of the table
  into a `Vec<RecordBatch>` in process memory** and rewrites it as **one
  single file via drop+recreate overwrite** — `target_file_size_bytes`
  is ignored for splitting, memory is O(table), snapshot history/time
  travel is destroyed by the overwrite, the rewrite sidesteps the G3
  commit-conflict path (a concurrent committer races the drop+recreate),
  it is not partition-aware, and delete files are not compacted. And no
  code anywhere (engine or platform) invokes maintenance automatically:
  the G7/G8 streaming sink and live tables append files every cycle, so
  small files and snapshot metadata grow without bound unless an
  operator manually CALLs a procedure that would OOM on a real table.
  Every production streaming lakehouse treats scheduled compaction +
  snapshot expiry as table-stakes (Databricks auto-compaction/OPTIMIZE,
  Iceberg maintenance actions). → Phase 52 task (compaction as a
  distributed batch job over the engine's own execution, partition-aware
  once partitioned writes land, normal-transaction commits under G3
  conflict handling); platform schedules it for live tables (seam).
- **Watermarks are a single global max — no per-partition tracking, no
  min-combine, no idleness handling.** Each streaming cycle reports
  `max_event_time_ms` over *all* records read
  (`fragment/streaming.rs:227-346` → `with_watermark_ms`). A Kafka
  partition lagging cycles behind a fast one already gets its events
  late-dropped today (the fast partition drags the global watermark
  forward). Under Phase 55 parallel subtasks this becomes structural:
  correctness requires per-split watermarks min-combined across splits
  and subtasks (coordinator/exchange level), plus an idleness timeout so
  an empty split doesn't stall every downstream window (Flink's
  per-split watermark + idle-source model). → Phase 55 task.
- **Memory accounting is three islands; the purpose-built unifier is
  parked** (the §2/§3 pattern again). Per-task engines each get their
  own FairSpillPool (task budget, else cgroup-derived env default), with
  an optional process-wide `KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES`
  reservation layer (unset ⇒ unlimited; 32 MiB min-grant bounds
  overcommit); dataflow windows/CEP hold their own `MemoryBudget`s;
  shuffle spillables their own. Nothing arbitrates *across* regions —
  and the SH7 `UnifiedMemoryManager` (Spark-style
  Shuffle/Execution/State soft regions, built for exactly this) has
  **zero callers outside shuffle**; its Execution and State regions are
  dead. Today state rides the coordinator so executor state pressure is
  minimal; the moment 52 (real shuffle buffers) and 55/57
  (executor-resident streaming/IVM state) land on the same process,
  per-subsystem islands overcommit against each other. → Phase 56 task
  (one executor-wide arbiter); Phase 51 wire-or-delete also picks up
  `tiered_store.rs` and `lease_persistence.rs` (no callers outside the
  shuffle crate) alongside the already-listed push shuffle store.

## 8. API surfaces

- **SQL**: DataFusion pinned at 53.1 / arrow 58.3 / iceberg 0.9.1
  (`Cargo.toml`); upgrade train tracked (#163 for iceberg 0.10 + DF 54
  alignment). Session layer, prepared statements ($N + JDBC `?` G12),
  SQLSTATE taxonomy (`krishiv-sql/src/sqlstate`).
- **SQL language coverage** (second pass, 2026-07-10): broader than
  typical — `krishiv-sql` ships a machine-readable ~90-entry feature
  matrix (`grammar.rs`), Spark extensions (`spark_sql_ext.rs`: LATERAL
  VIEW [OUTER], TABLESAMPLE, TRANSFORM, DESCRIBE EXTENDED, SHOW
  TBLPROPERTIES), PIVOT/UNPIVOT rewrites, recursive CTEs,
  ROLLUP/CUBE/GROUPING SETS, Spark 4 pipe syntax, a MATCH_RECOGNIZE
  subset (no DEFINE/MEASURES), Iceberg MERGE/DELETE/UPDATE and durable
  CTAS (G17), `CREATE FUNCTION … LANGUAGE SQL|PYTHON`. **Verified
  missing**: the entire JSON function family (`get_json_object`,
  `from_json`/`to_json`, `json_tuple` — zero hits in the tree),
  higher-order array/map lambdas (`transform`, `filter`, `aggregate`,
  `zip_with`), Spark session statements (`SET`/`RESET`, `USE`,
  `TRUNCATE`, `CACHE`), most of the `SHOW` family, `DESCRIBE
  FUNCTION|DATABASE|QUERY`, join hints beyond BROADCAST, Spark
  date-format patterns. The matrix itself has **drifted** (CTAS still
  marked Partial after G17), and only ~17 engine-side UDF registrations
  exist — the function library is essentially DataFusion's builtin set.
  → Phase 60 (measured Spark-reference parity).
- **SQL across the three engines** (fifth pass, 2026-07-10): the language
  has **three front doors**. Batch = full DataFusion planner +
  extensions; streaming = `compile_streaming_window_sql`
  (`streaming_window_plan.rs`, a hand-written AST matcher: plain SELECT
  only, exactly one TUMBLE/HOP/SESSION TVF, whitelisted aggregates, no
  joins/HAVING/subqueries); IVM = `IncrementalViewSpec.sql` into the
  delta planner (AUD-9 coverage, silent DiffBased fallback). The same
  window-TVF syntax is implemented **twice** (batch rewrites to scalar
  UDFs in `streaming_tvf.rs`; streaming hand-compiles) — the
  `SUM(CASE WHEN)` 409 (fixed `7b720ea`) was this divergence class in
  prod, and it recurs per construct until the front door is shared. No
  SQL DDL reaches streaming or IVM (no `CREATE MATERIALIZED VIEW`, no
  streaming `CREATE TABLE`/`INSERT INTO … SELECT`; continuous jobs are
  API/HTTP-only via `continuous_stream_http.rs`) — a Flight SQL/JDBC/BI
  client can never touch two of the three engines. `grammar.rs`'s
  `FeatureEntry` has a single `status`, so "supported-batch,
  partial-streaming" is unrepresentable and measured coverage silently
  means batch. No differential tests assert batch(q) ≡ streaming(bounded
  replay) ≡ IVM snapshot for the shared subset, though the oracle is
  free (DiffBased fallback *is* batch recompute). → Phase 60
  (engine-dimensioned matrix, one parser front door, cross-engine DDL,
  differential corpus), operator execution coverage in 55/57.
- **API fragmentation across the three engines** (third pass, 2026-07-10):
  the Python surface has a strong PySpark-shaped base — `PyDataFrame`
  with ~60 methods (select/filter/group_by/join/pivot/cube/rollup/
  sample/cache/…), `col()`/`Column` expressions, `session.read_stream()`
  + `df.write_stream()` — but a user faces **five parallel idioms**:
  (1) batch `DataFrame`; (2) structured-streaming
  `DataStreamReader/Writer`; (3) a Flink-style DataStream layer
  (`KeyedStream`/`WindowedStream`/`ConnectedStreams`/process functions +
  state types); (4) the raw IVM job protocol — `session.ivm()` →
  `register_view(sql, SchemaClass)` → `feed(DeltaBatch.from_inserts(…))`
  → `step()` → `snapshot()`, with user-visible `_weight` columns
  (`krishiv-python/src/incremental.rs`) — the least friendly path for
  the differentiator engine; (5) live-tables/pipelines. Method-variant
  sprawl compounds it (`filter`/`filter_column`,
  `select`/`select_columns`/`select_exprs`, `except_`/`except_all`/
  `except_distinct`, `*_with_options` twins). The Rust `krishiv-api`
  mirrors the same fragmentation (Session / DataFrame / StreamingBuilder
  / StreamingDataFrame / MaterializedTable / IvmJob). The engine-core
  spine promises one contract across engines; the API layer doesn't
  deliver it yet. → Phase 61 (unified DataFrame API, PySpark parity,
  delta-batch demoted to an internal protocol).
- **Sync/async: three calling conventions, none complete** (fourth pass,
  2026-07-10). Rust is **sync-first** (`Session::sql` at
  `session.rs:2165`, `DataFrame::collect` at `dataframe.rs:509` — both
  cross the `block_on` bridge), with ~20 `_async` twin methods
  (`sql_async`, `collect_async`, `read_*_async`, …) AND a partial
  8-method `BlockingSession` facade (`blocking.rs`, 151 lines) — three
  ways to run one query. The bridge itself
  (`krishiv-common/src/async_util.rs`) is well-engineered — it correctly
  handles multi-thread (`block_in_place`), current-thread (fresh-OS-
  thread hop around Tokio's per-thread nesting guard), and no-runtime
  (lazy fallback runtime, B3 thread cap) contexts — but every "sync"
  call inside an embedder's async app borrows a runtime worker. Python
  mirrors it: `Session` mixes sync methods (27 GIL-release sites — good
  discipline, not total) with method-by-method coroutines via
  `pyo3-async-runtimes` (`sql_async`, the B-1 fix) plus a separate
  `BlockingSession` pyclass. Engine internals are largely disciplined
  (`spawn_blocking` for storage I/O in shuffle/executor; sharded
  coordinator bypasses hot-path locks) but the GAP-4 hazard class (locks
  held across await) is policed by comments, not lints. → Phase 61
  (single sync/async contract: async-first Rust core + one complete
  blocking facade; Python sync-by-default + systematic async mirror) and
  Phase 51 (async hygiene as clippy lints: `await_holding_lock`,
  `disallowed-methods` for `block_on` in async contexts).
- **Flight SQL**: metadata RPCs + JDBC/ADBC verified (G1);
  `krishiv-sql-gateway` is explicitly **not** a wire server (API-12
  header) — an in-process SQLSTATE facade. There is no Postgres/JDBC
  wire endpoint besides Flight SQL. Query progress + cancellation are
  roadmap items, not implemented surfaces.
- **Python**: 12.5k LOC PyO3. A non-compiling `PySession::close` shipped
  in `1694143` — the per-PR `test-python` CI job did not block it
  (status.md 2026-07-09); CI honesty gap.
- **MCP**: 3.3k LOC single file, read-only ops surface. **UI**: 2.4k LOC
  embedded console. Both fine for scope.
- **Connectors**: ~30 sources/sinks with SDK, registry, maturity
  certification (`certification.rs`) — the strongest crate; the platform
  ADR-0021 confirms it as the connector home.

## 9. Testing & release infrastructure

Strong: per-mode build/test recipes, chaos crate + G6 kill-loop CI gate,
conformance suites, api-surface freeze (`api/` + CI check), cargo-deny,
release skill with signed checksums, BENCHMARKING.md discipline. Gaps: no
SQL correctness corpus (sqllogictest-style), no distributed-scale CI
(single machine), `test-python` not blocking, no benchmark **history**
publication (roadmap item 5), no soak gate in engine CI (soaks are
platform-driven).

## 10. Verdict → Track 6 (platform phases 51–63)

The engine's architecture (spine, seams, hygiene, certification
discipline) is production-grade; its **scale story is not**: one-task
batch jobs, one-task streaming jobs, 16 MiB IVM offload, test-gated
scheduler algorithms, unwired incremental checkpoints/barriers, single
coordinator. The pattern is consistent — correct narrow paths shipped
first, scale machinery built but parked. Track 6 wires and proves it, in
dependency order:

| Phase | Theme | Kills |
|---|---|---|
| 51 | Baseline: wire-or-delete audit, version train, correctness corpus, honest CI | parked-subsystem drift; DF 53 pin; test-python gap |
| 52 | Distributed batch v2: proto fragments, partition-parallel stages, real shuffle | §2 single-task ceiling |
| 53 | Scheduler v2: locality, fair pools, speculation, priority | §3 cfg(test) algorithms |
| 54 | AQE + statistics: coalescing, skew split, runtime filters | §3 one-shot skew; no stats |
| 55 | Streaming v2: key-group parallel jobs, live barriers, continuous joins | §4 one-task streaming; G5 |
| 56 | State v2: incremental + disaggregated checkpoints, rescaling, savepoint windows | §4 full-state-per-cycle |
| 57 | IVM scale-out: executor-resident state, arrow-row keys, retention, coverage | §5 AUD-6/7/8/9 |
| 58 | Fault tolerance GA: coordinator HA, shuffle recovery, history server, chaos matrix | §6 |
| 59 | Interfaces: progress/cancel, wire protocol decision, Python parity, CI honesty | §8 |
| 60 | SQL surface completeness: measured Spark-reference parity (JSON/lambda functions, SET/SHOW/USE, matrix drift) | §8 SQL coverage |
| 61 | Unified DataFrame API: one surface, three engines, PySpark parity; delta-batch demoted to internal protocol; single sync/async contract | §8 API fragmentation + sync/async |
| 62 | Production GA gate: certified matrix, public benchmarks + history, soak | the launch |

Two cross-cutting observations that individual phases only cover
implicitly, named here so they don't survive partially (2026-07-10,
end-of-session review):

- **Coordinator must become control-plane-only.** Every distributed data
  path today transits the coordinator: batch inline tables ride base64 in
  assignments (§2), streaming input is coordinator-pushed and output
  coordinator-drained per cycle (§4), IVM state round-trips per tick with
  the 16 MiB cliff (§5). Phases 52/55/57 each remove their leg; by Phase
  58 entry, query data on coordinator RPC is a regression, not a
  workaround.
- **The stringly-typed fragment protocol dies once, not per-engine.**
  `TypedTaskFragment.body: String` with `sql:` / `stream:loop:` /
  `delta:step:` prefixes spans all three engines; Phase 52's fragment ADR
  defines the typed proto envelope for all three body kinds, and 55/57
  adopt it.
- **SEC-1's pattern needs a surface sweep (SEC-3, Phase 63).** The
  merged-outside-middleware pattern was verified only on the two routers
  the external review named; the embedded console, MCP server,
  metrics/health endpoints, executor-side HTTP, and shuffle/Flight data
  services have not been audited against it.

Phase detail, gates, and platform-side seams live in the platform repo:
`docs/implementation/phases/phase-NN-*.md` and `plan.md` (Track 6).

### SOTA references consulted (2026-07-10)

- Morsel-driven parallelism / NUMA work-stealing (Leis et al., SIGMOD'14) —
  intra-node scheduling; DataFusion's streaming push model already gets
  comparable intra-node scaling, so Track 6 spends effort on *inter-node*.
- Apache DataFusion Ballista 53 (May 2026 release): scheduler/executor +
  shuffle architecture on the same DF base; TPC-H SF100 2.9× single-node.
- Sail (lakehq/sail): DataFusion-based Spark replacement; ~4× overall /
  up to ~8× per-query vs Spark on TPC-H-derived 100 GB with zero shuffle
  spill and released-per-query memory — the proof point that the
  streaming-end-to-end, no-per-query-setup discipline (§2b) is where the
  headroom is for an engine already on Rust/Arrow/DataFusion.
- Flink 2.0 disaggregated state / ForSt (VLDB'25) + async execution model;
  credit-based flow control (FLINK-7282) as the streaming data-plane model;
  buffer-timeout + per-key timer service as the low-latency model (§4b).
- Arroyo (arroyo.dev): Rust/Arrow/DataFusion streaming engine,
  millisecond-latency; Arrow batches over TCP between workers,
  `BATCH_SIZE`/`BATCH_LINGER_MS` as the latency/throughput dial,
  Chandy-Lamport checkpoints, control-plane-only controller — the
  external proof that Krishiv's embedded continuous loop (§4b) is the
  right architecture to promote into the distributed runtime.
- Spark AQE (coalesce/skew-split/dynamic broadcast) + runtime filters;
  push-based/remote shuffle (Magnet, Celeborn, Uniffle) as the shuffle
  service end-state — `krishiv-shuffle`'s ESS/push/tiered stores map to it.
- Delay scheduling (Zaharia et al., EuroSys'10) for the locality tier;
  speculative execution per MapReduce/Spark for stragglers.
- DBSP/Feldera (already the delta crate's declared basis) for IVM
  operator coverage direction; specifically (§5c): whole-program
  circuits make view-on-view delta cascades free, NVMe-spilled state
  for larger-than-RAM operation, millions of events/s single-node.
- RisingWave (§5c): shared arrangements (one indexed state serving all
  downstream MVs), delta joins (Δ(A⋈B⋈C) as a union of lookup terms —
  no per-join state duplication), Hummock disaggregated LSM state,
  two-phase aggregation, MV-on-MV as first-class.
