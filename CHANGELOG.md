# Changelog

All notable changes to Krishiv are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
Semantic Versioning as described in `docs/RELEASE.md`.

## [Unreleased]

### Added

- **Adaptive query execution + statistics** (Phase 54, 2026-07-12). The
  coordinator now re-optimizes real distributed stages at stage
  boundaries from measured shuffle output: small reduce partitions are
  **coalesced** into fewer tasks (`dfplan:v1:p1,p2,…` multi-partition
  bodies; default target 64 MiB per task,
  `KRISHIV_AQE_TARGET_PARTITION_BYTES`), and a **skewed** partition
  (≥ `KRISHIV_AQE_SKEW_FACTOR` × median and ≥
  `KRISHIV_AQE_SKEW_MIN_BYTES`) is split into map-task-range sub-tasks
  (`dfplan:v1:p/s0m2-4:…`) so one hot key no longer serializes the reduce
  stage — gated on a structural split-safety proof of the decoded plan
  (inner joins/filters/projections only; blocking operators fail closed).
  Every decision lands in the per-job adaptive decision log
  (`partition-coalesce` / `skew-split`) and new
  `krishiv_aqe_*` metrics; everything is disableable
  (`KRISHIV_AQE=off` master, `KRISHIV_AQE_COALESCE`,
  `KRISHIV_AQE_SKEW_SPLIT`) and result-neutral by construction and by
  test. Statistics collection is real: `ANALYZE TABLE <ref> [FOR COLUMNS
  (…)]` scans once (COUNT(*), per-column approx-NDV/min/max/null-count)
  and feeds the engine row-count registry plus a new process-global
  `TableStatsRegistry` (also auto-fed by Iceberg CTAS/DELETE row counts);
  the coordinator's AQE cost model now consumes it
  (`default_aqe_optimizer_with_stats`). DataFusion's native runtime
  (dynamic) filters are wired behind `KRISHIV_RUNTIME_FILTERS` (default
  on; the off switch clears the master **and** per-operator options —
  the master switch alone does not suppress them) with a probe-scan
  pruning proof (10 000-row fact scan emits 100 rows on a 3-key star
  join) and a dedicated runtime-filters-off corpus dual-run binary.

### Fixed

- **Incremental-view (IVM) job safety hardening** (2026-07-22). Two real bugs
  found while certifying cancel latency (#224), plus a design clarification.
  (1) A **central-computed** IVM tick — the fallback taken when no executor
  can accept work or a resident dispatch failed — ran unbounded, so a
  pathologically large delta could block the coordinator's HTTP handler *and*
  hold the per-job step lock indefinitely, wedging every subsequent tick and
  deletion for that job. It is now bounded by the same timeout as the
  resident-dispatch path (`503 Service Unavailable` on expiry, retryable).
  (2) Deleting an IVM job **raced** a concurrent `/step`: the step's trailing
  snapshot persist could land *after* deletion removed the snapshot,
  resurrecting a deleted job on disk. `DELETE` now holds the per-job step
  lock, so it either wins outright (the next tick 404s) or waits for the
  in-flight tick, whose persist then no-ops because the registry entry is
  gone; a regression test proves the handler serializes on the lock.
  Separately clarified (with a code comment) that an IVM tick is deliberately
  *not* drop-cancellable mid-flight — it applies already-accepted deltas that
  must be reflected for correctness, so the wall-clock timeout is the safety
  bound and any failure recovers by re-attaching from the coordinator's
  authoritative state mirror.

- **Task launch no longer livelocks when its executor is circuit-broken or
  lost** (Phase 58/53, 2026-07-13). If a task was assigned to an executor that
  then dropped out of the launch leases — filtered by the circuit breaker after
  crossing the failure threshold, or unregistered after a loss — the per-job
  launch aborted with `UnknownExecutor` and never cleared the stale assignment,
  so the coordinator's launch loop spun on it (`unknown executor: …`, every
  ~3s) until the job hit its batch-SQL timeout. The launch path now resets such
  an orphaned task to `Pending` (clearing the dead assignment) so the next
  assignment round re-places it on a healthy executor. Observed on a 3-node
  cluster: an executor killed *early* (before it produced any shuffle output)
  left one map task pinned to a filtered executor and the job timed out instead
  of recovering. (The mid-shuffle killed-*producer* case below already
  recovered; this closes the early-kill gap.)

- **Long-running distributed queries no longer abort at 30s**
  (Phase 59, 2026-07-13). The client→coordinator Flight channel was built with
  a fixed `.timeout(30s)`, which tonic applies to *every* request on the
  channel — so any distributed query (or result stream) that ran longer than
  30s was aborted with `do_action: Timeout expired`, regardless of the
  statement-level `--timeout`. This capped healthy multi-minute scans and made
  executor-loss recovery (which needs a regeneration cycle) unobservable from
  the blocking client. The hard per-request timeout is removed by default;
  query duration is now bounded by the coordinator's own statement timeout
  (`KRISHIV_BATCH_SQL_TIMEOUT_SECS`, default 300s), and a vanished coordinator
  is still detected mid-request via HTTP/2 keepalive (~50s). Operators who want
  a hard client-side cap can set the new `KRISHIV_FLIGHT_REQUEST_TIMEOUT_SECS`.
  (Surfaced while validating the killed-producer shuffle recovery below on a
  live 3-node cluster.)

- **Killed-producer shuffle now recovers instead of failing the job**
  (Phase 58, 2026-07-13). When a producing executor's pod was deleted
  mid-fetch, the downstream reduce's shuffle fetch got a *connection-refused*
  transport error which was retried to exhaustion and then surfaced as an
  opaque error that triggered no recovery — the reduce burned its whole
  task-retry budget re-hitting the dead endpoint and the batch job failed
  unrecoverably (observed live on a 3-node cluster — job `Failed`, one reduce
  task never recovered). Two changes close this end to end: (1) a shuffle
  fetch that exhausts its retries on a *transport* failure now surfaces as
  `NotFound` (the producer is gone) rather than a raw transport error
  (`FlightShuffleClient::fetch_with_retry`); and (2) the **dfplan staged-batch**
  reader (Phase 52's distributed path) — whose `Result<_, String>` trait
  boundary previously stringified that `NotFound` into an opaque message —
  now emits a structured missing-partition marker that the task runner
  recovers via `collect_missing_shuffle_partitions`; and (3) a consumer task
  that fails on a *missing upstream shuffle partition* (FetchFailed) is now
  retried under a generous budget (`MISSING_SHUFFLE_MAX_ATTEMPTS = 30`) rather
  than the default `max_task_attempts = 1` — FetchFailed is upstream data loss,
  not the task's fault (Spark parity), and a single multi-producer executor
  loss surfaces as *several sequential* fetch failures (one per lost producer),
  each of which would otherwise exhaust the strict one-attempt budget and fail
  the job before recovery could converge; the productive path stays bounded by
  `KRISHIV_MAX_SHUFFLE_REGEN` (a durable loss still fails cleanly) and this cap
  only backstops a degenerate report loop. The consumer then reports
  the partition missing and the coordinator regenerates exactly the lost
  producer map task (`invalidate_specific_shuffle_partitions`, keyed on the
  `sN.mM` sub-stage id, bounded by `KRISHIV_MAX_SHUFFLE_REGEN`); the reduce
  waits for the upstream to re-succeed on a healthy executor and relaunches
  against the fresh flight endpoint. The legacy `shuffle-write:` fragment path
  already mapped `NotFound → ShufflePartitionMissing`; this brings the dfplan
  path to parity. (An initial misdiagnosis — a redundant heartbeat-path
  shuffle audit, and a fix confined to the legacy path — was corrected after
  the on-cluster kill test still failed.) Reproduced and fixed on a 3-node
  k3s cluster.

- **Distributed dfplan shuffle no longer strands map output in an unserved
  store** (Phase 58, 2026-07-13). On a dev-local/`--shuffle-dir` (or URI)
  executor, typed dfplan shuffle writes (Phase 52 staged batch) target the
  `inmem_shuffle` store, but the executor unconditionally overwrote that with a
  *fresh* `InMemoryShuffleStore` whenever the durability profile allowed an
  unbounded store — while the shuffle-flight server kept serving the
  local-disk store. In-process tests read from the same in-memory store so they
  passed, but on a real multi-node cluster the cross-node reduce fetched each
  map partition over flight from the producer's flight server (serving disk)
  and missed it (`partition s0.mN/p not found`), failing every multi-stage
  batch job. The executor now wires the configured dir/URI store as **both**
  the flight-served store and `inmem_shuffle` (one instance), and the
  in-memory fallback only applies when no dir/URI store is configured.
  Reproduced and fixed on a 3-node k3s cluster.

- **Distributed batch SQL over an over-cap parquet no longer hangs**
  (Phase 58, 2026-07-13). When the distributed client could not inline a
  parquet table's Arrow IPC (it exceeded `KRISHIV_INLINE_IPC_MAX_BYTES`,
  default 64 MiB) it degraded the table to an empty `ipc_b64` with a real
  `path`, expecting path-based resolution — but the Flight SQL `BatchSql`
  handler dropped the path and shipped an **empty inline partition** the
  executor rejected (`inline ipc bytes cannot be empty`), and the launch
  loop retried the malformed assignment **forever** with the job stuck
  `Running`. The handler now splits wire tables into inline vs path tables
  (empty IPC + real path → `LocalParquet`, eligible for partition-parallel
  staged execution just like the HTTP path-table surface), so a large
  shared-filesystem parquet runs as a real multi-stage shuffle job. And a
  non-retryable executor rejection (`InvalidArgument` and peers) is now
  surfaced as a permanent `AssignmentRejected` error that fails the job
  terminally instead of retry-storming — a malformed task payload can no
  longer wedge the coordinator. Verified on a 3-node k3s cluster.

- **Assignment no longer demotes completed stages** (Phase 54). Applying
  task assignments used to stomp every stage's state to `Scheduling`,
  including `Succeeded` upstream stages — any post-success assignment
  (Phase 53 eager backlog drains, retries, AQE rewrites) permanently hid
  upstream success from the launch loop's upstream-ready check and
  wedged downstream tasks. Stage state now only moves for stages that
  actually received an assignment, and never out of a terminal state.

- **Scheduler v2 — locality, fair pools, safe speculation, strict capacity**
  (Phase 53, 2026-07-12). Placement is now locality-aware and live: reduce
  stages prefer the node holding the majority of their upstream shuffle
  bytes, with NODE→RACK→ANY tiers and delay scheduling (bounded wait for a
  local slot, `locality_wait_ms`, default 3 s); executors advertise node
  identity via `host` and an optional rack via `KRISHIV_RACK_ID`. Fair
  pools are real: per-pool weight/min-share quotas (largest-remainder
  weighted split) bound each assignment round, so two pools with 2:1
  weights converge to a 2:1 slot split under saturation. Speculative
  execution is now safe to enable: stragglers receive a `CancelTask`
  before being re-queued under a new attempt id — first completion wins,
  late updates from the cancelled original are fenced, and sink-contract
  tasks are never speculated. Saturation semantics are strict: placement
  stops at free-slot capacity (no silent oversubscription), overflow tasks
  wait in a pending backlog drained when slots free, a coordinator-side
  in-flight overlay closes the heartbeat-lag over-assignment window, and
  failure retries back off exponentially (`task_retry_backoff_*`). The
  500 ms launch tick is O(dirty jobs) with a periodic full-sweep fallback,
  and recovery-path streaming checks are one O(cluster) scan instead of
  per-executor scans. New observability: per-tier placement counters,
  speculation detected/preempted counters on the metrics endpoint. Scale
  proof: 10 000 tasks placed and launched across 100 executors in 3.3 s.

- **Distributed batch v2 — partition-parallel stages over a real shuffle**
  (Phase 52, 2026-07-12). Plain batch SELECTs are now cut at hash-exchange
  boundaries into ShuffleMap → Result stages (`dfplan:v1:` proto-encoded
  physical-plan fragments, ADR-0003), task-per-partition, with map outputs
  hash-partitioned into the shuffle store and fetched by downstream tasks —
  locally or over Arrow Flight when the map ran on another executor. The
  daemon submission path stages too: `POST /api/v1/batch-sql/submit` accepts
  `table_paths` (parquet paths readable cluster-wide) and stage-splits
  eligible queries; anything the stage builder cannot prove correct falls
  back to the single-task `sql:` path unchanged. Executor loss after a map
  completes invalidates that executor's shuffle partitions and re-runs the
  producing maps (chaos-tested). Batch hot path rebuilt in the same phase
  (#194): `target_partitions` defaults to available cores everywhere (was
  silently 1 — the Phase 51 4.5–8.9× finding), zero-materialization scans
  and sinks, engine overhead budget closed at 0.87–0.98× vs raw DataFusion
  (SF1 Q1/Q6/Q3; docs/BENCHMARKING.md); staged TPC-H Q1 verified
  byte-identical to inline execution.
- **`PARTITIONED BY` for Iceberg tables + partition-aware writes** (Phase 52
  #191, 2026-07-12). `CREATE TABLE … PARTITIONED BY (region, day(ts),
  bucket(16, id), truncate(4, s)) AS SELECT …` creates a real Iceberg
  partition spec and fans the landing stream out per partition value —
  spec-exact transform math reused from iceberg-rust (murmur3 bucketing
  included), hive-style data paths, bounded memory via largest-buffer-first
  flushing. Every rewrite path (DML copy-on-write, compaction, replace) now
  preserves the table's partition spec instead of silently recreating tables
  unpartitioned. `PARTITIONED BY` on a non-Iceberg target is a hard error,
  not a silent drop. See docs/partitioning-design.md (Domain C).
- **Table maintenance v2** (Phase 52 #192, 2026-07-12).
  `CALL system.compact_data_files` is now a partition-aware bin-pack: small
  files are merged within their partition value into ~target-size files, one
  bin in memory at a time; files already at target (and lone small files)
  are carried over untouched, and a no-op compaction commits nothing. The
  swap is guarded by a snapshot conflict check — a concurrent commit aborts
  the compaction (parts cleaned up) instead of being silently discarded; the
  remaining check-to-swap window closes with the iceberg-rust 0.10 atomic
  rewrite (#163). Tables with delete files are refused rather than silently
  dropped. New `CALL system.maintain_table('ns.tbl'[, '7 days'[, bytes[,
  retain]]])` runs compact → expire_snapshots → remove_orphan_files as the
  schedulable maintenance entry point.

- **Benchmark yardstick baselines** (Phase 51 close-out, 2026-07-11).
  First recorded, reproducible performance baselines in
  `docs/BENCHMARKING.md` ("Recorded baselines"): TPC-H SF1/SF10 ladder
  (embedded + coordinated), streaming per-batch latency, IVM tick at
  50 k–10 M accumulated rows, and Nexmark. New
  `crates/krishiv-bench/benches/tpch_overhead.rs` target measures the
  audit-§2b engine tax (same queries via raw DataFusion vs embedded
  `SqlEngine` vs `InProcessCluster`): **4.5–8.9× over raw DataFusion**,
  root-caused to `SqlEngine::new()`'s `target_partitions = 1` default —
  the tracked budget for the Phase 52 batch-hot-path work. The IVM ladder
  gained the 10 M-row point (`KRISHIV_BENCH_IVM_MAX_ROWS` caps it on
  small machines); `just bench-tpch` / `just bench-nexmark` recipes now
  exist (BENCHMARKING.md referenced them but they were never wired).

- **Property-test suites for the correctness-critical crates** (Phase 51
  audit §14, 2026-07-11). `krishiv-delta/tests/proptest_zset.rs` checks the
  Z-set laws against an independent model (consolidation = model addition,
  commutativity, additive inverse, idempotence, positive-part multiset
  expansion, serialization round-trip, `Trace` snapshot under arbitrary
  chunking); `krishiv-state/tests/proptest_checkpoint_kill.rs` kills the
  checkpoint write sequence after arbitrary prefixes and flips bytes in
  sealed epochs, asserting recovery always lands on the last sealed epoch
  with byte-exact snapshots; `krishiv-ivm/tests/proptest_ivm.rs` replays
  random multi-tick insert/retract histories and asserts incremental ==
  diff-based == one-shot DataFusion recompute == plain-Rust model.
- **External-service CI tier** (Phase 51 audit §14, 2026-07-11).
  `just test-external` + `tests/external/docker-compose.yml` +
  `scripts/external-test-services.sh` run the `#[ignore = "requires …"]`
  tests against real Postgres/MinIO/OTLP backends; a required
  `test-external` job in ci.yml runs them on every PR. The two live-cluster
  tests join Phase 58's multi-executor harness (see ci-tiers.md).
- **Coverage measurement** (Phase 51 audit §14, 2026-07-11). `just coverage`
  (cargo-llvm-cov over the exact required-gate scope) + nightly
  `coverage.yml` publishing the per-crate table and lcov artifact. First
  measured baseline (core crates common/delta/state/ivm): **77.95 % line /
  79.20 % region** coverage.
- **Flake quarantine** (Phase 51 audit §14, 2026-07-11).
  `.config/nextest.toml` `ci` profile retries (2×) scoped to the four
  sleep-based-sync crates (scheduler/executor/api/shuffle); retried-then-
  passed tests surface as FLAKY. Sleep→event conversions ride the Phase
  52/53/55 subsystem rewrites.
- **SQL correctness corpus across the three placements** (Phase 51,
  2026-07-11). New `krishiv-conformance` crate: sqllogictest 0.29 suites
  under `corpus/` run against embedded, single-node, and distributed
  placements (the non-embedded ones over an in-process Flight SQL
  coordinator). Scalar tier runs everywhere; the stateful (DDL/DML) tier is
  embedded-only until the Phase 60 SQL front door makes remote session
  state persist across statements — a divergence the corpus caught on its
  first run. Part of the required `just test-integration` CI tier.
- **Typed `KRISHIV_*` flag registry** (Phase 51, 2026-07-11).
  `krishiv_common::env_registry` declares all 135 runtime flags once with
  type/default/doc; daemon startups warn on unknown/ill-typed flags;
  `docs/reference/env-flags.md` is generated with a drift test and a
  source-scan test keeps the registry complete in both directions.
  `KRISHIV_COORDINATOR_URL` is canonical (deprecated aliases warn); one
  boolean parser (`truthy_env`) replaces four skewed definitions;
  `KRISHIV_LOG_FORMAT=json|pretty|compact` selects log output.
- **Shared bearer parsing + redaction** (Phase 51, 2026-07-11).
  `krishiv_common::auth_util::{bearer_token, redact_token}` replace four
  per-site implementations; a source-scan guard fails the build if
  hand-rolled Bearer parsing reappears.
- **CI tiers** (Phase 51): the required gate now also runs
  `just test-integration` (all tests/*.rs) and `just test-doc`; tier map
  with per-exclusion rationale at `docs/implementation/ci-tiers.md`.
  `clippy::disallowed-methods` denies `async_util::block_on` outside
  allow-listed boundary modules (`docs/implementation/async-contract.md`).
- **DataFusion 53.1 → 54.0** (Phase 51 version train, 2026-07-11);
  arrow/parquet stay 58.3 (DF 54's pin), iceberg stays 0.9.1 until 0.10
  releases (#163). Operator pool executors no longer pin
  `KRISHIV_TASK_SLOTS=2` (Option; unset derives from CPU).

- **Per-view delta statistics on IVM jobs** (#94, 2026-07-10). Every IVM
  tick path (structural `step_with`, the DataFusion publish loop, and the
  coordinator-authoritative `apply_computed_tick` offload) now counts each
  view's logical multiset changes — weight +3 is 3 inserts, −2 is 2
  retracts. `StepSummary` gains `total_inserted_rows`/`total_retracted_rows`,
  `IncrementalFlow`/`PartitionedIncrementalFlow`/`IvmJob` gain
  `view_delta_stats(view)` returning the new `ViewDeltaStats` (cumulative
  totals + last-tick counts; partitioned = summed across shards), and the
  coordinator exposes it at
  `GET /api/v1/ivm/jobs/{job_id}/views/{view_name}/stats` — a lightweight
  poll target (row count + counters) that avoids `/snap`'s full-snapshot
  base64 serialization. Counters are in-memory and reset on engine restart;
  pollers must diff consecutive reads and tolerate resets.

- **Delivery-guarantee metadata on the continuous registry** (#92,
  2026-07-10). `ContinuousJobView` (GET `/api/v1/continuous[/{job_id}]`)
  gains a `delivery` block derived from the job's sink contract and the
  connector capability registry — `sink`, `sink_guarantee`,
  `source_offsets_in_sink_transaction`, and the effective end-to-end label
  (`exactly-once` with an Iceberg two-phase sink, `at-least-once` for
  drain-only jobs whose replayed cycles can re-emit). `DeliveryGuarantee`
  gains `as_str()`/`Display`, and the Iceberg sink's capability metadata
  moved to the feature-independent
  `capabilities::iceberg_streaming_sink_capabilities()` (the sink delegates
  to it), so coordinators report guarantees without compiling the sink.
  `krishiv-connectors` is now a regular (lean, default-feature)
  `krishiv-scheduler` dependency.

- **G7: checkpoint-aligned streaming Iceberg sink** (#89, 2026-07-10).
  Continuous (`stream:loop:`) jobs can now land cycle output in an Iceberg
  table under two-phase commit aligned to the G5 checkpoint boundary:
  - New `OutputContractDescriptor::IcebergSink` (typed + proto wire fields +
    legacy string form `iceberg-sink:<root>|<table>|mode=…[|keys=…][|op=…]`),
    and an optional `sink` on continuous registration
    (`ContinuousSinkSpec`, plain + SQL entry points) that attaches the
    contract to the job's task spec.
  - `IcebergStreamingSink` (`krishiv-connectors::lakehouse::streaming_sink`,
    feature `iceberg`) implements `TransactionalSinkParticipant`: cycle
    output buffers in the open transaction, the checkpoint **barrier**
    durably stages it as Parquet before the ack, the checkpoint-**complete**
    notification commits covered epochs as Iceberg snapshots
    (`fast_append`; source offsets in the snapshot summary), and **restore**
    recover-commits ≤ epoch / aborts > epoch. Owns a background-shutdown
    runtime so dropping a participant from async contexts (job eviction) is
    safe.
  - **Row-level ops**: `mode=upsert` replaces rows by key columns and
    applies `delete` markers from an op column — copy-on-write
    (read-merge-overwrite) because iceberg-rust 0.9.1 exposes no delete-file
    write API; merge-on-read equality deletes arrive with the 0.10 bump
    (#163).
  - Executor wiring: `execute_loop_fragment` stages output + source offsets
    into the job's `TwoPhaseSinkRegistry` participant; the existing
    checkpoint lifecycle (`initiate_checkpoint_for_job` /
    `handle_checkpoint_complete` / restore) drives commit with no further
    changes. Built without the `iceberg` feature, an Iceberg-sink
    assignment fails loudly instead of dropping output. The `local` preset
    (prod build) now enables the executor's `iceberg` feature.
  - G8 wiring evidence: `stream_loop_iceberg_sink_commits_on_checkpoint_
    and_aborts_on_restore` drives two real window cycles through the loop
    executor, commits epoch 1 via the checkpoint path, restores to epoch 1
    (aborting epoch 2), and proves the reopened Iceberg table holds exactly
    the epoch-1 rows. Participant-level recovery (reopen-after-crash,
    monotonic epochs, offset summaries, upsert/delete) covered in
    `streaming_sink` tests.

### Fixed

- **Checkpoint recovery could miss the newest sealed epoch** (Phase 51
  audit §14, 2026-07-11; found by the new kill-model property test). A
  crash between `write_manifest` and `write_epoch_hint` left the hint
  naming the *previous* (still valid) epoch, and `latest_valid_epoch`
  trusted it — hiding the newer sealed epoch from recovery (re-committing
  exactly-once sink transactions on replay) and letting a restarted
  coordinator overwrite the sealed epoch's metadata through the
  monotonicity guard. Recovery now ignores the hint and validates epochs
  newest-first, also skipping corrupt epochs instead of erroring out when
  an older sealed epoch exists. The hint file is still written for
  tooling.
- **`postgres-catalog` un-quarantined and live-verified** (Phase 51,
  2026-07-11). Fixed the feature's iceberg-0.9.1 rot (`TableCommit::apply`,
  one-shot `OutputFile::write`/`InputFile::read`, `KrishivStorageFactory`
  injection, explicit creation location) and rewrote the concurrent-commit
  test through `Transaction` as a no-lost-update check. En route the live
  run caught a real bug: concurrent `migrate()` from two booting nodes
  races on Postgres's `pg_type` catalog (`CREATE TABLE IF NOT EXISTS` is
  not concurrency-safe) — migration now holds an advisory lock. Both tests
  run against real Postgres in `just test-external`.
- **IVM view-on-view double count** (Phase 51, 2026-07-11). A freshly
  built incremental aggregate over an upstream view seeded from the
  upstream's post-tick output and then applied the same tick's delta —
  COUNT over a filtered view returned 2 instead of 1. Operators now seed
  from frozen pre-tick snapshots; regression tests in krishiv-ivm and
  krishiv-api.
- Re-enabled two silently-disabled live Flight tests (missing dev-dep
  edges behind the `__disabled_flight_test` pseudo-feature): the
  krishiv-runtime distributed submit and krishiv-api remote-execution
  tests now run and pass. (Phase 51, 2026-07-11)

- **Continuous-cycle Iceberg sink epochs commit at cycle end** (G8,
  2026-07-10). Sink-attached continuous (`stream:loop:`) jobs staged every
  cycle's output into the two-phase registry but nothing ever drove the
  commit in daemon mode — continuous tasks are transient (one task per
  pushed cycle), so the barrier/complete checkpoint lifecycle never targets
  them (`JobSpec` has no `checkpoint_interval_ms`, and the coordinator's
  delivery predicates require a *running* task in the job). The table never
  advanced past its creation snapshot; found live during the G8
  certification leg on the prod cluster. The cycle is now the checkpoint
  boundary: new `TwoPhaseSinkRegistry::commit_cycle` (per-job monotonic
  epoch; prepare + commit in one step) runs at the end of
  `stage_iceberg_sink_output`, and a commit failure fails the cycle so the
  coordinator never persists a snapshot claiming uncommitted output.
- **Crash-safe Iceberg upsert overwrite** (CONN-3, 2026-07-10). The
  upsert sink's `overwrite_commit` was drop-table → create-table → commit →
  hint-flip: a process kill between the drop and the flip left
  `version-hint.text` pointing at purged metadata — the table unreadable
  until manual repair (observed live: the G8 kill test landed exactly in
  that window). New ordering: build the full replacement generation
  (creation metadata + `fast_append` snapshot) under a throwaway
  `MemoryCatalog` over the same root, then flip the hint atomically
  (temp + fsync + rename — the durable commit point), then rebind the live
  catalog and purge only the superseded generation. Every kill point now
  leaves the table readable at either the old or the new committed
  generation; regression test covers both crash states.
- **IVM joins actually run incrementally from SQL** (#160, 2026-07-10).
  The plan matcher read equi-join keys only from the logical plan's
  `join.on` — but the SQL planner leaves the ON condition in
  `join.filter` (the optimizer pass that lifts it never runs on the
  unoptimized plan the matcher inspects), so **every SQL-registered
  join view silently degraded to O(state) DiffBased** full recompute +
  diff. Equi-pairs are now extracted from `join.filter`, and a `WHERE`
  above the join (the `clean_trips` shape) decomposes by side onto the
  delta filters (right-side pushdown inner-join only — under LEFT
  OUTER it would change null-padding). Plan building also moved to an
  ephemeral schema-only context, so an empty/emptied source can no
  longer fail `ctx.sql` and pin a view to DiffBased after a restore.
- **Join trace state is checkpointed losslessly** (#160, 2026-07-10).
  `ViewPlan::Join` had no serializable state, so every
  `checkpoint_full` restore — coordinator restarts *and* each
  distributed `delta:step:` executor tick — re-seeded join hash traces
  from full source snapshots: O(|A|+|B|) rebuild per restore, and the
  materialized snapshot is a *set*, so duplicate-row multiplicity was
  reconstructed wrong (one retraction then deleted every copy).
  Traces (levelled Z-sets) and the LEFT-OUTER key-group weights now
  serialize via Arrow IPC into the existing plan-state section.
- **Snapshot materialization uses multiset semantics** (#160,
  2026-07-10). `apply_delta`/view publication collapsed a net weight-k
  row to one physical row on any retraction tick (while the insert-only
  fast path kept physical copies — inconsistent), silently
  under-counting duplicate rows vs what the equivalent SQL returns.
  `filter_positive_expanded` materializes weight k as k rows; the
  insert-only fast path now requires unit weights (it also wrongly
  admitted weight-0 rows). Legacy `restore_delta` keeps set collapse
  deliberately — stacked-restore idempotency depends on it (G2).
- **Keyed partitioner accepts `Utf8View`/`LargeUtf8` string keys**
  (2026-07-10). `partition_record_batches_by_key` rejected any key
  column outside `Int32/Int64/Float64/Utf8/Boolean`, but DataFusion
  emits `Utf8View` for string columns by default — so the IVM
  stream-bridge refused whole feeds ("key column 'region' has
  unsupported type Utf8View") and downstream live views silently
  stayed empty. All three Arrow string encodings now hash under the
  same domain tag, so a key lands in the same shard regardless of the
  producer's physical encoding.

### Added

- **Durable CTAS: `CREATE [OR REPLACE] TABLE <iceberg-table> AS SELECT`
  lands the result in the Iceberg warehouse** (G17, 2026-07-10). When the
  target resolves to a registered Iceberg catalog, `SqlEngine::sql`
  executes the inner query and streams the result into rolling Parquet
  part files (roll threshold `KRISHIV_CTAS_TARGET_FILE_BYTES`, default
  512 MiB of in-memory Arrow per part) committed via `fast_append` —
  peak memory is one part, independent of result size, and the statement
  returns a 1-row landing report (`rows_written, bytes_written,
  data_files, snapshot_id`) instead of the result set. Works identically
  in embedded, single-node, and coordinator modes (the coordinated batch
  path ships the statement text to an executor, so the write happens
  near the data and nothing large crosses a wire — previously a pipeline
  batch refresh streamed the full result over Flight SQL and died on the
  2 GiB result guard at 14 GB). Replace semantics are drop+recreate with
  data files written before the metadata swap; non-Iceberg targets fall
  through to DataFusion's session-local CTAS unchanged. New:
  `lakehouse::dml::land_ctas[_with_target]`,
  `arrow_schema_to_iceberg_schema` (hand-rolled — iceberg-rust 0.9.1
  pins arrow 57 vs workspace 58).

- **Conditional aggregates in streaming windows** (2026-07-09):
  `AGG(x) FILTER (WHERE …)` and the `AGG(CASE WHEN cond THEN x [ELSE
  0|NULL] END)` idiom now compile for TUMBLE/HOP/SESSION windows instead
  of 409-ing (`SUM(CASE WHEN c THEN 1 ELSE 0 END)` lowers to a
  conditional COUNT; `COUNT(CASE WHEN c THEN col END)` adds the implied
  IS NOT NULL). Predicates lower to a typed, serialized filter AST on
  `WindowAgg` (column-vs-literal comparisons, AND/OR/NOT, IS [NOT] NULL,
  bare boolean columns) that the dataflow operators evaluate once per
  batch as Arrow boolean masks — the window state stays per-key running
  accumulators, no row buffering. Wire-compatible: unfiltered specs
  serialize byte-identically; filtered specs use the lossless JSON
  fragment format.

### Changed

- **IVM ticks reuse one spill-capable `SessionContext` per flow** (G14,
  2026-07-09): `step_datafusion()` built a fresh context every tick, a
  fixed cost that dominated true O(Δ) work — the 2026-07-05 benchmark
  measured full recompute ~100× *faster* than an IVM tick, with the
  crossover extrapolated at ~23M rows. The flow now caches the context
  (async-mutex-guarded; discarded on tick error) and reconciles the
  table catalog each tick to exactly what a fresh context would hold.
  Re-benchmarked (same workload/hardware): ticks dropped to ~13–18 ms
  nearly flat across 50K–1M-row tables; the crossover is now ~500K rows
  and IVM wins at 1M (17.8 ms vs 24.3 ms). Remaining tick slope is the
  O(n) snapshot apply — tracked as the incremental-state follow-up.

### Fixed

- **Multi-file Iceberg snapshots no longer scan only their first data
  file** (2026-07-09): the DataFusion provider built its listing from
  `plan_files()`'s first path only, so any governed Iceberg table whose
  snapshot has more than one Parquet file silently returned a subset of
  its rows (single-file seeds masked this in prod). The listing is now
  multi-path over exactly the snapshot's files — every live file is
  scanned, orphaned files from superseded snapshots never are, and
  projection + Parquet row-group pruning still apply per file.
- **Window aggregates no longer feed NULL inputs into accumulators as
  zeros** (2026-07-09): the per-row accumulate path read `value(row)`
  without a null check, so a NULL in a SUM/MIN/MAX/AVG/STDDEV input
  column entered the aggregate as the Arrow default (0). NULL inputs are
  now skipped, matching SQL semantics.
- **8 stale krishiv-api pipeline tests** (2026-07-09): they asserted the
  pre-AUD-3 behavior that IVM `SUM(Int64)` outputs Float64; the typed
  aggregate rework (4a882d6) correctly emits Int64 (matching batch SQL),
  and that session did not run the krishiv-api suite. Expectations
  updated to Int64.
- **Chained DiffBased views no longer read stale upstream output within a
  tick** (2026-07-09): `SessionContext::register_table` errors on duplicate
  names, and the per-view upstream registration swallowed that error with
  `let _ =`, so a downstream DiffBased view executing after its upstream in
  the same tick kept the upstream's previous-tick MemTable. Registration now
  deregisters first (replace semantics).

- **Large batch results no longer OOM the engine pod** (2026-07-09): a
  collected batch result was materialized wholesale at every hop —
  executor `collect` → one giant unary `TaskStatus` gRPC message →
  coordinator in-memory job results → Flight encode — so the 10.2M-row
  NYC-taxi `clean_trips` join (~354 MB) killed the 2 Gi shared engine pod
  (exit 137) on every 15-minute schedule. Executors now stream query
  output and keep results inline only up to
  `KRISHIV_INLINE_RESULT_MAX_BYTES` (default 8 MiB); anything larger is
  written to one Arrow IPC spool file and delivered to the coordinator in
  3 MiB chunks over a new client-streaming `PushTaskResult` RPC before the
  terminal status (which carries `spooled_result_total_bytes`). The
  coordinator spools to disk (`KRISHIV_RESULT_SPOOL_DIR`, capped by
  `KRISHIV_RESULT_SPOOL_MAX_BYTES`, default 8 GiB), verifies size on
  claim (mismatch/missing → job cancelled, never silent missing rows),
  and consumers decode from the file; Flight `do_get` now encodes result
  batches incrementally instead of buffering the full IPC payload.
- **Disk spill now covers all three SQL modes** (2026-07-09): IVM /
  delta-batch ticks (diff-based recompute, plan fallback, `delta:step:`
  fragments) ran on unbounded `SessionContext`s; they now execute on
  `FairSpillPool` contexts sized by `KRISHIV_QUERY_MEMORY_LIMIT_BYTES`.
  When that env is unset, the per-query limit defaults to cgroup
  memory-limit/4 (explicit `0` still disables), so spill is armed by
  default inside memory-limited containers for batch, streaming, and
  delta-batch alike.
- `checkpoint_barrier_integration` test was stale against the
  ack-registry contract (acks are gated on checkpoint completion since
  the phantom-timeout fix) and failed deterministically; it now simulates
  the runner side (drain injector → `complete()`) and asserts the state
  handle round-trip (2026-07-09).

- **Heartbeats no longer stall behind checkpoint work** (2026-07-09):
  coordinator-issued restore/checkpoint commands ran inline in the
  executor's heartbeat loop, so a multi-second restore or checkpoint upload
  delayed the next heartbeat past the coordinator's timeout — evicting the
  healthy, mid-restore executor and triggering another rollback+restore
  livelock. Commands now run on a dedicated ordered worker (restores first
  within a batch) while heartbeats keep flowing.
- **Executor registry no longer grows without bound** (2026-07-09):
  Lost/Removed executor records are retained long enough for zombie fencing
  (40× the heartbeat timeout, ≥30 min) and then pruned; previously every
  k8s pod restart left a corpse the heartbeat tick iterated forever.
- `tick_period_ms` default corrected to the daemon's real 5 s cadence —
  checkpoint interval timers and ack timeouts convert ticks → ms with it and
  ran 5× slow under the old 1 000 default; the heartbeat clock's quiet-path
  per-job walk is also skipped when no executor was lost and debug logging
  is off (2026-07-09).
- **Healthy executors no longer lose their tasks to heartbeat-timeout lease
  churn** (2026-07-08): the default `heartbeat_timeout_ticks` (3 ticks =
  15 s at the daemon's 5 s tick) left one delayed heartbeat between a
  healthy executor and eviction against the executor's 10 s default
  interval; the eviction was silent (no log, no
  `krishiv_executor_lost_total` increment), and running tasks kept
  reporting the lease frozen into their assignment, so after the executor
  re-registered every status RPC was fenced `stale_lease` and the task
  runner aborted healthy work — the recurring "assigned but not running"
  stuck state, ending in a circuit-breaker launch loop. Timeout default is
  now 9 ticks (≈45 s, ≥3× the heartbeat interval), timeout evictions log
  and count like `mark_executor_lost`, and `send_task_status` stamps the
  freshest of the assignment lease and the live shared lease (B10
  precedent), letting a re-registered executor's tasks self-heal.

- **Deployed builds now include `rest-catalog`** (2026-07-08): `just
  build-k8s` / `build-bare-metal` compiled without the feature, so
  `KRISHIV_ICEBERG_REST_URI`/`_TOKEN`/`_WAREHOUSE`/`_NAME` were silently
  ignored (both `register_rest_catalog_from_env` call sites are
  `#[cfg(feature = "rest-catalog")]`) and governed `krishiv.<ns>.<table>`
  SQL failed with "table not found" from every deployed image. Both recipes
  now build with the feature. Known remaining limitation (platform gap
  G15): registration only works on the InProcess Flight host — a
  coordinator-delegated Flight host still warns and skips; run
  `krishiv flight-server` beside the coordinator until coordinator-side
  registration lands.

### Added

- **New benchmark: IVM vs full-recompute**
  (`crates/krishiv-bench/benches/ivm_vs_full_recompute.rs`,
  `cargo bench -p krishiv-bench --bench ivm_vs_full_recompute`). **Finding
  worth flagging, not just a new benchmark**: at table sizes up to 1M rows,
  a full recompute of a `GROUP BY SUM` query is ~100x *faster* in wall-clock
  time than one `IncrementalFlow::step_datafusion()` tick — every production
  call site constructs a fresh `SessionContext` per tick (confirmed via
  `grep -rn step_datafusion` across `krishiv-executor`/`krishiv-api`/
  `krishiv-scheduler` — all of them use the plain convenience method), and
  that fixed setup cost (~650-700ms) dominates the true O(Δ) aggregate
  work at these scales. Extrapolating measured full-recompute scaling, the
  crossover (where a full recompute costs as much as one current IVM tick)
  is ~23M rows. See `docs/implementation/status.md` for the full
  methodology, numbers, and root-cause read of `flow.rs`. Not fixed here —
  reusing a job-scoped `SessionContext` across ticks is the natural
  follow-up and is flagged for the engine team, not attempted this session.
- **G5: restorable checkpoints for continuous windowed jobs, exercised live on
  a cluster.** Every completed continuous cycle now ships the executor's
  post-cycle `stream:loop` operator state back to the coordinator
  (`TaskOutputMetadata.state_snapshot`, wire field 20; captured via
  `ContinuousWindowExecutor::snapshot()` — the `checkpoint()`-first variant, as
  `peek_snapshot_bytes` serializes a backend the live panes were never written
  into), and the coordinator persists it as the job's `ContinuousSnapshot`. As
  a result `POST /api/v1/continuous/{id}/checkpoint` returns real live state
  and `…/restore` rehydrates a recreated job. Verified live on k8s: seed a
  partial window → checkpoint → deregister → re-register + restore → closing
  the window emits the exact pre-kill accumulations.

### Fixed

- **G12 (JDBC/ADBC `?` parameter binding)**: JDBC/ADBC clients bind
  prepared-statement parameters as ordinal `?` marks, but the engine only
  recognized `$N` — every `?`-bound query counted zero parameters and
  failed with a placeholder error. New `normalize_question_mark_params`
  rewrites `?` to `$1, $2, …` (quote-aware — literal `?`s inside strings
  or quoted identifiers are untouched), wired into prepared-statement
  creation. Also fixes a real feature-gating regression found while
  building this: the G3 fix below used `uuid`, gated behind a narrower
  Cargo feature than the file it's in actually compiles under — any build
  enabling `lakehouse` without `iceberg` failed outright. Replaced with a
  std-only nanosecond+atomic-counter tag.
- **G3 (Iceberg concurrent-commit lost updates)**: `IcebergFsTable::append`
  committed metadata via an unconditional tmp-write + rename to a single
  `metadata.json`, so two concurrent committers — even two tasks sharing
  one instance in a single process, not just two separate processes —
  could last-write-win and silently drop one another's commit. Replaced
  with Iceberg-style versioned commits (`metadata-v{N}.json`, created
  atomically via `create_new`/O_EXCL; losers re-read and retry) and removed
  the in-memory state cache entirely — every read now reflects whatever's
  truly latest on disk. New test
  `concurrent_writers_with_independent_table_handles_lose_no_commits`
  proves 8 independently-instanced concurrent writers all survive.
- **G2 (memory-constrained sort spill)**: a `SqlEngine` configured with a
  memory limit under ~15MB had every sort fail immediately with "Not enough
  memory to continue external sort" — DataFusion's `sort_spill_reservation_bytes`
  defaults to a hardcoded 10MB reserved up front for the merge phase,
  regardless of the configured pool size, so the reservation itself didn't
  fit. `build_single_node_session_config` now scales the reservation down
  proportionally (`(limit / 4).clamp(64KB, 10MB)`) when a memory limit is
  set; deployments at or above 40MB are unaffected. New
  `crates/krishiv-sql/tests/memory_spill.rs` proves sort, grouped
  aggregation, and hash join spill correctly under a 2MB pool, with a
  negative control confirming the workload genuinely requires spill.
- Deregistering a continuous job now actually reaches its executor: the
  teardown uses `push_cancel_job` (broadened to cancel *assigned* streaming
  tasks — a `stream:loop` task is only `Running` inside a cycle), and the
  executor retires the job's identity on cancel — drops the stateful window
  executor + buffered inputs, purges the assignment inbox's
  `(job, task, attempt)` dedupe entries (`forget_job`), and clears the task
  tombstone. Without this, a recreated job reusing the same deterministic ids
  (`task-streaming`, attempts from 1) had its first cycle silently swallowed
  as an at-least-once duplicate, wedging the cycle fence so every later push
  409'd forever.
- A `Cancelled` continuous-cycle task now releases the input-cycle fence like
  a `Failed` one (previously only Succeeded/Failed cleared it).
- **Continuous-job recovery after executor loss**, found live via the
  Krishiv Platform executor fault loop (`tests/e2e/pipelines/fault_loop.py
  MODE=executor`): (1) the input-cycle fence (`continuous_input_cycles`)
  used to stay stuck forever if the executor holding the task was lost
  before it ever sent a terminal status update — `advance_heartbeat_tick`
  now releases it once a tick evicts the executor and the task shows no
  assignment; (2) a continuous task's `assigned_executor` is sticky across
  cycles by design, so once its executor was evicted and reset to
  `Pending`, nothing retried placement unless a *new* executor happened to
  register afterward — `reset_running_tasks_for_lost_executor` now treats
  an idle (`Succeeded`, between-cycles) continuous task the same as
  `Running`/`Assigned` for reassignment purposes; (3) a freshly reassigned
  task started with an empty accumulator, silently losing whatever the job
  had accumulated — the coordinator now seeds `pending_continuous_restores`
  from the job's latest persisted `ContinuousSnapshot` at reassignment
  time, the same recovery a manual `/restore` call would give, automatically;
  also, deregister now clears a job's persisted snapshot so a later job
  reusing the same id doesn't silently inherit a stale watermark; (4) the
  generic background task-launch loop doesn't understand continuous jobs
  are driven exclusively by an explicit `continuous-push` — it would
  auto-dispatch a spurious extra cycle the moment reassignment (2) set the
  task `Assigned`, racing the next real push — `should_consider_for_launch`
  now excludes streaming jobs outright. Result: zero data corruption or
  loss across ~40 live executor-kill iterations after all four fixes,
  versus consistent corruption/loss before them.
- Coordinator HTTP `POST /api/v1/continuous-register-sql`: register a continuous
  windowed streaming job from **SQL** (`SELECT key, AGG(col) FROM TUMBLE/HOP/
  SESSION(TABLE src, DESCRIPTOR(ts), <ms>) GROUP BY …`). The coordinator compiles
  the window TVF to a `WindowExecutionSpec` itself (`krishiv_sql::
  streaming_window_plan`), so callers pass SQL and stay decoupled from the
  operator spec type; the response returns the fed source table. Verified live on
  k8s: register → push timestamped Arrow IPC via `continuous-push` → `continuous-
  drain` emits exact per-region tumbling-window `SUM`/`COUNT` as the watermark
  closes each window.
- IVM incremental-operator state (per-group SUM/COUNT/AVG/MIN-MAX accumulators
  and DISTINCT multiplicities) is now serialized by `checkpoint_full` and
  reapplied on `restore_full`, so a maintained view is restored **losslessly**
  after a coordinator restart — including sources with genuinely duplicate rows,
  which the materialized source snapshot (a set, not a multiset) cannot capture.
  Verified live on k8s: `spike_b_ivm_kill.py --recreate` converges over 50
  destroy→rebuild→restore cycles (G6/F4).
- Coordinator HTTP `DELETE /api/v1/continuous/{job_id}`: deregister (cancel and
  tear down) a continuous windowed streaming job by id. Mirrors the IVM
  view-drop endpoint so an external reconciler can converge a windowed streaming
  table by removing it. Verified live on k8s as part of the pipeline reconcile
  Drop path (`streams: []` after drop).

### Changed

### Fixed

- Coordinator `submit_job` now **replaces** a terminal (Cancelled/Failed/
  Succeeded) job that shares the incoming job id instead of rejecting it as a
  `DuplicateJob`. `cancel_job` marks a job GC-ready but keeps it in the registry
  until the next GC tick, so a delete-then-recreate flow (e.g. a reconciler
  Replace: `DELETE /api/v1/continuous/{id}` then re-register the same id) raced
  the GC and hit `409 Conflict`, leaving the replacement job `Cancelled`.
  `submit_job` now evicts the terminal same-id job up front; a still-live same-id
  job is still rejected as a duplicate. Regression test
  `submit_job_replaces_a_terminal_job_with_the_same_id`; verified live on k8s
  (reconcile Replace converges to a `Running` job with the new window spec).

- IVM: a checkpoint-restored flow no longer loses its incremental aggregate
  accumulator, which previously made the second recreate-recovery cycle diverge
  (a non-retracting insertion corrupted the materialized view). Operators are
  restored from serialized state, or seeded from the restored source snapshot as
  a fallback (correct for distinct-row Join sources).
- connectors: panic-free vector point-id derivation (`first_chunk` instead of
  slice+`expect`) and Pinecone namespace injection (`as_object_mut` instead of
  index-assign), clearing `clippy::indexing_slicing`/`expect_used` under the
  workspace lint now that `vector-sinks` is feature-active.

## [0.1.0-rc.1] - 2026-06-26

### Added

- Public engine contracts, connector maturity, and durable metadata versions.
- Typed Rust/Python DataFrame APIs and Iceberg-first build defaults.
- Phase 5 open-source governance, security, compatibility, benchmarking, and
  release infrastructure.
- Stable API Phase A manifest, per-item metadata, generated Rust/Python/SQL inventories,
  Python type stubs, Rust signature reports, CI change classification, and a unique Python
  `DataFrame` identity.
- Phase B engine-owned expression/type AST shared by Rust, Python, and SQL.
- Phase C canonical DataFrame boundedness, relational operations, typed catalog identifiers,
  and prepared statements.
- Phase D typed I/O contracts, async reader/writer actions, physical file layout controls,
  and coordinator-owned Iceberg atomic commits.
- Phase E typed `QueryHandle`, `BlockingSession` explicit blocking facade, and genuine Python
  asyncio awaitables (`sql_async`, `submit_async`, `collect_async`).
- Phase F `DataStreamReader`/`DataStreamWriter` builders, `StreamingOutputMode`
  (Append/Update/Complete), `StreamingTrigger` variants, stream-table and stream-stream joins,
  deduplication, `foreach_batch`, and `StreamingQuery` lifecycle handle.
- Phase G typed stateful process API: `ProcessFunction`, `CoProcessFunction`,
  `BroadcastProcessFunction`; `ValueState<T>`, `ListState<T>`, `MapState<K,V>`,
  `ReducingState<T>`; event-time and processing-time timers; `OperatorUid`/`OperatorConfig`;
  `ProcessFunctionExecutor` with `snapshot()`/`restore()` for savepoint rescaling.
- Phase H SQL grammar feature matrix (`feature_matrix()`, `features_for_category()`,
  `features_by_status()`); SQLSTATE code mapping (`sqlstate_for()`); `OperationRegistry`
  for thread-safe operation cancellation; `SqlEngine::execute_with_timeout` and
  `SqlEngine::execute_with_operation_id`; `SqlError::OperationCancelled` and
  `SqlError::Timeout` variants.
- Phase I release gate: type/null/time/decimal/ordering/overflow conformance tests;
  embedded and single-node mode conformance tests; streaming delivery certification
  (failure-loop, idempotent re-run, checkpoint round-trip); TPC-H Q1/Q3/Q6/Q10 and
  Nexmark Q1/Q2/Q5/Q8 synthetic baseline gate; parity manifest validation
  (`check_parity_manifest.py`); SBOM and checksum generation (`generate_sbom.py`);
  migration note coverage check (`check_migration_notes.py`); master gate script
  (`check_phase_i_gate.py`); runnable examples (`basic_sql`, `streaming_word_count`).
- CI: replaced self-hosted runners with ubuntu-latest, optimized workflow triggers.
- Crate READMEs for all 24 workspace crates.
- Universal `skills/` directory for multi-agent skill sharing.

### Changed

- Rewrote the architecture document against the current workspace.
- `PySession::sql_async` upgraded from `block_in_place` to a genuine asyncio coroutine.
- `QueryHandle` now routes collect, writes, and stream submission through a single typed
  handle; use `DataFrame::submit_async()` to obtain a handle.

### Migration Notes

- **`Session.sql_async` (Python)**: Signature updated to align with the Rust Session API. Use `Session.sql_async (same name, updated signature)`.
- **`Stream._tumbling_window_secs_body` (Python)**: Internal helper renamed/updated. Underscore-prefixed, not part of the stable public API. Use `Stream.tumbling_window (public stable API unchanged)`.
- **`SqlDataFrame` (SQL)**: Derive set changed as part of SQL API surface cleanup. Use `SqlDataFrame (struct retained, derive set updated)`.
- **`DataFrameWriter::option` (Rust)**: Writer option() inventory id changed. Use `DataFrameWriter::option(mut self, key, value)`.
- **`StreamingDataFrame` (Rust)**: Gained `Clone` derive for Python streaming join bindings. Use `StreamingDataFrame (#[derive(Clone)] retained)`.
- **`DataFrame` (Python)**: The legacy `Relation` class (previously exported as the
  unified wrapper) was renamed before Phase A. Use `DataFrame` — `Relation` is a
  deprecated alias that will be removed in 1.0.
- **`sql_async` (Python)**: Now returns a true asyncio coroutine; existing code that
  called `asyncio.run(session.sql_async(...))` continues to work. Code that passed the
  return value to `loop.run_until_complete` without `await` must add `await`.
- **`BlockingSession`**: Callers who used hidden `block_on` internals in the Rust API
  should migrate to `BlockingSession::new(session)` for explicit blocking behaviour.
- **`execute_with_timeout` / `OperationRegistry`**: Replace ad-hoc timeout wrappers
  around `SqlEngine::sql()` with `SqlEngine::execute_with_timeout(sql, timeout_ms)`.

## [0.1.0]

Initial pre-1.0 development release line.
