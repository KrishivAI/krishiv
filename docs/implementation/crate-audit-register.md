# Crate audit register

A read-every-file audit of all 27 crates (~314k LOC): bugs, architectural
bottlenecks, bad practice, algorithmic and data-structure choices, dead code,
and test coverage. Coverage is **measured** with `cargo llvm-cov`, never
assumed.

Standing rule from the SF100 work, and the reason this register exists:
**every bug found so far was a guard or rewrite that silently did nothing, or
did the wrong thing, with a test too weak to notice.** Two of them had tests
that passed against broken code *by construction*. So:

> Every test added here must be checked to fail against the pre-fix behaviour.
> A test that cannot distinguish correct from broken is worse than no test,
> because it makes the gap invisible.

Top priority remains all 22 TPC-H SF100 queries genuinely distributed on the
cluster. This audit runs between cluster runs, not instead of them.

## Priority order

Ranked by (1) on the distributed TPC-H critical path, (2) blast radius of a
silent wrong answer, (3) size × test-thinness.

| # | crate | LOC | files | test files | why here |
|---|---|---|---|---|---|
| **Tier 1 — critical path** |
| 1 | krishiv-sql | 38,727 | 54 | 49 | first slice done; `lib.rs` alone holds 37% of the crate's uncovered regions |
| 2 | krishiv-executor | 21,867 | 32 | 27 | shuffle write/drain (D7), task running, the memory pool |
| 3 | krishiv-shuffle | 8,153 | 24 | 22 | the disk story lives here; no streaming write path |
| 4 | krishiv-scheduler | 39,142 | 56 | 56 | stage cutting, dispatch, the single-task fallback, SC11 breaker |
| **Tier 2 — correctness blast radius** |
| 5 | krishiv-plan | 14,385 | 25 | 19 | plan IR every surface depends on |
| 6 | krishiv-common | 7,525 | 22 | 19 | env registry, durability profiles, memory budget |
| 7 | krishiv-connectors | 39,088 | 95 | 53 | ingest correctness; 42 files with no tests |
| 8 | krishiv-state | 11,980 | 34 | 16 | checkpoints/restore; fewer than half the files tested |
| **Tier 3 — runtime & surfaces** |
| 9 | krishiv-api | 25,039 | 38 | 25 | |
| 10 | krishiv-runtime | 12,978 | 15 | 13 | |
| 11 | krishiv-dataflow | 18,016 | 37 | 32 | |
| 12 | krishiv-ivm | 6,387 | 8 | 5 | |
| 13 | krishiv-delta | 6,880 | 19 | 16 | |
| 14 | krishiv-flight-sql | 5,170 | 6 | 4 | |
| 15 | krishiv-proto | 8,130 | 12 | **3** | 8k LOC, 3 test files |
| 16 | krishiv-metrics | 3,731 | 6 | 5 | |
| 17 | krishiv-engine-core | 3,146 | 11 | 8 | |
| **Tier 4 — thin, tooling, structural smells** |
| 18 | krishiv-python | 12,892 | 35 | **8** | excluded from CI clippy — breakage is invisible |
| 19 | krishiv-operator | 4,878 | 19 | 5 | |
| 20 | krishiv-mcp | 3,296 | **1** | 1 | one 3,296-line file |
| 21 | krishiv | 6,194 | 19 | 9 | binary/CLI |
| 22 | krishiv-engines | 2,216 | **1** | 1 | one file |
| 23 | krishiv-ui | 2,384 | 4 | 1 | |
| 24 | krishiv-bench | 1,947 | 9 | 4 | |
| 25 | krishiv-sql-gateway | 541 | 3 | 1 | |
| 26 | krishiv-conformance | 209 | 1 | **0** | no tests at all |
| 27 | krishiv-chaos | **0** | 0 | 0 | empty crate — delete or fill |

---

## 1. krishiv-sql — first slice complete

Measured coverage: **78.01% regions, 70.84% functions, 76.87% lines.**

Uncovered-region concentration (this decides what to test next):

| file | uncovered | cover |
|---|---|---|
| lib.rs | **2540** | 40.71% |
| distributed_plan.rs | 469 | 90.28% |
| connector_table.rs | 309 | 18.68% |
| cep_sql.rs | 266 | 68.22% |
| udf.rs | 265 | 50.09% |
| kafka_table.rs | 239 → covered | 0.00% → fixed |
| lakehouse/merge.rs | 235 | 49.89% |
| lakehouse/providers.rs | 139 | 16.77% |

### Fixed

- [x] `semi_join_reduction` — `Arc::ptr_eq` chose the join child to rewrite;
      true for both orientations when a self-join shares one `Arc`, so the
      rewrite went into the wrong side. `4e9203e9`
- [x] `coop_amplifiers` — `unnest` documented as covered but never matched;
      rule not idempotent (`transform_up` added a wrapper per pass). First
      tests for the module. `d1c5752c`
- [x] `subquery` — streaming guard walked only `SetExpr::Select`; a streaming
      subquery in a UNION, CTE, derived table, `JOIN…ON`, **parentheses**, or
      `INSERT…SELECT` walked past it. Replaced with sqlparser's
      `visit_expressions`, −55 lines. `927c1243`
- [x] `analyze` — min/max compared as strings (Int `[9,10]` → min `"10"`);
      Decimal128/Date32 stringified via `Debug` of a one-row array. Now Arrow
      row encoding + `ArrayFormatter`. `5a7f60c0`
- [x] `recursive_cte` — fixpoint bound the self-reference to the accumulation
      instead of the working table (duplicates, no fixpoint); detection
      required literal `"WITH RECURSIVE"` prefix; one test accepted all three
      possible outcomes. Module has **no callers**. `48aa32e0`
- [x] `kafka_table` — Arrow's lenient cast turns unparseable values into
      nulls; the promised warning was never implemented, so Kafka fields were
      dropped silently. 0% → covered. `86abeff9`
- [x] `lakehouse/providers` — **time travel returned the present**. The AS OF
      clause is stripped from the SQL before DataFusion sees it, and only
      `delta.<path>` refs were honoured; a timestamp mapped to `None`, which
      means "latest". Both now error. `69bfc86d`

### Open

- [ ] `lib.rs` — 2540 uncovered regions, the single largest target anywhere
- [ ] `connector_table.rs` — `streaming_sources` set and the
      `has_streaming_sources` latch look **insert-only** (lib.rs 1329/1357/
      1497/1578, no removes): a dropped Kafka table may stay "streaming"
      forever. Unconfirmed.
- [ ] `connector_table.rs` — `is_object_store_url` is case-sensitive, so
      `LOCATION 'S3://…'` misroutes to the local-filesystem path
- [ ] `lakehouse/providers.rs` — `DeltaScanProvider::scan` and
      `HudiScanProvider::scan` drain the **whole table** into a `MemTable`
      before projection/limit apply. This is the exact pattern
      `connector_table.rs` already retired (Phase 52 #194); the streaming
      replacement pattern is next door in `BoundedConnectorPartitionStream`.
- [ ] `recursive_cte` — unreachable; wire it or delete it

---

## 2–27. Not yet started

Each crate gets the same treatment and its own section here: measured
coverage, a table of uncovered-region concentration, a fixed list with commit
hashes, and an open list. Sections are appended as the audit reaches them.

---

## Cross-cutting findings

Things that are not one crate's problem.

- [ ] **A killed executor never reclaims its shuffle scratch.** 74 GB was
      stranded on one node when its executor died in `Error`; normal
      termination reclaims fine (s1/s2 went GB → KB on their own). On 145 GB
      nodes one killed executor can take the node out of the cluster until a
      human intervenes.
- [ ] **Kubelet image GC deletes the engine image under DiskPressure**, so the
      replacement executor lands in `ImagePullBackOff` and the node stays down
      even after disk is freed. Local images with no registry have no recovery
      path.
- [ ] **D7 — `ShuffleWriteBuffer::drain_partition` reads every spilled run of
      a partition back into memory at once**, held by a `can_spill(false)`
      consumer, and `account_unavoidable` grows it past the pool
      unconditionally. In `FairSpillPool` an oversized *unspillable* total
      saturates `pool_size - unspillable` to zero, which zeroes **every**
      consumer's share. `ShuffleStore::write_partition` takes a whole
      partition and has no append — but `LocalDiskShuffleStore` uses
      `ArrowWriter`, which accepts batches incrementally, so a streaming write
      is buildable.
- [x] **Abandoning a benchmark query left it running on the cluster.** The
      coordinator has no notion of a client going away; the harness never
      cancelled. Every killed sweep left its query executing, holding slots
      and scratch. Two q10 jobs ran at once; the abandoned one held 3 tasks
      and 30 completed stages while the new one sat at `running=0` behind it.
      This faked a scheduling bug, a shuffle skew, and a disk eviction.
      Fixed `4de7e025` — cancel on poll failure, timeout, SIGINT/SIGTERM/
      SIGHUP, and interpreter exit.
- [ ] **One executor runs everything while the others idle.** Reproduces on a
      clean cluster with no orphan. `/api/v1/executors` reports all three
      `Healthy` with `running_task_count: 3` (9 total) while the job reports
      `run=3` and two executors burn 1 millicore. Six are phantom. **But**
      `running_tasks` is self-reported by the executor
      (`heartbeat_mapping.rs:14` <- `request.running_attempts()`), and if one
      node holds the map stage the others' reduce tasks are legitimately
      blocked on its shuffle output. Needs stage-level evidence before it is
      called a scheduler bug.
- [ ] **`krishiv-python` is excluded from `just test` and `just lint`**, so
      Rust breakage there is invisible to CI.
