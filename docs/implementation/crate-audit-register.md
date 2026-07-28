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

## 3. krishiv-shuffle — in progress

Measured: **81.07% regions, 68.67% functions, 78.07% lines** (11,475 regions,
2,172 uncovered).

| file | uncovered | cover | note |
|---|---|---|---|
| shuffle_svc.rs | **438** | **6.61%** | 44 untested fns — biggest hole in the crate |
| flight.rs | 296 | 76.64% | 61 untested fns; the shuffle transport |
| disk_store.rs | 281 | 73.76% | the store the cluster uses |
| range_partitioner.rs | 236 | 58.01% | |
| storage_uri.rs | **181** | **0.00%** | never executed by any test |
| sort_shuffle_writer.rs | 164 | 77.41% | |
| object_store.rs | 114 | 66.47% | |

### Fixed

- [x] **Spill files were reclaimable only by a boot that could not happen**
      (`7e8c9a26`). Spills are written flat in the scratch root so
      `scan_orphans` cannot delete a live task's data — which also made them
      invisible to the sweeper, leaving `cleanup_temp_files` at store
      construction as the only reclaim path. An executor died holding 74 GB on
      a 145 GB node; the kubelet GC'd the engine image to reclaim disk, the
      replacement hit `ImagePullBackOff`, and the boot that would have freed
      the space could not run. Added ownership-stamped spill names and
      `reclaim_foreign_spills`, wired into the periodic sweep.

      Two rules that look right and are not, recorded so they are not retried:
      **pid** (container PID namespace reuses it across restarts) and
      **mtime** (a spill lives for the whole map task, 20-60 min at SF100, so
      any safe threshold reclaims nothing).

- [x] **D7 streaming write** (`85d81234`). `ShuffleStore::write_partition_stream`
      takes a stream of batches. The default collects and delegates, so
      in-memory and tee-ing tiered stores are untouched; `LocalDiskShuffleStore`
      overrides it (batches cross a depth-2 channel to a blocking `ArrowWriter`
      and are dropped as each is serialised) and `write_partition` now routes
      *through* it, so one place opens the temp file, hashes, and commits.
      `ShuffleBackend` dispatches to the concrete store — forwarding to the
      trait default there would silently collect and undo the call.

- [x] **The push-shuffle completeness gate was write-only state**
      (`55d97dcb`). `expected_pushes` was written by `set_expected_pushes` and
      read by *nothing*, while three doc comments — the field, the setter, and
      the `POST /ess/expect/…` route — all promised `merge_read` waits for
      every declared push. A reduce task fetching `/ess/merged/…` between two
      map pushes got a well-formed Arrow stream missing some map tasks' rows.
      Nothing downstream distinguishes that from a genuinely smaller
      partition, so the query returned a **wrong answer and succeeded**.
      Alongside: `gc_job`/`gc_stage` never cleared the counts (leak, plus a
      stale gate on a reused job id), and `merge_read` cloned the chunk list
      then concatenated it, holding two copies of a partition on the path whose
      purpose is avoiding N fetches of it.

- [x] **`ess_read_partition` sized an allocation from the index file**
      (`55d97dcb`). `let len = (end - start) as usize` on a non-monotonic index
      underflows — debug panics in the handler, release wraps to near
      `u64::MAX` and hands it to `vec![0u8; len]`, aborting the process. A
      crash mid-index-write is enough. Descending pairs and offsets past EOF
      are now refused as bad requests.

- [x] **`shuffle_svc.rs` 6.61% → covered** (`55d97dcb`). Its auth is a
      per-handler `check_bearer_token` call rather than middleware, so a new
      handler that forgets it is silently open; the added test enumerates every
      data route and asserts both a wrong token and no token are refused.

- [x] **`storage_uri.rs` 0.00% → covered.** `s3_shuffle_store` extracted: the
      bucket/prefix split, the `"shuffle"` default, and the client construction
      were duplicated verbatim between the two callers with nothing keeping
      them in step. Empty bucket now refused by name.

- [x] **Staging files leaked on every write failure** (`85d81234`). Any error
      after `File::create` — a Parquet write error, a source-stream error, a
      poisoned lease lock — left a `*.tmp.N`, and the only thing that removed
      those was `cleanup_temp_files` at store construction, i.e. at executor
      boot, which a node that filled its disk cannot do. A `StagingFiles` drop
      guard reclaims at the point of failure. Same shape as the spill-file
      finding above, same fix.

### Open

- [ ] `flight.rs` 61 untested fns — the path a coalesced partition travels,
      and the tonic 4 MiB decode limit already broke q10 once
- [ ] `range_partitioner.rs` at 58.01%

---

## 2. krishiv-executor — in progress

### Map of the crate

The distributed-batch critical path is `fragment/` + `runner/`. **Which
map-write path SF100 takes** is the first thing to know: the bench submits SQL,
the coordinator plans dfplan fragments, and the failure strings say
`krishiv-fragment:{"version":1,"execution_kind":"Batch","body":"dfplan:v1:…`.
That is `drain_into_store` (batch.rs ~1156) — **not** `execute_shuffle_write`
(~758) and not `execute_inmem_shuffle_write` (~1322).

### Fixed

- [x] **The drain declared a whole partition to the pool** (`85d81234`).
      `ShuffleWriteBuffer::drain_partition_stream` reads spilled runs back one
      Arrow batch at a time, unlinks and evicts each run's file the moment it
      is exhausted, and coalesces to the 8 MB target incrementally. What it
      declares is the un-spilled tail plus a constant 24 MB window.

      Measured on the same input: the collecting drain declared 8.5 MB for a
      ~8 MB partition and 34.2 MB for a ~32 MB one; the streaming drain
      declares ~25 MB for both. The test asserts the gap stays within the soft
      ceiling and fails against the old behaviour by 25.6 MB.

- [x] **The result spool left its whole size in the cgroup's page cache**
      (`31dae758`). The same accounting hole, in the same process, that was
      already found and fixed on the shuffle path — memory the DataFusion pool
      cannot see, growing with data size, invisible as a spill. The write left
      every byte resident and the sequential read back left the whole file
      resident until `SpooledTaskResult` drops, which is *after* the push.
      Write side now fsyncs then evicts (`DONTNEED` skips dirty pages, so the
      order matters); read side evicts each chunk once it is in the buffer
      being sent.

- [x] **`truncate_on_char_boundary` could return `max + 2` bytes**
      (`26267f9d`). `…` is three UTF-8 bytes and was appended *after*
      truncating to `max - 1`. It is the last guard before a task failure goes
      on the wire. Neither it nor `format_failure_message` had any test, though
      `format_failure_message` produces every failure line an operator reads
      off a cluster run.

- [x] **`PushShuffleClient::with_timeout` reported a timeout it did not
      enforce** (`26267f9d`) — it set the field unconditionally and rebuilt the
      HTTP client only `if let Ok(...)`. The existing test asserted the private
      field, so it could not catch this. `fetch_merged` also ended in
      `bytes.to_vec()`, a second full copy of a merged partition.

- [x] **`LocalParquetPartition` parsing had no tests** (`26267f9d`), including
      the duplicate-table-name refusal — a second partition with the same name
      would silently shadow the first in the session context, so the task would
      read one file and report success for both.

### Open

- [ ] **`PushShuffleClient` has zero constructors in the workspace.** The ESS
      server routes are live via `krishiv shuffle-svc`, but nothing builds the
      client; `ctx.push_store` is the in-process `PushShuffleStore`, a
      different type. Wire-or-delete decision, not an audit fix.
- [ ] **The reduce side is the mirror of D7 and still collects.**
      `ShufflePartitionReader::read_partition` (krishiv-sql) is typed
      `Result<Vec<RecordBatch>, String>`, so `InmemDfplanShuffleReader`
      materialises one whole partition per call. Bounded by partition size
      rather than by the stage, so much milder than the map side was — but
      fixing it means changing the trait, i.e. a krishiv-sql change.
- [ ] `read_shuffle_flight_partitions` (`fragment/common.rs`) collects **every**
      shuffle input of a task into one `Vec` before the engine sees any of it,
      outside the pool. Only the legacy typed paths (batch.rs:76, :299) call
      it, not dfplan.
- [ ] `execute_shuffle_write` (batch.rs ~758) still collects; it has the
      push-shuffle IPC mirror, which needs all batches when `ctx.push_store` is
      `Some`, so converting it means a conditional path. Not on the SF100 route.
- [ ] `fragment/common.rs` (2172) and `fragment/run_loop.rs` (1480) not yet
      read end to end.
- [ ] Executor `running_task_count: 3` self-reported on all three nodes while
      the job reports `run=3` total, two nodes near-idle. It comes from
      `heartbeat_mapping.rs:14` ← `request.running_attempts()`, and a blocked
      reduce task waiting on an upstream map stage looks identical. Needs
      stage-level evidence before it is called a scheduling bug.

---

## 4–27. Not yet started

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
- [x] **D7 starvation — one drain's overshoot zeroed every consumer's share**
      (`c8736a52`). Confirmed as the q10 SF100 failure on a clean cluster with
      no orphan: `Failed to allocate additional 877.0 B for HashJoinInput`.
      `account_unavoidable` grew an unspillable reservation past the pool;
      `FairSpillPool` computes `pool_size - (unspillable + spillable)` in both
      branches, so that saturates availability to zero for everyone — and
      nothing can back off, because the reservation that did it cannot spill.
      The bytes are still admitted and logged; they are no longer written into
      the pool's arithmetic. Also explains why `UnspillableHeadroomPool` never
      logged its ceiling: it bounds spillable consumers and delegates
      unspillable ones straight through.
- [ ] **D7 remainder — `ShuffleWriteBuffer::drain_partition` reads every spilled run of
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
