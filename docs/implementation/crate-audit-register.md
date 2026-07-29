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

## 1. krishiv-sql — 7 of 54 files read whole ("first slice")

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

## 3. krishiv-shuffle — 7 of 24 files read whole

**A crate is "covered" only when every file has been read end to end.** This
section previously implied more than was done: the open-item list was closed,
which is not the same thing. Read whole so far — `store.rs`, `local_store.rs`,
`disk_store.rs`, `shuffle_svc.rs`, `push_shuffle.rs`, `storage_uri.rs`,
`orphan.rs` (3,956 of 9,269 lines). Remaining, largest first: `flight.rs`
(1486), `partitioner.rs` (743), `sort_shuffle_writer.rs` (584),
`range_partitioner.rs` (536), `object_store.rs` (520), `memory_store.rs` (382),
`spillable.rs` (308), `tiered_store.rs` (125), `token_auth.rs` (123),
`metadata.rs` (104), `error.rs` (94), `compression.rs` (67),
`lease_persistence.rs` (50), `path.rs` (36).

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

## 2. krishiv-executor — 5 of 32 files read whole

Read whole: `fragment/shuffle_write_buffer.rs`, `runner/partition.rs`,
`runner/result_spool.rs`, `transport.rs`, `ess_client.rs`. `fragment/batch.rs`
and `fragment/common.rs` read in regions only. Everything else unread —
`cli.rs` (2154), `runner/executor_task_runner.rs` (1992),
`fragment/run_loop.rs` (1480), `fragment/streaming.rs` (1358) are the largest.

Measured **2026-07-29**: **75.34% regions, 62.44% functions, 73.96% lines**
(27,312 regions, 6,736 uncovered).

| file | uncov | cover |
|---|---|---|
| cli.rs | **1400** | 39.13% |
| fragment/batch.rs | 709 | 68.92% |
| fragment/streaming.rs | 683 | 53.09% |
| runner/executor_task_runner.rs | 622 | 61.44% |
| fragment/run_loop.rs | 471 | 69.79% |
| fragment/common.rs | 470 | 79.15% |
| transport.rs | 368 | **46.90%** |
| runner/task_output.rs | 220 | 59.56% |
| grpc.rs | 194 | 58.01% |
| barrier_grpc.rs | 145 | **33.18%** |

Already strong: `assignment_inbox.rs` 96.19%, `runner/partition.rs` 94.99%,
`fragment/shuffle_write_buffer.rs` 85.04%.

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

- [x] **`heartbeat_request` cleared the progress buffer while *building*
      the request** (`684d4863`). A heartbeat that then failed lost those
      reports — and a coordinator outage is exactly when every heartbeat fails
      and exactly when someone is watching freshness. Any caller who built a
      request to inspect it also drained the buffer. Now cleared only after a
      delivered heartbeat, on all three send paths.

- [x] **The heartbeat's network counter summed loopback** (`684d4863`). Every
      loopback byte is counted once as sent and once as received, and an
      executor pod uses loopback constantly, so the number both double-counted
      and reported traffic that never crossed the interconnect it exists to
      measure.

- [x] **The three `/proc` parsers had their file path baked in**, so nothing
      could exercise them (`684d4863`) — they run on every heartbeat and their
      output is what an operator sees as an executor's memory and network.
      Split over `&str` and covered, including that a missing or unparseable
      value reads as *unknown*, not zero: the heartbeat omits the field on
      `None`, and a reported 0 bytes of RSS looks like a healthy idle executor.

- [x] **`LocalParquetPartition` parsing had no tests** (`26267f9d`), including
      the duplicate-table-name refusal — a second partition with the same name
      would silently shadow the first in the session context, so the task would
      read one file and report success for both.

### Open

- [ ] **Two of the three map-write paths are unreachable.** `dfplan` is the
      only live one.

      * `execute_shuffle_write` (batch.rs ~758): nothing anywhere *constructs*
        a `shuffle-write:` fragment. The string appears only in the executor's
        own dispatch (`batch.rs:209`), one error message, and five doc
        comments — no scheduler, no coordinator, no test. Checked before
        converting it to the streaming write, which would have been effort
        spent on dead code.
      * `PushShuffleClient`: zero constructors. The ESS server routes are live
        via `krishiv shuffle-svc`, but nothing builds the client;
        `ctx.push_store` is the in-process `PushShuffleStore`, a different type.
        It is at least **verified** now (`d666748f`) —
        `tests/ess_push_shuffle_e2e.rs` stands the real service up on a real
        port and drives the real client at it, so the two are known to agree on
        routes, auth, and the merged-read gate.

      Both carried real bugs fixed this pass that could never have been hit.
      Wire-or-delete is a product decision, not an audit fix — but it should be
      made, and it now rests on information rather than a guess.
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

- [ ] **The streaming dials exist twice, byte-identically.**
      `idle_tick_interval()` (`KRISHIV_IDLE_TICK_MS`, default 500) is duplicated
      between `krishiv-executor/src/fragment/run_loop.rs` and
      `krishiv-engines/src/lib.rs`, and the `KRISHIV_STREAM_PROFILE`
      `"throughput"` test is duplicated between `run_loop.rs::stream_linger` and
      `krishiv-engines`' `StreamProfile::parse`. They agree **today** — checked,
      no drift — but this is exactly the shape that produced the
      `task_engine_parallelism` bug, where one site read `KRISHIV_TASK_SLOTS`
      and the other read the real slot count, so `--slots 1` on a 4-core
      executor silently used a quarter of the CPU.

      Not fixed here because `krishiv-engines` does not depend on
      `krishiv-common` (where `env_registry` already lives), so the consolidation
      is a new crate dependency rather than a move. Worth doing; wants a
      deliberate decision, not a 4 a.m. one.

      A sweep of every `KRISHIV_*` read from more than one site found no other
      duplicate *behavioural* dial — the rest are either registry/doctor
      declarations or genuinely independent consumers.

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

## krishiv-shuffle — unreachable surface (2026-07-29)

A reachability pass over every remaining module found that a large part of
this crate is not reachable from the running engine. Recorded here rather
than left implicit, because "it exists and has tests" reads as "it works".

- [x] **`SaltedHashPartitioner` / `SaltSpec` — DELETED** (373 lines incl.
      tests, `2379763c`). Nothing constructed one; `salt` does not appear in
      krishiv-scheduler, krishiv-executor or krishiv-proto at all, so its doc
      claim that "the scheduler never applies salt overrides to streaming
      jobs" implied a batch path that did not exist. Skew mitigation is
      already shipped by reduce-side range splitting in
      `coordinator/aqe.rs` — flag-gated (`KRISHIV_AQE_SKEW_SPLIT`), metered
      (`aqe_skew_splits_total`), tested. A second unreachable implementation
      of a solved problem, whose "only safe when the consumer merges
      sub-partitions" scope was a comment rather than a precondition.
- [ ] **`sort_shuffle_writer.rs` (584 lines) — unwired, ESS family.**
      Constructed only when `ctx.ess_index.is_some()`; `ess_index` is `None`
      at all three construction sites (`executor/src/cli.rs:618,629,661`) and
      nothing anywhere sets it. **Blocker before wiring**: every output path
      is `{job_id}_{stage_id}` — data, index, and spill files — with no
      map-task identity, and `SortShuffleIndex` is keyed the same way. A
      stage has one task per partition and the executor runs three slots, so
      two tasks of one stage on one executor overwrite each other and the
      second `register()` evicts the first. Asserted by
      `two_tasks_of_one_stage_overwrite_each_others_output`, which proves
      task A's rows disappear with no error. Spark's layout carries `mapId`;
      there is no counterpart here. Invert that test into the correctness
      check when task identity is added.
- [ ] **`range_partitioner.rs` (536 lines) — unwired.** `RangePartitioner`,
      `RangeSampler` and `RangeBound` are referenced *only* by the `pub use`
      in `lib.rs`. Range partitioning is the basis of a distributed sorted
      output; today ordering is produced by a final merge instead, which is
      adequate at TPC-H result sizes and not equivalent in general. Wire or
      delete — do not leave.
- [ ] **`spillable.rs` (308 lines) — unwired.** `SpillableShuffleBackend` has
      no constructor outside its own tests; the only other mentions are a doc
      comment and the `pub use`. It is a thin composition of
      `InMemoryShuffleStore` + `LocalDiskShuffleStore` that callers already
      build directly, plus budget/UMM accounting.
      **Consequence worth naming: `MemoryRegion::Shuffle` is never populated
      in production.** Outside this file the region appears only in the
      `UnifiedMemoryManager`'s own definition and tests, so the UMM reserves
      `shuffle_min_fraction` of the pool for a consumer that never reports a
      byte. Second hazard: this store does not override
      `write_partition_stream`, so wiring it as-is would silently inherit the
      collecting default — the D7 bug.
- [ ] **`etcd` feature fails the repo's own clippy bar.** `just lint` runs
      `--workspace` without `--all-features`, so the 8 `indexing`/`unwrap`
      denials in `etcd_metadata.rs` and the `block_on` in
      `coordinator_daemon.rs:232` are invisible to CI. Same blind-spot class
      the justfile already documents for `krishiv-sql` immediately above the
      lint recipe.

## Shuffle bytes cross a 7.6 MB/s wire uncompressed (2026-07-29)

Not a bug — a gap, and on this cluster probably the largest remaining
performance lever. Recorded with evidence so it can be measured rather than
argued.

**Nothing on the shuffle path compresses anything.**

- Disk: `LocalDiskShuffleStore`'s default is `ShuffleCompression::None`
  (`disk_store.rs:216`), and the production construction site
  (`executor/src/cli.rs:640`) never calls `.with_compression()`. Only
  `shuffle_svc.rs:107` (the ESS, itself unwired) asks for Lz4.
- Wire: no `send_compressed`, `accept_compressed`, or `CompressionEncoding`
  anywhere in `krishiv-shuffle`, `krishiv-executor`, or `krishiv-runtime`.
  `tonic` is declared with `features = ["transport", "tls-ring"]` — no
  `gzip`/`zstd` — so gRPC compression is not merely off, it is not
  compiled in.

**Why it matters here.** Measured 2026-07-29: node-local object reads run at
150–286 MB/s, while pod-to-pod traffic runs at ~7.6 MB/s (39 GiB mirror,
timed). Reduce tasks fetch shuffle partitions from other executors over Arrow
Flight, so roughly two thirds of every shuffle crosses the slow link. The
existing note that q8/q9's ~36 GiB shuffle is "55 minutes of pure wire" is the
same observation from the other end.

**The right lever is Arrow-native, not gRPC-level.** Arrow IPC supports body
compression (`LZ4_FRAME`, `ZSTD`) via
`IpcWriteOptions::try_with_compression`, carried in the record-batch metadata
and decompressed by any conforming reader with no protocol negotiation. That
composes with `FlightDataEncoderBuilder::with_options` and needs no tonic
feature change. LZ4 compresses at roughly 100x the speed of this link, so
there is no plausible regime on *this* cluster where it loses; on a 10 GbE
fabric the trade-off is real and it should stay configurable.

Note when reading the numbers: the Krishiv SF100 results recorded on
2026-07-29 were produced with **no shuffle compression of any kind**. That is
headroom, not a caveat on their validity.

- [x] Enable Arrow IPC body compression on the shuffle Flight path, keep it
      configurable. Done (`b80d4559`), default LZ4, 4.54x measured on a
      shuffle-shaped payload.
- [x] Compress partitions at rest too — both stores defaulted to `None` on
      every path a distributed query uses (`7ead8933`), 7.55x measured.
- [ ] **A/B both on q8/q9** (the two most shuffle-heavy queries). Neither has
      been measured end to end on the cluster: the running image predates them,
      and rebuilding mid-sweep would have invalidated the locality comparison
      and the Spark baseline. Until that A/B runs, the ratios above are
      microbenchmarks, not query speedups — a partition that never crossed the
      wire saves nothing.

### krishiv-shuffle coverage as of 2026-07-29

Read end to end (findings fixed and committed): `flight.rs`, `disk_store.rs`,
`shuffle_svc.rs`, `sort_shuffle_writer.rs`, `partitioner.rs`, `memory_store.rs`,
`compression.rs`, `push_shuffle.rs`, `store.rs`, `lease_persistence.rs`.
Deleted after confirming they were unreachable: `range_partitioner.rs`,
`spillable.rs`.

The remaining eleven — `orphan.rs`, `object_store.rs`, `storage_uri.rs`,
`local_store.rs`, `tiered_store.rs`, `token_auth.rs`, `metadata.rs`,
`error.rs`, `lib.rs`, `tests.rs`, `path.rs` — have now been read end to end
as well. **The crate is complete.**

#### What the reads found that the scan did not

The eleven had been through a *targeted scan* for the five defect shapes this
audit had been finding (silent skip on a failed lookup, `debug_assert` guards
that vanish in release, branch decisions taken by matching an error's rendered
string, whole-collection materialisation on a hot path, a lock held across
`.await`). That scan came back clean. Reading them found five more defects,
none of which matches any of those five shapes:

| File | Defect |
|---|---|
| `tiered_store.rs` | `register_partition_lease` used `try_join!`, cancelling one tier on the other's error — contradicting the comment two functions below explaining why that is wrong. Tiers could hold different fencing tokens while reads fall back local→remote. |
| `coordinator_daemon.rs` (found *from* `orphan.rs`) | The coordinator's orphan GC called `cleanup_orphans` directly, reclaiming on the first absence. `OrphanReclaimTracker`'s own doc names `job_coordinators.keys()` — the coordinator's set — as the hazard, and the executor got the protection while the coordinator did not. |
| `storage_uri.rs` | `open_tiered_shuffle_backend` stated its durability-profile rule in a doc comment and enforced nothing, so the same `s3://` URI was accepted or refused depending on whether a local dir was also set. |
| `object_store.rs`, `disk_store.rs` | Both stores defaulted to `ShuffleCompression::None`; the only production caller that ever set a codec was `shuffle_svc`. Every backend a distributed query uses wrote partitions raw. 7.55x measured. |
| `object_store.rs` | `stream_partition` copied the whole fetched object via `to_vec()`, justified by a comment claiming `Bytes` could not be moved into `spawn_blocking` — it can. |

`local_store.rs` turned out to be a `#[cfg(test)]`-only parallel store with its
own on-disk format that nothing ships; deleted, with its one load-bearing
property re-proven against the real formats.

#### The conclusion to carry forward

A clean scan is weaker evidence than a read, and this is now measured rather
than asserted: the scan cleared eleven files, and reading those same eleven
produced five defects — one of them a durability bug in the fencing path, one a
GC that could delete a live job's shuffle output on a coordinator failover.

Every one was a comment and its code disagreeing. No grep finds that, because
the defect is not in the code's shape — it is in the gap between what the code
does and what the file says it does. Scanning tells you a crate has none of the
defects you already know about. Only reading tells you what it has.

## krishiv-executor: barrier subsystem read 2026-07-29

Three files read end to end: `barrier_grpc.rs` (the crate's worst-covered
module at 33%), `barrier_transport.rs`, `barrier.rs`.

**Two defects fixed.**

- `11de2e99` — `ExecutorBarrierService::background_tasks` was documented as
  "aborted when the service is dropped"; nothing aborted them. One leaked task
  per `barrier_stream` RPC on a long-lived executor. The obvious fix is wrong:
  the service is `Clone`, so a `Drop` on it would abort streams the surviving
  clones still serve. Moved the `Vec` behind its own type inside the shared
  `Arc`, so the abort runs once when the last holder goes.
- `3b32bf70` — `BarrierInjector::next_barrier` popped a stale barrier,
  discarded it, and returned `None`. `None` means "nothing to inject", so one
  duplicate hid a valid barrier behind it for a whole poll. The coordinator
  re-sends barriers when an ack is slow, so this cost most exactly when it
  could least afford to. Two existing tests encoded the bug.

**Open finding — the simulator has diverged from production.**

`barrier.rs` is entirely `#[cfg(test)]`: a `BarrierSimulator` plus 11 tests,
with ~5 more in `sections/gap6.rs.inc`. Every one of those tests exercises the
simulator's *own* methods (`process_barrier`, `snapshots`). It implements
`BarrierSource`, so it could serve as a test double for production operators —
nothing uses it that way.

That was merely phantom coverage until `3b32bf70`. It is now worse: the
simulator models stale-epoch handling as **`Err`**, while production now
**skips and continues**. A reader comparing them would take the simulator for a
specification and be wrong, and ~16 tests report green on a policy the engine no
longer implements.

Recommended disposition is delete (the capability — barrier epoch ordering —
ships as `BarrierInjector`, which now has direct tests for both the skip and the
monotonicity). Not done here: it spans three files at the tail of a long
session, and a half-applied multi-file deletion is worse than a recorded
finding. Whoever picks this up should delete `barrier.rs`, the `BarrierSimulator`
block in `gap6.rs.inc`, and the import in `core.rs.inc` — or, if the model is
wanted, make it call the real `BarrierInjector` so it cannot drift again.
