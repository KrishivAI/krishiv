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

**LOC and file counts corrected 2026-08-02.** Every figure below previously came
from a `*.rs`-only walk, which cannot see `src/sections/*.rs.inc` — this repo
keeps 441 tests in those. The old numbers understated the workspace by ~55 files
and ~30k lines, and they are what ranked this table. The worst offender was
krishiv-scheduler: **78 files / 51,438 lines**, not 56 / 39,142 — it is the
largest crate in the workspace, not the second. Counts are `find src tests -name
'*.rs' -o -name '*.rs.inc'`.

| # | crate | LOC | files | read whole | why here |
|---|---|---|---|---|---|
| **Tier 1 — critical path** |
| 1 | krishiv-sql | 46,632 | 61 | 7 | first slice done; `lib.rs` alone holds 37% of the crate's uncovered regions |
| 2 | krishiv-executor | 28,927 | 40 | **40 — COMPLETE 2026-08-02** | second crate fully read; 3 defects fixed |
| 3 | krishiv-shuffle | 14,329 | 36 | **36 — COMPLETE 2026-08-02** | first crate fully read; 4 defects fixed |
| 4 | krishiv-scheduler | **51,438** | **78** | 35 (in progress) | largest crate in the workspace; stage cutting, dispatch, single-task fallback, SC11 breaker |
| **Tier 2 — correctness blast radius** |
| 5 | krishiv-plan | 14,371 | 25 | 0 | plan IR every surface depends on |
| 6 | krishiv-common | 7,966 | 23 | 0 | env registry, durability profiles, memory budget |
| 7 | krishiv-connectors | 39,930 | 97 | 0 | ingest correctness; 42 files with no tests |
| 8 | krishiv-state | 12,357 | 37 | 0 | checkpoints/restore; fewer than half the files tested |
| **Tier 3 — runtime & surfaces** |
| 9 | krishiv-api | 25,111 | 38 | 0 | |
| 10 | krishiv-runtime | 13,648 | 17 | 0 | |
| 11 | krishiv-dataflow | 18,107 | 38 | 0 | |
| 12 | krishiv-ivm | 7,019 | 10 | 0 | |
| 13 | krishiv-delta | 7,098 | 20 | 0 | |
| 14 | krishiv-flight-sql | 5,199 | 6 | 0 | |
| 15 | krishiv-proto | 8,130 | 12 | 0 | 8k LOC, 3 test files |
| 16 | krishiv-metrics | 3,731 | 6 | 0 | |
| 17 | krishiv-engine-core | 3,146 | 11 | 0 | |
| **Tier 4 — thin, tooling, structural smells** |
| 18 | krishiv-python | 12,892 | 35 | 0 | excluded from CI clippy — breakage is invisible |
| 19 | krishiv-operator | 5,128 | 20 | 0 | |
| 20 | krishiv-mcp | 3,296 | **1** | 0 | one 3,296-line file |
| 21 | krishiv | 8,257 | 24 | 0 | binary/CLI |
| 22 | krishiv-engines | 2,192 | **1** | 0 | one file |
| 23 | krishiv-ui | 2,384 | 4 | 0 | |
| 24 | krishiv-bench | 3,362 | 14 | 0 | |
| 25 | krishiv-sql-gateway | 541 | 3 | 0 | |
| 26 | krishiv-conformance | 353 | 3 | 0 | no tests at all |
| 27 | krishiv-chaos | **0** | 0 | — | empty crate — delete or fill |

---

## 1. krishiv-sql — 7 of 61 files read whole ("first slice")

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

### Closed since, verified against the tree 2026-08-02

Four of the five items below were carried as open while already being fixed.
That is not free: a stale "Open" list sends the next session to re-derive work
that is done, and on 2026-08-02 it cost a wasted change to a dead code path
(see krishiv-executor). **Re-verify an item against the tree before working
it** — this section is a record of what was found, not a queue.

- [x] `connector_table.rs` — `streaming_sources` insert-only: fixed `1d98f9b6`.
- [x] `connector_table.rs` — `is_object_store_url` case-sensitivity: fixed in
      the same commit; `eq_ignore_ascii_case` over the scheme, with a test at
      `connector_table.rs:524` covering `S3://`, `S3A://`, `Gs://`.
- [x] `lakehouse/providers.rs` — Delta **and** Hudi scans no longer build a
      `MemTable`. Both resolve the snapshot's Parquet file list and hand it to
      DataFusion's Parquet scan, so projection pushdown, row-group pruning, the
      limit and per-file parallelism all apply.
- [x] `recursive_cte` — resolved by deletion (`f0742b30`): DataFusion already
      implements it. The file no longer exists.

### Open

- [ ] `lib.rs` — the single largest target anywhere. A coverage target, not a
      known defect: nothing here claims `lib.rs` is wrong, only untested.

---

## 3. krishiv-shuffle — COMPLETE, 36 of 36 files read whole (2026-08-02)

**A crate is "covered" only when every file has been read end to end.** This is
the first crate to reach it: all 36 files, **14,329 lines**, including the 11
`src/sections/*.rs.inc` test sections and both files under `tests/`.

**Methodology correction that this crate forced.** `grep --include=*.rs` cannot
see `*.rs.inc`, and this repo keeps 441 tests in them (krishiv-scheduler 266,
krishiv-executor 80, krishiv-shuffle 95). A "no callers anywhere" conclusion
about `ShuffleMetadata::mark_pending` survived review and then failed `cargo
test` with 16 errors from two `.rs.inc` files holding three dedicated tests of
exactly that API. **Every reachability grep in this repo must pass both
`--include='*.rs'` and `--include='*.rs.inc'`.** Earlier sections of this
register were written under the narrower grep and their "uncalled" claims should
be re-checked before being relied on.

The inverse trap matters just as much: a guard can be *fully unit-tested inside
`.rs.inc` and still be unreachable from production*. Green tests prove
behaviour, not reachability. Two of the four defects below were of exactly that
shape.

### Defects found and fixed reading it (2026-08-02)

- [x] **`ShuffleMetadata`'s partition cap was enforced by nothing** (`c6581a4d`).
      `max_partitions` (65536) was checked only in `mark_pending`, which no
      production caller invokes — the scheduler reaches this type solely through
      `mark_available` / `mark_failed`, both of which insert unconditionally. So
      `with_max_partitions` configured a bound that could not fire, while three
      unit tests asserted it firing at limits 2 and 3 and passed. Removed rather
      than extended to the live paths: refusing to record a partition already
      written to disk would make the consumer treat live data as missing and
      recompute its producer, which is worse than the unbounded map. A cap
      belongs at admission and there is no admission call site.

- [x] **Inline view values were charged twice in partition accounting**
      (`723986c9`). `views_bytes` charged every `ByteView` `16 + length`, but a
      value of ≤12 bytes lives *inside* the view — no data buffer holds it.
      Measured 24000 B reported against 16112 B held on a 1000-row 8-byte
      column, **1.49x**. The figure feeds `ShufflePartitionOutput::size_bytes`,
      `aqe.rs` reduce parallelism and `ShuffleWriteBuffer`'s spill decision, so
      any stage keyed on short strings — TPC-H's `l_returnflag`,
      `l_linestatus`, `c_mktsegment`, `l_shipmode` are all inline — was sized
      from an inflated number. Same class as the shared-buffer over-report this
      module was written to fix, in the opposite direction.

- [x] **The Flight serve budget was rounded up past the capacity limit**
      (`333906a4`). `serve_with_token` converted bytes to response units with
      `div_ceil(INLINE_READ_LIMIT)` and `with_serve_limit` multiplied back,
      commented as converting "straight back". Rounding up meant the server
      allowed up to one whole 32 MiB `INLINE_READ_LIMIT` MORE resident than
      `ExecutorCapacity` reported — the one direction a bound guarding
      out-of-pool memory in the map-task process must not err in.

- [x] **The HTTP shuffle auth test only proved rejection** (`79ec9cf9`).
      `http_shuffle_svc_token_auth_enforced` covered no-token → 401 and
      wrong-token → 401 but never that the configured token is *accepted*, so it
      passed identically against a handler that rejected everything and broke
      every shuffle read. Its Flight counterpart always had all three cases.

### Recorded, deliberately not "fixed"

- **A missing `.blake3` sidecar fails CLOSED on the object store and OPEN on
  local disk.** Identical crash window (data written, sidecar not), opposite
  dispositions, both deliberately tested, neither stating why. Documented at
  `ObjectStoreShuffleStore` instead of aligned: fail-open weakens integrity on
  the tier whose bytes travel furthest, fail-closed converts a recoverable crash
  into a stage recompute, and choosing needs a measurement of how often the
  window is hit. Do not assume one is simply a bug.

- **`partition_memory_bytes` uses `get_array_memory_size()` on purpose**, not
  `logical_partition_bytes`. A memory cap needs bytes *held*, not *referenced*.
  The two agree only because `partitioner::partition_batch` compacts every
  bucket at the moment it is produced; a reader in `compression.rs` had no way
  to see that invariant, so it is now stated there.

- **`SortShuffleWriter` is wired but correctly dead.** `fragment/batch.rs`
  builds one iff `ShuffleContext::ess_index` is `Some`, and every production
  site sets `None`. Its output paths carry no map-task identity, so arming it
  would let two tasks of one stage overwrite each other's data and index files
  with the query still reporting success. The warning lived in krishiv-shuffle
  while the switch lives in krishiv-executor; it is now recorded at the field.

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

## 2. krishiv-executor — COMPLETE, 40 of 40 files read whole (2026-08-02)

**Second crate fully read.** 28,927 lines: every production `.rs` file and all
four `.rs.inc` test sections (`core` 1,838, `gap6` 1,250, `stream_loop` 835,
`recovery` 797). The last files read were `fragment/run_loop.rs` (1,461) and
those four sections.

Three defects fixed, two recorded with reasons (below).

### Fixed reading it

- [x] **A run-loop resumed from pre-restore offsets after a checkpoint
      restore** (`6eeea3c4`). `execute_run_loop_fragment` snapshotted
      `runner.source_restore_offsets` once, *before* the loop, and used that
      copy at every source open inside it. A `RestoreFromCheckpointCommand`
      arriving mid-run rewrites that table and then calls
      `clear_continuous_connector_sources_for_job`, which evicts this subtask's
      cached sources — they key on `{job}#{subtask}`, which the eviction prefix
      matches. The next iteration therefore reopened them and re-applied the
      loop-start offsets: the subtask resumed from its *pre-restore* position,
      replaying or skipping records depending on which way the checkpoint moved.

      The cycle model never had this — `read_continuous_registry_sources` is
      called once per cycle and so reads the table fresh every time. Promoting
      the loop (Phase 55) hoisted the lookup out and froze it. Fixed by reading
      the table at the point of use.

      **Why it survived**: `sections/stream_loop.rs.inc:573` is exactly the
      missing test, one model over — it seeds `source_restore_offsets` and
      proves a *cycle* applies them, across all three durability profiles.
      There is no `stream:rloop:` equivalent. Likewise
      `sections/recovery.rs.inc:463` asserts the source cache is evicted on
      restore *"so restored offsets apply"* — true of the eviction, false of
      the run-loop that consumes it.

- [x] **A zero-partition shuffle write reported success with no output**
      (`dca44555`). `execute_inmem_shuffle_write` — the `sql:`-body map path —
      took `write_cfg.num_partitions` unguarded and counted rows *before* the
      skip, so with 0 partitions it counted every row, wrote none (`for p in
      0..0`) and returned `shuffle_write(total_rows, vec![])`: task green, rows
      reported, nothing stored, and nothing could have read it anyway. Its
      sibling `execute_dfplan_fragment` applies `.max(1)` and so does
      `job/record.rs` on dispatch, which is exactly why this path's omission was
      invisible. Fixed at the protocol boundary —
      `shuffle_write_config_from_wire` validated `stage_id` and nothing else,
      and now rejects `num_partitions == 0` — and again in the executor, because
      `ShuffleWriteConfig` is also built in-process by
      `krishiv-scheduler/distributed_batch.rs`, which never crosses the decoder.

### Found, NOT fixed — needs a decision first

- [ ] **GAP-WATERMARK is open, and three places say it is closed.** The
      coordinator injects a `WatermarkHint` input partition for downstream
      stages; `fragment/streaming.rs` decodes it, logs it, and **discards it**.
      The comment above that block described applying it as the initial
      `prev_watermark_ms`, and `WatermarkHint`'s own doc in
      `krishiv-proto/src/task.rs:952` says the same. Neither happens.

      It is not a dropped variable: `WindowExecutionSpec` has no
      `prev_watermark_ms` field to carry the value. It lives inside
      krishiv-dataflow's operators, initialised to `i64::MIN`
      (`window/session.rs:95`), with no path in from the spec.

      **Consequence.** A downstream stage starts its watermark at `i64::MIN`,
      so every event from the upstream stage scores as in-order however late it
      actually is — the stage reports "no late events" by construction, and
      `allowed_lateness_ms` / late-firing never engage on any stage but the
      first.

      Closing it needs a field in krishiv-plan, threading through
      `execute_bounded_window` into each window operator, and a re-baseline of
      late-event counts across the streaming corpus — three crates and a change
      to lateness semantics. The misleading comment is corrected in place so the
      code no longer claims the fix; the work itself is scoped, not done.

- [ ] **`cancelled_tasks` grows without bound for cancelled batch tasks.**
      `ExecutorAssignmentInbox` bounds its `seen` set deliberately
      (`MAX_SEEN_ENTRIES` = 10,000, FIFO eviction) and leaves its sibling
      `cancelled_tasks` unbounded. The test
      `forget_job_purges_its_own_cancel_tombstone` states the intent — "so a
      long-lived executor process does not accumulate an unbounded
      `cancelled_tasks` set" — but `forget_job` is called from exactly one
      place, `grpc.rs:251`, **gated on `had_cycle_executor || had_rloop`**. That
      is only true for continuous/streaming jobs with a registered loop
      executor.

      For a batch cancel: `cancel_task` removes the assignment from the queue
      *and* plants the tombstone; the gate is false so neither `forget_job` nor
      the eager `clear_cancelled_task` runs; and the runner's own
      `clear_cancelled_task` (`executor_task_runner.rs:814`/`:936`) never fires
      because the task was removed from the queue and will never be dequeued.
      One leaked `(JobId, TaskId)` per batch task cancelled while queued.

      **Why the obvious fix is wrong.** Clearing the tombstone when
      `cancel_task` returns `removed == true` looks right — nothing can read it
      if the assignment is gone. But tombstones are keyed `(job, task)` with no
      attempt, so a *queued attempt 2* and a *running attempt 1* of the same
      task share one entry. Clearing on removal of the queued attempt would let
      the running attempt escape cancellation — trading a slow memory leak for
      a correctness bug in the cancel path.

      The real question is whether `cancelled_tasks` should be keyed by attempt.
      That changes `is_task_cancelled`'s contract for the mid-execution cancel
      watch (#217) and for run-loop tasks that poll it, so it wants deliberate
      design rather than a patch. Recorded rather than guessed at.

### Read and found clean

`fragment/common.rs`: the one known issue is B3 (the shuffle read materialises
a whole fragment outside every budget, `read_shuffle_flight_partitions`), which
is already instrumented in place and carries its own explanation of why a
`MemTable` cannot simply become a stream — a fragment's SQL may scan the same
shuffle table twice. No new defect.

`execute_shuffle_write_fragment` in `batch.rs` remains dead (nothing constructs
a `shuffle-write:` fragment) and is marked as such at the function; its
leftover `if output_schema.fields().is_empty()` fallback is unreachable now
that the schema comes from the stream, and was left alone rather than tidied
inside a dead path.

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

## 4. krishiv-scheduler — 53 of 78 files read whole (in progress, 2026-08-03)

Third crate. The largest in the workspace: 51,438 lines. Working down the
distributed-batch critical path — `lib.rs` (145), `distributed_batch.rs` (198),
`job/mod.rs` (199), `job/record.rs` (2,381), `job/scheduler.rs` (1,462),
`job/snapshot.rs` (302), `coordinator/mod.rs` (2,384),
`coordinator/job_lifecycle.rs` (1,475), `coordinator/task_assignment.rs`
(1,496), `coordinator/executor_ops.rs` (1,341), `heartbeat.rs` (881),
`cluster_control.rs` (462), `config.rs` (579), `admission.rs` (280),
`coordinator/aqe.rs` (501), `coordinator/recovery.rs` (565),
`coordinator/streaming.rs` (279), `coordinator/snapshots.rs` (200),
`coordinator/observability.rs` (182), `metrics.rs` (201), `result_spool.rs`
(550), `coordinator_sharded.rs` (752), `coordinator/checkpoint_ops.rs`
(1,005), `barrier_dispatch.rs` (826), `checkpoint.rs` (1,337),
`job_coordinator.rs` (419), `adaptive.rs` (101), `error.rs` (189),
`coordinator/heartbeat_mapping.rs` (87), `barrier_tracker.rs` (108),
`rpc_drain.rs` (223), `leadership.rs` (71), `http_auth.rs` (224),
`queryable_state_http.rs` (262), `bounded_window_http.rs` (83),
`coordinator_daemon.rs` (2,518), `continuous_stream_http.rs` (3,041),
`sections/placement.rs.inc` (2,281), `ivm_http.rs` (2,273), `store.rs`
(1,996), `sections/core.rs.inc` (1,772), `sections/chaos_jcp.rs.inc`
(1,407), `sections/prr_parallel.rs.inc` (276), `etcd_metadata.rs` (1,250), `ivm.rs`
(1,107), `batch_sql.rs` (1,065), `grpc.rs` (1,040), `auth.rs` (1,029),
`rocksdb_metadata.rs` (531), `etcd_lease.rs` (501), `batch_sql_http.rs` (467),
`bounded_window.rs` (395).
**44,699 of 51,438 lines.**

**Remaining, in intended order** (25 files, ~6,740 lines):
`sections/checkpoint.rs.inc` (865), `sections/adaptive.rs.inc` (717),
`sections/retry_streaming.rs.inc` (612), `sections/streaming_recovery.rs.inc`
(586), `sections/recovery.rs.inc` (544), `unified_jobs_http.rs` (377),
`tests/r2_k8s_manifests.rs` (376), `sections/savepoint.rs.inc` (367),
`in_process.rs` (353), `sections/chaos_basic.rs.inc` (285),
`sections/dur1.rs.inc` (213), `barrier_client.rs` (204),
`sections/validation.rs.inc` (180), `sections/queue_manager.rs.inc` (162),
`sections/checkpoint_timer.rs.inc` (160), `sections/barrier_oob.rs.inc` (141),
`sections/chaos_restart.rs.inc` (128), `tests.rs` (124),
`sections/failover.rs.inc` (86), `tests/distributed_e2e.rs` (71),
`bin/krishiv_coordinator.rs` (58), `bin/krishiv_clusterd.rs` (56),
`bin/krishiv_job_coordinator.rs` (55), `sections/etcd_sim.rs.inc` (53),
`tests/coordinator_executor_integration.rs` (40), `transport.rs` (38).

**The recurring shape in this crate**: *state removed at the head of a path as
"consumed by this batch", never restored when the batch turns out to be empty
— and cleanup that covers every `return` but not the function failing to
return at all.* Six of the nine defects below are one of those two.

### Fixed reading it

- [x] **Invalidated shuffle partitions stayed "available" on every dfplan job**
      (`e8fed5cf`). `invalidate_specific_shuffle_partitions` built its
      `ShufflePath` from `m.stage_id()` — the key the *consumer* reported —
      while `apply_task_update` records availability from `update.stage_id()`,
      the coordinator stage id, which is also the map key and the form
      `store.rs` rebuilds from. `ShuffleMetadata` keys on the whole
      `ShufflePath`, so a dfplan consumer's `sN.mM` sub-stage key (never equal
      to `dist-sN`) inserted a second entry and left the Available one standing.

      **Scope, stated precisely**: nothing gates scheduling on this map —
      regeneration is driven by task state and `missing_report_addresses_task` —
      so no job was mis-scheduled. It corrupts what the coordinator *reports*
      and *persists*: `shuffle_partitions_available`, the
      `krishiv_shuffle_partitions_available` gauge, and
      `PersistedJobRecord::from`, which serialises exactly the Available paths.
      A partition known to be lost was persisted and restored as available.

      The comment at the site asserted the opposite rationale — that the
      consumer's key marks "the entry that served the fetch" and the re-run
      producer "re-registers under the same key". Neither holds;
      `apply_task_update` is the only writer of availability and only ever uses
      the coordinator form. **A comment describing an invariant the code does
      not have is how this survived review.**

- [x] **The gc-ready cap dropped jobs without collecting them** (`6ad38e30`).
      `on_job_terminal`'s `MAX_GC_JOBS` cap pops the oldest id off
      `gc_ready_jobs` to make room, but `take_gc_ready_jobs` is the only caller
      of `evict_completed_job` — so a dropped id was never evicted. Its
      JobRecord, inline results, result spools, input partitions, checkpoint
      coordinator and index entries stayed in memory forever, **and** because
      `active_job_ids()` (the shuffle orphan sweep's live set) is exactly
      `job_coordinators.keys()`, the sweep kept treating it as a running job
      and never reclaimed its partition files either. The cap bounds only the
      queue. Reachable whenever >1000 jobs go terminal inside the 30 s GC
      grace window — one IVM flow ticking at 100 ms is 300, ten flows is 3000.

- [x] **`validate_job` reported a duplicate stage id as a cycle** (`6ad38e30`).
      Cycle detection indexes stages by id; with a duplicate the Kahn drain
      decrements one index twice and re-queues a stage already at in-degree
      zero, so `processed` overshoots `n`. Duplicate *task* ids were already
      rejected by name ten lines below.

- [x] **The stall watchdog reported the wrong duration** (`6ad38e30`). It
      triggers on `last_progress_ms` but reported time since `assigned_at_ms`.
      Not confined to a log line — `apply_stall_resets` bakes the number into
      the task's `last_failure_reason`, so a task that worked nine hours and
      then went quiet for one was recorded as "no progress for 10 hours".

- [x] **Two one-shots consumed by batches that never launched** (`a2f615c3`).
      (a) `apply_assignment_dispatch_responses` gated on
      `accepted == responses.len()`, vacuously true for an empty batch, and so
      marked a pending continuous restore snapshot consumed when nothing had
      been dispatched; `job_input_partitions` held the only other copy and
      `abort_continuous_input_cycle` drops that, losing the checkpoint.
      (b) `launch_assigned_task_assignments` removed the hot-key skew override
      as a one-shot even when the launch errored or produced no assignments.
      The sibling `task_window_parts` on the same path was already restored for
      exactly this reason.

- [x] **Hot-key repartitioning counted dead executors** (`274125d8`).
      `process_hot_key_reports` sized the override with `executors.list()`,
      which retains Lost/Removed records for 40x the heartbeat timeout (~30
      min) so zombie leases still validate. A 3-executor cluster that had
      restarted its pods twice sized the override at 9 and shredded the stage
      into partitions no executor existed to run.

- [x] **Promotion left every per-job map behind** (`ce415a3a`).
      `recover_from_store` runs on every standby→active promotion, not only at
      process start, and cleared two maps out of twenty. A job absent from the
      store afterwards kept its inline results, spools, input partitions,
      checkpoint coordinator, adaptive log, skew override and GC entry — and
      its `continuous_input_cycles` fence, which makes every later push to a
      job of that id 409 forever. Fixed by extracting `purge_job_scoped_state`
      from `evict_completed_job` and running it for the ids that did not
      return.

- [x] **Eviction dropped the forward streaming index, not the reverse one**
      (`ce415a3a`). `streaming_job_task_index` leaked a `Vec<TaskId>` per
      evicted streaming job. Not only a leak: the forward index is keyed by
      bare `TaskId` (task ids like `t0` repeat across jobs), so a stale reverse
      entry lets a later `remove_streaming_task_index` for the dead job delete
      a **live** job's entry — after which its watermark and source-offset
      reports are silently dropped, because `apply_streaming_task_state`
      returns early on an unindexed task.

- [x] **The observability report measured heartbeat age off the wrong clock**
      (`ac79403d`). `heartbeat_age_ticks` used `exec.ticks_since_restart`,
      which is initialised to `u64::MAX` (so a never-recovered coordinator is
      never inside the streaming re-attach grace window) and only resets to 0
      in `recover_from_store`. `last_heartbeat_tick` is stamped from
      `ExecutorRegistry::current_tick`. Every executor in the report therefore
      showed ~1.8e19 ticks of staleness on any fresh process — in the one
      report an operator reads to decide whether an executor is alive. Also
      populated `upstream_stage_ids`, which was hard-coded empty in a report
      whose purpose is "why has this stage not started".

- [x] **A cancelled result-spool receive leaked its partial file**
      (`2c09c31f`). `receive_task_result_spool` cleaned up at every early
      return via a `cleanup(&path)` closure, which says nothing about the
      future being dropped — the normal outcome when a `PushTaskResult` client
      disconnects mid-transfer or the request is cancelled. Multi-GiB partial
      files stayed on disk with nothing sweeping the spool directory. Replaced
      with a `PartialSpoolFile` destructor disarmed only after the final
      fsync, so cancellation and every error path share one mechanism.

- [x] **Env-flag defaults pinned to the registry** (`d9c4e93e`) — a guard, not
      a fix; all five were correct. `declared_default_number` exists because
      seven flags had drifted (one by 8x) and the registry generates
      `docs/reference/env-flags.md`. `KRISHIV_AQE_SKEW_FACTOR` is a ratio the
      accessor cannot parse as an integer, so it was pinned against the
      declared string rather than left silently exempt.

- [x] **`barrier_sent` grew one entry per committed epoch, forever**
      (`016fa2f5`). `clear_checkpoint_notify_for_epoch` runs on the commit path
      and dropped the epoch's `notify_sent` entry but not its `barrier_sent`
      one. Nothing else reclaims it on a healthy job: it is a `HashSet`, so the
      insertion-ordered `prune_sent_set` cap cannot apply, and the per-epoch
      cleanup in `advance_heartbeat_clock` only fires for epochs that *abort*.
      A job checkpointing every 10 s accumulated 8,640 permanent entries a day
      for its whole lifetime — on exactly the long-running streaming workload
      the feature exists for. `CheckpointInner::clear_notify_for_epoch` has
      always cleared both; this is the outer copy drifting from it.

- [x] **A failed savepoint commit lost the operator's label** (`00c60e70`).
      `commit_epoch` read the label with `.take()` before the fallible manifest
      read and the three storage writes, while `pending_is_savepoint` is
      cleared only on success — so a retry after a storage failure sealed an
      unnamed savepoint. `extract_commit_data`, the async sibling, already
      clones, and the success path clears both fields anyway. Narrow in
      practice (quorum is already reached, so the epoch usually times out
      rather than retrying), fixed because it is the same two-implementations
      shape as the entry above.

- [x] **`KRISHIV_JCP_POLL_INTERVAL_SECS=0` busy-polls the coordinator**
      (`2d3f4582`). `--poll-interval-secs` clamped with `.max(1)`; its env twin
      did not. `run_job_coordinator_daemon`'s watch loop has no delay other
      than this interval, so zero turns the status poll into an unbounded
      request flood against the CCP's HTTP surface — the same surface that
      serves `/readyz`, so one misconfigured JCP pod degrades the
      coordinator's Kubernetes probes cluster-wide. The env path is the one
      Kubernetes actually takes (the operator sets env, not argv), so the
      *unvalidated* path was the deployed one. Both now route through
      `jcp_poll_interval`.

- [x] **Circuit-breaker reset reported success for an unknown executor**
      (`2d3f4582`). `ExecutorRegistry::reset_task_failures` no-ops silently on
      an unknown id and `api_executor_reset` answered `{"reset": true}`
      regardless. An operator calls that endpoint precisely because an
      executor is pinned at the failure threshold and is being skipped for
      assignment; a typo'd or already-removed id reported the one signal they
      have as "fixed" while the cluster stayed degraded. Now 404 +
      `reset: false`; `reset_task_failures` returns bool so the distinction
      exists. Same shape as the cancel 404-vs-409 bug already fixed in this
      file — the sibling test only ever reset an executor that exists, which
      is why the false success survived.

- [x] **JCP help text named a port nothing serves** (`2d3f4582`). It
      advertised `http://127.0.0.1:2002` — the *daemon's* default HTTP port —
      while the code defaults to 18080, the port `krishiv local start`
      publishes and `docs/running-examples.md` uses.

- [x] **The crate was failing CI's fmt gate** (`033d3f38`). `cargo fmt
      --check` reported 21 diffs across 10 files, and
      `.github/workflows/ci.yml` runs `just fmt` (a workspace-wide
      `cargo fmt --check`) — so main was red on that job. Four of the ten
      (`job_lifecycle.rs`, `observability.rs`, `job/scheduler.rs`,
      `result_spool.rs`) were introduced by this audit session; six predate
      it. Committed separately from the fixes so the audit diff stays
      reviewable. **Run `cargo fmt -p krishiv-scheduler` before each audit
      commit.**

- [x] **The continuous surface explained one of its nine rejections**
      (`85d7b0d8`). `scheduler_error_response` exists in
      `continuous_stream_http.rs` because a bare status code with an empty body
      "cost a full bisection" during the Phase 62 soak — its own doc comment
      says so — but only `api_continuous_push` was converted. The eight
      siblings still returned `StatusCode` alone. `drain` was the sharpest
      case (callers drive push and drain in the same loop, and push's message
      was already fixed); `register_sql` discarded the SQL compiler's
      diagnostic entirely behind a bare 400. Converted all eight to
      `(StatusCode, String)`; statuses unchanged. `api_continuous_list` left
      alone — no error path.

      Verification note worth keeping: the first revert I tried (drain's
      `job_snapshot` call) did **not** fail the test, because an unknown job
      fails earlier at `run_loop_targets`. A discrimination check that passes
      means the wrong line was reverted, not that the test is weak.

- [x] **Planning width counted slots on circuit-broken executors**
      (`ee118c60`). `total_schedulable_slots` is the width a query is planned
      to: it feeds the AQE coalesce floor (`job_lifecycle.rs:740`, "coalescing
      must not shrink a stage below the cluster's schedulable width") and the
      staged-batch target partition count (`batch_sql.rs:465`, "plan against
      the capacity that will actually run the query"). It filtered on
      `is_schedulable` alone. A circuit-broken executor passes that test but
      is excluded from both paths that could give it a task —
      `assign_pending_tasks_capped` and `launch_assigned_task_assignments` —
      so the planner sized stages against slots that would never be offered.
      On a 3×3 cluster with one node broken (five consecutive failures: the
      `bench-s3` NoSuchBucket shape), stages were still cut 9 wide and the 3
      tasks with nowhere to run queued behind a half-empty second wave.

      **This is the 2026-07-30 livelock's rule applied to the third consumer
      of "eligible".** That incident was diagnosed as launch and placement
      disagreeing; both were fixed and `circuit_broken_executors` was written
      so they could not drift again. Planning was the consumer nobody looked
      at — instance four of the *fix the call site that hurt, then leave*
      shape.

      Routing through the same helper inherits the starvation floor for free:
      when every executor is broken the breaker admits them all and so does
      the count. A hand-rolled `consecutive_task_failures >= threshold` filter
      would instead report width 0, which callers read as "capacity unknown"
      and collapse onto the local machine — while the cluster is about to run
      the query. `planning_width_keeps_everyone_when_the_starvation_floor_admits_them`
      pins that boundary; it is the test that discriminates against the
      *wrong* fix rather than against the bug.

- [x] **Ten IVM endpoints 404'd on jobs that were durably stored**
      (`d51c3f83`). `ensure_ivm_job` rehydrates a job from the coordinator's
      durable snapshot when this process has never seen it. It exists because
      the IVM registry is process-local and **nothing repopulates it at
      startup** — `restore_durable_snapshot` is reachable only from
      `ensure_ivm_job` and `api_ivm_create_job` — so after a coordinator
      restart or a standby failover, rehydration is entirely lazy. Six
      handlers called it; ten called `registry.get` and 404'd.

      The read side was the visible half. `/stats` exists to be polled ("the
      platform freshness sampler can hit it every few seconds", per its own
      doc comment) and answered 404 — a live table reported as missing —
      until an unrelated `/feed` or `/step` happened to resurrect the job.
      `/checkpoint`, the backup path, failed the same way. `/stream-bridge`
      is the sharpest case: its two sibling ingest routes, `/feed` and
      `/stream-delta`, both rehydrate. Instance five of *fix the call sites
      that hurt, then leave*.

      **Second defect, same file: `/restore` never persisted.**
      `register_view`, `drop_view` and `step` all call `persist_ivm_job`
      after changing authoritative state. `/restore` answered
      `{"success": true}` and left the store holding the pre-restore
      snapshot, so a restart before the next `/step` silently undid the
      rewind. Reverting only that line reads 1000.0 (advanced) where 100.0
      (restored) is correct.

      Why the existing test missed it: `evicted_job_is_restored_from_the_durable_snapshot`
      resurrects the job with `/feed` before reading, so its snapshot
      assertion never exercised the read path's own rehydration. The new test
      evicts again between every handler.

      One correction made while writing it: `/output` returns 200-with-null
      after a restart, not a delta — the durable snapshot carries source and
      view state, not the last tick's emitted delta. That is the true answer;
      the point is that it is distinguishable from "no such job".

- [x] **The bounded events log memmoved its whole buffer on every append**
      (`cf0163d7`). `InMemoryMetadataStore::evict_until_fits` looped
      `Vec::remove(0)` until the incoming event fit. `remove(0)` shifts the
      entire tail, and each append frees roughly its own size, so exactly one
      removal ran per append once the log was full — the *steady* state for a
      long-running streaming coordinator, not a rare one. At the 64 MiB cap
      that is a memmove of ~280k elements (~36 MB) per task-state change.
      The comment above it claimed "amortized O(1) per appended event because
      it only fires when the buffer is full", which inverts the truth: full is
      exactly when it fires on every append. One `drain` now frees a slab
      (1/8 of the cap). Measured: reverting only the headroom term gives
      **15,585 eviction passes in 15,585 appends**; with it, ≤32.

      The ring buffer had **no test at all** — `evicted_event_count` and
      `events_byte_size` are `pub` and documented "for tests and metrics",
      and nothing read either. Added cap + FIFO coverage.

- [x] **The SC3 stall-tracking tick fields were dead** (`cf0163d7`).
      `PersistedTaskRecord::assigned_at_tick` / `last_progress_tick` were
      documented as carrying a stalled task's timeout window across a
      coordinator restart. `From<&TaskRecord>` hardcoded both to `None`,
      `TryFrom` ignored them, and no other file in the workspace referenced
      either name — while two doc comments and two tests asserted the
      round-trip worked. **The tests passed because they built a
      `PersistedTaskRecord` by hand and checked serde carried its fields;
      neither went through the conversions that were broken.** Replaced with
      one that runs the real path and asserts what must survive (attempt,
      failure_count, executor_loss_count — the retry budgets) and what must
      not (stall clocks, launch guard).

      **Removed, not wired**, same disposition as the unreachable
      `ShuffleMetadata` cap: a tick is meaningless across a restart because
      the tick clock is rewound, and persisting the wall-clock variant would
      reintroduce the failure that killed SF100 q5 — a healthy long-running
      task failed for "no progress" it was in fact making. The heartbeat
      refresh path is authoritative. If cross-restart stall budgets are ever
      wanted, persist `last_progress_ms` and keep that path in charge.

- [x] **The executor-persistence test asserted on memory, not the store**
      (`a129c0fa`). `tonic_service_register_executor_persists_descriptor` set
      up a RocksDB store, registered through the tonic service, awaited
      `store.flush()` — then asserted on `executors().list()`, the in-memory
      registry, which `register_executor` populates whether or not it
      persists anything. Deleting the `save_executor` call left it green.
      R10 exists so a re-attaching executor is recognised after a restart,
      and only the durable copy can do that. Now reads through
      `store.inner()`. `cancel_job_marks_active_tasks_cancelled` in the same
      file already used that idiom — this call site never adopted it.
      Verified: 0 descriptors where 1 is required.

- [x] **A chaos suite that could not fail** (`4f871420`).
      `sections/chaos_jcp.rs.inc` held ~72 tests. Almost all exercised only
      `MiniSimulationHarness` — a test double — and never reached production
      code; their own comments said so ("In real flow this would be…").
      Four defects, each the founding failure mode of this register:

      * **Tautologies.** The dominant assertion,
        `assert!(!h.is_partitioned(&x) || h.current_tick() > 10)`, ran after
        a `simulate_partition_and_recovery` (which un-partitions) and 14
        ticks. Both disjuncts true by construction.
      * **Assertions on the double's own setters** — `h.partition(x)` then
        `assert!(h.is_partitioned(&x))`.
      * **Twelve verbatim duplicates** of one body, identical down to the
        executor-id string, named `_stress` and `_stress_v2` … `_v12`.
      * **Names that promised what the body never did.**
        `..._clear_assignments_for_bad_executor_works` never called it;
        `chaos_jcp_running_task_count_under_failure` asserted a string
        contained a substring of itself; `prr_new_surfaces_all_green_…` had
        an empty body; `..._exposes_raw_udf_limits_…` had no assertion, its
        comment claiming compilation was the assertion.

      They also hid **un-awaited futures**: every JCP accessor is `async`, so
      `let _eligible = jc.has_tasks_eligible_for_launch();` in a sync
      `#[test]` builds a future and drops it unpolled. The file already
      documented that bug being found and fixed in *one* place and left seven
      more — they survived `clippy::let_underscore_future` because a **named**
      `_eligible` binding does not trip a lint that only fires on the bare `_`
      pattern. The lint was satisfied by renaming, not by awaiting. Instance
      six of *fixed where it hurt, then left*.

      Replaced with six tests that call those surfaces and assert what they
      do (detach-vs-requeue semantics, the heartbeat-forget half of
      `handle_executor_loss` observed through the staleness seam, the
      threshold in both directions including a backwards clock jump, launch
      eligibility across assignment/guard/Running, the work summary, the UDF
      budget). **Test count 595 → 530 — that is the point.**

- [x] **The notify test always timed out and discarded the result**
      (`5caadb7c`). `notify_wakes_on_executor_registration_and_deregistration`
      registered an executor, *then* built the wait future, then wrote
      `let _ = timeout(100ms, wait).await`. Tokio's
      `Notify::notify_waiters()` wakes only waiters **already registered** and
      stores no permit — unlike `notify_one()`. Every producer in this crate
      uses `notify_waiters()` and `wait_for_change` consumes it with
      `notified().await`, so a notification fired before anyone is parked is
      dropped. The test's own comment asserted the opposite of what the API
      guarantees; it always timed out and `let _ =` swallowed the
      `Err(Elapsed)`. Deleting `notify_waiters()` from `register_executor`
      left it green.

      Rewritten so the waiter parks first, with `tokio::select! { biased; … }`
      making the ordering deterministic rather than timing-dependent, and split
      per notifier. Verified: removing that one line fails the registration
      test after the full timeout and leaves the deregistration test green.

      **Worth carrying forward:** because `notify_waiters()` stores no permit,
      the coordinator's Notify is a latency optimisation, not a delivery
      guarantee — a state change landing between a daemon's work pass and its
      next park is simply missed, and the daemon waits out its timer. That is
      why the live loops pair the wait with a sleep in `select!`. Do not treat
      a `notify_waiters()` call as "the daemon will definitely see this".

      Also deleted from that file: two pairs of duplicate tests that only
      registered an executor and asserted it was registered / its lease bumped
      (both facts already covered properly in `core.rs.inc`),
      `real_job_coordinator_extraction` (asserted a string equalled itself),
      and `MiniSimulationHarness` with its five self-tests — the closed loop
      recorded below, now closed. Test count 530 → 521.

- [x] **The etcd backend's tests never ran anywhere** (`da425ce3`). `etcd` is
      a non-default feature, so `just test` (`--workspace --lib`, default
      features) compiles none of `etcd_metadata.rs` and runs not one of its
      tests. `lint-features` already compiles it with `--features etcd` and
      its own comment argues the case — *"etcd is a supported metadata
      backend; lint it like one"* — reasoning never extended from clippy to
      tests. Instance seven of *fixed where it hurt, then left*.

      Unrun: the regression guards for three production incidents recorded in
      that file — the unbounded-RPC-under-lock wedge (Phase 58, `/leaderz`
      dead 10+ min), the dedicated-runtime deadlock, and the IVM snapshot size
      cliffs. `--features etcd` yields **547 tests against the default 521**.
      `ci-tiers.md` opens with "the split is a committed decision, not an
      accident of one cargo flag", and etcd appears nowhere in its exclusions
      table — so this was exactly that accident. Added `just test-etcd`, wired
      it into the required CI job, documented it in the required-tier table.

- [x] **The Phase 58 wedge guard did not guard the constant that wedged**
      (`da425ce3`). Found by checking that the new gate actually catches
      something: regressing `ETCD_RPC_TIMEOUT` to 24 h (the pre-fix
      "effectively unbounded" state) left **both** tiers green.
      `etcd_block_on_bounded_times_out_a_hung_future_instead_of_blocking_forever`
      passes its *own* 200 ms bound, so it proves the helper honours what it is
      handed — never that the production entry point hands it anything sane.
      Added a constant guard pinning `ETCD_RPC_TIMEOUT` (≤30 s, non-zero) and
      its ordering against `ETCD_REFRESH_TIMEOUT`, with the incident in the
      failure message. It is a constant guard, not a runtime proof, and says
      so — same shape as the env-registry default pinning already in this
      crate.

      **General lesson:** a test that takes the bound as a parameter tests the
      mechanism, not the policy. When a regression test exists for an incident
      caused by a *value*, check that the value itself is pinned.

- [x] **The single-flow pin did not survive rehydration** (`ab4a48d4`).
      `create_unpartitioned` pins a job to a single flow so a view-DAG can
      cascade off it; the pin lived only in the process-local `pinned_single`
      set, and neither `durable_snapshot` nor `restore_durable_snapshot`
      carried it. `shape` cannot stand in: `api_ivm_create_job` persists at
      create time, so a `partitioned: false` job reaches the store as
      `Single` with **no views** — byte-identical to an ordinary job that has
      not registered one yet. After a restart the first `GROUP BY` view
      auto-partitions a job that explicitly asked to stay single, and the
      composition is silently impossible.

      **Self-inflicted reachability:** `d51c3f83` (this session) taught ten
      more handlers to rehydrate, so far more paths now reach
      `restore_durable_snapshot` before a view exists. Worth remembering that
      widening a recovery path can promote a latent bug to a live one.

      Persisted with `serde(default)` under an **unchanged version 1** —
      deliberately not a bump, because `restore_durable_snapshot` rejects
      unrecognised versions and a bump would make every already-persisted IVM
      job unloadable on upgrade. Three tests: the pin survives; an ordinary
      job still auto-partitions (so the fix is not "pin everything"); a
      field-stripped snapshot still loads.

- [x] **A finished batch-SQL query could hang for its full 300s timeout**
      (`d3836145`, `29fec838`). Both batch-SQL wait loops used
      `sleep_until(deadline)` as the `select!` fallback, so the only early
      exit was a change notification — and `Coordinator::notify` is fired with
      `Notify::notify_waiters()`, which wakes only waiters **already parked**
      and stores no permit. The recheck-before-park dance was written to close
      that window and cannot: `Notified` does not register until first polled,
      which happens inside the `select!`, *after* the recheck released the
      lock. A job terminating in that window notified nobody, so the caller
      slept the whole `KRISHIV_BATCH_SQL_TIMEOUT_SECS` and then reported a
      timeout — and cancelled the job on the way out — for a query that had
      already finished.

      **Four copies of this wait loop exist.** `run_ivm_fragment_job` had it
      right (100 ms tick); `batch_sql.rs` ×2 and `bounded_window.rs` did not.
      Fixed by sharing one `await_job_change` helper rather than patching a
      third copy. Same root cause as `5caadb7c`, where it was only a test
      defect — here it is production behaviour on the coordinated-query path.

- [x] **Two terminal branches reported a caller's query error as an internal
      fault** (`d3836145`, `29fec838`). The batch-SQL *sink* path and
      `bounded_window` both returned `SchedulerError::Transport` on
      Failed/Cancelled, which the interface layer classifies Retryable/opaque
      ("internal error; contact the operator"), discarding the reason the
      coordinator had already recorded. `poll_batch_sql_outcome` does the
      opposite and says why (Phase 63 / audit §11), and
      `first_task_failure_reason` sat right there serving only that one
      caller. The sink path's *timeout* branch already carried the matching
      #222 fix and points at the sibling — only the terminal branches were
      left behind. Both now share `first_task_failure_reason`.

      One test covers both defects; each was reverted alone to prove it
      discriminates (taxonomy → `Transport` in 0.06 s; tick → `Elapsed` after
      the full timeout).

- [x] **A standby accepted savepoint restores** (`ddc642a3`).
      `restore_job_from_savepoint` had no `ensure_active`, unlike every sibling
      mutation including `activate_job_restore_from_checkpoint_with_fencing` —
      the *other branch of the same gRPC handler*. A standby therefore copied a
      savepoint epoch into the active chain and the handler then did
      `checkpoint_inner.replace_data_from(...)`, a full replace on a non-leader.
      The fencing token guards storage against a stale *leader*, not this.

- [x] **The SEC-7 anonymous-access policy had no real test** (`ddc642a3`). The
      only test that named it called `set_allow_anonymous_when(false)`, which
      returns `Err` on its first line without consulting production mode or the
      durability profile — both guards could have been deleted and stayed
      green. Extracted `anonymous_allowed_for(production, profile)` and tested
      the matrix. Also corrected six `grpc.rs` comments calling the per-handler
      auth check "redundant when server-level interceptor is active": the
      interceptor enforces only `Role::Reader`, so those checks are the **sole**
      enforcement of `Role::Writer` on every mutating RPC.

- [x] **The ephemeral RocksDB store deleted its own directory** (`a0082f52`).
      `in_memory` passed its `TempDir` to `open_at` as `_tempdir`, which nothing
      stored, so the guard dropped and removed the directory under the open
      `DB`. Linux keeps the open descriptors working, which is why every test
      passed; RocksDB fails the moment it needs a new file. Also: three of four
      deleters ignored the DUR-6 sync policy that `remove_ivm_snapshot` already
      honoured.

- [x] **Bounded-window jobs were rejected on every durable profile**
      (`a2abcef8`). `prepare_bounded_window_job` emitted a raw
      `window:{topic}:{spec}` fragment; the executor rejects untyped fragments
      unless `dev-local` outside production. `batch_sql.rs` claims its own fix
      covered "the last untyped batch-SQL emitter" — true, but
      `bounded_window.rs` was the last untyped emitter in the crate. Instance
      eight of *fixed where it hurt, then left*.

### Recorded, not fixed — needs a decision

- **`EtcdLeaseElection::last_renewed_at` is written and never read.** Three
  writes, zero reads — dead state shaped exactly like a liveness guard.
  `is_leader()` has no staleness check, so if the renew loop stalls the node
  keeps reporting itself leader while its etcd lease expires and another node
  legitimately acquires. The fencing token still prevents split-brain *writes*,
  but `is_leader()` also gates `/leaderz` and Service routing. Not fixed here
  because self-demotion on renew staleness changes HA behaviour under GC pauses
  (leadership flapping) — a design call, not an obvious defect. Either wire it
  or delete it.

- **`load_prefix` fails the whole load on one bad record.** `connect` /
  `refresh` return `Err` on the first job, executor, or history value that
  will not decode, so a single corrupt or schema-incompatible record in etcd
  fails every coordinator's startup and every standby promotion —
  cluster-wide. `load_ivm_snapshots`, in the same file, deliberately logs and
  skips ("so one bad record never blocks the coordinator from loading the
  rest"). The IVM path learned the lesson; the other three did not. Not fixed
  here because skip-vs-fail for a *job* record is a durability policy call
  (silently dropping a job vs refusing to start), not an obvious defect —
  but the asymmetry is unintended and worth a decision.

- ~~**`MiniSimulationHarness` is now a closed loop.**~~ **RESOLVED** in
  `5caadb7c` — deleted with its self-tests.

  Original note kept for the reasoning: **`MiniSimulationHarness` is now a closed loop.** It lives in
  `sections/prr_parallel.rs.inc`, and after the deletion above its only
  remaining consumers are its own self-tests (`richer_simulation_harness_…`,
  `simulation_harness_advanced_failure_modes`, `…_concurrent_partitions`,
  `…_timeout_detection`, `…_frozen_executor_progress_stall`) — a test double
  tested by tests of itself, with no production coverage. That file also
  holds `real_job_coordinator_extraction`, which asserts a string equals
  itself. Read `prr_parallel.rs.inc` next and decide whether the harness
  earns its keep or follows the chaos suite.

- **`MetadataStore` has no `remove_job`.** Events are byte-bounded, history
  is capped at `MAX_JOB_HISTORY`, executors / continuous snapshots / IVM
  snapshots all have removers — but `jobs` grows for the life of the
  process, one full `JobRecord` (specs, stages, every task description) per
  distinct job id, and the coordinator's own GC eviction never reaches the
  store. Not fixed here because it is a trait change across four backends
  (in-memory, JSON file, RocksDB, etcd) plus a GC wiring decision about when
  a durable job record may be forgotten — a feature, not a defect fix.

- **The JCP never exits when its job is gone.** `run_job_coordinator_daemon`
  treats a 404 from `/federation/v1/jobs/{id}` exactly like a transient
  network error: log a warning, sleep, retry, forever. Terminal states are the
  only exit. So a JCP pod that restarts *after* its job finished — or whose
  job has aged past the 30 s `KRISHIV_JOB_GC_GRACE_SECS` window and been
  evicted, or been dropped by the `MAX_GC_JOBS` cap — polls a job that will
  never come back and the Kubernetes Job never completes. Not fixed because
  the threshold is a judgement call and 404 is genuinely ambiguous: a CCP that
  has just failed over and not yet finished `recover_from_store` also answers
  404 for a job that is alive. Wants either a bounded consecutive-404 budget
  distinct from the transient-error path, or a CCP response that distinguishes
  "never heard of this job" from "not ready yet".

- **`advance_heartbeat_clock` does not `release_workers` on eviction.** The
  heartbeat-timeout path inlines `mark_executor_lost`'s cleanup (its comments
  enumerate the parity list twice) but omits
  `cluster_manager.release_workers(1)`, which `mark_executor_lost` and
  `drain_executor` both call. Two paths to the same `Lost` state with
  different dynamic-allocation accounting.

  Not fixed because both patterns are defensible and the choice changes live
  cluster scaling: a timed-out pod is usually one Kubernetes will restart
  under the existing replica count (so releasing would scale down work that is
  coming back), while conversely `register_executor` currently releases a
  worker for an executor that is re-registering in that same call. Needs a
  stated intent for `ClusterManager` semantics — "release on any Lost
  transition" vs "release only on deliberate teardown" — before either side
  is changed.

- **`streaming_task_index` is keyed by bare `TaskId`, not `(JobId, TaskId)`.**
  Two live streaming jobs with a task id in common collide: the second
  `index_streaming_tasks` overwrites the first, and the first job's watermark
  reports then update the wrong record. Not fixable here —
  `StreamingTaskState` carries no job id on the wire, so the heartbeat itself
  cannot disambiguate. Protocol change, not an audit edit.

- **`CheckpointInner::pending_checkpoint_complete_for_executor` and
  `pending_restore_commands_for_executor` have no callers outside their own
  tests.** The live path is `Coordinator::pending_*` on the *outer* copy
  (`checkpoint_ops.rs`), and `prune_sent_set` is duplicated verbatim in both
  files. Concrete hazard if the inner ones are ever wired: the inner lock
  would record delivery in `checkpoint_complete_sent`, and the next periodic
  `apply_monotonic_from` full-replaces that field from the outer copy, which
  never saw the insert — so every commit signal is re-delivered. Either
  delete the inner pair or make `apply_monotonic_from` union the
  delivery-tracking sets; both are decisions about which copy owns delivery.

- **`shuffle_bytes_written` (stage and job) has no task-state filter** while
  the `shuffle_partitions_available` count two lines above filters on
  `Succeeded`. A task reset to Pending by shuffle invalidation keeps its old
  `output_metadata`, so its bytes keep counting until it re-succeeds. Whether
  the metric means cumulative I/O or currently-fetchable output is a
  reporting-semantics decision. **Not established** as the explanation for the
  q10 `dist-s1` 1.74 TB observation, which stays open.

- **Nothing sweeps the result-spool directory at startup**, so files orphaned
  by a coordinator *crash* (as opposed to a dropped future, now handled)
  persist. A sweep must distinguish a previous incarnation's files from a
  co-located coordinator's live ones; the filename carries a pid, but pids are
  reused. Needs a decision on spool-dir ownership.

### Noted, no defect

- `stage_specs_from_plan` (`distributed_batch.rs`) passes
  `shuffle.num_output_partitions` into `ShuffleWriteConfig` with no floor, and
  this path builds the config in-process — it never crosses the
  `shuffle_write_config_from_wire` zero-guard added in `dca44555`. Not a defect:
  `launch_assigned_task_assignments` applies `.max(1)` when copying the config
  onto the assignment (`record.rs:644`), so the executor never receives 0.

- Job-level and stage-level availability come from *different sources*:
  `JobRecord::shuffle_partitions_available_count` reads the `shuffle_output`
  map, `StageRecord::snapshot` counts succeeded tasks' metadata lengths
  (`record.rs:1545`). They can disagree. The stage-level one tracks task state,
  which regeneration does reset, so it was the more accurate of the two even
  before the fix above. Left as-is — unifying them is a reporting-semantics
  decision, not a bug fix.

- `reset_running_tasks_for_lost_executor` appears to skip `refresh_state` when
  only its shuffle-invalidation half fires (`job_affected` is set after the
  `if job_affected { job.refresh_state() }` line).
  `invalidate_executor_shuffle_partitions` calls `self.refresh_state()` itself,
  so the job state is refreshed either way.

- `deregister_executor` prunes the gRPC channel *after* calling
  `executors.deregister`, the opposite order from `mark_executor_lost`. Not a
  leak: `ExecutorRegistry::deregister` only sets state `Removed` and keeps the
  record (pruning happens later in `advance_clock_excluding`'s retention
  sweep), so the endpoint lookup still resolves.

- `AssignmentRejected` classifies `FailedPrecondition` and `Unimplemented` as
  permanent and cancels the whole job. That would be over-broad if an executor
  returned either transiently from `assign_task` — it does not: the executor's
  only `failed_precondition` sites are the checkpoint-fanout path, and its
  `unimplemented` sites are on `stream_exchange`. Re-check if `assign_task`
  ever grows a drain/backpressure rejection.

- `collect_bounded_assignment_futures` discards every successful response when
  any one future fails, so the launch loop's error branch clears in-flight for
  tasks that *were* delivered and re-dispatches them. Not a defect today: the
  executor inbox dedupes and answers `Duplicate`, which the response handler
  counts as accepted. It does cost redundant RPCs and under-reports `launched`.

- `KRISHIV_AQE_TARGET_PARTITION_BYTES=0` is accepted by the env reader while
  the `with_aqe_target_partition_bytes` setter applies `.max(1)`. It cannot
  divide by zero: the skew-split sizing already guards with `.max(1)`, and the
  coalescing loop degenerates to one group per partition, which returns "no
  rewrite". Behaviourally identical to the setter's floor.

- `reset_running_tasks_for_lost_executor` sets `job_affected` after the
  `if job_affected { refresh_state() }` line when only its shuffle-invalidation
  half fires. `invalidate_executor_shuffle_partitions` calls
  `self.refresh_state()` itself, so job state is refreshed either way.

- **SEC-1 re-verified end to end, including the seam above it.**
  `coordinator_http_router` folds `ivm_routes` and `qs_routes` into `protected`
  *before* `require_coordinator_bearer` is layered — the fix holds.

  But `spawn_coordinator_sidecars` then does
  `coordinator_http_router(..).merge(factory(coordinator))`, i.e. it merges the
  `extra_http_factory` routes **after** that layer. That is the same shape as
  the original SEC-1 bug one level up, and the only supplier is
  `krishiv_ui::embedded_router`, whose protected group includes
  `POST /api/v1/sql` (arbitrary SQL execution).

  Not a bypass: the UI router applies **its own** bearer layer, and
  `resolve_ui_token` is fail-closed — with no token configured under a profile
  that needs one it returns `Some("")`, a token nothing can match, denying the
  whole group. The two predicates cannot drift because
  `profile_requires_authenticated_ui(p)` is defined as `requires_http_auth(p)`.

  One asymmetry, deliberate-looking and left alone: the coordinator's
  `http_auth_required` honours `allow_anonymous_http_override()` while the UI's
  token resolution does not, so the dev escape hatch opens the coordinator API
  but not the UI. That errs strict, which is the right direction.

  **This is why the merge-after-layer must not be "tidied" without care**: it is
  safe only because every current supplier self-protects. A future
  `extra_http_factory` that does not would be unauthenticated with nothing in
  the type system to catch it.

- `try_tick` guards `expected_task_count == 0` before initiating, so the
  checkpoint interval timer cannot open an epoch nobody will ack.
  `trigger_checkpoint_for_job` omits `savepoint_job`'s `.max(1)` on the
  expected count, but a zero-count epoch still cannot false-commit: the quorum
  test lives inside `receive_ack`, which needs at least one ack to run.

- `recover_from_storage` does not reset the `pending_*` fields the way
  `activate_restored_epoch` does. Only ever called on a freshly-constructed
  coordinator where they are already default.

- `CheckpointBarrierTracker::timed_out` is never called — `dispatch_barrier_plan`
  enforces the deadline per target inside `inject_barrier` instead. Dead
  accessor, not a missing timeout.

- `InFlightTracker::drain` registers its `Notify` waiter *after* checking the
  count, so a `notify_waiters` in that window is missed (`Notify` stores no
  permit for future waiters). Correctness is unaffected — the loop re-checks
  the count at the top after the timeout elapses and returns `true` — but a
  demotion can wait the full drain timeout instead of returning as soon as the
  last call finishes, widening the split-brain window it exists to close.

- `JobCoordinator::has_in_flight_tasks` counts *failed* tasks as in-flight, and
  `has_tasks_eligible_for_launch` uses a different terminal-state predicate
  from `should_consider_for_launch`. Both are only consumed by `debug!`-gated
  logging, so neither drives a scheduling decision.

- The cascade breaker's window (30 s) is shorter than its cooldown (60 s), so
  by the time it re-closes the losses that tripped it have aged out of
  `cascade_loss_timestamps` and the next single loss cannot re-trip it. An
  operator who configures `cascade_window_ms > cascade_cooldown_ms` gets
  immediate re-tripping, which is arguably what that configuration asks for.

---

## 5–27. Not yet started

Each crate gets the same treatment and its own section here: measured
coverage, a table of uncovered-region concentration, a fixed list with commit
hashes, and an open list. Sections are appended as the audit reaches them.

---

## Cross-cutting findings

Things that are not one crate's problem.

- [x] **The streaming dials exist twice, byte-identically.** Consolidated
      `76f3fbba` into `krishiv_common::streaming_dials`; both
      `krishiv-executor/src/fragment/run_loop.rs` and `krishiv-engines` now
      import `idle_tick_interval`/`stream_linger`/`StreamProfile` from it. The
      deliberate decision this asked for was taken. Original note follows.

- [ ] ~~**The streaming dials exist twice, byte-identically.**~~
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
- [x] **D7 remainder — re-verified 2026-08-02, and mostly stale.** Two of the
      three claims no longer hold:

      * "`account_unavoidable` grows it past the pool unconditionally" — no.
        `c8736a52` (the item above) changed it to `try_grow` and, on failure,
        *log without growing*. The two bullets described the same mechanism,
        one as fixed and one as broken.
      * "the drain reads every spilled run back at once" — true of
        `drain_partition`, but the **live** map-write path does not call it.
        A TPC-H map task carries a `dfplan:` body, and
        `execute_batch_fragment` dispatches dfplan *before* the generic
        `shuffle_write` branch (the code says so, at the branch). That reaches
        `drain_into_store` -> `drain_partition_stream`, whose peak is the
        in-memory tail plus a window.

      What remains is genuinely narrow: `drain_partition` is still used by
      `execute_inmem_shuffle_write` (non-dfplan `sql:` fragments) and by
      `execute_shuffle_write_fragment`, which is **unreachable** — see the
      krishiv-executor section, and the warning now carried at that function.
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
