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
| 1 | krishiv-sql | 46,632 | 61 | **61 (COMPLETE 2026-08-15)** | 34 live defects fixed in the first pass + 2 in the closing pass (untrusted-footer allocation abort in vector_index, an always-failing env-gated integration test); 4 unreachable modules found; Unicode-folding bug class swept. The big four read whole and found pristine — every guard carries a measured cluster incident and a revert-proven test |
| 2 | krishiv-executor | 28,927 | 40 | **40 — COMPLETE 2026-08-02** | second crate fully read; 3 defects fixed |
| 3 | krishiv-shuffle | 14,329 | 36 | **36 — COMPLETE 2026-08-02** | first crate fully read; 4 defects fixed |
| 4 | krishiv-scheduler | **51,438** | **78** | **78 (COMPLETE)** | largest crate in the workspace; stage cutting, dispatch, single-task fallback, SC11 breaker |
| **Tier 2 — correctness blast radius** |
| 5 | krishiv-plan | 14,371 | 25 | **25 (COMPLETE)** | plan IR every surface depends on; 3 can't-fail tests fixed, 2 unreachable AQE rules with latent outer-join bugs documented, 4 dead pub surfaces recorded |
| 6 | krishiv-common | 7,966 | 23 | **23 (COMPLETE)** | 2 wrong-answer fixes (Float64 signed-zero shard split, UMM available `max`→`min`), a heartbeat env busy-loop, 3 declared-default drifts, registry `.rs.inc` scan blindness |
| 7 | krishiv-connectors | 39,930 | 97 | **97 (COMPLETE)** | ~60 defects fixed incl. avro silent corruption, kafka/cdc offset-before-delivery, NULL-predicate DELETE, orphan-cleanup deleting live MoR delete files, LanceDB fragment loss, 2PC later-epoch drops; streaming_unify deleted; ~120 tests added |
| 8 | krishiv-state | 12,357 | 37 | **37 (COMPLETE)** | 9 revert-proven fixes incl. savepoint-restore skipping manifest coverage, key-group task-index formula diverging from ranges, 1M-entry restore cap, DFS unstable-hash filenames + collision returning wrong key's value, sequential "async" executor; 2 can't-fail tests strengthened |
| **Tier 3 — runtime & surfaces** |
| 9 | krishiv-api | 25,111 | 38 | COMPLETE | 9 defects fixed (A1–A9); see §9 |
| 10 | krishiv-runtime | 13,648 | 17 | COMPLETE | 3 defects fixed (R1–R3); see §10 |
| 11 | krishiv-dataflow | 18,107 | 38 | COMPLETE | 6 defects fixed (D1–D6); see §11 |
| 12 | krishiv-ivm | 7,019 | 10 | COMPLETE | 3 defects fixed (I1–I3) + 1 needs-decision (watch-channel delta coalescing); see §12 |
| 13 | krishiv-delta | 7,098 | 20 | COMPLETE | 5 defects fixed (K1–K5) + cross-crate hash_row re-key; see §13 |
| 14 | krishiv-flight-sql | 5,199 | 6 | COMPLETE | 2 defects fixed (F1–F2); see §14 |
| 15 | krishiv-proto | 8,130 | 12 | COMPLETE | 0 defects — cleanest crate; 1 note (unaligned_buffers wire drop); see §15 |
| 16 | krishiv-metrics | 3,731 | 6 | COMPLETE | 2 defects fixed (M1–M2); see §16 |
| 17 | krishiv-engine-core | 3,146 | 11 | COMPLETE | 0 functional defects; forbid(unsafe_code) added; see §17 |
| **Tier 4 — thin, tooling, structural smells** |
| 18 | krishiv-python | 13,101 | 35 | 35 | COMPLETE — PY1–PY7 fixed (§18); direct clippy now 0 |
| 19 | krishiv-operator | 4,878 | 19 | 19 | COMPLETE — OP1 scale-down leak + OP2 lease-release clobber fixed (§19) |
| 20 | krishiv-mcp | 3,296 | **1** | 1 | COMPLETE — zero functional defects; one E-class doc fix (§20) |
| 21 | krishiv | 8,472 | 24 | 24 | COMPLETE — C1 --timeout no-op wired + 2 doc fixes (§21) |
| 22 | krishiv-engines | 2,184 | **1** | 1 | COMPLETE — E1 cannot-fail test rewritten + E2/E3 (§22) |
| 23 | krishiv-ui | 2,384 | 4 | 4 | COMPLETE — U1 fail-open UI auth + U2 null timestamps (§23) |
| 24 | krishiv-bench | 4,942 | 23 | 23 | COMPLETE — B1 name-asserts-X/silent-skip test fixed (§24) |
| 25 | krishiv-sql-gateway | 541 | 3 | 3 | COMPLETE — zero defects; honest not-a-wire-protocol doc (§25) |
| 26 | krishiv-conformance | 353 | 3 | 3 | COMPLETE — zero defects; prior "no tests at all" was WRONG (§26) |
| 27 | krishiv-chaos | 829 | 1 | 1 | COMPLETE — KEEP (not empty: 25-test suite); X1+X2 cannot-fail tests fixed (§27) |

---

## 1. krishiv-sql — 38 of 61 files read whole (2026-08-09 → 08-11)

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

#### `lib.rs` (read whole, 2026-08-08/09)

- [x] Four detached doc comments reattached; `SqlEngine` itself had none.
      19 rustdoc warnings → 0. `1c6f7056`, `93d1c0c3`
- [x] **`CREATE OR REPLACE FUNCTION` returned the old function's results.**
      Nothing invalidated the plan cache on UDF registration, and a cached
      `LogicalPlan` pins what the planner resolved — `Expr::ScalarFunction`
      holds an `Arc<ScalarUDF>`, a table function is resolved all the way to
      the `TableProvider` the *previous* definition returned. Re-running
      identical query text within the 30 s TTL executed the old body. A wrong
      answer, not a stale-cache annoyance. Fixed by making `bump_udf_version()`
      — already the canonical "UDF set changed" signal — carry the whole
      invariant; deliberately *not* placed in `sync_*_udfs`, which are called
      when the set has not changed. `62f357c2`
- [x] **Plan cache evicted FIFO while documenting LRU.** `get(&self)` could not
      touch `order`, so `order` only recorded insertion. Under FIFO a burst of
      distinct query texts evicts precisely the hot repeated query the cache
      exists for. Made the code match the documented intent rather than
      downgrading the doc; `get` now also drops an entry it finds expired
      instead of holding a dead `LogicalPlan` until eviction reaches it.
      `f92f690e`
- [x] `query_memory_limit_from_env` — recorded as having zero callers and
      being the third copy of the same parse. `93d1c0c3`

#### `connector_table.rs` + `kafka_table.rs` (read whole, 2026-08-09)

- [x] **Kafka tables declared themselves bounded to the optimizer.**
      `KafkaPartitionStream::execute` never ends a stream — `Ok(None)` is a poll
      gap, so it flushes, sleeps 20 ms and retries; its own comment says "the
      polling loop can run indefinitely". Both `StreamingTable` construction
      sites omitted `.with_infinite_table(true)`, and DataFusion's default
      `infinite: false` maps to `Boundedness::Bounded`
      (datafusion-physical-plan-54/src/streaming.rs:158). A pipeline-breaking
      operator over a Kafka table was therefore *accepted* and then blocked
      forever — no output, no error. Now a plan-time error. `2b8bddaa`
      **Still owed: a live-broker exercise confirming the error surfaces where
      the hang was.** No Kafka available in this environment.
- [x] Verified NOT defects while reading: `BoundedConnectorProvider` is
      genuinely finite and `create_continuous_table` terminates on sender close,
      so both are correctly left bounded; `deregister_table` already clears
      `streaming_sources` (fixed earlier, documented in place); the `kafka`
      DDL branch accepting arbitrary options is deliberate (librdkafka surface)
      where the `jdbc` branch's closed option set is also deliberate.

#### `udf.rs` (read whole, 2026-08-09)

- [x] **Aggregate UDF `evaluate()` consumed its state.** DataFusion's
      `Accumulator::evaluate` contract (datafusion-expr-common-54) is explicit:
      "must not consume the internal state … Consuming the internal state can
      cause the next invocation to have incorrect results."
      `KrishivAggregateAccumulator` did `std::mem::take`. Reachable two ways,
      both silently wrong numbers: a **window frame** calls `evaluate` once per
      row, and `AggregateStream::maybe_update_dyn_filter` calls it *mid-stream*,
      selecting accumulators **by function name alone**
      (`eq_ignore_ascii_case("min"|"max")` — flagged as a HACK in
      datafusion#18643), so a UDAF named `min`/`max` was hit too. `b0eabb9e`
- [x] **Zero-argument scalar UDFs now rejected at registration.** `create_udf`
      takes a `Fn(&[ColumnarValue])`, erasing the `number_rows` that
      `ScalarFunctionArgs` carries — with no args the bridge built a 0-row batch
      and returned a 0-length array for a projection over N rows. `b0eabb9e`
- [x] **Documented why `sync_table_udfs` has no durability gate** where scalar
      and aggregate do, so it is not later "fixed" with an over-broad one. The
      gate keeps arbitrary *native* code out of a durable engine; the only two
      producers of a `TableUdf` are a Rust closure (caller already runs native
      code in-process) and a `LANGUAGE SQL` body (no native code). The remote
      cloudpickle vector lands in the gated scalar/aggregate registries.
- [x] Checked, NOT a defect: the aggregate path re-resolves the durability
      profile while the scalar path takes a snapshotted `NativeScalarUdfPolicy`.
      Both bottom out in `profile_forbids_native_scalar_udfs`, which honours
      `KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS` and is `OnceLock`-cached — they cannot
      disagree.

#### `cep_sql.rs` (read whole, 2026-08-09)

- [x] **Streaming `MATCH_RECOGNIZE` fabricated a match from every event.** A row
      carries no stage label, so both executors offer it to each stage name in
      turn; `SequentialPatternMatcher` starts a partial at stage 0 then accepts
      `stage_index + 1`, so one row was started at A, advanced to B, then C —
      returning a "match" of that row repeated N times. The **batch** path
      guards this by diffing `(stage_index, start_time_ms)` around each
      `process_event`; the **streaming** path never got the same guard. Fixed by
      adding `PartitionedCepMatcher::partial_signature` (the streaming path owns
      state through the wrapper and could not observe the partial) and applying
      the identical check. `eebe037b`
- [x] **Two reachable parser panics**, both reversed-range slices on ordinary
      SQL: `find(" FROM ")` scanned the whole statement so `FROM` could follow
      the match position (`SELECT ' MATCH_RECOGNIZE ' FROM t` — keyword in a
      string literal); `rfind(')')` likewise, so `SELECT (1) FROM t
      MATCH_RECOGNIZE (` gave `body_end < body_start`. Both now search only the
      correct sub-range. `eebe037b`
- [x] Checked, NOT defects: `window_ms` defaults to `60_000` when `WITHIN` is
      omitted, so the streaming TTL eviction `max_ts - 2 * window_ms` cannot
      collapse to "evict every key"; and byte offsets taken from `upper` are
      safe to slice `trimmed` with, because `to_ascii_uppercase` remaps only
      ASCII `a-z` and never changes length.
- [ ] **Open, deliberately not fixed:** `extract_parenthesized_after` takes the
      *first* `)` after the keyword, so `PATTERN ((A B) C)` silently truncates
      instead of erroring, and `parse_within_ms` matches `WITHIN` as a bare
      substring (a column named `within_x` would be misread). Both are
      limitations of a hand-rolled parser for a subset that does not support
      nesting; fixing them properly means parsing the body rather than
      string-scanning it, which is a larger change than this audit should make
      unannounced.

#### `lakehouse/merge.rs`, `lakehouse/as_of.rs`, `streaming.rs` (read whole, 2026-08-09)

- [x] **`MERGE INTO` never worked at all.** `MERGE_RE`'s two `WHEN` arms were
      `(?:…)` — non-capturing — so the regex defined three groups and
      `caps.get(4)`/`get(5)` were always `None`. Every statement was rejected
      with "requires at least one WHEN MATCHED or WHEN NOT MATCHED clause".
      **Zero tests touched the entry point**: three asserted on
      `MERGE_RE.is_match`, three on `KEY_COL_RE`, and the one end-to-end test
      called a private helper. `3200c987`
- [x] Same file: `merge_delta(…, true, true)` hardcoded both arms while the
      parsed flags went unused, so `WHEN NOT MATCHED THEN INSERT *` also updated
      every matched row. And the `iceberg:` branch ran a **dry run** and
      returned its counts as the statement's result — a merge that never
      happened, reported as success (DUR-1 class). Now refuses. `3200c987`
- [x] **An `AS OF` qualifier the mapper could not read was silently dropped.**
      `version.take()` removed the clause unconditionally but only recorded a
      ref when the mapping succeeded — so a time-travel query ran against the
      *current* version. This is the same "time travel returned the present"
      bug fixed earlier in `lakehouse/providers`; that fix made the provider
      refuse an unpinnable ref, but the clause was discarded before a ref was
      ever built, so the provider never saw it. Also added the
      `TIMESTAMP AS OF TIMESTAMP '…'` spelling, which was handled for
      `FOR SYSTEM_TIME AS OF` but not here. `51f66bd1`
- [x] **`ContinuousTableInput::cancel()` was a graceful close.** Documented as
      an A-8 hard cancel that discards queued batches; it only dropped the
      sender, and a tokio mpsc receiver drains its buffer before reporting
      end-of-stream. Now shares an `AtomicBool` with the consumer. Both `new()`
      constructors removed so a half-wired (uncancellable) pair cannot be
      built. `b6d28985`

#### Batch 4 — small modules (read whole, 2026-08-09)

- [x] **`sqlstate.rs`: every DataFusion error was `XX000` "engine fault".** That
      variant carries syntax errors, unknown tables, column typos, failed casts
      and object-store timeouts, and JDBC/ODBC clients key on SQLSTATE — so a
      user typo was reported as an engine bug. Classified by DataFusion's own
      `error_prefix()` literals (one match in datafusion-common/src/error.rs),
      verified by round-tripping real `DataFusionError` variants. Also puts
      `UNDEFINED_TABLE`/`DATA_EXCEPTION`/`SYSTEM_ERROR`/`GENERAL_ERROR` to work
      — all four were defined and referenced only by the "codes are 5 chars"
      test. `190b92ea`
- [x] **`unnest_sql.rs`: the `",LATERAL UNNEST("` pattern was unreachable.**
      `contains_lateral` requires `" LATERAL "` *with a leading space*, so
      `FROM t,LATERAL …` returned early — the pattern written for exactly that
      form could never run. `3b6d924f`
- [x] **`pipe_syntax.rs` was an orphan file: no `mod` declaration anywhere.**
      Never compiled, so the documented "P10: SQL Pipe Syntax" feature did not
      exist and its six tests ran zero times (`running 0 tests`). Wired in, but
      only after fixing what the dead code hid: repeated stages silently
      overwrote (`|> WHERE a |> WHERE b` dropped `a`), and a filter after a
      `GROUP BY` was emitted as a `WHERE` instead of a `HAVING`. `7cf02964`
- [x] Read and found clean: `lakehouse/mod.rs`, `catalog/object_store_io.rs`,
      `catalog/rest_catalog_wrapper.rs`, `object_store_registry.rs`,
      `streaming_table_ddl.rs`, `scalar_udf.rs`, `join_estimates.rs`
      (the last is exemplary — it documents *why* two rules must read the same
      statistics differently), `introspection_sql.rs`.

**Orphan-file sweep**: after wiring `pipe_syntax`, no source file in
krishiv-sql lacks a `mod` declaration. Repeated across the whole workspace, the
only hits are eight `src/bin/*.rs` — Cargo auto-discovers binary targets, so
those are false positives, not findings.

#### Batches 5–12 (read whole, 2026-08-09 → 08-11)

**Wrong answers**

- [x] **A `HOP` window put each event in one window instead of `size/slide`.**
      `streaming_tvf`'s own doc says "each event appears in (size/slide)
      windows"; the rewrite was a *scalar projection*, which cannot fan one row
      into several, so every grouped aggregate over a HOP undercounted. Fixed
      with `hop_first_start` + `generate_series` + `unnest`. Running the **full**
      suite exposed the coupling: `streaming_window_plan` pattern-matches the
      rewrite's *shape* to recover kind/size/slide, so the fan-out broke the
      streaming compile until `extract_window` learned to resolve a
      column-alias `window_start` through the `_tvf_hop` layer. `f30df557`
- [x] **`PIVOT(` with no space parsed `SUM` as `UM`** — the parser advanced by
      the length of the *spaced* keyword unconditionally. Every test used the
      spaced form. And `strip_select_star_prefix` used `rfind(" FROM ")`, so a
      subquery source was truncated to a fragment with an unbalanced paren; now
      scans for the first `" FROM "` at paren depth 0. `89d7c85c`
- [x] **Unicode case folding shifted byte offsets into the original SQL** —
      a whole bug *class*, swept crate-wide. These parsers fold a copy to find a
      keyword then index the original; `to_uppercase` is not length-preserving
      (U+FB01 -> "FI"). In `live_table.rs` it truncated the registered name and
      **panicked** on a non-char boundary; also fixed in `spark_sql_ext`,
      `pipe_syntax`, `incremental_view` (3 sites) and `pipeline_ddl`.
      `222c4fb2`, `87353591`, `229af32d`
- [x] `SHOW SCHEMAS IN <catalog>` ignored the catalog and listed every one;
      ``USE `my.schema` `` split a quoted identifier on its dot. `b0000e95`
- [x] `incremental_view` LATENESS: `splitn(_, char::is_whitespace)` does not
      collapse runs, so a second space shifted every field; and the code carried
      the comment "tokens[1] should be INTERVAL" without checking it.
      `87353591`

**Unreachable code — four modules**

- [x] **`pipe_syntax.rs` had no `mod` declaration anywhere**: never compiled,
      its six tests ran zero times (`running 0 tests`), and the documented
      feature did not exist. Wired, after fixing what the dead code hid —
      repeated stages silently overwrote, and a filter after `GROUP BY` was
      emitted as `WHERE`. `7cf02964`
- [x] **`spark_sql_ext.rs`**: all 11 public fns have zero callers. Its rewrites
      named things that do not exist (`explode`, and
      `information_schema.table_properties`, zero hits in datafusion-catalog-54),
      and `rewrite_transform` returned its input while documenting a rewrite.
      Made correct-if-wired; **wire-or-delete left as a recorded product
      decision**, not guessed at. `458047bc`
- [x] **`subquery.rs`'s streaming guard had no callers, so it guarded nothing.**
      Wired into `SqlEngine::sql`. Note the register itself recorded this module
      as *fixed* in `927c1243` — a real fix to a guard nothing invoked. `5af86e97`
- [x] `coverage.rs` looked dead by the same measure and **is not**: it is a
      self-exercising test harness. Checked before believing.

**Honesty / published surface**

- [x] The feature matrix reported `aggregate`/`reduce` as `planned`; they ship
      and have end-to-end tests. Split the bundled entry, added the checklist
      case the CI rule then required. The drift guard caught the docs pages and
      names its own bless command — the one mechanism in this crate that
      actively *prevents* doc/code divergence. `958fa098`
- [x] Every `SqlError::DataFusion` mapped to `XX000` "engine fault", so a user's
      typo was reported to JDBC/ODBC clients as an engine bug. Classified by
      DataFusion's own `error_prefix()` literals, verified by round-tripping real
      error variants. `190b92ea`
- [x] `MERGE INTO` was rejected unconditionally (non-capturing `WHEN` arms);
      insert-only merges also updated; the `iceberg:` branch reported a merge
      that never happened. `3200c987`
- [x] An `AS OF` qualifier the mapper could not read was silently dropped, so
      time travel returned the present. `51f66bd1`
- [x] `ContinuousTableInput::cancel()` was a graceful close despite documenting
      a hard cancel; a tokio mpsc receiver drains its buffer first. `b6d28985`

**Verified correct (recorded so they are not re-derived)**

`join_estimates.rs` (documents *why* two rules must read the same statistics
differently), `higher_order_functions.rs` and `spark_functions.rs` (alias onto
DataFusion's exact impls; refuse to approximate), `create_function_ddl.rs` —
whose `CREATE_FUNCTION_RE` shows the correct optional-capture idiom `MERGE_RE`
got wrong, and whose `bind_sql_body_args` is a real tokenizer, so the
`LANGUAGE SQL` UDTF body is **not** injectable. Also `json_functions.rs`,
`object_store_registry.rs`, `rest_catalog_wrapper.rs`, `streaming_table_ddl.rs`,
`scalar_udf.rs`, `introspection_sql.rs`, and `REFRESH PIPELINE … FULL` (looks
like the folding bug, slices by `rest.len()`, is safe — pinned by a test).

Five lessons from these files, all about *my own* method:

1. `register_parquet` looked like a missing-invalidation site to a
   grep-for-`invalidate_plan_cache`-in-body heuristic. Reading the body showed
   it delegates to `register_parquet_with_primary_key`, which does invalidate.
   **A body-scanning heuristic cannot see through a delegation** — every hit it
   produces has to be read before it is believed.
2. The LRU doc I "corrected" earlier in this same audit (from "random
   eviction" to "LRU") was still wrong: the code was FIFO. **Correcting a
   comment against the name of a field rather than the behaviour of the code
   just relocates the error.**

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

- [x] **GAP-WATERMARK — CLOSED 2026-08-16 (§30 below).** Left here verbatim as
      the original finding; the close-out records what the scoping got wrong.

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

## 4. krishiv-scheduler — 78 of 78 files read whole (COMPLETE, 2026-08-04)

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

**All 78 files / 51,438 lines read.** The final pass covered
`unified_jobs_http.rs` (377), `in_process.rs` (353), `barrier_client.rs` (204),
`transport.rs` (38), the three `bin/` entry points (169), and every remaining
`sections/*.rs.inc` plus `tests/` file:
`sections/checkpoint.rs.inc` (865), `sections/adaptive.rs.inc` (717),
`sections/retry_streaming.rs.inc` (612), `sections/streaming_recovery.rs.inc`
(586), `sections/recovery.rs.inc` (544), `tests/r2_k8s_manifests.rs` (376),
`sections/savepoint.rs.inc` (367), `sections/chaos_basic.rs.inc` (285),
`sections/dur1.rs.inc` (213), `sections/validation.rs.inc` (180),
`sections/queue_manager.rs.inc` (162), `sections/checkpoint_timer.rs.inc` (160),
`sections/barrier_oob.rs.inc` (141), `sections/chaos_restart.rs.inc` (128),
`tests.rs` (124), `sections/failover.rs.inc` (86),
`tests/distributed_e2e.rs` (71), `sections/etcd_sim.rs.inc` (53),
`tests/coordinator_executor_integration.rs` (40).

### The shape the test files taught (new to this crate)

*A test name is an assertion nobody checks.* Eight tests in this crate claimed
behaviour their bodies did not test, and three of them were documenting a live
defect as intended behaviour:

- `streaming_reattach_does_not_affect_batch_tasks` asserted that a streaming
  heartbeat **did** update a batch task's watermark. That was the visible edge
  of a real bug: `streaming_task_index` is keyed by a bare `TaskId`, task ids
  are unique only within a job, and the distributed planner names them
  positionally — so jobs collided, and `apply_streaming_state` refreshes
  `last_progress_ms`, which the stall watchdog reads. Fixed by indexing only
  streaming jobs (matching what `recover_from_store` always did) *and*
  requiring the reporting executor to own the task.
- `metadata_store_persists_job_on_submit` / `..._task_state_on_update` asserted
  the in-memory registry, never the store; one built a store handle and dropped
  it unused.
- `checkpoint_coordinator_rejects_non_quorum_ack_as_stale_epoch` asserts
  `Accepted`.
- `circuit_breaker_actually_clears_assignments_from_bad_executor` never looked
  at an assignment; `assignment_flood_protection_basic` was the same loop with a
  weaker assertion.
- `executor_failover_reassigns_task_to_surviving_executor` accepted either
  executor and either state — and tightening it revealed the setup never made
  the task Running or the executor lost, so no failover was being exercised.
- `executor_max_losses_permanently_fails_task` hardcoded the threshold and could
  pass after a single loss.

**The generalisable check**: for every test, ask what single line of production
code you would delete to make it fail. If the answer is "none", or "a line
unrelated to the name", it is not a regression test. Two discrimination checks
in this session initially passed because they targeted the wrong function —
that is the check failing, not the fix.

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

## 5. krishiv-plan — 25 of 25 files read whole (COMPLETE, 2026-08-14)

Fifth crate. 14,390 lines. The plan IR every other crate lowers through, so a
silent wrong answer here has the widest blast radius in the workspace. It is
also the cleanest surface audited so far: no `async`, no `Notify`, no
`block_on`, one `#[allow]` (a `clippy::question_mark` in the dead DPP rule), no
`#[ignore]`d tests, no real TODO/HACK (the only "XXX" hits are the
`secretXXX` governance test fixture).

**The headline is a reachability finding, not a live wrong answer.** The
optimizer rules that *could* miscompile a plan are the ones that cannot run.

### Fixed reading it

- [x] **`node_op_variants_round_trip` could not fail** (test-validity, C). The
      body did `op.clone(); assert_eq!(&cloned, op)` — a value is always equal to
      its own clone, so no production serialization line could be deleted to make
      it fail, despite the name promising a round trip. `NodeOp` derives
      `serde::{Serialize, Deserialize}` and crosses the wire as JSON (task
      fragments). Rewrote to `serde_json::to_string` → `from_str` → `assert_eq`,
      which now exercises both derives. `lib.rs`.
- [x] **`empty_pattern_compile_rejected` tested the opposite of its name**
      (test-validity, C). It compiled a **one-stage** pattern and asserted it
      *succeeded* — never touching the `stages.is_empty()` guard it claimed to
      test. Rewrote to `Pattern::default().compile()` → assert
      `Err(EmptyPattern)`, plus the one-stage boundary on the other side.
      **Revert-proven**: commenting out `return Err(CepCompileError::EmptyPattern)`
      in `pattern.rs` now turns this red (it stayed green before). `cep/matcher.rs`.
- [x] **`partial_match_default_values` was a pure tautology** (test-validity, C).
      It built a `PartialMatch` from struct literals and asserted those same
      literals back — zero production code in the assertion path. Deleted; real
      advancement of `stage_index` / `captured_event_count` is covered by
      `stage_ordering_enforced` and
      `out_of_order_stage_after_partial_resets_correctly`. `cep/matcher.rs`.
- [x] **Documented that two AQE rules are unreachable and carry latent
      correctness bugs** (honesty, E). `default_aqe_optimizer_with_parallelism`
      registers `BroadcastRuntimeRule`, `AutoPartitionRule`, `CoalesceRule`,
      `SkewJoinRule`, but only `CoalesceRule` has a live effect. Both production
      call sites are in krishiv-scheduler `job_lifecycle.rs`: the stage-succeeded
      path (`:786`) applies the optimizer to a synthesised placeholder built only
      from `Exchange` + `Sink` nodes and reads back *only*
      `plan.coalesced_partition_count()` — no `Join` node for `SkewJoinRule`, no
      `Broadcast` node for `BroadcastRuntimeRule`, node-type rewrites discarded;
      `submit_physical_plan` (`:1272`) passes the real plan but with **empty
      stats**, on which both rules no-op. And `NodeOp::SkewJoin` has **no
      consumer** in lowering or any executor crate. So neither rule can fire
      today. Their latent obligations — `SkewJoinRule` salts **any** `JoinType`
      (outer/anti included) with no per-side guard (would duplicate null-extended
      rows of a FULL/RIGHT outer join once a `SkewJoin` executor exists);
      `BroadcastRuntimeRule` demotes a colocating `Broadcast` to a
      non-colocating `RoundRobin` (would drop join matches for any consumer
      relying on the broadcast for colocation) — are now recorded at the
      registration site so they are not "known safe because registered." `optimizer.rs`.

### Recorded, not fixed — needs a decision (wire-or-delete)

Per the register's standing rule, wire-or-delete is a product decision, not an
audit edit. Four dead pub surfaces, each safe to delete with zero behaviour
change:

- **`DynamicPartitionPruningRule`** (`optimizer/dynamic_partition_pruning.rs`,
  424 lines) — a fully implemented `AqeRule` **never registered** in any
  pipeline; only its own `mod`/`pub use`/tests reference it. Benign even if it
  fired (it only relabels the join node with the identical op), so the defect is
  pure unreachability. Carries the crate's one `#[allow(clippy::question_mark)]`.
- **`SkewJoinRule` / `BroadcastRuntimeRule`** — registered but unreachable (see
  above); wire the guards before wiring the executor, or delete the node-type
  rewrites and keep only the coalesce hint.
- **`diff_plans` / `PlanDiff`** (`lib.rs:763`) — doc claims operators use it for
  adaptive-repartition diffs (R7/R9); repo-wide grep finds only its own tests.
- **`PlanNode::with_exchange`** (`lib.rs:412`) — doc names `DataFrame::repartition()`
  as the caller; that method does not call it. Zero callers repo-wide.

### Verified correct (recorded so they are not re-derived)

- **`graph.rs::validate_plan`** — the highest-blast-radius function in the crate.
  Correct Kahn cycle check + duplicate-node-id, missing-input, self-reference,
  blank-id, duplicate-input-edge, and `MAX_PLAN_NODES` cap. Reachable in
  production via `lower_to_physical` (3 external callers). Indegrees mutated
  during traversal are correctly reused to report blocked nodes — no
  state-not-restored bug.
- **`predicate_pushdown.rs`** — conservative and correct: a conjunct is pushed
  only when all its columns belong to exactly one scan; only descends through
  `JoinType::Inner`; qualified columns require an exact table match. The one
  thing to verify cross-crate: pushdown *removes* the `Filter` node once all
  conjuncts land in `Scan.filters`, which is sound only if the execution layer
  is guaranteed to apply scan filters (check when auditing krishiv-sql's scan
  providers).
- **`constant_folding.rs`** — every ambiguous case falls through to `Unknown`
  (unchanged); integer arithmetic is checked (`checked_add`/`div`-by-zero),
  string comparison only folds `=`/`!=`, precedence (OR<AND<NOT<cmp) is correct.
- **`join_reorder.rs`** — correctly restricted to commutative `Inner`/`Cross`;
  outer/semi/anti left untouched.
- **`skew_join.rs` detection / `coalesce.rs` / `auto_partition.rs` /
  `broadcast.rs` / `stats.rs` / `small_file.rs`** — the count/partition changes
  are answer-preserving; `coalesce.advise` is a true partition of the index set
  (`every_input_partition_survives_grouping` pins it); all correctly return
  `None` on no-op.
- **`optimizer.rs` driver** — single-pass (not a fixpoint loop); "applied"
  detection correctly gates on `new_plan != current`, so a no-op rule is not
  recorded and cannot mask later rules.
- **`cep/matcher.rs` state machine** — advancement requires
  `stage_idx == partial.stage_index + 1` exactly; window expiry clears the
  partial before the stage check, so the streaming MATCH_RECOGNIZE fabrication
  shape (fixed in krishiv-sql) is not present here; `evict_stalest` always finds
  a victim, `evict_keys_before` retains `>= cutoff`. Window boundary
  (`event - start > window_ms`) is off-by-one-correct.
- **`window.rs` validation** — thorough and consistent with its ST11/R5.2 docs
  (Session `size==0` exemption, Count `slide<=size`, `ttl==0` / `lateness==0`
  rejections). HOP/SLIDING event *assignment* lives in krishiv-dataflow, not
  here, so the size/slide fan-out bug class cannot occur in this crate.
- **`expression.rs`**, **`lowering.rs`** (SQL-injection guard on scan tables is
  real), **`task_fragment.rs`** (version + legacy-profile rejection),
  **`governance.rs`** (constant-time key comparison is genuinely
  non-short-circuiting), **`udf.rs`** (pure trait/registry plumbing — the
  min/max-by-name HACK is in krishiv-**sql**'s `udf.rs`, not this one).

### Low severity, noted

- `window.rs::encode_stream_fragment`'s single-aggregate path skips
  `validate_window_execution_spec` (the multi-agg/JSON path calls it), so an
  invalid single-agg spec encodes and then fails loudly at *decode* — an
  asymmetry, not a silent wrong answer.

### Open

- [ ] Coverage not yet measured with `cargo llvm-cov` (tool installed this
      session). The read pass found the defects above; the uncovered-region
      table is still owed and will decide what to test next — `udf.rs` (1635)
      and `window.rs` (1256) are the largest untested-surface candidates.

---

## 6. krishiv-common — 23 of 23 files read whole (COMPLETE, 2026-08-14)

Sixth crate. 8,065 lines. The foundation crate (env registry, durability
profiles, memory budget, the sync/async `block_on` bridge, the distributed
partition-key hash). Two of the fixes below are genuine silent-wrong-answer
correctness bugs, one is a busy-loop DoS reachable from a Kubernetes env var,
and three are declared-vs-real default drift that made the published env
reference and `krishiv doctor` lie.

### Fixed reading it

- [x] **Float64 partition keys did not canonicalise signed zero** (wrong answer,
      A). `digest_for_key`'s `Float64` arm canonicalised NaN payloads but not
      `-0.0`: `+0.0` (bits `0x0`) and `-0.0` (bits `0x8000…`) produce different
      SHA-256 digests and route to different shards, though SQL treats them equal
      (`0.0 = -0.0`). A co-partitioned join on a Float64 key with `-0.0` on one
      side and `+0.0` on the other silently drops the matching pair. Fixed by
      mapping `value == 0.0` (true for both zeros) to `+0.0` bits, right beside
      the existing NaN canonicalisation. **Revert-proven**:
      `partitioning_canonicalizes_signed_zero` fails against the pre-fix bits.
      `partition.rs`.
- [x] **`UnifiedMemoryManager::available_for_region` used `max` where the doc
      (and the only caller) needs `min`** (wrong accounting, A + E). It returned
      `total_free.max(region_headroom)` — the region's soft-min headroom even
      when the pool is full — while the doc says "returns 0 when the total pool
      is exhausted OR the region exceeded its soft minimum" (that is `min`). The
      sole caller, krishiv-executor `fragment/common.rs:1171`, does
      `available_for_region(Execution).min(want)` then `try_reserve(remaining)`;
      the over-reported value fails the real global-total check in `try_reserve`
      and the task falls through to the **unreserved over-commit** path instead
      of taking the smaller-but-real grant. Concrete: total 1000, Shuffle holds
      850, Execution min 400 → `max`=400 (ungrantable), `min`=150 (the true
      free pool). **Revert-proven**:
      `available_for_region_is_bounded_by_the_free_pool` fails against `max`.
      Also recorded in place: a region's protected minimum is NOT reclaimable
      once other regions over-borrow (`try_reserve` has no region-aware path), so
      "protected minimum" is advisory. `unified_memory_manager.rs`.
- [x] **`KRISHIV_HEARTBEAT_INTERVAL_SECS=0` busy-loops the coordinator**
      (config, A — cross-crate, fixed in krishiv-executor). The argv path
      `--heartbeat-interval-secs` explicitly rejects 0, but the env path
      (`cli.rs:1188`, `unwrap_or(10)`) had no zero-guard and fed
      `sleep(Duration::from_secs(0))`. The env var is the one Kubernetes injects
      (it is in the operator's `ALLOWED_EXECUTOR_ENV_VARS`), so the *deployed*
      path was the unguarded one — the exact argv-clamped/env-unvalidated shape
      the scheduler audit already found for the JCP poll interval. Fixed with
      `.filter(|&v| v > 0)`. `krishiv-executor/src/cli.rs`.
- [x] **Three env-registry declared defaults drifted from the real accessor
      defaults** (honesty, A/E). The registry `default` field generates
      `docs/reference/env-flags.md` and `krishiv doctor`; none of the three is
      pinned by `declared_default_number`, so the drift was live and untested:
      `KRISHIV_MCP_MAX_ROWS` declared 1000, real 100 (10×);
      `KRISHIV_PLAN_CACHE_MAX_ENTRIES` declared 128, real 256 (2×);
      `KRISHIV_HEARTBEAT_INTERVAL_SECS` declared 5, real 10 (2×). Corrected each
      declared value to the code's real default and re-blessed the reference doc
      (`KRISHIV_BLESS_ENV_REFERENCE=1`). `env_registry.rs`.
- [x] **The registry-rot guard was blind to `.rs.inc` sections** (reachability
      of the guard itself, B/C). `every_flag_read_in_source_is_declared` /
      `every_declared_flag_still_exists_in_source` scanned only files whose
      extension is `rs`; the repo keeps 36 `.rs.inc` include sections that
      contain real `KRISHIV_*` reads, whose extension is `inc`. A flag read only
      from a `.rs.inc` would neither be caught as undeclared nor keep a declared
      entry alive. Same `--include='*.rs'`-only blind spot the shuffle/scheduler
      audits were burned by. Extended the scan to `*.rs` + `*.rs.inc`; no new
      undeclared flags surfaced (so no latent rot today), but the guard now
      actually covers what it claims. `env_registry.rs`.
- [x] **`write_commit.rs`'s durability path had no on-disk test** (test-coverage,
      C). Publish/cleanup were covered only for parsing/name-formatting — you
      could make `publish_staged_outputs` return `Ok(default)` and every test
      passed. Added three on-disk tests (a self-cleaning `TempDir`, no external
      crate): publish moves staged bytes to the final path and removes staging;
      cleanup removes staging without publishing; and the OverwriteDynamic
      produced-partition clear. `write_commit.rs`.
- [x] **`write_commit.rs` OverwriteDynamic skipped the foreign-file clear on an
      idempotent re-publish** (edge-case correctness, A). The per-partition
      foreign-file clear ran *after* the `final_path.exists()` skip, so a
      re-publish into an already-committed dynamic partition left foreign files a
      first-time publish would have removed — the partition was not fully "owned."
      Moved the clear before the skip (our own final file is excluded by name, so
      clear-then-skip is safe). **Revert-proven**:
      `overwrite_dynamic_clears_foreign_in_produced_partition_even_on_republish`
      fails against the old ordering. `write_commit.rs`.
- [x] **Corrected the misleading per-stage-cap comment** (honesty, E) at
      `unified_memory_manager.rs:309`: it claimed stage bytes are "counted again
      … so a single stage cannot blow the pool," but `try_reserve_stage`
      delegates to the global `try_reserve` — there is no independent per-stage
      cap; `by_stage` is pure bookkeeping. Comment now states the real behaviour.

### Verified correct (recorded so they are not re-derived)

- **`async_util.rs`** — the `block_on` bridge is correct: multi-thread runtime →
      `block_in_place`; current-thread runtime → hop to a fresh OS thread via
      `thread::scope` (Tokio's nesting guard is per-OS-thread, not
      per-runtime-instance); no runtime → fallback. `Send` bounds present. The
      three regression tests each pin a real nesting case that used to panic.
- **`partition.rs` / `hash.rs`** — the distributed-shuffle hasher is **stable**:
      SHA-256 with a fixed `krishiv.partition-key.v1` domain over fixed-endian
      bytes, not `DefaultHasher`/`RandomState`/address-based. The three string
      encodings (Utf8/LargeUtf8/Utf8View) share one tag so a value routes
      identically regardless of Arrow encoding.
- **`memory_budget.rs`**, **`page_cache.rs`** (FIFO trim + saturating add/sub,
      ceiling sourced from `ExecutorCapacity`), **`backpressure.rs`** (registers
      the `Notify` listener before the second load — no missed wakeup; no lock
      across `.await`), **`executor_capacity.rs`** (every fraction truncates with
      `as u64` — rounds *down*, the safe direction; the three fractions sum to
      the accounted-ceiling and `budgets_always_fit_inside_the_container` sweeps
      512 MiB–64 GiB) — all clean on the wrong-accounting axis.
- **`durability.rs` / `production.rs` / `validate.rs` / `streaming_dials.rs`** —
      every gate checks its override *first* then the gated predicate (not the
      "returns Err on line 1" anti-pattern); `parse_idle_tick_ms` deliberately
      allows explicit `0`; profile predicates use the right ordering.
- **`write_commit.rs`** — durability-clean: `publish_staged_outputs` /
      `cleanup_staged_outputs` are synchronous (no dropped-future path), staged
      files survive a mid-way error and re-publish converges via the
      `final_path.exists()` skip (idempotent), `move_file` falls back to
      copy+delete on `CrossesDevices`. No "dry-run reports success" path.
- **`sql_util.rs`** (no case-folding, so no fold-then-index-original panic;
      quote-doubling correct), **`auth_util.rs`** (parse/redact only — no
      constant-time compare lives here), **`compute_pool.rs`**, **`chaos.rs`**,
      **`panic_util.rs`**, **`stream_quality.rs`**, **`test_fixtures.rs`** — clean.
- No dead `pub fn`: every public symbol has an external caller across `*.rs`
      and `*.rs.inc`.

### Open

- [ ] Coverage not yet measured with `cargo llvm-cov`; `env_registry.rs` (1873)
      is the largest remaining surface to profile (`write_commit.rs` now has its
      durability path under test).

---

## 7. krishiv-connectors — 97 of 97 files read whole (COMPLETE, 2026-08-15)

Read via 11 parallel reader agents over every `src/**/*.rs` + `tests/` file
(no `.rs.inc` sections in this crate; zero orphan files; zero `unsafe`),
each finding verified against the code and its workspace callers before any
edit. ~60 defects found; **all fixed in this session** (user directive: no
deferrals), including completing missing features rather than rejecting.
Pre-fix coverage baseline (default features): 66.09% lines.

### Fixed — silent wrong answers / data loss (highest severity)

- [x] **avro.rs was a corruption machine.** Unsupported Arrow types were
      *written* as their type-name debug string for every row (`"Date32"` in
      every cell) while the schema mapped them to `string`, so writes
      succeeded; decode-side type mismatches silently became NULL; `long`
      values were truncated `as i32`; multi-variant unions decoded to Rust
      debug strings (`"Int(5)"`); UInt64 > i64::MAX wrapped negative; the
      doc-promised `bytes`/`fixed` → Binary read path could not build its
      arrays. Rewrote both directions: mismatches and unsupported types are
      hard errors (only Avro's own `int→long→float→double` promotions
      accepted), and completed Binary, Date32, Time32/64,
      Timestamp(ms/us incl. local), Struct (recursive), and List (recursive)
      support in schema + values, both read and write. 7 new revert-proven
      tests incl. binary/timestamp/struct+list round-trips.
- [x] **kafka.rs committed offsets for undelivered rows** — offsets were
      recorded per message *before* decode, so a decode/build error advanced
      the checkpoint past rows the pipeline never received. Offsets now stage
      in a local map and merge only after the batch is successfully built
      (`commit_staged_on_success`; revert-proven both ways). The
      krishiv-sql `kafka_table.rs` manual-commit path had the same shape at
      the next layer (broker commit before `flush_pending`) — reordered.
- [x] **dml.rs `iceberg_delete_where` deleted NULL-predicate rows** —
      survivors were `WHERE NOT (pred)`, so rows where the predicate is NULL
      (e.g. `name = 'x'` with `name` NULL) were silently deleted. Now
      `(pred) IS DISTINCT FROM TRUE`, with the deleted count counting only
      pred-TRUE rows (revert-proven).
- [x] **MERGE fixes (dml.rs):** `rows_affected` reported the whole post-merge
      table size; `COALESCE(s.col, t.col)` made it impossible to set a column
      to NULL from source (now match-marker CASE on the join key); duplicate
      source keys silently fanned out target rows (now rejected, matching the
      in-memory twin); `merge_delta` (delta_lake.rs) with
      `when_matched_update=false` *deleted* every matched target row on an
      insert-only merge (matched rows now kept).
- [x] **maintenance.rs deleted live data.** `remove_orphan_files` ignored
      equality/position-delete files (`t.deletes`), so a MoR table's live
      delete files were "orphans" — deleted rows resurrected.
      `expire_snapshots` deleted data files but never expired the snapshots
      from metadata (time-travel to them silently broke; re-runs
      double-counted) — now uses `Transaction::expire_snapshots` and is
      idempotent. Compaction commit-failure no longer leaves an empty table
      (restore fallback extended past drop+recreate).
- [x] **streaming_sink.rs `pre_commit` lost the epoch's rows** when the DUR-2
      sidecar write failed after `mem::take` — taken batches/offsets are put
      back and the staged parquet removed, so a retry re-stages them
      (revert-proven).
- [x] **iceberg_native.rs `overwrite_commit` with empty batches wiped the
      committed-offset oracle** the DUR-2 idempotency gate depends on
      (delete-everything epochs re-opened the duplicate/resurrection window).
      Empty-batch commits now durably record the cumulative kafka offsets and
      return a real commit identity.
- [x] **two_phase.rs `commit_through`/`abort_after` dropped every later
      epoch's prepared handles on a mid-iteration failure** (durably prepared
      data orphaned as `.tmp` forever under a committed checkpoint) — all
      unprocessed epochs are re-queued before returning Err (revert-proven).
      Partial `prepare` failure in `pre_commit` now aborts the handles staged
      so far instead of leaking them.
- [x] **Vector store:** LanceDB fragment ids were epoch-only — a second batch
      at the same epoch overwrote the first's Parquet on disk (data gone
      after restart; now content-derived ids, revert-proven);
      `delete_by_ids` could never remove a fragment (point-level ids vs
      batch-level manifest keys — deletes resurrected on reopen; now a
      point→fragment index with fragment rewrite); zero-row fragments
      crashed reload; all four remote sinks (pinecone/weaviate/pgvector/
      qdrant) silently ignored `PayloadFilter` — filter pushdown implemented
      per backend; Pinecone query URL broke on scheme-prefixed hosts and
      returned internal hex ids with empty text (metadata now stored and
      read back); Weaviate used non-UUID ids (422 on real servers) and an
      invented GraphQL shape — both fixed to the real API contract;
      the krishiv-runtime bridge minted a fresh epoch per upsert call,
      breaking ADR-R17.3 idempotent upsert (now stable ids, revert-proven);
      dimension mismatches zip-truncated into plausible wrong scores (now
      errors).
- [x] **jdbc.rs keyset pagination looped forever returning duplicates** when
      the key column wasn't BIGINT (`try_get::<i64>` error swallowed — now
      surfaced); checkpoint-before-first-read invented `last_key=-1`
      (now `Option<i64>`); composite upsert dedup keys were forgeable via
      embedded `\u{1f}` (now escaped).
- [x] **elasticsearch_sink `_id` fell back to the batch-local row index**
      (cross-batch overwrites: N batches ended as only the last batch) — a
      configured id column that is missing/null/mistyped is now an error,
      Int64 ids supported.
- [x] **pulsar at-least-once never completed:** `ack_all_pending` had zero
      callers (unbounded pending growth incl. full payloads; total redelivery
      on restart) — wired into the python streaming loop, payloads no longer
      retained; messages consumed in a failed batch are no longer queued for
      ack; `next_batch` no longer blocks until `max_messages` (poll timeout)
      and stream-idle is no longer treated as end-of-topic.
- [x] **local_delta.rs commits were not put-if-absent** — concurrent writers
      clobbered each other's data file and `_delta_log/N.json` silently (now
      `create_new` version claim with bounded retry, both write_table and the
      2PC sink); numeric file stats compared as strings (`min "10" > max
      "9"` → wrong file skipping); vacuum deleted files still referenced by
      prior versions despite its Safety doc; 2PC commits lacked
      `commitInfo.timestamp` so AS OF skipped them; `table_schema`
      materialized the whole table and ignored the pinned version.
- [x] **delta.rs RocksDbDeltaStore keys were little-endian** — scan order was
      wrong after 255 appends (deletes replayed before their inserts) — now
      big-endian with legacy-format detection; namespace prefix scan leaked
      sibling namespaces (`orders2` under `orders`); Rdkafka store's
      `len()=0`/no-op `truncate()` lies are now explicit Unsupported errors.
- [x] **hudi.rs:** 2PC sink staged only in-process and `commit` of an unknown
      handle returned `Ok` (at-most-once as 2PC) — now durable disk staging
      with restart recovery and unknown-handle errors; CoW read-modify-write
      lost concurrent upserts (now conflict detection at commit);
      `snapshot_rows` reported batch rows not table rows; `delete_by_key`
      silently no-opped on raw (untyped) key values.
- [x] **CDC:** `CdcOffsetTracker` had zero callers — the "state feature
      persists offsets" promise was false; completed the feature
      (`resume_from` seam + startup filtering by the sink's committed
      offsets, closing the crash-window duplicate the false "upsert
      semantics" comment papered over). Tombstone offsets now merge into the
      snapshot summary (the C6 claim is true now). Registry-decoded CDC with
      envelope semantics fails closed (append-only opt-in) instead of
      resurrecting deletes.
- [x] **Registry/drivers:** IcebergSource advertised rewindable but rewind
      returned zero rows (reset now real); IcebergSink dropped its false
      `idempotent` claim; descriptor-vs-instance capability mismatches
      aligned (cassandra/hbase/es/csv); malformed driver options now error
      instead of silently defaulting (batch sizes, `recursive`,
      `start_position`, bool flags, multi-byte delimiter);
      `jdbc_sink_delivery` wired to a reachable surface; duplicate driver
      registration warn-logs; `storage_factory` `adls://` reached the
      local-path branch (bucket now parsed); cassandra column identifiers
      quoted; hbase `zookeeper_quorum` renamed to `thrift_address` with a
      guarded alias; `sql.rs` `parse_jdbc` no longer treats a port as a
      table name.
- [x] **Parquet:** exhausted `ParquetSource` re-opened and re-decoded the
      whole file on every poll (now an `exhausted` flag, cleared by
      reset/restore — revert-proven by deleting the file after exhaustion);
      `pushdown_filters` doc claimed a `true` default the derive didn't give.
- [x] **schema_normalize.rs fast-path skipped configured renames** when the
      source schema coincidentally equaled the target.
- [x] **kinesis:** checkpoint restore only worked through one of the two read
      APIs and a checkpoint-after-restore-before-read regressed to empty;
      production `.expect()` in a CI-unlinted feature (the lint gap itself is
      recorded below).

### Fixed — honesty, dead code, tests that could not fail

- [x] `lakehouse/streaming_unify.rs` DELETED — documented exactly-once/2PC,
      caps, and "None = latest", none implemented; zero callers.
- [x] `check_write_precondition` (TOCTOU twin of `check_and_append`, zero
      callers) deleted incl. both re-exports.
- [x] Iceberg branch/tag references were write-only — added
      `scan_reference()` so they are readable, with clear errors when
      compaction expired the pinned snapshot; in-memory time-travel to a
      compacted-away snapshot now errors instead of silently returning zero
      rows.
- [x] Kafka transactional sink: unreachable epoch-monotonicity check made
      real (`last_finalized_epoch`); commit/abort now verify the handle
      (stale-handle commit of a *different* open transaction was accepted);
      fencing simulator let fenced zombies keep writing (certification could
      not catch a coordinator ignoring fences) — fenced sinks now error.
- [x] `tests/exactly_once.rs` encoded an at-most-once protocol and hardcoded
      the recovery position its own assertions were supposed to derive —
      rewritten to couple offset commits to sink commits, resume from
      recovered offsets, abort the crashed stage, and prove 9000-rows-exact.
- [x] Certification headers/tests claiming "Kafka/S3 exactly-once" over
      in-memory/local-FS sinks renamed to what they exercise; LanceDb added
      to vector certification alongside memory.
- [x] ~15 cannot-fail tests fixed or deleted across kafka.rs, kinesis.rs,
      s3.rs, pulsar, jdbc, transactional.rs, src/tests.rs, cdc/mod.rs,
      integration_connector_lakehouse.rs (DLQ secondary-sink forwarding now
      actually observed), hudi/delta_lake derive-only tests deleted,
      hudi vacuum orphan-removal path now genuinely exercised.
- [x] Kafka restore: all-or-nothing assignment validation before any seek
      (no partial-restore state); the false "assign() bypasses rebalance"
      doc replaced with the honest per-assignment-epoch guarantee.
- [x] Feature-gated lint debt exposed and cleared: CI clippy runs without
      `--all-features`, so avro/schema-registry/vector/kinesis/etc. carried
      ~53 violations incl. a production `.expect()` and ~40
      indexing/slicing sites — all fixed properly (no blanket allows);
      `cargo clippy --all-features --all-targets` is now clean and is the
      new bar for this crate.

### Verification

- 683 lib + 18 integration/doc tests pass (`--all-features`); krishiv-sql
  775 lib tests pass; krishiv-runtime bridge test passes; krishiv-python and
  krishiv compile clean; workspace clippy (CI config) clean; fmt clean on
  every touched crate.
- Revert-proofs: agents proved most per-finding; I independently re-proved
  the five headline fixes red→green (NULL-predicate delete, commit_through
  later-epoch requeue, pre_commit buffer restore, LanceDB fragment
  uniqueness, kafka staged-offset guard).

### Open

- [ ] CI still runs clippy/tests without `--all-features` for this crate —
      the lint gap that hid the feature-gated debt. Add an all-features lane
      (cross-cutting; affects other crates too).
- [ ] Kafka group-rebalance re-seek (ConsumerContext hooks) — the honest doc
      now states the per-assignment-epoch guarantee; full rebalance-aware
      restore is a design item.
- [ ] Coverage re-measure with the ~120 new tests (baseline 66.09% lines,
      default features).

---

## 8–27. Not yet started

Each crate gets the same treatment and its own section here: measured
coverage, a table of uncovered-region concentration, a fixed list with commit
hashes, and an open list. Sections are appended as the audit reaches them.

---

## 8. krishiv-state — 37 of 37 files read whole (COMPLETE, 2026-08-15)

Coverage before: 83.0% regions / 78.4% lines (`cargo llvm-cov`). Weakest files:
backend.rs 27% (default trait methods), storage_uri 32%, ephemeral 59%, io 63%.
All 12 findings below were fixed in this session; every behavioral fix was
proven red by reverting the production line (noted per item). Commit: see
`fix(krishiv-state)` audit commit on this date.

**Defects fixed (revert-proven unless noted):**

1. **F1 — `restore_savepoint` skipped the metadata⊆manifest coverage check**
   (`checkpoint/io.rs`). `validate_epoch` rejects a metadata-declared snapshot
   missing from `manifest.sha256`; restore accepted the same inconsistency and
   copied the never-hash-verified file into the live checkpoints dir,
   re-hashing it as valid. Restore now reads the savepoint manifest once,
   validates its entries, and requires `manifest.contains(rel)` per declared
   snapshot. Test: `restore_savepoint_rejects_metadata_snapshot_not_in_manifest`
   (red on revert). `validate_manifest_at_prefix` became test-only.
2. **F2 — `task_index_for_key_group` disagreed with
   `key_group_ranges_for_parallelism`** whenever `NUM_KEY_GROUPS % p != 0`
   (first divergence p=5 at kg=19661): the O(1) floor formula vs the
   remainder-first ranges. Zero production callers today — a landmine, not a
   live bug — but any future caller would route keys to a task that does not
   own them. Reimplemented as the exact inverse of the range assignment; the
   old test used only p=4 (32768%4==0 — cannot fail) and now sweeps
   p ∈ {1..7,11,16,100} over every key group (red on revert).
3. **F3 — portable-snapshot decode capped at 1M entries while encode had no
   cap** (`snapshot.rs`): a backend with >1M keys checkpointed fine and failed
   only at restore. Replaced the arbitrary cap with the structural bound
   `count ≤ (len−12)/32` (each entry needs ≥32 bytes), keeping the forged-count
   DoS guard. Tests: `snapshot_decode_accepts_more_than_a_million_entries`
   (red on revert) + `snapshot_decode_rejects_forged_entry_count`.
4. **F4 — DFS backend filenames used 64-bit `DefaultHasher` and `get()`
   ignored the record's embedded key** (`dfs_backend.rs`): (a) `DefaultHasher`
   is documented as unstable across Rust releases — a toolchain upgrade would
   orphan every durable DFS record; (b) a hash collision made `get(a)` silently
   return `b`'s value, and `put` overwrote the shared file. Filenames now use
   `sha256_hex(key)`; `get()` errors `CorruptEntry` when the embedded key
   mismatches. Tests: `dfs_filenames_use_stable_sha256_of_key`,
   `dfs_get_rejects_record_with_mismatched_embedded_key` (both red on revert).
5. **F5 — DFS cache-size accounting inflated on overwrite**: `write_to_cache`
   charged `+len` without releasing the old entry's bytes, so hot keys inflated
   the counter until LRU eviction thrashed the cache empty. Old size is now
   released first. Test: `cache_size_stays_flat_when_overwriting_same_key`
   (pre-fix accounted 1000 bytes for one 100-byte value; red on revert).
6. **F6 — DFS `load_snapshot` allocated `vec![0; len]` from untrusted u64
   lengths** before any bounds check: a corrupt snapshot aborted the process
   (capacity overflow) instead of erroring. Extracted `read_len_prefixed` that
   validates the claimed length against the remaining buffer first. Test:
   `load_snapshot_rejects_oversized_length_prefix` (panics on revert).
7. **F7 — `AsyncOperatorExecutor::drive_futures` was sequential** while
   documented "concurrently via Tokio… bounded concurrency" — the batching was
   a no-op, and `executor_drives_futures_concurrently` could not tell. Batches
   now run under `join_all`. Test:
   `drive_futures_overlaps_accesses_within_a_batch` measures peak overlap with
   a 50ms-sleeping backend (peak=1 pre-fix; red on revert).
8. **F9 — incremental SST dedup keyed on filename alone**
   (`incremental_checkpoint.rs`, unwired Phase-56 surface): a same-named SST
   with different content (file-number reuse after restore into a fresh DB)
   was never re-uploaded, so restore would fetch the stale blob and fail its
   hash check. Dedup map is now `filename → sha256`. Test:
   `upload_reuploads_same_filename_with_different_content` (red on revert).
9. **F11 — TTL `list_keys` reported a corrupt (<8-byte) entry as live while
   `get` errors `CorruptEntry` on the same entry.** Aligned to error. Test:
   `ttl_list_keys_errors_on_corrupt_entry_like_get_does` (red on revert).

**Efficiency/honesty fixes with no behavioral delta (not revert-provable):**

10. **F8** — `BatchedStateAccess::get_batch` did per-key `get`s, bypassing
    `StateBackend::get_batch` overrides (RocksDB single read txn). Now
    delegates; results identical, covered by `batched_state_access_works`.
11. **F10** — `incremental_gc_retains_newest_epochs` ended in
    `let _ = manifest; let _ = up;` ("verify gc didn't panic") — a cannot-fail
    test. Now asserts retained epoch 4 dedups and GC'd epoch 1 re-uploads.
12. **F12** — `list_valid_epochs` warned "skipping non-numeric checkpoint
    epoch directory" for `latest_epoch.json` (its own hint file) on every
    listing. The hint filename is now skipped silently.

**Recorded, not fixed (design notes, no defect):**
- `LocalFsCheckpointStorage::full_path("")` resolves to the base dir, so
  `delete_prefix("")` would delete every job; all callers pass epoch/savepoint
  dirs. Temp-file (`*.tmp.N`) leak possible if the process dies between create
  and rename — invisible to restore (manifest-driven), disk-only.
- DFS `load_snapshot` uses `from_utf8_lossy` for op/state names (decode path
  elsewhere errors on non-UTF-8); v2 format is DFS-local, never redistributed.
- TTL semantics on restore: snapshot strips expiry prefixes and load re-arms
  `now + ttl` — documented (P0.16), noted as a restore-extends-TTL behavior.
- `SavepointCoordinator::delete_savepoint` durable delete is best-effort by
  design (in-memory removal wins); `list_savepoints` (io.rs) silently skips
  non-numeric names where the checkpoint listing warns.
- checkpoint/tests.rs, proptest_checkpoint_kill.rs are genuinely strong
  (commit-or-abort kill model, byte-flip fencing, committed v1 savepoint
  fixture as the format-compatibility exit gate).

Verification: krishiv-state 356 lib + 8 integration tests green (8 new
regression tests added), `just lint` clean, `cargo clippy -p krishiv-state
--all-targets --all-features` 0 warnings, `cargo fmt` clean, full `just test`
workspace gate green.

## 1b. krishiv-sql — closing pass: the last 25 files (COMPLETE, 2026-08-15)

Read whole: `distributed_plan.rs` (8,467), `spillable_join.rs` (3,533),
`grace_hash_join.rs`, `late_materialize.rs`, `grammar.rs`, `sql_tests.rs`,
`statement_completion.rs`, `runtime_filter_exec.rs`, `python_udf.rs`,
`ann_rewrite.rs`, `window_functions.rs`, `unspillable_headroom.rs`,
`catalog/iceberg_rest.rs`, `catalog/iceberg_catalog_bridge.rs`, the vector
stack (`vector_search`, `vector_index`, `vector_quantize`, `vector_functions`,
`vector_footer`, `vector_metric`), and `tests/{comprehensive, memory_spill,
sql_compat, broadcast_row_ceiling_is_width_blind, stage_reuse_duplicate_scan}`.
That closes krishiv-sql at 61/61 files.

**Overall verdict:** the "big four" are the best-audited code in the workspace
— every guard in `distributed_plan`/`spillable_join`/`grace_hash_join` carries
a measured SF100 incident, a named regression, and a revert-proven test. The
closing pass found two defects, both fixed:

**S1 — a crafted Parquet footer could abort the process**
(`vector_index.rs::from_bytes`). The decoder called
`Vec::with_capacity(nlist * dim)` and `with_capacity(len)` with counts read
from untrusted footer bytes; a foreign file claiming `nlist = dim = u32::MAX`
spun an unbounded push loop / capacity-overflow abort instead of the
documented "degrades to None". Fixed with a structural byte-length bound
(`checked_mul`, remaining-bytes check) before any allocation — the same class
as krishiv-state's DFS `load_snapshot` fix. Regression tests in
`foreign_or_truncated_footer_is_none_not_panic` (pre-fix: 60s timeout kill;
post-fix: 0ms pass).

**S2 — `tests/stage_reuse_duplicate_scan.rs` failed every plain
`cargo test --tests` run.** Its precondition
`assert!(stage_reuse_enabled())` requires `KRISHIV_STAGE_REUSE=1`, which the
default environment does not set — so the standard `just test-integration`
gate could never be green with this test compiled in. Now
`#[ignore = "diagnostic report requiring KRISHIV_STAGE_REUSE=1"]`, matching
the crate's convention for env-gated tests. (Revert-proof is the captured
failing run itself.)

**Recorded, not fixed (notes, no defect):**
- `dfplan_body_is_split_safe` decodes on a bare `SessionContext::new()`, not
  `fragment_decode_session_context()`: a body referencing an engine UDF fails
  decode and is conservatively declared split-unsafe. Safe direction — a
  missed AQE skew-split, never a wrong answer.
- `python_udf::global_pool` never respawns a dead worker: after a `python3`
  crash every Python UDF in the process fails until restart. Fail-loud, not
  silent; a respawn path is a feature, recorded for the wire-up backlog.
- `ann_search`/`build_vector_index` interpolate caller-supplied identifiers
  into SQL text; callers already hold `engine.sql`, so no privilege boundary
  is crossed (grants re-apply on the generated query).
- The tests in this half are uniformly strong: staged TPC-H fixtures cover
  all four broadcast x spill-conversion cells, the shuffle serve-permit
  deadlock is pinned with a forced-ordering reader, memory_spill carries a
  GreedyMemoryPool negative control, and broadcast_row_ceiling pins
  DataFusion's mechanism before the fix that depends on it.

Verification: krishiv-sql 775 lib tests green, all integration suites green
(the S2 test now ignored-by-default), clippy --all-targets 0 warnings, fmt
clean.

## Cross-cutting findings

Things that are not one crate's problem.

- [x] **Live connector test matrix (2026-08-15/16, follow-on to crate 7 +
      python integration audit).** Every locally-installable connector backend
      exercised through the Python API with real data (NYC yellow-taxi Jan-2024
      parquet, 2.96M rows; taxi-zone lookup CSV, 265 rows; 2,000 real JSON
      trip events), across batch, delta batch, and streaming: 25/25 scenarios
      green — parquet/CSV/registry ConnectorSource+Sink, Delta
      (append/time-travel/merge-honesty), Hudi (append/upsert/accounting),
      Iceberg-fs (write + the fixed real read), Kafka via Redpanda (continuous
      source → memory sink, 2,000 events; batch sink), MinIO S3 (the
      `#[ignore]` round-trip, un-ignored), live Postgres (jdbc sink/source,
      upsert redelivery, keyset guard; postgres-catalog 6/6 incl. concurrent
      commit), Qdrant + pgvector (filter pushdown live), LocalStack Kinesis
      (snapshot + continuous — first runtime proof of the previously
      never-compiled arm), Pulsar (earliest-position snapshot), OpenSearch,
      Scylla, Weaviate. Live-only defects found and fixed:
      - **`collect()` on SQL over any unbounded source blocked forever across
        the whole api/python surface**: the `KrishivDataFrameOps` impl of
        `krishiv_logical_plan` hardcoded `ExecutionKind::Batch`, so the
        existing collect-guard never fired despite correct engine
        classification. One-line kind propagation +
        `tests/streaming_collect_guard.rs` (hangs pre-fix, passes in 10ms
        post-fix).
      - **pgvector**: `CREATE EXTENSION; CREATE TABLE` in one prepared
        statement (Postgres rejects multi-command prepares — connect always
        failed against real PG); query read `doc_id` from payload JSON but
        upsert never stored it (every hit came back with an empty doc_id) and
        the result payload was dropped.
      - **Weaviate**: upsert used `PUT /v1/objects` (405 on real servers —
        now class-scoped PUT with POST-create fallback); queries requested
        `chunk_index`, a property upsert never writes, so real servers
        errored — and the GraphQL in-band `errors` field was swallowed into
        an empty result set (now surfaced).
      - **Pulsar**: no initial-position support existed — brand-new
        subscriptions started at Latest, so pre-existing messages were
        invisible; added `with_start_at_earliest` + python
        `start_position="earliest"` default.
      - **Cassandra**: no consistency knob (driver LOCAL_QUORUM default is
        unusable on single-node clusters) — added
        `with_consistency{,_name}` + python `consistency=` kwarg.
      - **Python surface**: `read_iceberg` registered its table under a name
        embedding the whole catalog path (unqueryable) — now
        `iceberg_{ns}_{table}`; `ConnectorSink`/`ConnectorSource` were missing
        from the package `__init__` re-exports; no `jdbc` pip extra existed.
      Rig: docker containers `audit-{redpanda,minio,pg,qdrant,localstack,
      opensearch,scylla,weaviate,pulsar}` on ports clear of production;
      harness scripts in the session scratchpad (rig-specific, not
      committed).

- [x] **Python↔connector integration audit (2026-08-15, follow-on to crate 7).**
      Traced every source/sink connector's reachability from the Python API
      across batch, SQL, delta-batch, and streaming modes. Findings, all fixed:
      - **The `kinesis` and `pulsar` feature arms of krishiv-python did not
        compile at all** (removed pyo3 `allow_threads` API, `_py`/`py`
        mismatch, and `register_unbounded` no longer returning the push
        handle) — the "continuous streaming" registration paths for both
        connectors had never been built. Fixed via a new
        `Session::register_unbounded_source` (krishiv-api) that returns the
        `ContinuousTableInput` handle, `send()` instead of the removed
        `push()`, and the pyo3 `detach` API.
      - **krishiv-api's own `kafka` feature did not compile** (KafkaConfig
        grew `decode_columns`; `streaming_builder.rs` and python `sinks.rs`
        initializers were never updated).
      - **`read_iceberg` returned empty data by construction** — it scanned a
        freshly created in-memory table and registered zero rows for every
        existing table. Now scans real filesystem catalogs via
        `IcebergFsTable` (the same layout `krishiv.sinks.iceberg` writes),
        keeps the empty in-memory table only for an empty `catalog_uri`, and
        rejects other URIs loudly.
      - **No registry-generic source existed** — `ConnectorSink` reached every
        registered sink driver but batch *reads* had no equivalent. Added
        `ConnectorSource` (registry `open_source` → `read_batches()`, with a
        mandatory `max_batches` for unbounded sources).
      - Cleared the python crate's accumulated lint debt (8 denials incl.
        prod `println!`/`expect`/indexing + 23 warnings) now that the crate
        is lintable at all.
      - **Guard:** `just lint` now lints `krishiv-python --all-features` and
        `krishiv-connectors --all-features --all-targets`, closing the
        CI blindspot that let both never-compiled feature arms ship.
      Mode map after the fixes: batch (per-kind sinks + ConnectorSink/
      ConnectorSource + parquet/kinesis/pulsar snapshot reads), SQL
      (session.sql over registered connector tables; engine DDL registry
      falsification test already guards driver presence), delta batch
      (read/write_delta, read/write_hudi — real, merge mode errors honestly),
      streaming (register_kafka/kinesis/pulsar_source continuous paths +
      DataStreamWriter kafka/parquet/iceberg via streaming_builder, pulsar
      acks wired).

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

## §4b — `block_in_place` on a current-thread runtime (defects 35–38, `25a52b8c`)

Found by sweeping a *class* rather than a file. `tokio::task::block_in_place`
panics outright when the current runtime is `current_thread`. Four sites
hand-rolled the sync/async bridge with a bare call, so each was a latent abort
rather than a style question:

- **`IcebergCatalogBridge`** — every `CatalogProvider` / `SchemaProvider` method
  (`schema_names`, `schema`, `table_names`, `table_exist`). Reproduced with a
  failing test at `iceberg_catalog_bridge.rs:107` *before* fixing.
- **`RocksdbBackend::snapshot_async` / `load_snapshot_async`** — on the durable
  checkpoint write path reached from `krishiv-scheduler`. Both carried the doc
  "Requires a multi-threaded Tokio runtime" — a documented precondition with no
  enforcement.
- **The DUR-1 sink publish path** (`in_process.rs`) — a panic mid-publish would
  take the commit/fail resolution with it.
- **`SqlBodyTableUdf::call`** — guarded the flavor but *rejected* the call, so
  SQL UDTFs worked on one of the three runtime contexts. They now work on all
  three.

**The tests encoded the workaround instead of catching the bug.** Both existing
iceberg-bridge tests pinned `flavor = "multi_thread"`; the RocksDB ones did too;
and `sql_body_udtf_without_runtime_returns_typed_error` asserted the
*limitation* by name. Replaced with tests that exercise the real entry points
under all three runtime contexts.

**Why nothing caught it.** `clippy.toml` opens with "sync-over-async bridging is
policed by lint, not convention" — but the rule only named
`async_util::block_on`. A raw `block_in_place` was policed by nothing, so the
policy had a hole exactly the size of these four bugs. Added
`tokio::task::block_in_place` to `disallowed-methods` and promoted the guarded
helper (previously private to `krishiv-engines`) to
`async_util::run_blocking`. Three pre-existing bridges were already correct and
keep their deliberate semantics behind justified allows: `storage_trait.rs`
returns a diagnosable error on current-thread, and `host.rs` hops to its own
thread so a Flight SQL handler cannot stall a single reactor.

**Lesson.** A lint that names one helper does not police a pattern. Every
correct implementation in the tree — `async_util::block_on`,
`snapshot_nonblocking`, `run_blocking_on_tokio`, `host::run_blocking` — was
evidence the authors knew the hazard; the four broken ones were written by
people who did not, and no gate told them.

Also fixed, surfaced only once the build got past the first error (both
pre-existing, from other sessions): an `.err().expect()` in `krishiv-runtime`
(`93272cd3`), two clippy errors in the just-landed `vector_search.rs`
(`a3c4cd8e` — **main was red when this branch rebased onto it**), and nine
`KRISHIV_*` flags read in source but never declared in `env_registry::FLAGS`,
now declared with their real defaults.

## §5 — catalog/ (defects 39–41, `759f80b1` `2d5f7310` `b6677e2a`)

Read whole: `object_store_io.rs`, `rest_catalog_wrapper.rs`, `unity_catalog.rs`,
`glue_catalog.rs`, `unified.rs`, `iceberg_table_provider.rs`,
`local_catalog.rs`, and the first third of `postgres_catalog.rs`. Not yet read:
the rest of `postgres_catalog.rs`, `iceberg_rest.rs` (1,537), `mod.rs` (2,398).

**39. The unity-catalog and glue-catalog test suites had never compiled.**
Both test modules call `std::env::set_var`/`remove_var` inside `unsafe`, and
this crate is `#![forbid(unsafe_code)]` — which an inner `allow` cannot
override. On the pristine tree `cargo check --all-targets --features
unity-catalog,glue-catalog` exits 101. Five tests that read as coverage of two
advertised catalog backends could not be built. They were racy besides: two
tests in each file fought over one process-global variable, and the Glue pair
set `AWS_REGION`, which unrelated S3 code reads. Fixed by extracting the
resolution rules into pure `UnityEnvConfig::resolve` / `GlueEnvConfig::resolve`
taking a lookup closure — no env, no `unsafe`, no race. 5 uncompilable tests
became 10 passing ones, covering two rules nothing asserted before (the
`AWS_DEFAULT_REGION` fallback and its precedence).

**40. `lint-features` checked half of every optional feature.** The recipe runs
`cargo hack check --each-feature --no-dev-deps`; `--no-dev-deps` skips test
targets, so `#[cfg(test)]` code behind a feature was policed by nothing. This is
the same shape as §4b: the recipe's comment says "every optional feature must
compile on its own" and it had a structural hole. Added an `--all-targets` pass;
green across all 13 features after 39, failing on glue-catalog before it.

**41. Postgres `create_namespace` reported properties it never stored.**
`ON CONFLICT DO NOTHING` (correctly — callers depend on idempotency) followed by
returning the *caller's* properties. On an existing namespace the stored
properties were untouched while the caller was handed its own values back as
though applied. Now uses `RETURNING properties`, reading the row back when the
insert did not happen.

**Open — Iceberg time travel is implemented and unreachable.**
`iceberg_table_provider_at_snapshot` is a complete snapshot-pinned provider with
**zero callers**. Meanwhile `lakehouse/providers.rs::apply_as_of_refs` errors on
any table not named `delta.<path>`, telling the user "AS OF is currently
resolved only for Delta tables". So `VERSION AS OF <n>` on an Iceberg table is
refused by a code path that sits next to its own implementation. Wiring it needs
a catalog handle threaded into `apply_as_of_refs`, which today takes only
`&SessionContext` — a feature change, not an audit fix, so it is recorded rather
than done. The error is at least honest (it refuses instead of silently reading
the current snapshot), which is why this is a gap and not a defect.

**Open — doc drift in `iceberg_table_provider.rs`.** The module doc says the
workspace uses "DataFusion 53.x" and that `iceberg-datafusion 0.9.1` targets
52.x. The workspace is on DF 54.

## §5b — postgres_catalog.rs + catalog/mod.rs (defects 42–45, `5f72fa45`)

**42–45. The Postgres backend reported success for four operations it never
performed.** A DELETE or UPDATE matching no rows is a successful *statement*,
not a successful operation, and this backend conflated the two:

- `drop_table` on a table that does not exist → `Ok(())`.
- `rename_table` from a source that does not exist → `Ok(())`.
- `drop_namespace` deleted the namespace row without checking for tables.
  There is no foreign key from `krishiv_tables`, so every table in it was
  orphaned: absent from `list_namespaces`, still served by `list_tables`, still
  loadable by `load_table`.
- `list_namespaces` took a `parent` and discarded it (`_parent`), so listing the
  children of `a` returned the entire catalog.

All four now error or filter correctly. The `parent` query uses `starts_with`
rather than LIKE deliberately: namespace names may contain `_`, which LIKE reads
as a wildcard, so a LIKE pattern would report prefix-sharing siblings as
children. The `None` case still returns every namespace flattened — a
*deliberate* divergence, because this catalog is surfaced through DataFusion,
whose schema space is flat, so a nested `a.b` must appear as its own schema to
be queryable at all.

**Verified against a real Postgres 16 in a throwaway container, not by
inspection.** The two pre-existing `#[ignore]`d integration tests still pass,
including the CAS concurrent-commit test that asserts no lost update. Three new
tests cover the four behaviours; spliced onto the *unfixed* code they fail 3/3,
so they discriminate rather than merely pass.

The CAS itself (`UPDATE … WHERE metadata_location = $expected`) is correct and
genuinely well tested — that test asserts the surviving property (no lost
update), not the mechanism.

**Open — `DataFusionCatalogBridge::invalidate` is unreachable, and its own doc
argues for it with an impossible scenario.** Its only caller is its own test.
The doc says "without this invalidation hook a second
`register_table_with_batches` for the same name would not be visible to the
DataFusion query plan" — but `register_table` returns `TableAlreadyExists`, so a
second call for the same name cannot succeed, and `table_data` is therefore
never mutated for an already-registered name. The cache it guards cannot go
stale through the public API. Disposition is wire-or-delete: either support
re-registration (making the hook real) or remove it. Recorded rather than done —
supporting re-registration is a feature decision.

Not yet read in this crate: `catalog/iceberg_rest.rs` (1,537 lines), the back
half of `catalog/mod.rs`, plus `distributed_plan.rs` (8,464) and
`spillable_join.rs` (3,307).

## §6 — closing every open item from this session (`8a358a49` … `fccd5994`)

**46. SQL time travel was unreachable — for Delta as well as Iceberg.**
This corrects §5 above, which recorded the gap as Iceberg-only.
`preprocess_as_of_sql` strips the clause before DataFusion sees the query;
`apply_as_of_refs` registered the pinned provider under
`table.replace('.', "_")`, a name the rewritten SQL never mentions. No spelling
of a time-travel query reached a pinned snapshot.

**No test had ever run an `AS OF` query** — `as_of.rs` tests the preprocessor,
`providers.rs` tests only the error paths. Both halves green, feature dead: the
same shape as the MERGE gap in §2 and the `block_in_place` tests in §4b. Fixed
by renaming the reference to a generated `__krishiv_as_of_<n>` alias in the AST
and registering under it. `tests/as_of_end_to_end.rs` is the missing test:
3 rows at v0, 6 at v1, `VERSION AS OF 0` must return 3.

Note the test that had to change: `parses_version_as_of` asserted
`sql.contains("FROM orders")` — that the name survives the rewrite. It was
asserting the bug.

**47. `spark_sql_ext` wired — and wiring exposed a live defect.**
Resolves the wire-or-delete decision. `contains_transform` matched `TRANSFORM(`
anywhere, so `transform(array, x -> x * 2)` — the higher-order function this
crate supports — was rejected as "Spark TRANSFORM has no SQL equivalent". Three
tests failed the moment the module was wired. Spark's TRANSFORM *clause* is
always `SELECT TRANSFORM(cols) USING '<script>'`, so the guard now requires the
`USING`. **The defect was invisible for as long as the module was dead** —
which is the argument for wiring over deleting.

**48. `cep_sql`: nested PATTERN groups truncated, keywords matched inside
identifiers.** `extract_parenthesized_after` took the *first* `)`, so
`PATTERN ((A B) C)` yielded `(A B)` — a different, still-valid pattern, so the
query ran and matched the wrong sequence. Both it and `parse_within_ms` found
their keyword by substring, so a partition column `my_pattern` shadowed
`PATTERN` and a pattern variable `WITHINRANGE` looked like a `WITHIN` clause.
Added balanced-paren scanning and `find_keyword` (word boundaries). Two tests;
against the old logic they fail 2/2.

**49–50.** `iceberg_table_provider.rs` claimed DataFusion 53.x (it is 54.x);
`DataFusionCatalogBridge::invalidate` documented itself as guarding a scenario
`register_table`'s `TableAlreadyExists` makes impossible. Both corrected.

Still open and now the only items left from this crate: `iceberg_rest.rs`
(1,537 lines), the back half of `catalog/mod.rs`, `distributed_plan.rs` (8,464)
and `spillable_join.rs` (3,307) are unread; and the live-Kafka exercise of the
boundedness fix is still owed.


## 9. krishiv-api — read end to end (2026-08-15)

Method: full-file read of all 41 src files (session.rs 3,951; tests.rs 2,839;
streaming_builder.rs 2,349; dataframe.rs 2,024; connector_runtime.rs 1,935;
streaming_dataframe.rs 1,812; pipeline/* 2,559; io.rs 960; the 25 smaller
files), tests/streaming_collect_guard.rs, and the three examples. Clean files
(no findings): session.rs, blocking.rs, error.rs, lib.rs, types.rs, query.rs,
stream.rs, window.rs, catalog.rs, prepared.rs, expression.rs,
materialized_table.rs, sql_job.rs, connector_runtime.rs,
streaming_dataframe.rs, io.rs (behavioural), process.rs, timers.rs, compute/*,
incremental_flow.rs, conformance/delivery-cert/mode-conformance/differential
corpus test files, pipeline/{source,sink,spine,connector_factory}.rs.

Defects (all fixed this session, one commit):

**A1. `StreamingQueryManager` was permanently empty (B: dead wiring behind a
live public surface).** `register` carried `#[expect(dead_code)]`; nothing ever
called it, so `active_count`/`active_ids`/`get`/`get_by_name` always returned
empty/None — and the Python test papered over it (`got is None or …`, a
cannot-fail test). Fix: `StreamingQuery` is now a cheap clone over
`Arc<StreamingQueryInner>` (the task aborts when the *last* clone drops);
`DataStreamWriter::start` registers into the attached manager, whose entries
hold `Weak<StreamingQueryInner>`. API change: `get`/`get_by_name` return
`Option<StreamingQuery>` (was `Option<Arc<StreamingQuery>>`); krishiv-python
adjusted. Test `manager_sees_registered_query_while_handle_is_alive` —
revert-proven red by removing the `register` call.

**A2. Streaming checkpoints were fabricated (A/E: progress lied).**
`run_streaming_task` opened an *ephemeral* `LocalFsCheckpointStorage`,
ignoring the user's `checkpointLocation`, and never wrote a byte — while every
progress snapshot still reported `last_checkpoint_epoch = Some(n)`. Fix:
storage is rooted at the configured location and `commit_checkpoint_epoch`
writes `epoch-<n>/commit.json` + `latest_epoch.json` before the epoch is
reported; a write failure fails the query. Test
`checkpoint_location_receives_epoch_commits` — revert-proven red by restoring
the report-without-write line.

**A3. Update-mode "dedup" tracker enforced nothing (A+C).** The memory and
console sinks kept a per-row map whose key embedded the row *index* and the
whole column's `{:?}` rendering — no two epochs could ever collide, so Update
mode behaved exactly like Append while the code (and comments) claimed
writer-layer dedup enforcement; `output_mode_update_emits_rows` could not
fail. Removed the tracker; the sinks now honestly append each epoch's
update rows (Spark memory-sink Update semantics — the upstream stateful
operator owns which rows are "updates") and the comments say so.

**A4. `Once` trigger reported "AvailableNow" in progress snapshots (E).**
`drain_and_call` hardcoded the label. Now receives the real trigger label.
Test `once_trigger_progress_label_is_once` — revert-proven.

**A5. `RunPolicy::EveryMs` stepped on every feed in the IVM driver (A: knob
did the wrong thing).** `maybe_step` matched `EveryMs(_) => true`, silently
degrading the time-coalescing contract ("at most every ms") to OnChange. Fix:
`StepPacer` tracks `last_step`; steps only when rows are pending and the
interval elapsed. Test `every_ms_policy_coalesces_feeds_between_steps` (huge
interval ⇒ exactly one flush step, proven via the persisted job's tick
counter) — revert-proven.

**A6. Pipeline streaming checkpoints went nowhere (E).**
`save_streaming_checkpoint` captured source offsets and *logged* them;
`restore_streaming_checkpoint` was `#[cfg(test)]`-only — checkpointing was
decorative. Fix: new `StreamingConfig.checkpoint_dir`; when set, offsets
persist as `<dir>/<cp-id>/<source>.offset` + `latest.txt`; when unset the
debug log now says offsets are not persisted. Test
`streaming_checkpoint_dir_receives_persisted_checkpoints` — revert-proven.

**A7. Dead config knobs removed (G).** `StreamingConfig.execution_profile` and
`.output_buffer` (api-local duplicates of the krishiv-dataflow types) were
never read by the driver — config enforced by nothing. Deleted both fields and
the duplicated `StreamingExecutionProfile`/`OutputBufferPolicy` types (no
consumers anywhere in the workspace); the dataflow crate's enforced types are
unaffected.

**A8. Parity matrix contradicted itself (E).** `Column::eqNullSafe` appeared
twice — `Planned("not exposed")` *and* `Supported(Expr::eq_null_safe)` (which
exists) — double-counting the surface; `DataFrame::foreachBatch` was `Planned`
although `DataStreamWriter::foreach_batch` ships. Removed the stale entry,
re-classified foreachBatch as Partial, re-blessed
`docs/reference/pyspark-parity.md` (now 122/127 = 96%). New matrix test
`no_duplicate_contradictory_entries` — revert-proven by re-adding the dup.

**A9. Empty-result writes silently produced no file (A).**
`write_parquet`/`write_csv`/`write_json`/`*_with_options` returned `Ok(())`
without creating anything when the result had zero batches — and the embedded
staged-sink fast path publishes no part files for a zero-row result either, so
`df.write_parquet(p)` could "succeed" with nothing at `p`. Fix: csv/json
always create the file; parquet writes a schema-only file (schema from the
plan when no batch carries one); the sink fast path falls through to the
local writer when it published nothing. Tests
`write_csv_of_empty_result_still_creates_the_file` and
`write_parquet_of_empty_result_writes_schema_only_file` — both revert-proven.

Doc-only: `StreamingQuery::memory_batches` said "Drain" while it clones and
leaves the batches in place; `as_sql_backed`'s empty guard was
`all(rows==0) && is_empty()` (the `all` clause vacuous) — both corrected.

Notes / not defects: `interval_join` H-1 per-key fix verified with its four
regression tests; connector_runtime's registry fallthrough (#197) is fully
tested; `prepared.rs`'s `unwrap_or_default()` on a missing bind parameter is
unreachable (bind() length-checks against the validated max placeholder);
`repartition()` only annotates the logical plan (documented Partial in the
parity matrix — left as is). Coverage re-measured post-fix (see summary in the
commit).

Gates: `cargo test -p krishiv-api` (293 lib + 1 integration, 2 env-ignored),
`just test`, `just lint`, `cargo clippy -p krishiv-api --all-targets`,
`cargo fmt` — all green. krishiv-python compiles (`cargo check`); its `.so`
link needs libpython3.14, absent on this host (pre-existing, unrelated).


## 10. krishiv-runtime — read end to end (2026-08-15)

Method: full-file read of all 16 src files (in_process.rs 2,043;
execution_runtime.rs 1,677; flight_client.rs 1,654; coordinator_http_client.rs
1,494; flight_protocol.rs 1,321; continuous_stream.rs 1,034; flight_action.rs
919; in_process_cluster.rs 859; lib.rs 833; ivm_job.rs, local_streaming.rs,
stream_kafka.rs, plan.rs, streaming_job.rs, vector_sink_bridge.rs) plus
tests/{integration_distributed,spooled_results}.rs. Coverage pre-fix: 72.5%
regions / 69.7% lines (`cargo llvm-cov -p krishiv-runtime`).

The crate is in unusually good shape — the in-process driver loop
(in_process.rs run_terminal_task) and the do_action size-cap/fallback ladder
(flight_client.rs) carry a comment-per-guard record of the live incidents that
shaped them (all-slots-busy bench holes, #217 cancelled-query resurrection,
OutOfRange decode classification), each with a regression test that names the
incident. Clean files: lib.rs, execution_runtime.rs, in_process.rs,
in_process_cluster.rs, continuous_stream.rs, flight_action.rs,
flight_protocol.rs, plan.rs, local_streaming.rs, stream_kafka.rs, ivm_job.rs,
streaming_job.rs, vector_sink_bridge.rs, both test files.

Defects (fixed this session, one commit):

**R1. `FlightClientPool::with_alternate` could diverge the endpoint and
health lists (B: unreachable failover target).** The endpoint was pushed
unconditionally while the matching `EndpointHealth` entry was added only when
`Arc::get_mut` succeeded — on an already-cloned pool the health entry was
silently dropped, leaving an alternate endpoint no health walk (failover /
select_healthy_endpoint iterate the health list) could ever select. No current
caller clones before configuring, so this was latent, but the builder is pub.
Fix: the endpoint is added only together with its health entry; a shared pool
logs a warning and skips both, keeping the lists in lockstep. New
`endpoint_count()` accessor + test
`with_alternate_keeps_endpoints_and_health_in_lockstep` — revert-proven red.

**R2. IVM coordinator routes interpolated caller-supplied names into URL
paths without percent-encoding (G: inconsistent hardening).** The job/
continuous routes used `urlencoding::encode`; the ten IVM routes (views, feed,
step, checkpoint, checkpoint-delta, snapshot incl. view name, restore,
restore-delta, stream-bridge, stream-delta) and the batch-sql poll URL
interpolated raw `{job_id}`/`{source_name}`/`{view_name}` — a `/` or space in
a job name silently re-shaped the route (wrong endpoint → opaque 404), and a
`?` truncated it into a query string. Fix: shared `seg()` helper
percent-encodes every caller-supplied path segment. Test
`path_segments_are_percent_encoded` — revert-proven red (helper reverted to
identity).

**R3. Poll-timeout message hardcoded "300s"** instead of interpolating
`BOUNDED_WINDOW_POLL_TIMEOUT_SECS` — correct today, silently wrong the day the
constant changes. Message now uses the constant (no test: tautological while
the value is 300).

Notes / not defects: `streaming_job.rs` is 0%-covered — a 64-line delegation
shell over the coordinator HTTP functions, exercised by the daemon-gated
integration and Python live tests; `coordinator_http_client.rs` sits at 10%
for the same reason (network calls; its pure logic — payload decode, URL
normalization, jitter — is unit-tested). `select_healthy_endpoint` returning
the current endpoint when none are healthy is deliberate (fail with the real
connect error, not a synthetic one). The five `#[ignore]`d tests are
env/daemon-gated with accurate reasons (TPC-H data dir, local cluster,
sandbox TCP listeners).

Gates: `cargo test -p krishiv-runtime` (350 lib + 13 integration, 5 ignored),
`cargo clippy -p krishiv-runtime --all-targets`, `just lint`, `just test`,
`cargo fmt` — all green.

## 11. krishiv-dataflow — read end to end (2026-08-16)

All 35 production files + lib_tests.rs + watermark_e2e.rs +
tests/streaming_window_float64.rs read end to end. The crate is broadly
battle-hardened (Wave/GAP/ST/STREAM/H-series incident comments with proptests
throughout: tumbling double-emission guard, GAP-14 idle-source policy,
STREAM-3 bounded join buffers, H-2 typed join-key tags, LRU key caps on every
keyed executor — except one, see D4). Coverage pre-fix: 82.9% regions /
81.6% lines (`/tmp/claude-1000/dataflow_cov.txt`).

**D1 (A) temporal_join.rs — TimestampSecond/Utf8 join-key collision.**
`format_column_value` tagged `Timestamp(Second)` values with `"S"`, the same
tag as Utf8 — `TimestampSecond(123)` and the string `"123"` produced identical
join keys, so cross-type rows falsely matched: exactly the H-2 class the
function's own doc table claims to fix (the table did not list
TimestampSecond at all). Fix: distinct `t<s>` tag + doc-table entry. Test
`timestamp_second_key_does_not_collide_with_utf8_key`; revert-proven (tag
back to `"S"` → red).

**D2 (A) window/count.rs — non-deterministic flush order.**
`CountWindowOperator::flush()` iterated `self.key_states` (HashMap), so
end-of-stream partial windows were emitted in random per-process order —
unlike the tumbling/sliding/session flushes, which sort for deterministic
replay. Fix: collect + sort by key before emitting. Test
`flush_output_order_is_deterministic_sorted_by_key` (8 keys × 5 fresh
operators); revert-proven (unsorted iteration → red).

**D3 (E) queue.rs — Unaligned-mode `recv()` doc contradicted behavior.**
The doc claimed post-barrier records are "held in the in-flight buffer (not
delivered to the operator)… drained back when the next barrier arrives". The
implementation actually tees each post-barrier record into the buffer and
delivers it on the same loop iteration (step 0 pops what step 5 buffers) —
which is the correct unaligned-checkpoint contract (no withheld delivery;
`drain_unaligned_buffer` captures the set for the snapshot). Doc rewritten to
match the code. Wiring note: `CheckpointAlignment::Unaligned` still has no
production constructor — the executor builds aligned `operator_queue` only
(engine-core "once wired"); recorded as an open wiring gap, not guessed at.

**D4 (F) connected_streams.rs — unbounded per-key state in CoProcessExecutor.**
Every sibling keyed executor (process_fn, broadcast_state, group_state, cep)
caps per-key state at an LRU bound; `CoProcessExecutor` (reachable from
Python via `krishiv_api::CoProcessExecutor` / streaming_dataframe.rs:240) grew
one entry per distinct key forever. Fix: `max_keys` (default 100k) +
IndexMap access-order LRU, `with_max_keys` builder, access order carried in
the snapshot (`#[serde(default)]` keeps old snapshots loadable). Test
`co_process_state_is_capped_by_max_keys`; revert-proven (drop `maybe_evict()`
call → red).

**D5 (A) dedup_operator.rs — ambiguous multi-column key encoding.**
`row_key` concatenated `"{col}="` + tag + raw Utf8 bytes with no length
prefixes, so multi-column keys `("Xb=s:Y", "Z")` and `("X", "Yb=s:Z")`
encoded to identical bytes and the second distinct row was silently dropped
as a duplicate (production path: `StreamingDataFrame::drop_duplicates`).
Also `sep.encode_utf16()` truncated to `u8` was lossy for non-ASCII column
names. Fix: u32-LE length prefix on column name and Utf8 value (Int64 stays
fixed 8-byte). Note: this changes the persisted dedup key format — old-format
seen-keys no longer match, so a restored pre-fix dedup state re-admits each
old key once; accepted (old format was wrong). Test
`multi_column_utf8_keys_do_not_collide_across_boundaries`; revert-proven
(prefix-free encoding → red).

**D6 (B) envelope.rs, profile.rs, buffer.rs — dead modules, deleted.**
Zero callers anywhere in the workspace and 0% coverage all three. The real
`StreamingExecutionProfile` / `OutputBufferPolicy` live in krishiv-proto
(api's pipeline/mod.rs says so explicitly), and the api-side duplicates were
already deleted in §9 (A4) — these were the orphaned dataflow-side
counterparts (`StreamEnvelope` included, never constructed anywhere).
`AutoProfileManager::new(_config)` even ignored its own argument. Deleted
all three modules + their `pub mod` lines; `CheckpointAlignment` (the one
type envelope.rs re-exported) lives in queue.rs and is unaffected.

Clean files (no defects): adaptive.rs (SpaceSaving + RateLimiter + advisor,
P1.28/M8 guards all real), aggregate.rs, barrier_align.rs, broadcast_state.rs,
cep.rs (budget-reservation edge cases carefully handled), continuous.rs,
delta_join.rs (STREAM-3/4), group_state.rs, interval_join.rs (G5 snapshots),
join.rs, live_table.rs, memo.rs (single-mutex LRU, TOCTOU-free),
operator_config.rs, operator_runtime.rs, process_fn.rs, queue.rs (impl; doc
was D3), schema_normalize.rs (lossy-widen rejection incl. Int64→Float64),
side_output.rs, state_descriptor.rs, state_persistence.rs, state_tumbling.rs,
temporal_join.rs (impl beyond D1), tumbling/sliding/session/count (beyond D2),
watermark_join.rs, watermark_util.rs, watermark_e2e.rs, lib_tests.rs,
streaming_window_float64.rs. Minor note (not a defect): watermark_join's
`snapshot_roundtrips_spec_and_watermark` tail deliberately doesn't assert the
post-restore match count — the head of the test asserts real snapshot values.

Gates: `cargo test -p krishiv-dataflow` (298 lib + 1 integration green — note:
restoring reverted files with `mv` preserves the old mtime and cargo skips the
rebuild; `touch` after restore before trusting a green run), clippy
`--all-targets` clean, `just lint`, `just test`, `cargo fmt` — all green.

## 12. krishiv-ivm — read end to end (2026-08-16)

All 8 src files (error, spill, provenance, vector_sink, plan, partitioned,
flow at 3.6k LOC, lib) + tests/property_tests.rs + tests/proptest_ivm.rs read
end to end. The crate is heavily self-audited (AUD-1..9, G2/G5/G6/G14, #160,
IVM-6/7 comments all verified against the code — each one checks out,
including the bounded_capacity length-prefix-attack clamp and the G6/F4
5-cycle recreate convergence test). Coverage pre-fix: 74.0% regions / 73.6%
lines (`/tmp/claude-1000/ivm_cov.txt`); vector_sink.rs is the outlier at 11%.

**I1 (F) provenance.rs — `forget`/`forget_many` leaked `input_epochs`.**
Both removed the `input_to_outputs` entry but left the epoch-metadata entry
behind, so epoch-tracked hashes forgotten individually accumulated forever.
Fix: remove from both maps. Test `forget_also_drops_epoch_metadata`;
revert-proven (drop the epoch removal → red).

**I2 (B) plan.rs — `min_by`/`max_by` mapped to plain Min/Max.**
`expr_to_aggregation` matched `"min" | "min_by"` (and max) and aggregated
arg0 — which for MIN_BY(a, b) is semantically wrong (it returns a at the
minimum of b, not min(a)). Currently *unreachable*: this DataFusion build has
no `min_by` function ("Invalid function 'min_by'"), so `ctx.sql` fails in
`build_view_plan` and such views already degrade to DiffBased. Removed the
arms anyway — the moment a DataFusion upgrade adds min_by, they would have
become a live A-class silent wrong answer on the O(Δ) path. Test
`min_by_max_by_degrade_to_diff_based` pins the DiffBased contract; it cannot
go red against today's pre-fix build (SQL fails either way — verified with a
plan-shape probe) but trips if a future build makes the bad arms reachable.

**I3 (A) vector_sink.rs — null Int64 id silently became "0".**
`extract_string_at`'s Int64 fallback skipped the null check the Utf8/LargeUtf8
paths have; a null id row upserted/deleted vector-store point "0". Fix: error
on null like the string paths. Test
`null_int64_id_errors_instead_of_becoming_zero`; revert-proven (drop the
null check → red).

**Not fixed — needs a decision (D/A): vector-sink delta coalescing.**
`spawn_vector_view` subscribes via `IncrementalView::subscribe()` — a tokio
`watch` channel, which retains only the *latest* value. If two ticks emit
output deltas between task wakeups, the earlier delta is dropped; unlike a
snapshot, a missed delta is a permanently missed upsert/retraction in the
vector index. Fixing properly means an mpsc/broadcast (or ack'd pull)
publish surface on `krishiv_delta::IncrementalView` — a cross-crate design
change to the view-output contract, recorded here rather than guessed at.
Mitigation today: single-consumer flows step and drain synchronously, so the
race window is a busy sink awaiting `upsert_batch` across ≥2 ticks.

Clean files: error.rs, spill.rs (FairSpillPool sizing + honest tiny-pool
test), partitioned.rs (drain-correct snapshot differentiation at the
partitioned level, shard-count-validated checkpoints, Utf8View regression
pin), flow.rs (dirty-bit topo scheduling, cached tick context reconciliation,
resident/authoritative tick mirroring, fence protocol docs), both property
test files (incremental == diff-based == recompute == Rust model, randomized
with group-emptying retractions).

Gates: `cargo test -p krishiv-ivm` (62 lib + 6 + 1 integration green),
clippy `--all-targets` clean, `just lint`, `just test`, `cargo fmt` — green.

## 13. krishiv-delta — read end to end (2026-08-16)

All 20 files read (delta_batch, trace, view, lateness, coalesce,
behavior_version, error, lib, gap_tests, the 9 operator modules, and
tests/proptest_zset.rs). The Z-set core is proptest-verified against a plain
Rust model (consolidation = model addition, commutativity, negation cancels,
idempotence, serialization exact round-trip, trace snapshot = model positive
part). Coverage measured pre-fix (`/tmp/claude-1000/delta_cov.txt`).

The crate-wide defect family: **string-keyed equality with a colliding
`"NULL"` sentinel and incomplete type coverage** — the exact class D5/H-2
already produced in dataflow. Five heads of it, one shared fix:
`key_util::scalar_to_group_key` (null → `"n"`, value → `'v' + value`; a Utf8
`"NULL"` can never equal a SQL null again).

**K1 (A) consolidate.rs — SQL null vs the string "NULL" consolidated
together.** `consolidate_batch` (used by `apply_delta`, trace merges, IVM
coalescing — the hottest correctness path in the crate) keyed rows via
`scalar_to_string`, so an insert of Utf8 `"NULL"` cancelled a retraction of an
actual null. Test `null_and_null_string_do_not_consolidate_together`;
revert-proven (helper back to bare `scalar_to_string` → red, together with
K3/K4's tests).

**K2 (A) trace.rs — probe keys collapsed unsupported types into one bucket.**
The private stringifier lacked Utf8View / LargeUtf8 / temporal / binary
coverage: every value of such a type rendered `<unsupported:…>`, so probes
matched *all* rows of that type ("region" Utf8View keys — the exact encoding
modern DataFusion emits, see the 2026-07-10 prod incident pinned in
partitioned.rs — falsely joined everything); plus the `"NULL"` collision.
Now delegates to `scalar_to_group_key` (full key_util type coverage). Tests
`trace_probe_utf8view_keys_match_exactly` and
`trace_probe_null_does_not_match_null_string`; both revert-proven.

**K3 (A) distinct.rs — same "NULL" collision in the multiplicity map**, and
its persisted state format now carries a `DST2` magic: pre-v2 blobs (whose
keys the new encoding can never match) fail restore loudly and the caller
reseeds, instead of silently keeping phantom counts. Tests
`null_and_null_string_are_distinct_rows`,
`restore_rejects_pre_v2_state_blob`; revert-proven.

**K4 (A) join.rs — `scalar_eq` matched nothing for most key types.** The
same-tick ΔA⋈ΔB cross term and both probe-output builders compared keys with
a function that handled only Int64/Int32/Utf8 and returned `false` for
everything else — a join keyed on Utf8View/Int16/Float/timestamp silently
emitted no rows on those paths. Fix: fall back to `scalar_to_key` equality
(injective over all key_util-supported types; nested types stay
non-matching). Test `join_on_utf8view_keys_matches`; revert-proven
(fallback → `false` → red).

**K5 (F) view.rs — registry `drop_view` leaked the receiver entry** (one
watch receiver retained forever per dropped view). Test
`drop_view_releases_receiver_entry`; revert-proven.

**Cross-crate: krishiv-ivm `hash_row`** (content-addressed dedup +
provenance) shared the "NULL"-sentinel hashing and now uses
`scalar_to_group_key`, so a legitimate Utf8 `"NULL"` row is no longer dropped
as a re-delivery of a null row. (In-memory hashes only — no persisted-state
compat impact.)

Clean files: delta_batch.rs (magic-prefixed versioned IPC with legacy
fallback), stream.rs (RowConverter-keyed differentiate — already null-safe;
#160 multiset apply_delta), aggregate.rs (exceptional: AUD-3/AUD-7 typed
readers, arrow row-format group keys — no string keys at all, AGGS2 portable
state), filter.rs, map.rs, recursive.rs, lateness.rs, coalesce.rs,
behavior_version.rs, gap_tests.rs, proptest_zset.rs. Minor notes (not
defects): MIN/MAX orders i64 values as f64 keys (exact only to 2^53);
distinct's `build_output` "key not found → sentinel row 0" branch is
unreachable (keys always come from the same batch) — its comment overstates.

Gates: `cargo test -p krishiv-delta` (121 lib + 8 proptest green),
`cargo test -p krishiv-ivm` green on the re-keyed hash_row, `just lint`,
`just test`, `cargo fmt` — green.

## 14. krishiv-flight-sql — read end to end (2026-08-16)

All 6 files read (service.rs 2.5k, host.rs 1.3k, lib.rs tests 1.1k,
actions.rs, session_limits.rs, bin/krishiv_flight_server.rs). The front door
is deeply incident-hardened: SEC-2 default-deny folded into a single
`authenticate_request` enforcement point, #211 spool streaming (schema from
the IPC header — no eager decode), the Phase 55/58 drain-ack put-back (both
the oversized-do_action and mid-stream-death cases pinned by live-regression
tests), G1/G12/G16/G17 JDBC-driver gaps all pinned, honest no-op transaction
semantics with statement-count warnings, error taxonomy with opaque internal
statuses. Coverage: 71.1% regions / 72.1% lines
(`/tmp/claude-1000/flightsql_cov.txt`).

**F1 (A) actions.rs — `$N` inside quotes was a parameter.**
`count_sql_params` and `substitute_sql_params` scanned raw bytes with no
quote awareness (the `?`→`$N` normalizer directly above them is
quote-aware): `SELECT '$1' AS tag, $1 AS v` counted one param but substituted
BOTH sites, silently rewriting the literal with the bound value. Fix: a
shared `QuoteState` tracker; `$` inside `'…'`/`"…"` is text. Test
`dollar_placeholders_inside_quotes_are_not_parameters`; revert-proven
(`in_quotes() → false` → red).

**F2 (A) actions.rs — unsupported bound-param types bound as SQL NULL.**
`col_literal` rendered List/LargeList/FixedSizeList/Struct/Map/anything-else
as the literal `NULL` — a client binding an array param got NULL silently
substituted into its query. Fix: `substitute_sql_params` now returns
`Result` and rejects with `invalid_argument` naming the parameter and type.
Test `unsupported_param_type_errors_instead_of_null`; revert-proven
(sentinel arm → `"NULL"` → red).

Notes (not fixed, recorded): timestamp params render as `TIMESTAMP '<raw
ticks>'`, which fails loudly at plan time rather than silently (E-minor —
correct formatting is future work); the typed `BatchSql` do_action path does
not take the per-session statement guard (only `do_get_statement` does) —
the global semaphore still applies; `do_put_prepared_statement_update`
ignores per-row bound params beyond returning -1 (documented honest
unknown).

Clean files: service.rs, host.rs (incl. the flavor-checked `run_blocking`
and the DrainDeliveryGuard drop-path), session_limits.rs (RAII statement
guard, sweep-on-access idle eviction, MAX_TRACKED_SESSIONS cap), bin
(pre-tracing eprintln documented), lib.rs test suite (auth matrix, SEC-2
asymmetry regression, declared-default guard).

Gates: `cargo test -p krishiv-flight-sql` (94 green), clippy, `just lint`,
`just test`, `cargo fmt` — green.

## 15. krishiv-proto — read end to end (2026-08-16)

All 12 src files + build.rs read (ids, lifecycle, checkpoint, io, job,
executor, management, services, task at 2.2k, wire at 2.1k, tests at 1.4k,
lib). **Zero fixes needed** — the cleanest crate so far. The contract layer
is exactly what the A–G checklist wants to see everywhere else: typed
validated IDs (empty/zero rejected at construction, saturation warnings on
`next()`), explicit presence flags for every optional scalar (`has_*`) so
zero and absent stay distinguishable across the wire (the P0.17 lesson,
pinned), `live_jobs_authoritative` separating "no jobs" from "coordinator
didn't report" (shuffle-GC safety, pinned with the three-state test),
decoder-boundary invariants (zero-partition shuffle write cannot decode —
the boundary makes the silent-green-task shape impossible), loud parse
errors for malformed sink contracts, and proptest never-panics fuzz across
all 17 `from_wire` decoders.

One recorded note (no fix): `CheckpointAckRequest.unaligned_buffers` has no
protobuf field — `to_wire` drops it and `from_wire` hardcodes `Vec::new()`.
Consistent with the crate-11 §11 finding that Unaligned checkpointing has no
production constructor; when it is wired, the proto field must be added or
unaligned acks will silently lose their in-flight buffer refs across the
wire. Also noted: `StreamingTaskStateWire.watermark_ms` is uint64 on the
wire with a documented bit-pattern cast for negative sentinels (deliberate,
commented on both sides).

Gates: `cargo test -p krishiv-proto` green via the workspace suite; no code
changes, so no new gates needed beyond the register entry.

## 16. krishiv-metrics — read end to end (2026-08-16)

All 6 files read (counters.rs 1.8k, lib.rs tests, observability_report.rs
schema, grpc.rs, init.rs, system.rs). Strong overall: per-metric bucket sets
with rationale (µs gRPC, µs stream-record, wide query-latency), label-value
escaping with injection tests, error-ref opaque internal statuses with a
leak test, exactly-one-HELP/TYPE discipline pinned. Coverage post-fix:
61.6% regions / 62.2% lines (`/tmp/claude-1000/metrics_cov.txt`).

**M1 (A) system.rs — CPU gauges always read ~0.**
`refresh()` built a fresh `sysinfo::System` on every call, but sysinfo
computes CPU usage as a delta between two refreshes of the SAME instance —
so `krishiv_process_cpu_usage` and `krishiv_system_cpu_usage` were
deltas-from-nothing, reporting ~0 forever on every dashboard. Fix: a
persistent `Mutex<System>` sampler inside `SystemMetrics` (plus an explicit
`refresh_processes` for the pid so process CPU/memory stay current). Test
`cpu_gauges_read_nonzero_after_spaced_refreshes` (300ms busy-spin between
refreshes); revert-proven (fresh `System::new_all()` per call → red).

**M2 (A) counters.rs — `remove_job` wiped global metric families.**
It called `.clear()` on `output_buffer_flushes` (labeled by reason),
`sink_prepare/commit/abort_duration` (labeled by sink_id), and
`object_store_requests` (labeled by operation) — none of which are
job-scoped — so completing ANY job reset those global monotonic counters
and histograms, breaking Prometheus `rate()` for every consumer. Fix:
remove the clears (all five are small bounded families). Test
`remove_job_preserves_non_job_scoped_families`; revert-proven (clears
restored → red).

Clean files: grpc.rs (trace propagation + error-ref taxonomy + duration
layer), init.rs (OTLP/stdout/in-memory exporter wiring, honest re-init
semantics), observability_report.rs (serialization-only schema), lib.rs
(thorough counter/render/escaping/thread-safety tests).

Gates: `cargo test -p krishiv-metrics` (83 green), clippy, `just lint`,
`just test`, `cargo fmt` — green.

## 17. krishiv-engine-core — read end to end (2026-08-16)

All 11 files read (mem.rs 819, consolidate.rs, runtime.rs, job.rs, upsert.rs,
durable.rs, kind.rs, changelog.rs, engine.rs, error.rs, lib.rs). **Zero
functional defects** — the three-engine spine is fresh, small, and already
carries its own audit fixes with honest documentation: BATCH-1 (transient
error classification no longer retries parse errors), B-5 (per-source
in-flight persistence in CheckpointPayload with serde-default compat), B-6
(binary keyed-state snapshot format), P-4 (cached RowConverter sort fields),
crash-durable checkpoint publish (fsync-temp → rename → fsync-dir),
value-based deterministic FNV shuffle partitioning, and the bounded
unmatched-retraction consolidator with eviction + warn. Coverage: 86.1%
regions / 81.9% lines (`/tmp/claude-1000/enginecore_cov.txt`); engine.rs and
error.rs 0% are trait/type-only files exercised by the engine adapter crates.

One hardening change (H): added the missing `#![forbid(unsafe_code)]` to
lib.rs — this was one of the four crates without it (the plan's hotspot
index); no unsafe existed, so this closes the door rather than fixing a
live issue. No revert-proof needed (compile-time attribute, not behavior).

Cross-references: `CheckpointPayload.in_flight` is the landing pad for the
unaligned-checkpoint wiring gap recorded in §11 (dataflow D3) and §15
(proto note) — the "once wired through the operator runtime" comment here
is the third leg of that same open item.

Gates: `cargo test -p krishiv-engine-core` (36 green), clippy, `just lint`,
`just test`, `cargo fmt` — green.

## 18. krishiv-python — read end to end (2026-08-16)

All 35 Rust files read (13,101 LOC): session.rs 1847, udf.rs, dataframe.rs,
vector_sinks.rs, lakehouse.rs, streaming.rs, process_api.rs,
streaming_dataframe.rs, sources.rs, expression.rs, sinks.rs, incremental.rs,
pipeline_api.rs + 22 smaller. CI-excluded crate — clippy run directly
(10 warnings found, all fixed; now 0). No `tests/` dir, no `.rs.inc`.

**Host-test unblock**: `cargo test -p krishiv-python --lib` previously failed
to link (`mold: library not found: python3.14`). Fix: the shared object lives
off the default search path — `RUSTFLAGS="-L /usr/lib/python3.14/config-3.14-\
x86_64-linux-gnu" cargo test -p krishiv-python --lib` links and runs all
tests (40 green). This crate's Rust tests are runnable on this host after all.

Defects fixed (commit pending, this section's commit):

- **PY1 (A, security surface)** session.rs `Session.with_policy(policy=
  "role_based")` silently mapped to `AllowAllPolicyHook` — a request for
  access control answered with none. Now extracted to `resolve_policy_hook`
  and `role_based` is rejected loudly. Test
  `with_policy_role_based_is_rejected_not_silently_allow_all`;
  revert-proven (arm restored to the Ok side → red).
- **PY2 (A)** arrow_fast.rs `record_batches_to_py_table` empty-input path
  built the PyArrow schema object, discarded it (`_empty_schema`), and
  returned `pa.table([])` — a zero-column table, losing the caller's schema.
  Now `Table.from_batches([], schema=...)`. Test
  `test_record_batches_to_py_table_empty_preserves_schema`;
  revert-proven (old body restored → red).
- **PY3 (G)** udf.rs: `KRISHIV_PYTHON_UDF_TIMEOUT_MS` is a registry-declared
  flag and is named in the timeout error message, but nothing read it —
  `call_python_udf` hardcoded 30 s. Now wired via `python_udf_timeout_ms()`
  (LazyLock over `env_registry::env_u64`). Per the workspace's established
  pattern (late_materialize.rs), the env read itself cannot be flipped from a
  test (`set_var` unsafe under edition 2024); the pure resolution
  `timeout_from` is pinned by `timeout_from_prefers_env_value_over_default`,
  and `slow_udf_times_out_at_configured_ms` pins the timeout mechanism.
- **PY4 (E)** arrow_fast.rs `record_batch_from_py_fast` doc claimed it skips
  the `Table.from_batches` route; the body is identical to the slow path.
  Doc corrected (kept as a seam for a future C-data-interface path).
- **PY5 (B)** relation.rs dead `_cached: Mutex<Option<PyQueryResult>>` field
  (never read) removed; migration.rs dead `let session = ...; let _ =
  session;` construct removed; vector_sinks.rs `PyInMemoryVectorSink`
  deduplicated onto the shared `parse_payloads`/`parse_filter`/`chunks_to_py`
  helpers all other sinks use.
- **PY6 (lint, CI-invisible)** 10 direct-clippy warnings fixed: unused `py`
  under `#[cfg(not(feature))]` (sources.rs ×2 — added to the discard tuple),
  dead-in-default-build `consistency` field (sinks.rs — now also shown in
  `__repr__`, making it read in every build), unneeded `return`s in five
  feature-gate error branches (session.rs ×3, sources.rs ×2).
- **PY7 (E)** lakehouse.rs `write_delta` accepts `"merge"` but the docstring
  advertised only append/overwrite — doc corrected.

Notes (not defects): sinks/sources honestly document at-least-once semantics
and feature gates; ConnectorSink/-Source flush failures fail the call;
memo.rs documents its unused `_schema_json`; udf.rs cogroup/map_pandas_iter
bridges are `#[allow(dead_code)]` with honest "not yet wired" docs (staged
feature, recorded as a product decision, not deleted); Timestamp columns in
the dict-based UDF path only support nanosecond arrays — non-ns units fail
loudly with InvalidArgument (not silent). The `python/` package (~8.6k LOC
.py + pytest suite + prebuilt .so) is out of this register's Rust scope; the
prebuilt `.so` predates this session's fixes, so pytest was not used as a
gate for them.

Gates: direct clippy 0 warnings, `cargo test -p krishiv-python --lib`
(40 green, via the RUSTFLAGS -L workaround), `just lint`, `just test`,
`cargo fmt` — green.

## 19. krishiv-operator — read end to end (2026-08-16)

All 19 files read (4,878 LOC incl. crd/): main.rs, controller.rs, lease.rs,
pod_manager.rs, cluster_manager.rs, reconciler.rs, webhook.rs, crd/job.rs,
tests.rs 882 + 10 smaller. Feature-gated k8s modules audited with
`--features k8s` (default `just test` compiles them via the workspace).

Defects fixed:

- **OP1 (A)** cluster_manager.rs `release_workers` called `next_pod_name()` —
  minting a fresh, never-created pod name (and burning the index) — then sent
  Delete for it. Scale-down deleted nothing (the actor's 404 arm decremented
  the worker counter anyway), so real pool pods leaked while
  `current_workers()` drifted down. Fixed with an `enqueued` LIFO of actually
  created pod names; release pops real names (channel-full pushes back). Test
  `release_workers_deletes_a_created_pod_name`; revert-proven (old fresh-name
  body restored → red).
- **OP2 (D)** lease.rs `k8s_release` patched `holderIdentity: null`
  unconditionally — no holder check, no resourceVersion. A pod releasing
  after its lease expired clobbered the NEW leader's lease; that leader's
  next renew saw a holder mismatch and self-demoted, leaving the cluster
  leaderless until the next acquire tick. Now GETs the lease, skips the patch
  when the holder is no longer us (`release_patch_allowed`, unit-tested), and
  carries resourceVersion when present. The k8s I/O leg needs a live API
  server; the decision rule is pinned by
  `release_patch_only_allowed_for_current_holder` (same pattern as the S7
  resourceVersion checks and PY3).

Notes (not defects): controller.rs `event?` exits the watch loop on a watcher
stream error — acceptable because Kubernetes restarts the operator pod, but
worth revisiting if operator restarts show up in soak logs. request_workers
reserves no capacity between the atomic read and the actor's increment, so a
burst of concurrent callers can briefly overshoot max_workers — single-caller
(scheduler tick) today. jcp_pod/webhook/pod_failure/pod_manager/reconciler/
main are defensively written (S7 fencing, env allow-list, secret-ref-only
tokens, owner refs gated on UID, DUR-1 Committing→Running). tests.rs is
genuine behavior coverage incl. failover fencing-token rejection.

Gates: cargo test -p krishiv-operator --features k8s (54 green incl. 2 new),
Coverage: 49.4% regions crate-wide (live-k8s I/O paths uncovered by design;
reconciler 82%, webhook 92%).
clippy 0, `just lint`, `just test`, `cargo fmt` — green.

## 20. krishiv-mcp — read end to end (2026-08-16)

One file, lib.rs 3,296 lines, read fully. **Zero functional defects.** The MCP
frontend is a disciplined typed facade over `krishiv_api::Session`: read-only
SQL gate (first-token check + `KRISHIV_MCP_ALLOW_WRITE_SQL` opt-in), row caps
via `capped_limit` everywhere rows are materialized, LIMIT-wrapping of
SELECT/WITH before execution, identifier quoting, base64 validation with
loud errors, and the distributed-vs-local fallback policy is both implemented
and *self-described* in `deployment_capabilities`. 12 genuine behavior tests
incl. continuous-stream and IVM checkpoint/restore base64 round-trips (the
restore test proves checkpoint3 == checkpoint1 after restore).

One fix (E): `explain_sql mode=analyze` printed `output_rows` and
`result_rows` as two stats that were the same value by construction; now one
honest `result_rows` line with a comment on why per-operator stats aren't
available at this surface. Cosmetic — no revert-proof needed beyond the
existing explain test coverage.

Notes: `looks_read_only_sql` is a first-token filter — multi-statement
bypass is closed by the engine (single-statement SQL parse) and by the
LIMIT-wrap producing a parse error for embedded `;`; recorded, not a defect.
The read-only default depends on `allow_write_sql=false` default (verified).

Gates: cargo test -p krishiv-mcp (12 green), clippy 0, fmt — green.

## 21. krishiv (CLI) — read end to end (2026-08-16)

All 19 files read (6,445 LOC): cli.rs 1680, query_cli.rs, relation.rs,
daemon_cmd.rs, local_cluster.rs, cluster_cmd.rs, stream_cmd.rs,
doctor_cmd.rs, remote_client.rs, pipeline_cmd.rs, ivm_cmd.rs, table_cmd.rs,
main.rs, lib.rs, capabilities.rs + 4 small. The tree's one real `unsafe`
(process_util.rs pre_exec+setpgid) is sound: setpgid(0,0) is async-signal-
safe and the closure captures nothing.

Defects fixed:

- **C1 (G)** query_cli.rs: `--timeout <SECS>` was parsed, documented in
  `sql_help` ("Timeout in seconds for remote queries (default: 30)"), and
  then dropped via `#[expect(dead_code, reason = "wired to session in
  planned PR")]` — the register's "PR #XXX will plumb this through" marker.
  A user setting a timeout got none, silently. Now applied via
  `with_query_timeout` around each statement's planning+collection future in
  both `run_sql` and `run_explain`, covering default/local/remote/api-key
  paths uniformly; help text corrected. Tests
  `zero_timeout_cancels_a_pending_future` and
  `run_sql_honors_the_timeout_flag`; revert-proven (wiring reverted to
  `with_query_timeout(None, …)` → red).
- **C2 (E)** cluster_cmd.rs `executor_port_pair` doc claimed
  `idx=0 → (50055, 50056)`; the code (and its own test) give 2005/2006.
- **C3 (E)** relation.rs `key_by` doc claimed "Returns an error when called
  on a batch relation"; it returns unchanged like its siblings.

Notes (not defects): main.rs single-query declaration is tripwired by
`every_pool_sharing_process_exits_through_the_daemon_dispatch` (the mcp
trap); StreamHandle::completed() builds a throwaway Session with an
`expect` — reachable only after a successful sink write, acceptable;
relation.rs documents the R5.2 multi-source-watermark gap honestly;
query_cli's S3-path-guard fix and NDJSON format carry excellent
why-comments and revert-shaped tests already.

The 5 tests/ files (r1 golden + contract, integration_batch_sql 15 tests,
integration_streaming, streaming_architecture_test) were read/scanned too —
all genuine behavior tests with real assertions (golden-file CLI contract,
primary-key answer-equivalence, watermark alignment); no tautologies.

Gates: cargo test -p krishiv (120 green across bin+lib+integration),
clippy 0, `just lint`, `just test`, `cargo fmt` — green.

## 22. krishiv-engines — read end to end (2026-08-16)

One file, lib.rs 2,184 lines, read fully: the three-engine dispatch
(`run_job`), `BatchEngine`, `IncrementalEngine`, `StreamingEngine` (windowed
+ stateless paths), the continuous loop, and 19 tests.

Defects fixed:

- **E1 (C — a test that could not fail)** the test named
  `streaming_loop_survives_transient_checkpoint_failure` claimed to inject an
  I/O failure; its doc comment even narrated a plan to chmod-000 a durable
  dir and then said "Actually, the simplest regression test is…". The body
  used `InMemoryCheckpointService`, whose `persist` returns `Ok(())`
  unconditionally — so it exercised the happy path and would have stayed
  green if the loop died on the first checkpoint error, which is the exact
  behaviour it was named for. Replaced with a `FlakyCheckpointService` that
  fails the first persist, records every epoch attempted, and lets the second
  (final, on-stop) persist succeed. The test now asserts both legs: the loop
  survives (stop → Completed) **and** the failed epoch is retried with the
  same number rather than skipped (the B-3 gapless-epoch property).
  Revert-proven twice — reverting either the in-loop `next_epoch = prev_epoch`
  retry or the conditional final advance turns it red.
- **E2 (B)** `drain_changelog_source` was `#[allow(dead_code)]` with zero
  callers (the incremental engine streams feed+step per batch instead of
  buffering, per the A-6 note). Deleted.
- **E3 (API)** `krishiv-engine-core` did not re-export `JobId`, which appears
  in the public `CheckpointService::persist`/`restore_latest` signatures — a
  downstream crate could not implement the trait without an otherwise
  undeclared `krishiv-proto` dependency. Added `pub use krishiv_proto::JobId`
  (this is what the new test needed, and it is a real published-surface gap).

Notes (not defects): the batch engine's 2 GiB cumulative drain cap is checked
*per source as it lands* with an honest comment about the prior parallel-drain
OOM; the streaming continuous loop's B-3/S-3/STREAM-5/ST-4/H-14 fixes all
carry why-comments and are now genuinely covered. Recorded open item: the
incremental engine opens every source once for schema probing and then
re-opens for the drain — safe for the bounded/rewindable CDC sources it
targets, but a non-rewindable source would lose its first batch; the engine
should probe from the drained stream instead. Needs a product decision on
whether non-rewindable sources are in scope for the incremental engine.

Gates: cargo test -p krishiv-engines (19 green), clippy 0 (both crates),
`just lint`, `just test`, `cargo fmt` — green.

## 23. krishiv-ui — read end to end (2026-08-16)

All 4 files read (2,384 LOC): lib.rs 640 (state + 27 route tests), handlers.rs
925, views.rs 522, router.rs 297. Static assets (`style.css`,
`krishiv-auth.js`, `krishiv-live.js`, `krishiv-sql.js`, `openapi.json`) are
served via `include_str!` and are vendored — the CDN-free property is tested.

Defects fixed:

- **U1 (A / security — fail-open)** `resolve_ui_token` early-returned `None`
  when `KRISHIV_UI_TOKEN_FILE` was set to an **empty string**, jumping over
  the production fail-closed check immediately below it. A deployment that
  renders that variable empty — an unset Helm value, `FOO=${MISSING}` in a
  manifest — therefore built the router with **no bearer middleware at all**,
  serving every `/api/v1/*` and `/ui/*` route anonymously in production while
  the operator believed auth was configured. The empty/blank path is now
  normalised to "no file configured" so it falls through to the guard, and the
  decision matrix moved into a pure `resolve_ui_token_from(inline, file,
  production_requires_auth)` (the env reads cannot be flipped from a test —
  `set_var` is unsafe under edition 2024, the same constraint as PY3/C1).
  Three tests pin the matrix; revert-proven (restoring the early return turns
  two of them red).
- **U2 (A — silent wrong answer)** `scalar_array_to_json` downcast **every**
  `Timestamp(_, _)` to `TimestampSecondArray`. DataFusion's timestamps are
  normally microseconds or nanoseconds, so the downcast failed and the
  `unwrap_or(Null)` rendered every timestamp cell in the UI SQL editor as
  `null` — the common case silently wrong, not an edge case. This is the same
  incomplete-Arrow-type-coverage family as §13 K2 (trace.rs) and §11 D1.
  Now matches on `TimeUnit`; `Date32`/`Date64` (which fell to a catch-all that
  printed the *type name* in place of the value) are handled; `Utf8View` is
  handled; and the remaining catch-all renders through
  `arrow::util::display::array_value_to_string` so an unhandled type shows its
  VALUE rather than its type name. Test
  `every_timestamp_unit_renders_its_value_not_null`; revert-proven.

Notes (not defects): `require_bearer` is genuinely hardened — constant-time
compare, `str::get` instead of indexing (a multi-byte UTF-8 cut point cannot
panic the server), empty expected token denies everything; the CSP is
`script-src 'self'` with the scripts vendored, and both are tested. The
metrics handler's poisoned-lock recovery has a real poisoning test.

Gates: cargo test -p krishiv-ui (29 green incl. 5 new), clippy 0,
`just lint`, `just test`, `cargo fmt` — green. Coverage 54.2% regions.

## 24. krishiv-bench — read end to end (2026-08-16)

23 files, 4,942 LOC: lib.rs (TPC-H query texts + scale ladder), tpch_queries,
phase_i, comparison, tpch_fixture, tpcds, 4 bins, 8 benches, 5 integration
tests. 35 tests green.

One fix:

- **B1 (C — name asserts X, body tests Y + silent skip)** the test named
  `q10_shuffle_payload_is_dominated_by_columns_custkey_determines` never
  checked that claim — it is a diagnostic that prints a per-stage byte
  breakdown — and its staging arm was
  `_ => { println!("declined to stage"); return; }`, so if the stage cutter
  ever stopped staging q10 the diagnostic silently emitted nothing and the
  test still passed. Renamed to `q10_stage_shape_dump_and_sf100_must_stage`
  and the SF100 arm (the no-broadcast options that match the shape the
  cluster actually runs) now asserts staging happened, with the cutter's own
  reason in the message; a zero-stage plan is also rejected. This is a
  test-hygiene fix: the thing it replaces could not fail by construction, so
  there is no pre-fix red to demonstrate — that is precisely the defect.

Notes: `scale_dirs()` skips unset scale factors with an eprintln so
`cargo bench` still runs on machines without the datasets — deliberate and
documented. Heavy benches remain gated behind the Tranche G date (task #74).

## 25. krishiv-sql-gateway — read end to end (2026-08-16)

All 3 files read (541 LOC): lib.rs, error.rs, session.rs. **Zero defects.**
20 genuine tests. The crate-level doc is unusually honest: it states in the
first paragraph that despite the "JDBC/ODBC gateway" name this is **not** a
wire-protocol server, and points at `krishiv-flight-sql` as the real ingress
— exactly the kind of published-surface honesty the register's E checklist
asks for. `SessionPool` recovers from a poisoned mutex rather than
propagating the poison, respects `capacity` on return (dropping the excess),
and creates on demand when empty; all three properties are tested, including
the capacity-0 edge. SQLSTATE mapping is covered per `KrishivError` variant.

Recorded, not changed: `InvalidConfig` maps to `0A000`
(feature_not_supported) rather than a configuration-error SQLSTATE. It is
deliberate and tested; changing it is a product decision about the published
error taxonomy, not a bug fix.

## 26. krishiv-conformance — read end to end (2026-08-16)

All 3 files read (353 LOC): lib.rs (sqllogictest drivers for the three
placements), tests/corpus.rs, tests/corpus_dual_run.rs. **Zero defects.**

**The register's own summary row was wrong**: it recorded "no tests at all".
The crate has 4 test entry points over a 7-file `.slt` corpus (scalar +
stateful tiers), run against embedded, single-node, and distributed-in-process
placements, plus a dual-run binary that replays the whole embedded corpus with
runtime filters OFF and requires identical results — the corpus-neutrality
rule that catches an "optimization" which changes an answer. `corpus_files`
asserts the corpus is non-empty, so an empty/missing corpus dir fails loudly
instead of vacuously passing. Row corrected.

Note: `KRISHIV_BLESS_CORPUS=1` rewrites expectations instead of checking them.
That is the documented regeneration path, but it means the gate is only a gate
when that variable is unset — worth keeping out of any CI environment.

## 27. krishiv-chaos — read end to end (2026-08-16)

One file, tests/chaos_suite.rs (829 lines, 25 tests after this section's
merge). **The register's summary row was wrong here too**: it recorded
"empty crate — delete or fill, decide first". The crate is a test-only
package (no `src/`, only a `[[test]]` target — which is why it looked empty)
carrying a real 25-test chaos suite: fencing-token rejection, split-brain,
dead-letter sink actions, barrier-ack idempotence, leader election, IVM
restart convergence. It is also one of the two CI-excluded crates, so clippy
was run directly against it (0 warnings). **Decision recorded: keep, do not
delete.** Row corrected.

Defects fixed (both C-class — tests that could not fail):

- **X1** `checkpoint_prepare_failure_leaves_no_committed_state` was
  `let r: Result<(),String> = Err(..); if r.is_ok() { committed = true }`
  followed by `assert!(!committed)` — a tautology over a locally-constructed
  `Err`. It exercised no production code; no engine change could make it
  fail. Rewritten against the real `LocalFsCheckpointStorage`: a
  never-committed epoch must be absent from `list_valid_epochs` and read back
  as `None`. Revert-proven (stubbing the epoch list red).
- **X2** `policy_hook_denies_table_access` and `policy_hook_allows_table_access`
  each defined a local `DenyAllPolicy`/`AllowAllPolicy` returning a constant
  and asserted that same constant back — assertions on the test double's own
  body, the exact shape the register's C checklist names. Merged into one
  test that exercises the **shipped** `AllowAllPolicyHook` through
  `Arc<dyn PolicyHook>` (the form the engine holds) and keeps a targeted deny
  policy that must deny only its named table — so a deny hook that denied
  everything would now fail.

Gates (24–27): cargo test -p krishiv-bench (35), -p krishiv-sql-gateway (20),
-p krishiv-conformance, -p krishiv-chaos (25) — all green; clippy 0 on all
four including the CI-excluded chaos crate; `just lint`, `just test`,
`cargo fmt` — green.

---

**Register complete.** All 27 crates have been read end to end.

---

## §28 — post-register close-out: the unaligned-checkpoint three-leg gap (2026-08-16)

The register's only cross-crate open item, recorded in three places that each
looked like a local note — §11 (dataflow D3), §15 (proto: "unaligned_buffers
wire drop"), §17 (engine-core: "once wired through the operator runtime"). Read
together they are one defect: **unaligned checkpointing is documented as a
shipped Flink-parity feature and is not wired at any of its three legs.**

`UnalignedBufferRef` is constructed in **zero** production sites — every one of
the 20 occurrences across 8 crates is `Vec::new()`.

**Leg 1 (dataflow) — the capture: absent, and the doc claimed otherwise.**
`AlignmentMode::Unaligned` and `BarrierAligner::unaligned_capture_inputs` are
complete and tested, but nothing constructs an unaligned aligner: the sole
production caller (`watermark_join`) uses `BarrierAligner::new` — aligned. No
operator serializes the buffered records the capture-inputs list names. The doc
comment nonetheless read "exactly-once is preserved **without** the alignment
stall. This is the Flink `execution.checkpointing.unaligned` behavior."
Selecting the mode today would snapshot without in-flight data and **lose
records on recovery**. Fixed as an E-class honesty correction: the variant now
states it is unreachable, half-built, and lossy if selected. Not deleted — the
bookkeeping half is correct and is the foundation the capture will sit on.

**Leg 2 (proto) — a silent drop at the wire. FIXED.** The domain struct
`CheckpointAckRequest` carries `unaligned_buffers`, but the protobuf message
had **no such field**: `checkpoint_ack_request_to_wire` had nowhere to write it
and `..._from_wire` hard-coded `unaligned_buffers: Vec::new()`. A task that
captured buffers would have had them discarded between executor and
coordinator with no error and no log. Fix: added `message UnalignedBufferRef`
and field 9 to `CheckpointAckRequest` (backward-compatible addition), plus
encode/decode. Test `checkpoint_ack_unaligned_buffers_roundtrip`;
revert-proven — restoring `Vec::new()` in the decode turns it red on the
round-trip equality.

**Leg 3 (scheduler) — a second silent drop at the coordinator. FIXED.** Both
`CheckpointMetadata` builders in `checkpoint.rs` hard-coded
`unaligned_buffer_refs: Vec::new()` while their siblings `source_offsets` and
`sink_transactions` were collected from the acks — so even a buffer ref that
survived the wire died before `metadata.json`. Fix: `collect_unaligned_buffers`
(the exact sibling of DUR-2's `collect_sink_transactions`), sorted for a
byte-stable metadata. Test
`unaligned_buffer_refs_reach_the_checkpoint_metadata`; revert-proven — making
the collector yield nothing turns it red.

**Deliberately NOT carried: the rescale path.** `checkpoint_ops.rs` rebuilds
metadata for a rescaled epoch and still writes `Vec::new()`. This is now
correct-by-comment rather than silent: a buffer ref is keyed by `(operator_id,
channel_index)`, and rescaling changes the channel layout, so the source
epoch's indices do not name the same channels under the new parallelism.
Replaying in-flight buffers across a rescale needs them re-partitioned by key
first.

**Still open — needs a product decision (wire-or-delete).** The transport and
coordinator legs are now lossless end to end; the capture itself is the one
remaining step, and it is a feature build (spill in-flight channel buffers to
durable storage on the first barrier; replay them into the channels before
processing resumes on restore), not an audit fix. The alternative is to delete
`AlignmentMode::Unaligned` and the four `UnalignedBufferRef` types outright.
Recorded, not guessed at — matching the register's standing rule. Nothing is
lost in practice today either way, because no production path selects the mode.

Gates: `cargo test -p krishiv-proto` (91), `cargo test -p krishiv-scheduler
--lib` (548), `just lint`, `just test`, `cargo fmt` — all green.

## §29 — unaligned checkpointing deleted (2026-08-16)

§28's open wire-or-delete decision, resolved: **delete**. The user chose it
after the reachability finding below.

Removed across 8 crates (~880 lines): `AlignmentMode` and
`BarrierAligner::unaligned()`/`unaligned_capture_inputs()` (dataflow),
`CheckpointAlignment` + `UnalignedBuffer` + the whole unaligned branch of
`OperatorQueueReceiver::recv` (dataflow/queue.rs), `UnalignedBufferRef`
(proto + state), `CheckpointMetadata::unaligned_buffer_refs`,
`CheckpointAckRequest::unaligned_buffers` and proto field 9 (now `reserved 9`
so the number is never reused), `InitiateCheckpointRequest::alignment`,
`CheckpointPayload::in_flight`, and the `krishiv_unaligned_in_flight_bytes`
gauge (metrics — zero callers).

**Why delete and not build.** Unaligned checkpointing exists to remove the
stall where an operator blocks one input while waiting for another's barrier.
The engine has no such stall: that path lives only in
`execute_window_join_aligned`, which has **zero callers outside its own file**.
The live path `execute_window_join_fragment` hands the operator whole partition
sets per cycle, with barriers arriving out-of-band via the barrier injector —
nothing blocks, nothing buffers, so there is no in-flight data to capture.
Building the capture would have been machinery for an execution model the
engine does not run.

**A-class bug removed with it:** `UnalignedBuffer::push` silently evicted the
oldest record on hitting its 64-entry cap, bumping a `dropped` counter that no
production code read — a silent record drop wearing a metric that never
surfaced. Exactly the shape §1 of this register was opened to hunt, hiding
inside a feature that never ran.

No behavioural change: no production path selected the mode, so no checkpoint,
ack or restore differs. Docs corrected rather than rewritten — status.md's
H-12 and "Lever 2 (DONE, core)" entries stay as the historical record with a
dated correction appended, and the design note is marked superseded.

Gates: workspace build (all targets, python/chaos built separately),
`just lint`, `just test`, `cargo fmt` — green.


## §30 — GAP-WATERMARK closed: downstream stages no longer start at `i64::MIN` (2026-08-16)

The last A-class open item in the register (§executor, "found, NOT fixed —
needs a decision first"). The coordinator injected a `WatermarkHint` input
partition carrying the upstream stage's output watermark; `fragment/streaming.rs`
decoded it, logged it, and discarded it. Every stage after the first therefore
started at `i64::MIN` and scored an event the upstream stage had *already*
declared late as perfectly in-order — the stage reported "no late events" by
construction, and `allowed_lateness_ms` / late-firing never engaged past stage
one.

**The original scoping was wrong in one way that made it cheaper, and wrong in
one way that made the first attempt fail.**

Cheaper: it called for a `prev_watermark_ms` field on `WindowExecutionSpec` and
therefore a krishiv-plan change plus 80 struct-literal sites. But the hint is
*per-assignment*, not part of the plan — the same compiled spec runs on every
stage, so putting it in the spec would have been wrong as well as expensive. It
is threaded as a parameter instead: `execute_bounded_window_seeded(batches,
spec, state_dir, initial_watermark_ms)`, with the existing
`execute_bounded_window` delegating with `i64::MIN`. No serialized format
changed and no existing caller moved.

Harder: seeding the `WatermarkState` / `MultiSourceWatermarkState` trackers is
*not sufficient*, and the first version of the fix was green-by-accident until
the test said otherwise. The late threshold an operator actually enforces is
its own `prev_watermark_ms` field (`tumbling.rs:233`, and the same shape in
`sliding.rs` / `session.rs`), not the tracker's — the tracker only supplies the
`new_watermark_ms` argument. Both halves are needed and both are now seeded:

- `WatermarkState::with_initial_watermark` / `MultiSourceWatermarkState::
  with_initial_watermark` install a **floor**, `max`ed with the event-derived
  watermark rather than replacing it, so an event past the hint still advances
  and an event below it cannot walk the watermark backwards.
- `{Tumbling,Sliding,Session}WindowOperator::seed_initial_watermark` (exposed
  through the `state_backed_window_op!` wrapper) raises `prev_watermark_ms`.
  Applied *after* `new()` has restored from state and taking the `max`, so a
  watermark restored from a checkpoint is never walked backwards.

Count windows are untouched: they are not event-time based and have no
watermark.

`i64::MIN` — no hint, a first stage, every embedded/single-stage path — is a
no-op by construction, so nothing outside multi-stage streaming changes
behaviour. `execute_streaming_window` is deliberately *not* seeded: its only
callers are local/embedded single-stage paths where there is no upstream stage.

**Tests, all three revert-proven behaviourally (not by a compile break):**

- `seeded_stage_treats_pre_watermark_events_as_late` (operator_runtime) runs the
  *same* batch through the *same* spec twice, differing only in the seed:
  unseeded emits both windows, seeded drops the pre-watermark event. Red at
  `left: 2, right: 1` with `TumblingWindowOperator::seed_initial_watermark`
  reverted to a no-op — this is also what caught the trackers-only version of
  the fix.
- `seeded_watermark_is_a_floor_in_both_directions` and
  `seeded_multi_source_watermark_is_a_floor` pin both directions of the floor.
  Both red at `left: -9223372036854775808, right: 500` with the `.max(floor_ms)`
  reverted.

`WatermarkHint`'s doc in `krishiv-proto/src/task.rs` — one of the three places
that claimed this was done — is now accurate as written and needed no edit; the
executor comment that admitted the gap is replaced by one describing the fix.

Gates: `cargo test -p krishiv-dataflow -p krishiv-executor`, `just lint`,
`just test` (0 failures), `cargo fmt --all` — green.

## §31 — the last four "needs a decision" items, resolved (2026-08-16)

A sweep of every remaining `not fixed — needs a decision` entry in this
register. **Three of the four were already closed by later sessions and the
register never said so** — which is the register committing its own founding
sin, a record that reads as current and is not. Each is re-verified against the
code below, not against memory.

**Already closed, entries were stale:**

- **`EtcdLeaseElection::last_renewed_at` (§scheduler).** Recorded as
  "three writes, zero reads — dead state shaped exactly like a liveness guard."
  It now has a reader (`etcd_lease.rs:463`, returning the renewal age) and a
  test, `is_leader_does_not_depend_on_renewal_age`, that pins the decision the
  entry asked for: `is_leader` stays a plain flag, because safety comes from the
  fencing token and self-demoting on a stale clock flaps the cluster. Decided
  and tested, not open.
- **`load_prefix` fails the whole load on one bad record (§scheduler).** Both
  `load_prefix` and `load_json_prefix` now skip-and-log per record and funnel
  through `admit_partial_prefix_load`, which logs the prefix, the decoded count
  and the skipped count at `error!` — the asymmetry with `load_ivm_snapshots`
  the entry flagged is gone.
- **The MATCH_RECOGNIZE parser (§sql).** Both silent-wrong-answer cases are
  gone. `extract_parenthesized_after` matches the *balanced* close paren (its
  in-code comment names the exact old bug: `PATTERN ((A B) C)` yielded `(A B`,
  "a different pattern that still parsed, so the query ran and matched the wrong
  thing"), and keyword lookup goes through `find_keyword`, which requires
  non-identifier bytes on both sides — so a `within_x` column can no longer be
  read as `WITHIN`.

**Decided and acted on now (user call, 2026-08-16):**

- **Three dead krishiv-plan pub surfaces DELETED.** `DynamicPartitionPruningRule`
  (the whole 424-line module, plus `DppAdvice` / `DPP_MAX_BUILD_ROWS` /
  `DPP_MAX_KEYS` and the `optimizer.rs` re-export) — a complete `AqeRule` never
  registered in any pipeline. `diff_plans` / `PlanDiff`, whose doc claimed
  operators used it for adaptive-repartition diffs when only its own seven tests
  called it. `PlanNode::with_exchange`, whose doc named `DataFrame::repartition()`
  as the caller — that method does not call it. Zero callers repo-wide across
  `*.rs` and `*.rs.inc`, so zero behaviour change; the workspace builds
  all-targets clean with them gone. No test was added, because there is no
  behaviour to pin: the deletions' proof is the build.
- **`SkewJoinRule` / `BroadcastRuntimeRule` stay registered, guard debt stands
  documented.** They are registered but cannot fire on any current path (both
  production call sites are analysed in the AUDIT comment above
  `default_aqe_optimizer_with_parallelism`), so there is no live bug. The block
  already records the precondition for any future wiring — `SkewJoinRule` salts
  ANY `JoinType`, outer/anti included, with no per-side guard, and
  `BroadcastRuntimeRule` demotes a colocating Broadcast — and states plainly
  that the registration must not be read as evidence the rewrites are safe. No
  code change: the documentation the decision called for was already in place
  and accurate.

With §30 and this section, every item in this register is closed.

## §32 — batch-mode API parity across embedded / single-node / distributed (2026-08-16)

Not a crate read: an end-to-end trace of the **batch** surface through all
three placements, prompted by the question "how do these actually differ?"
Three defects, each the register's house shape — a divergence between two paths
that read as equivalent, with nothing that could tell them apart.

**The placement seam itself is sound.** `ComputeEngine::run(job, rt)` takes an
`EngineRuntime` of trait objects and `BatchEngine::run` branches exactly once,
on `rt.query_executor`: `None` runs the query in-process, `Some` hands it off.
`build_execution_runtime` rejects every mismatched (mode, placement) pair, and
Distributed has no local fallback by construction. What follows are leaks
around that seam, not flaws in it.

**1. Python UDFs shipped on one remote path and not the other. FIXED.**
`prepend_python_udfs` had exactly one caller, `execute_remote_async`. The path
`session.sql(q).collect()` takes shipped the query text bare, so a registered
UDF reached executors through one entry point and failed with "unknown
function" through the other — on the same session, same query. The directives
are now attached in `dataframe_from_sql`, and only when the session actually
executes remotely: embedded resolves UDFs in process, never reads the field,
and prepending comments there would only corrupt diagnostics. Planning is
unaffected either way — the `DataFrame` plans against `sql_dataframe`, built
from the plain query. Tests both directions; the remote one is revert-proven.

**2. `register_record_batches` was invisible to the cluster. FIXED.** It wrote
to the local `SqlEngine` only, while its sibling `register_parquet` writes to
both the engine and `registered_parquet` — the set that actually ships. So in a
remote session the query planned locally and then failed at the coordinator
with an unresolved table. A remote session now also spills the batches to
parquet and registers the path, which puts the table on the road a
`register_parquet` table already travels: the Flight client inlines it as Arrow
IPC, so it reaches executors in other pods with no shared filesystem. The spill
directory is owned by an `Arc<SessionSpillDir>`, so it survives every `Session`
clone and is removed when the last drops. Embedded sessions write nothing.
Both directions tested, the remote one revert-proven.

**3. The staged-vs-single-task decision was invisible. FIXED (observability).**
`plan_staged_batch_stages` either returns stages (partition-parallel) or `None`
(the whole query on one executor), and nothing reported which. A distributed
query that ran serially was indistinguishable from one that scaled — the single
fact an operator sizing a cluster most needs. The gate is now a pure
`staged_decline_reason` function, unit-tested including its *ordering* (a
multi-gate decline must report deterministically), and the coordinator logs the
outcome with its reason. No behaviour change: every decline was and remains
correct.

**Corrected while tracing this:** the earlier reading that "distributed usually
means one executor" was wrong. The Rust Flight client inlines parquet only up
to `inline_ipc_max_bytes`; past the cap inlining fails, the table becomes a
path table, and staging becomes eligible. Small tables running single-task is
the design, not a defect.

**Recorded, not fixed — the honest gap.** `mode_conformance.rs` opens with "The
same query run through `Embedded` must produce byte-for-byte identical results;
mode selection is purely about where data-plane work executes." Every executing
test in it builds an *embedded* session. The one single-node test is
`#[ignore]`d and there are no distributed tests; the only other cross-mode
assertions in the tree compare `session.mode()` to a label.
`differential_corpus.rs` is genuine differential testing, but across engines
(batch vs IVM), not placements. So the parity claim is asserted in a doc
comment and verified by nothing that runs in CI — which is exactly how all
three defects above survived. A cross-mode harness (Embedded vs a live
in-process-cluster daemon, canonical row-set comparison) is the next piece of
work this points to; it needs a daemon fixture, so it is scoped rather than
smuggled into this change.

Also recorded, no defect: embedded batch streams to sinks batch-by-batch while
the remote path materializes the full result before streaming (the `BATCH-2`
comment says so in place), with a 256 MiB warn and a 2 GiB hard error. A job
that streams a 50 GB result embedded fails at 2 GB distributed. Honest in code,
absent from user-facing docs.

Gates: `cargo test -p krishiv-api -p krishiv-scheduler`, `just lint`,
`just test`, `cargo fmt --all` — green.

## §33 — batch API review: SQL, Rust, Python (2026-08-16)

A review of the three front doors onto the batch engine, and the fixes it
produced. Findings graded by how they fail rather than by a generic severity
scale: none of these were silent wrong answers.

**A1 — the DataFrame API stopped at the process boundary. FIXED.** A
`DataFrame` carries both an `Arc<dyn KrishivDataFrameOps>` (the real plan) and
an `Option<String>` of originating SQL, and remote execution shipped *only the
string*. Every transform routes through `with_new_ops`, which sets
`sql_query: None`, so `select` / `filter` / `join` / `group_by` / `union` /
`limit` / `distinct` — roughly forty methods — produced a DataFrame that could
not be collected in any session that executes remotely. Python inherited it
exactly. Verified by execution, not reading: a filtered DataFrame on a
Distributed session returned `remote execution requires a SQL query`.

The fix was smaller than the finding: `KrishivDataFrameOps::to_sql` already
unparses the *current* DataFusion logical plan via
`datafusion::sql::unparser::plan_to_sql` — the DataFrame layer simply never
called it. `DataFrame::query_for_remote` now prefers `sql_query` when present
(keeping the user's own text, comments and formatting intact for the
untransformed case) and falls back to the unparser otherwise. Plans that
genuinely cannot be unparsed still fail, but with a message that names the
mode and the two workarounds instead of naming a missing input.

The Python UDF prelude moved to its own `remote_prelude` field in the same
change. It had been prepended into `sql_query` (§32), which a transform
discards — so directives would have been silently lost exactly when the
unparser path started being used. Held apart, it survives.

**A2 — inline tables were dropped by the single-node backend. FIXED, and found
by the new harness on its first run.** `FlightExecutionHost::execute_batch_sql_with_paths`
accepted `inline_tables` and, on the `InProcess` arm, never read it: only the
path catalog reached the cluster. The Rust Flight client inlines any parquet
table under the inline-IPC cap, so **every such client hitting a single-node
daemon failed with "table not found" for a table it had just registered.**
Inline tables now travel alongside the catalog tables, and
`in_process::execute_inline_sql` registers their decoded batches instead of
canonicalizing an empty path. This is a field that was accepted and ignored —
the same shape as the Phase 68/69 defect class.

**A3 — three Python `_async` methods were not. FIXED.**
`DataFrame.collect_async`, `DataFrame.execute_stream_async` and
`Session.sql_with_timeout_async` blocked despite their names (the last was a
literal alias for the sync method), while `Session.sql_async` and
`QueryHandle.collect_async` on the same objects were genuine coroutines. So
`await df.collect_async()` raised `TypeError` and calling it without `await`
stalled the caller's event loop. All three are now `future_into_py` coroutines;
the synchronous forms (`collect`, `sql_with_timeout`) already existed, so no
capability was lost. GIL handling underneath was already correct — 124
`py.detach` sites — this was naming, not concurrency.

**A4 — Arrow PyCapsule interface. ADDED.** `QueryResult.__arrow_c_stream__`
exposes an `FFI_ArrowArrayStream`, the protocol pyarrow / Polars / DuckDB /
pandas 2.x negotiate zero-copy handoff through, so `pa.table(result)` and
`pl.DataFrame(result)` work without the `to_arrow()` round-trip. `arrow`'s
`ffi` feature is enabled on krishiv-python only, keeping the C-ABI surface out
of the engine's build graph.

**A5 — the cross-mode gate that was missing. BUILT.** `mode_conformance.rs`
claimed "the same query run through Embedded must produce byte-for-byte
identical results" and every executing test in it built an *embedded* session;
the one single-node test was `#[ignore]`d and there were no distributed tests.
That absent gate is how §32's three defects and A1/A2 above all survived.
`live_conformance` now stands up a real in-process Flight SQL server, points a
remote session at it, and runs a ten-query corpus (aggregation, ordering,
grouping, joins, DISTINCT, NULL handling, HAVING) through both modes comparing
rendered row sets, plus a transformed-DataFrame case. It needs no external
daemon, so it runs in CI with everything else. It failed 9 of 10 on its first
run, which is what surfaced A2.

**Corrected from the earlier reading:** distributed batch is not "usually one
executor". The client inlines parquet only up to `inline_ipc_max_bytes`; past
the cap inlining fails, the table becomes a path table, and staging becomes
eligible. Small tables running single-task is the design.

**Recorded, not fixed.** Two items are scoped rather than done, and both are
substantial rather than deferred out of convenience:

- **Table-generating function machinery.** One absent capability blocks
  `json_tuple`, `stack`, `posexplode`, `inline` and LATERAL VIEW together;
  building it once clears most of the SQL matrix's `planned` column. That is a
  feature build, not an audit fix.

**A6 — BATCH-2, remote result materialization. FIXED (was scoped out, then
found to be already half-built).** Embedded streams to sinks batch-by-batch
while the remote path collected into a `Vec` first, so a job that streams 50 GB
embedded failed at the 2 GiB result cap distributed — the cluster made the job
*less* capable than one process. This was recorded as needing a transport-level
change; it did not. `FlightClientPool::stream_sql` already delivers over
`do_get`, a genuine streaming transport. Only the seam above it was missing.

`ExecutionRuntime::stream_batch_sql` is new, with a default that collects and
replays so no runtime implementation breaks, overridden by the remote runtime to
stream. Both remote batch paths in `RuntimeQueryExecutor` use it: the
all-parquet fast path and the mixed-connector path. The 2 GiB
`enforce_result_size_limit` and its constant are deleted — nothing calls them
now, which is the proof the limit is gone rather than relocated.

One real lifetime change fell out, and the existing test caught it: the spilled
source directories delete themselves on drop and were dropped straight after
dispatch, which was correct while the result was collected first. With a lazy
read that would pull the files out from under a stream still reading them, so
the guard moved into the stream's closure. `remote_runtime_spills_csv_sources_to_parquet_registrations`
now pins the stronger invariant — the spill exists while the stream is alive and
is gone once it drops.

`stream_sql` needed `use<>` precise capturing: it borrows neither `self` nor its
`sql` argument at runtime (the tonic `Streaming` owns its channel), but Rust
2024's `impl Trait` rules captured both, making the stream un-returnable.

Test `remote_batch_results_stream_instead_of_materializing` counts produced
batches: taking one item must have produced exactly one. Revert-proven — with
the stream collected before return it fails `left: 3, right: 1`. Its runtime
double also makes `collect_batch_sql_async` an error, so a silent fallback to
collecting fails loudly instead of buffering unnoticed.

Gates: `just lint`, `just test`, `cargo fmt --all` — green.

---

## §34 — streaming execution, source to sink, across three placements (2026-08-17)

Read the whole streaming path — three API entry points, two distributed
execution models, the executor fragments, the continuous registry — against
the A–H checklist. Six parallel read-only audits plus a synthesis pass;
every load-bearing claim re-verified by hand before acting.

**One correction to my own earlier analysis, recorded because it changed what
got edited.** I reported that `Session::stream` was capped at Cycle mode by
the Flight `ContinuousRegister` seam. It is not: `Session::stream` never
touches Flight. Its distributed arm goes `session.rs` →
`RemoteStreamingJob::create` → `coordinator_http_client.rs` → POST
`/api/v1/continuous-register`, which is the **full-options** handler. It
yielded Cycle/1 because the *client* declared a two-field request body
against a seven-field API. There are in fact three optionless seams funnelling
into Cycle/1 (HTTP client, Flight action, `unified_jobs_http.rs:217`), not one.

### Fixed

- [x] **The bounded final flush was a guard that did nothing** (`dd47d50`).
      `run_streaming_bounded` ended with `executor.tick(i64::MAX)` under a
      comment claiming it closed every window. `ContinuousWindowExecutor::tick`
      deliberately ignores its argument for tumbling/sliding (STREAM-1) and
      flushes against the last *event-time* watermark — which the trailing
      window's own events set, so it could never close itself. **Every bounded
      tumbling or sliding job silently dropped its last window and reported
      `Completed`.** Count windows were a second leg (`flush_due` returned empty
      for them). STREAM-1 and B-4 landed at different times against one method
      with opposite requirements; the later neutered the earlier. Added
      `flush_all`, distinct from `tick` so they cannot be conflated again.
      Revert-proven `left: [5], right: [5, 7]`. **Not distributed-only — this
      was the embedded path, the most-used one.** No audit caught it; it came
      out of the synthesis pass.

- [x] **A run-loop launch that dispatched nothing reported success** (`aa4c5e1`).
      `if accepted < responses.len()` compared two numbers that shrink together
      — undeliverable targets are partitioned out *before* responses exist — so
      an all-in-process executor set evaluated `0 < 0` and returned `Ok(())`
      having launched no subtasks. Both siblings in the same file use
      `!= target_count`. The comment above it recorded that a test drove the
      relaxation, and then every run-loop test in the file inherited the hole,
      because they all build in-process coordinators. Fixed both legs (count
      comparison + in-process rejection). All three tests report `got Ok(())`
      against the pre-fix code.

- [x] **Bounded distributed streaming truncated at 256 batches** (`b5d840f`).
      `run_streaming_job_via_runtime` buffered the whole source, pushed once,
      drained once — but a drain consumes at most `DEFAULT_MAX_DRAIN_BATCHES`.
      Everything past it stayed queued forever with the job `Completed`. With
      CSV's 1024-row batches that is **silent data loss from row 262,145**,
      silent up to ~1M rows where QueueFull finally fires. Framed as a memory
      problem; it was a correctness one. Now pushes/drains in bounded cycles
      with the bound *derived from* the drain cap so they cannot drift.
      Revert-proven `saw [293]` against a cap of 256. Also corrected
      `drain_job`'s doc comment, which told callers to loop until the output is
      empty — that terminates early and is how a caller silently truncates its
      own source.

- [x] **The `stream-kafka:` path was dead and armed** (`ed54bf5`). Sole encoder
      had zero callers outside its own tests, across every file type. The
      executor branch was therefore unreachable — but when it fired it
      **overwrote the compiled plan's `key_column`/`event_time_column` with the
      literals "key"/"ts"**, i.e. ran a different query than the one compiled.
      Deleted parser, rewrite, and encoder together: removing the parser alone
      would have made the clobber unconditional; removing it without a
      replacement reader would have produced `Ok(0 rows)/Succeeded`. Branch now
      reads `InlineIpc`. The 9 fixture tests are ported to real Arrow batches
      rather than deleted — they had been asserting on a format no coordinator
      produces, using the very column names the rewrite wrote.

- [x] **`Session::stream` could not ask for the run-loop engine** (`054a064`).
      Client body declared `{job_id, spec}` against a seven-field handler.
      Added `ContinuousRegisterOptions` (`run_loop`/`with_checkpointing`/
      `with_source`), all `skip_serializing_if` so defaults serialise to a
      byte-identical body. Fails closed outside distributed mode rather than
      quietly running the single-subtask loop. Revert-proven
      `left: Null, right: "run-loop"`. Also fixed `stream_async` resolving the
      coordinator URL with `coordinator_grpc_url()` and handing it to a
      function that treats it as an HTTP base.

### Found, NOT fixed — recorded with what each needs

- [ ] **The Flight `ContinuousRegister` seam is still optionless.** Reached from
      `Session::submit` (not `stream`). Needs the additive `options` field on
      `ContinuousRegisterBody`, all four `FlightHost::register_continuous_stream`
      call sites, and — critically — **a non-empty server response the client
      verifies**. There is no `deny_unknown_fields` anywhere in the repo, so a
      new client asking for parallelism 8 against an old coordinator gets a
      success that registered Cycle/1, and the register arm currently returns
      `Ok(Vec::new())` unconditionally, so there is no echo to check. Without
      the echo this is a feature that lies. A new action *variant* does not help:
      `do_action_fallback` dispatches on the body's `kind`, and an unknown
      variant fails as `invalid_argument`, which `is_server_unimplemented` does
      not match. `unified_jobs_http.rs:217` is a third optionless seam.

- [ ] **`Session::submit_streaming` still rejects distributed placement.**
      Should route to run-loop when every source maps to an executor-owned
      `ContinuousRegistrySource` and every sink to a `ContinuousSinkSpec`, and
      fail naming the offending source otherwise. Blocked on the Flight leg.
      Validate that neither `kind` nor `table` contains `':'` — sources are
      flattened to `registry-connector:<kind>:<table>:<json>` and re-parsed by
      splitting on `':'`. Keep the H-22 policy check above the mode dispatch.

- [ ] **A failed run-loop launch wedges the job id.** Every failure path runs
      after the spec is upserted, so the job stays registered with subtasks
      assigned, and an identical re-registration is a deliberate no-op — so it
      can never launch on retry. Rolling back is not obviously right (it would
      leave run-loop registration with no positive test in this crate); making a
      failed launch retryable is a design question about the upsert contract.
      Pinned by an assertion so a later fix must update it deliberately.

- [ ] **Two run-loop dial divergences.** `run_loop.rs` computes
      `idle_floor.max(fallback_tick)` where the floor is 50 µs and the tick 5 ms,
      so the max is *always* 5 ms and `RLOOP_IDLE_FLOOR_US` has no other use,
      while the module doc claims a microsecond floor. And the idle tick fires
      only when the *combined* input is empty, so a subtask with one chatty and
      one silent split never ticks — the embedded loop ticks whenever its reader
      yields `None`.

- [ ] **DUR-5 remains open, plus two confirmed bugs beside it.**
      `return_continuous_stream_payloads` is mode-blind: for a run-loop job it
      unshifts into `job_inline_results`, which no drain of that job reads — the
      exact loss the unshift machinery exists to prevent, plus a RAM leak. And
      `drain_run_loop_output` `?`s on executor N+1 and discards everything
      already collected from N, with no put-back RPC. Persisting inline results
      before ack is *rejected*: the obvious store is a single un-chunked etcd
      value against a ~1.5 MiB ceiling while the drain budget is 48 MiB, the
      handle drops writes on a full channel, and the map is shared with IVM,
      which stuffs a non-Arrow blob down it. Fail-closed admission is right but
      must land *after* the Flight leg, or it takes Flight streaming from lossy
      to non-functional (100% of Flight-registered jobs are Cycle-with-no-sink).

- [ ] **Cycle's missing idle watermark tick is inherent, not a bug to close.**
      Each push launches a fresh task assignment round, so between pushes there
      is no live thread to own wall clock. Adding one means putting wall clock
      in the control plane. Run-loop already ticks through the same shared dial
      as the embedded loop. The answer to Cycle's gap is to make run-loop
      reachable and demote Cycle, not to fake a tick.

### Known-broken in run-loop, deliberately untouched — schedule before pointing traffic at it

Making run-loop reachable promotes a less-tested path. These are verified and
must not be read as covered by a green suite: fresh-process restore does a
job-keyed `pending_restores.remove`, so the first subtask to construct its
executor consumes the whole entry and siblings start empty; key-group routing
hashes `to_be_bytes` while the operator persists the key as ASCII decimal, so
a redistributing restore leaves subtasks holding state for keys they will
never see; `route_batch_by_key_group` handles only Utf8/Int64 while
Int32/Float64/Bool are declared-legal key types, and the rest are treated as
"fully owned" by whichever subtask wins the race; egress is a 512-batch
drop-oldest ring — silent loss under backpressure alone.

**Also unverified and important:** whether the out-of-repo platform pipeline
reconciler posts `mode: "run-loop"` today. If it does, run-loop is already
live in production while every in-repo Rust caller got Cycle — which makes the
list above urgent rather than pre-emptive. Nothing in this repo can answer it.

### Test-harness honesty problem found while working

`mode_conformance.rs`'s `make_flight_sql_server` builds
`FlightExecutionHost::from_env()`, which is hardcoded to `Self::embedded()`.
So the "distributed" conformance harness compares in-process DataFusion
against in-process DataFusion over a socket. Its own guard asserts
`remote.execution_runtime().uses_remote_execution()` — the *client's*
placement, not the server's backend — under a message saying "or this harness
silently compares embedded against embedded — the exact failure it was written
to end". It passes while doing exactly that. A real harness needs
`FlightExecutionHost::with_coordinator` over a live `Coordinator` +
`ExecutorTaskRunner`, and belongs in krishiv-runtime (krishiv-api has no
krishiv-executor dependency).

Gates on every commit: `just lint`, per-crate `cargo test`, `cargo fmt --all`.

---

## §35 — the streaming remainder, with run-loop now reachable (2026-08-17)

Continuation of §34 after confirmation that **nothing is in production**, which
removed two constraints: the run-loop engine's latent defects could be fixed
directly (they become live the moment run-loop is reachable), and DUR-5's
fail-closed admission no longer had to wait behind the Flight seam.

Four commits: `b72579f`, `7dbcd52`, `388e4fd`, `5b74102`.

### Fixed

- [x] **Two run-loop idle dials that did not do what they said** (`b72579f`).
      The loop slept `idle_floor.max(fallback_tick)` — 50 µs vs 5 ms, so the max
      was *always* 5 ms and `RLOOP_IDLE_FLOOR_US` had no other use, while the
      module doc advertised a microsecond floor. The embedded loop it mirrors
      uses its 5 ms fallback only when a source has no notify; a run-loop
      subtask always has both, so the fallback had no role and is deleted. And
      the ST-4 idle tick was gated on `input.is_empty()`, tying a wall-clock
      obligation to whether that iteration found input — under sustained
      arrival it never fires. Hoisted to elapsed-wall-clock alone.

      **No regression test, deliberately.** I wrote one and deleted it: it went
      green against the reverted line, because any fixture pushing at a
      test-reasonable rate lets the input empty between batches. A test that
      cannot tell correct from broken is worse than none — the standing rule
      applies to my own work.

- [x] **Two DUR-5 put-back bugs** (`7dbcd52`), both inside the machinery built
      to stop consume-once loss. `return_continuous_stream_payloads` was
      mode-blind: it unshifted into `job_inline_results` while the drain path
      returns early for run-loop jobs and serves executor egress — so a
      run-loop put-back wrote into a map no drain would read. Now refuses,
      because there is no put-back RPC to egress and pretending otherwise is
      what lost the data. And `drain_run_loop_output` `?`d on each executor,
      discarding everything already collected — but `drain_continuous_output`
      CLEARS egress as it reads, so failing on executor N+1 deleted N's output
      rather than retrying it. Now partial-with-warning; the error surfaces
      only when nothing was collected.

- [x] **Routing hashed different bytes than persistence** (`388e4fd`). Live
      routing hashed `i64::to_be_bytes`; the operator persists
      `extract_agg_key(..).to_string()` — ASCII decimal — and redistribution
      re-hashes the persisted form. For parallelism P each Int64 key had a
      (P-1)/P chance of being redistributed to a subtask that never sees its
      rows: owner restarts from zero, wrong subtask flushes a stale partial
      anyway, and re-persists it at the next barrier. Invisible while running
      (all subtasks route identically); Utf8 agreed by coincidence, and the
      only routing test was Utf8. Fixed by deriving routing bytes from the same
      call persistence uses, so agreement is structural. Same edit makes
      Int32/Float64/Bool/LargeUtf8/Utf8View routable — they are declared-legal
      key types that fell into an "unroutable, process locally" fallback, which
      with index-partitioned splits meant overlapping key sets and one output
      row per subtask for the same window. No savepoint migration note needed:
      `key_group_for_key` and the persisted bytes are untouched.

- [x] **Restore gave one subtask the whole job's state** (`5b74102`), at two
      sites. The **live** path had a `rloop_execs.len() == 1` fast path that
      applied every snapshot to the single local subtask — and one subtask per
      process is the normal deployment, so every node loaded full job state and
      each re-emitted the full pre-checkpoint aggregate. Deleted; redistribution
      at parallelism 1 is an identity union. The **fresh-process** path did a
      job-keyed `pending_restores.remove` inside a (job, subtask)-keyed
      constructor, so the first sibling to construct took everything and the
      rest started empty. Now redistributes and reads rather than removes, with
      a teardown removal so a re-created job cannot inherit a dead
      incarnation's checkpoint. Also retires a watermark bug in the old merge
      loop: `wm:` appears in every snapshot and merge order let the LAST win,
      where redistribution deliberately keeps the MINIMUM. And publishes
      `rloop_parallelism` before the executor becomes visible, closing a window
      where a restore redistributed into the wrong bucket count.

### Found, NOT fixed — needs a decision

- [ ] **The run-loop egress ring drops the oldest batch on overflow.** 512
      batches, warn + metric, no fault required — sustained backpressure alone
      loses computed output. This is the worst shape in this register (a
      streaming operator silently discarding results), but every fix is a
      contract change and I am not choosing one unilaterally:
      **backpressure** (stall the loop until drained) risks wedging a
      sink-attached job forever, because output is copied to egress *and* the
      sink, so a job whose real consumer is the sink and which nobody drains
      would stop; **fail closed** turns a documented best-effort API into a
      hard error; **persist** puts unbounded data in the coordinator, which
      §34 already rejected for DUR-5 on store-size grounds. The cheap
      strictly-better step, if the full decision waits: track a cumulative
      dropped-batch count per job and return it on drain, so a consumer can
      *detect* the gap instead of silently receiving one. Related and probably
      a prerequisite: stop double-shipping to egress when a sink is attached.

### Still open from §34

**Superseded by §36**, which closed the Flight options seam and its echo,
`Session::submit_streaming` distributed routing, and `unified_jobs_http.rs` as
a third optionless seam — and **retracted** the `mode_conformance.rs` claim as
false. The failed-launch job-id wedge remains open; see §36 for the current
list.

Gates on every commit: `just lint`, per-crate `cargo test`, `cargo fmt --all`.

---

## §36 — streaming residuals closed, and one recorded claim retracted

Continues §34/§35. Five commits: `e26055f` (F12), `7944146` (F13), `8c224de`
(F14), `64147ee` (F15), `7540dd5` (F16). Every fix carries a test proven red
against the reverted production line; three tests were written and then
**deleted** for failing that bar.

### The retraction first

§34 recorded that `mode_conformance.rs` "compares embedded against embedded
while asserting it does not". **That is false against the current file and is
withdrawn.** An adversarial re-read falsified it two ways: structurally, the
two sides use different constructors (`with_execution_mode(Embedded)` vs
`with_coordinator(..) + with_remote_execution(true)`), land on different
`ExecutionPlacement`s, and build different `ExecutionRuntime` impls, with
`session.rs` failing the build closed rather than degrading; and empirically,
enabling server-side auth (`KRISHIV_API_KEYS=k1=alice`) makes all 10 corpus
queries diverge and both live tests fail, so the remote side is a real socket
round-trip whose assertion can and does fail. The claim described the
pre-2026-08-16 state that `live_conformance` already closed.

Two narrower defects in that file were real and are fixed in F14.

### F12 `e26055f` — the options seam, and the echo that makes it honest

Three seams could accept run-loop options; one could ask for them; none could
tell you whether the answer was real. Neither wire body sets
`deny_unknown_fields`, so a coordinator predating Phase 55 discards the options
and answers success.

`AppliedContinuousRegistration` is the fix: the shape a registration actually
applied, returned by the one function that decides it, carried out through both
wire surfaces, and compared against the request by `verify_ack`. Absent fields
read as "options dropped", never as defaults. Default requests skip the check.
`sources` is in the echo because it is the quietest failure — right
parallelism, no source, reads nothing, looks like an idle topic.

Closed rather than plumbed: the SQL-comment fallback and the in-process Flight
backend (which `from_env()` makes the *default* server). Deleted the dead
`flight_client::execute_remote_continuous_register`.

### F13 `7944146` — a NULL key was a poison pill; cancel kept the read position

1. `extract_agg_key` rejects NULL keys by design and routing called it per row
   with `?` **inside the run-loop's own input loop** — one NULL row ended the
   job, and took the whole in-flight buffer with it (`input` is filled by
   destructive `remove`). NULL is legal data in every source the engine reads.
   Rows are now quarantined and counted; the guard sits **above** the
   `parallelism <= 1` short-circuit, or it would miss the single-subtask shape
   entirely. Type errors stay fatal: those are plan faults, not bad rows.
2. `CancelTask` dropped the window state and kept `continuous_connector_sources`
   — the **advanced read offsets**. Only the restore path cleared them. A
   same-process re-register resumed at the dead incarnation's offset against
   empty state and silently skipped everything since the last checkpoint. State
   and position are now retired together. Run-loop jobs also retire their sink
   handle and loss counters in their own fragment teardown, under the same
   last-subtask rule that guards `pending_restores`.

### F14 `8c224de` — the third seam, and a sort that disarmed the harness

`POST /api/v1/jobs {"kind":"streaming"}` documented itself as delegating to
`/continuous-register` while accepting a strict subset of its body, and does
not deny unknown fields — so a copied body was silently eaten. Sharper edge:
because the derived shape (cycle/1) mismatched a *running* run-loop job's
shape, re-submitting a live job's id made the upsert cancel its subtasks, evict
it, drop its snapshot and re-register it single-subtask. A duplicate submit
that destroyed a running job.

`render()` in the conformance harness sorted unconditionally, contradicting its
own docstring and disarming the 7 corpus queries that pin an order — exactly
the cross-task-sort divergence the corpus comment says it exists to catch.

### F15 `64147ee` — the drop counters reach someone (a defect in my own F9)

`egress_dropped_batches` carried a doc comment *I wrote* saying the count is
reported upward and makes the loss detectable. It was not: `on_progress` never
copied it to the report and the wire had no field. Two additive proto fields
now carry it (and `null_key_rows_dropped`) to `record_streaming_progress`,
which **warns** — everything else in a progress report is a gauge you go
looking for; loss has to come find you. Guarded on non-zero so an older
executor's absent field (proto3 → 0) reads as "no report", not "nothing lost".

### F16 `7540dd5` — submit_streaming distributed

Routes to the run-loop engine, whose subtasks own the sources. Refuses a
bounded source, a sourceless job, and an unplannable query — each would
otherwise produce a job that idles forever reporting Running. Connector
properties go through the embedded path's own mapping, exposed not copied, so
a source means the same thing wherever the job lands. `RunningJob::supervised`
keeps `stop()`'s contract: `Completed` means stopped, not "stop requested".

### Tests deleted for being untestable-by-reversion

- proto: one asserting the drop fields default to 0 — asserts a derive.
- api: one asserting the non-window error contains "TUMBLE" — the raw SQL
  error already says TUMBLE, so it could not tell the guard from its absence.

### Closed in §37 (F17, F18)

The egress dials, the stop-cost honesty gap, the CORPUS overclaim, and the
per-incarnation counter leak. What remains after those is listed at the end of
§37, not here.

### Was open after §36

- **Run-loop clean stop takes no final checkpoint.** The loop breaks on
  cancellation and its teardown is bookkeeping only, so a stop loses everything
  accumulated since the last barrier epoch. Not force-flushing is *correct*
  (`flush_all` is scoped to exhausted bounded sources; forcing it would emit
  partial aggregates) — the missing piece is the snapshot that would let a
  restart resume. A run-loop job registered without checkpointing, which
  registration permits, has **no non-lossy stop at all**.
- **`CancelTask` destroys the egress buffer before the loop observes the
  cancel.** F13 added a warn naming the discarded batch count; moving the
  teardown into the loop's own path is the real fix and is a lifecycle change.
- **Egress ring dials.** `RLOOP_EGRESS_CAP = 512` is a hard-coded const with no
  env override — unlike every neighbouring streaming dial — and is a per-JOB
  budget shared by co-located subtasks, so headroom is 512/parallelism. The
  Prometheus counter also increments by 1 per overflow *event*, not by the
  number of batches dropped.
- **Conformance staged coverage.** The "remote" server is built by
  `make_flight_sql_server()` → `FlightExecutionHost::from_env()` → *embedded*,
  and every corpus query takes `InProcessCluster`'s inline fast path. The
  transport, unparser and table-shipping seams are genuinely covered; the
  partial/final aggregate split and cross-task sort merge named in the CORPUS
  comment are **not**. Either soften that comment or stand the server on the
  coordinator backend.
- **Not re-run this session:** pod-kill proof of the F8/`70b535c` restore
  redistribution, and a live Kafka `registry-connector:` run-loop test.

Gates on every commit: `just lint`, per-crate `cargo test`, `cargo fmt --all`.


---

## §37 — the §36 residuals, and one fix an existing test refused

Two commits: `5ab3565` (F17), `d98e432` (F18).

### F17 — the egress dials, and saying what a stop costs

- **`KRISHIV_RLOOP_EGRESS_CAP`.** The cap was a hard-coded `const` while every
  neighbouring streaming dial took an env override. It is a durability dial in
  everything but name — it bounds how much computed output a slow drain
  consumer loses before catching up — and it is a per-JOB budget shared by
  co-located subtasks, so real headroom is cap/parallelism. Zero is refused
  along with garbage: a 0 cap discards every batch on staging, silently turning
  the job into a no-op. The env-registry and reference-doc guards both caught
  the declaration being missing, which is exactly what they are for.
- **The drop metric counted events, not batches.** `inc_output_buffer_flush`
  added 1 per overflow regardless of size, so it under-reported worst exactly
  when the loss was largest. Added `add_output_buffer_flush(reason, n)`.
- **A stop said nothing about what it discarded.** The teardown neither flushes
  nor snapshots. Not force-flushing is *correct* — `flush_all` is scoped to an
  exhausted bounded source and forcing it would emit partial aggregates as
  complete — so the fix is to say so: at registration (a run-loop job without
  checkpointing has no savepoint and therefore no non-lossy stop at all) and at
  stop (only when state is actually open). `has_open_windows` is built on
  `peek_snapshot_bytes`, not `self.operator`, because a job that restored a
  checkpoint and stopped before its first batch has no operator and its entire
  state to lose — the same lazy-init hazard behind two of F10's defects.
- **The conformance CORPUS comment claimed coverage that does not exist.** It
  named the partial/final aggregate split and the cross-task sort merge as its
  rationale, but the server is `from_env()` → embedded and every corpus query
  takes the inline fast path, so those run on neither side. The transport,
  SQL-text encoding, inline-IPC shipping and unparser *are* covered; the
  comment now says exactly that and names what closing the gap needs.

### F18 — and one fix backed out, by a test that was right

Per-incarnation counters are retired on teardown (reusable job ids otherwise
hand a dead job's drop count to its successor).

The egress buffer is **deliberately not**. I removed it too, reasoning that the
`CancelTask` handler's removal races the loop it is cancelling — which is true,
and the re-created entry can outlive the job.
`run_loop_parallel_three_matches_parallel_one` then read 0 windows where it
expected 60. **The test was right.** A run-loop only stops via cancellation, so
that teardown IS the stop path, and windows the job genuinely emitted are
output a consumer may still drain; the cure destroyed real data. Whether a
drain may follow a stop is the DUR-5 contract question §34 deferred, and
settling it as a side effect of a leak fix would be deciding it by accident.
Code and test both record the rejected idea and why.

Second time this session an existing test refused a change of mine and was
right (F10 was the first). Running the full suite, not just the new tests, is
what catches it.

### Still open

- **No final checkpoint on a clean run-loop stop.** The loss is now stated at
  both registration and stop, but a job registered without checkpointing still
  has no recoverable stop. The real fix is a self-initiated final checkpoint
  delivered through the existing ack path — which needs an epoch the
  coordinator will accept, i.e. a protocol contract, not a patch. Writing
  snapshot bytes nobody reads would reproduce F15's exact failure.
- **Drain-after-stop contract (DUR-5).** Now load-bearing: F18 shows a real
  test depends on the current permissive behaviour.
- **The failed-launch job-id wedge** (§34) — still open, but no longer a
  guess. Both obvious fixes were built and measured (§38): keying the launch on
  "already launched" is necessary and insufficient (the retry re-enters the
  launch, then fails with "produced no launchable assignments", because the
  first attempt already moved its tasks out of Assigned); rolling the
  registration back works but breaks four tests, two of which are this crate's
  ONLY coverage of run-loop shape-building and convergence — they read the job
  record after a launch that necessarily fails, because the fixture registers
  `IN_PROCESS_TASK_ENDPOINT` and a launch against it can never succeed. The
  real requirement is a coordinator primitive returning tasks to Assigned, or a
  fixture with a dispatchable executor. The in-code comment now records this.
- **Conformance staged coverage** — needs the coordinator backend, i.e. a real
  multi-executor harness.
- **Not run this session:** pod-kill proof of the F8/`70b535c` restore
  redistribution, and a live Kafka `registry-connector:` run-loop test.

Gates on every commit: `cargo clippy -D warnings`, per-crate `cargo test`,
`cargo fmt --all`.


---

## §38 — the failed-launch wedge: two candidate fixes, both measured, neither shipped

No commit changes behaviour. The value here is that the §34 note stopped being
a guess: I built both obvious fixes, ran them, and recorded what each does.

**Candidate 1 — key the launch on "has this job actually been LAUNCHED".**
Added a `run_loop_launched` set on the coordinator, cleared on both retirement
paths, so a retry is not swallowed by the "already running what you asked for"
no-op. It is necessary and it is not sufficient: the retry *does* re-enter the
launch, and then fails differently — `produced no launchable assignments` —
because the first attempt had already transitioned its tasks out of Assigned
before dispatch failed. **Retryability needs a clean slate, not permission to
try.** That is the fact the original note was missing.

**Candidate 2 — roll the registration back on failure.** Cancel, evict, drop
the snapshot, clear the mark, so the next attempt is a genuine fresh submit.
This works, and it broke four existing tests. Two of them —
`run_loop_registration_builds_parallel_subtasks` and
`run_loop_reregistration_is_convergent` — are this crate's only coverage of
run-loop shape-building and convergence, and they work by inspecting the job
record *after* a launch that necessarily fails:
`make_coordinator_with_executor` registers `IN_PROCESS_TASK_ENDPOINT`, and a
launch against it can never succeed. Rollback deletes the record they read.

So candidate 2 trades a rare, recorded wedge for permanently losing the only
test coverage of the registration path. That is the objection the original
note raised, and it turns out to be right for a sharper reason than it stated.

**Reverted both.** What this actually needs is one of:
- a coordinator primitive returning a job's tasks to Assigned, after which
  candidate 1 works and the job record survives for the tests; or
- a test fixture with a dispatchable executor, so a launch can succeed and the
  coverage stops depending on the failure path.

Both are real work. Shipping candidate 2 to close the item would have been
trading a known bug for an unknown one — the tests would have been "fixed" by
rewriting them around the missing record, and nothing would then notice if
shape-building or convergence regressed.

The in-code comment at the launch site now carries this evidence, so the next
attempt starts from it.

---

## §39 — Streaming duplication: the architecture, in eight steps

A deep review of duplicated streaming implementations, then the eight-step
procedure it produced. Commits `a22240f`, `79aa93d`, `9f02293`, `95a866b`,
`e95af21`, and the steps 6–8 commit.

### What the review actually found

Four independent end-to-end reads of the driver loops against one fixed
twelve-axis rubric. Of twelve decision axes, **exactly one — operator stepping
— was shared across all four loops.** That is the axis people point at when
calling this a single streaming core. Meanwhile `egress` and `error_handling`
were local implementations four times over.

Three confirmed silent wrong answers, each verified by a skeptic instructed to
refute it by default:

1. **`eos-flush-missing-on-both-distributed-loops`.** `ExecutionMode::Distributed`
   can only construct `RemoteExecutionRuntime`, which inherited the trait's
   defaulted flush. `connector_runtime.rs` caught the resulting `Unsupported`,
   logged a warn, and returned `Completed`. A bounded windowed job was short one
   row per group with no other sign.
2. **`cycle-has-no-idle-watermark-tick`.** Session windows can never close on
   the DEFAULT registration mode. Task #120 had closed the embedded/run-loop
   pair; the cycle model still had the gap.
3. **`run-loop-egress-drops-oldest`.** The only loop whose egress is lossy, plus
   a false alarm on top: the buffer is filled unconditionally *before* the sink
   dispatch, so a durable-sink job reported permanently nonzero drops and the
   coordinator escalated a complete output to "incomplete".

Plus one behavioural split — `coercion-only-on-run-loop` — where the same job
ran on one placement and aborted with `ts must be Int64` on the others.

### The correction that matters most

I had previously reported the flush seam as "fails closed rather than silently
truncating", on the strength of the trait default being an error. That was
wrong at the system level: the trait failed closed and the caller converted it
straight back to `Completed`. **Naming a loss in a log line the caller never
reads is not reporting it** — the same defect F15 found, wearing different
clothes. Worth remembering as a review habit: tracing a guard to its definition
is not the same as tracing it to its caller.

### The mechanism, and its honest ceiling

`StreamingLoop` is a closed enum; `policy()` and `ordinal()` and `name()` are
exhaustive matches; `DriverPolicy` has no `Default`, no `#[non_exhaustive]`, no
builder, and deliberately no `Test` variant. Both halves were run against rustc:
an incoherent policy fails with `E0080` naming the rule, a sixth variant fails
with three separate `E0004`s.

**It gates adding LOOPS, not inventing DECISIONS.** Someone who implements a
genuinely new axis in one loop and never adds a `DriverPolicy` field is not
caught. The only counterweight is the cross-loop corpus: a new decision that
changes output shows up there, and one that does not change output was arguably
not an axis. There is also a smaller residual hole documented on
`VARIANT_COUNT` — a variant added to the enum and all three matches but to
neither `ALL` nor the count is never coherence-checked. Closing it needs a
derive macro and is not worth one.

### Proofs, all executed rather than asserted

| What was reverted | What went red | What stayed green |
|---|---|---|
| Cycle `idle_tick` → `WallClock` | build, `E0080` | — |
| A sixth `StreamingLoop` variant | build, 3× `E0004` | — |
| `on_stop` flush arm (in krishiv-dataflow) | driver unit test **and** the krishiv-api conformance arm | — |
| `Timestamp` coercion arms → `None` | `Timestamp(Millisecond) != Int64` | — |
| Flush tag → `CONTINUOUS_DRAIN` | tag identity test | — |
| `stream-eos:` read → `if false` | the EOS test, `left: [] right: [(a,30),(b,5)]` | both no-directive tests |
| `on_idle` → early return | the run-loop session test | the cycle half |
| `requires_wall_clock` → not Session | the cycle-refusal test | the scope test |
| `has_durable_sink_contract` → false | the durable-sink test | the egress-cap test |

The third row is the one that matters structurally: a one-line change in
`krishiv-dataflow` turned a test red in `krishiv-api`. That cross-crate link is
exactly what did not exist when `dd47d50` and `8756b41` fixed the same bug weeks
apart.

### What this did NOT fix

- **The loops are not unified, and were never going to be.** Source polling,
  checkpoint cadence, barrier alignment, key-group routing, peer forwarding and
  restore handling stay per-loop and untyped. Roughly two thirds of each loop
  body is untouched and can still drift.
- ~~**No end-to-end coordinator arm.**~~ **CLOSED** — see §39a.
- **Cycle mode still has no wall clock.** Step 7 refuses session-in-cycle
  rather than fixing it. Inherent: a cycle task exists for one invocation, so
  adding a timer means putting wall clock in the control plane.
- **Egress buffers remain four different things.** Only the accounting is
  shared; the buffers are genuinely different mechanisms.
- ~~**Four leads were raised and never verified.**~~ **CLOSED** — all four
  resolved in §39a: two real defects fixed, one recorded as an axis, one
  refuted.

### Behaviour break

A Distributed bounded streaming job that reported `Completed` with a short
answer now **fails**. Correct direction, but it needs a release note.
`KRISHIV_ALLOW_UNFLUSHED_BOUNDED=1` opts back in, and the error names the flag
so an operator has a move rather than just a refusal.

---

## §39a — Closing the two things §39 left open

### The four capped leads, resolved

The audit's verification fan-out was capped at four, leaving four claims
unverified. All four were taken to the code.

**`checkpoint-model-four-ways` — CONFIRMED, real defect, fixed.** Not the
"four models" framing, which is a legitimate lifecycle difference, but the
swallow buried inside it. Two sites wrote

```rust
.ok().filter(|bytes| !bytes.is_empty())
```

which maps `Err(..)` and `Ok(vec![])` to the same `None`. Downstream `None`
means "ship no checkpoint", so a task whose state capture FAILED reported
success, shipped nothing, and left the job's restore point stale — a recovery
then replays from an older epoch and reprocesses, with nothing anywhere having
said so. Empty and failed are now distinct (`classify_snapshot` →
`SnapshotOutcome`), the failure carries its cause, and the classification lives
in one place instead of two. Revert-proven: collapsing `Failed` into `Empty`
fails `a_failed_snapshot_is_distinguishable_from_an_empty_one`.

**`stream-profile-disjoint-halves` — CONFIRMED, honesty defect, fixed.**
`StreamProfile`'s doc claimed `Throughput` means "micro-batch before draining
AND checkpoint less often". Measured: `stream_linger()` is reached only by the
run-loop, `checkpoint_every` only by the embedded continuous loop. **No single
placement applies both halves**, so `KRISHIV_STREAM_PROFILE=throughput` does
something materially different depending on where the job landed. The doc now
carries the measured table, and
`each_profile_half_is_reached_by_exactly_one_placement` pins it by scanning the
workspace — brittle on purpose, so wiring either half into the other loop forces
the table to be revisited.

**`null-key-poison-policy` — CONFIRMED as a divergence, recorded as an axis.**
`extract_agg_key` returns `InvalidInput` on a NULL key and every operator
propagates it, so "fatal" is the operator's default and the run-loop is the only
loop that intercepts (`split_null_key_rows` + a counted drop). Neither side was
a decision anyone wrote down — one loop grew an interceptor and the rest
inherited the default. Now a `NullKey` axis on `DriverPolicy`, behaviour
unchanged, with a coherence rule that a transient loop may NOT declare
`QuarantineAndCount` because it cannot accumulate a count across invocations —
and an uncounted quarantine is the one thing quarantining must never be.

**`run-loop-second-watermark-implementation` — REFUTED.** `SplitWatermarks`
feeds `report_streaming_progress` only: the watermark *reported to the
coordinator* for cross-subtask min-combining. It never reaches the operator,
whose watermark still comes from the shared `advance_effective_watermark` via
`ContinuousWindowExecutor::drain`. Different purpose, not a duplicate — and the
run-loop is the only loop with multiple subtasks, so it is the only one that
needs a cross-split combine at all. Residual: `batch_max_event_time` and
`watermark_util::max_event_time_ms` are two implementations of "max event time
in a column" differing only in `Timestamp` support. Minor, recorded, not fixed.

### The coordinator arm now exists

`crates/krishiv-executor/tests/coordinator_eos_conformance.rs` stands up **two
real gRPC servers** in one process — the coordinator's executor-facing server
and the executor's task server on real TCP ports — plus a runner drive loop, and
drives register → push → drain → flush end to end.

It had to live in `krishiv-executor`: that crate already dev-depends on
`krishiv-scheduler`, and adding the reverse dev-dep would be a cycle.

Three things it taught, all of which had been invisible:

1. `ExecutorDescriptor`'s registration endpoint and its **task** endpoint are
   separate fields. Omitting the second gives "has no task endpoint for
   assignment push".
2. The coordinator's executor gRPC **denies anonymous by default**, so every
   task-status report failed `UNAUTHENTICATED` — and a runner that cannot report
   is indistinguishable from one that never ran. The drive loop now surfaces
   that error instead of sleeping through it.
3. **The first version of this test passed vacuously**, and caught itself only
   because the corpus carries a partial-loss fixture. Written against
   `trailing_window_never_closed_by_watermark`, whose `expected_without_flush`
   is empty, the drain assertion could not tell "the cycle ran and closed
   nothing" from "the cycle never ran" — and the cycle had in fact never run.
   Switching to `closed_window_plus_trailing_window`, whose
   `expected_without_flush` is `[("a",30)]`, made the failure immediate. That
   entry exists in the corpus for exactly this reason, and it earned its keep on
   the first test that needed it.

Revert-proven: replacing the `stream-eos:` read with `if false` fails this test
with `left: [] right: [("a", 7)]`. **Step 5's coordinator fix is now
demonstrated rather than reasoned.**

One semantic the test records: the coordinator's inline result store is
consume-once, so a drain that already took the watermark-closed window leaves
the flush returning only the trailing one. The job's whole answer is the union,
which is what a caller must assemble.

---

## §40 — The streaming API surface: SQL, Python, Rust

A three-surface audit against one fixed ten-axis rubric, then the fixes.
Commits `808da36`, `fe99ae5`, `0021e01`.

### The structural finding

The three surfaces are **not three streaming implementations**. They are three
lossy translators into one 14-field `WindowExecutionSpec`, which is the sole
contract between "what the user asked for" and "what any loop runs". Exactly
three production sites build it from user intent — one per surface. Everything
below them is genuinely single.

Every confirmed defect reduces to one sentence: **a field of the request never
reached the spec.**

That is a different axis from the eight-step rework, which de-duplicated the
*when-to-step* decisions behind the driver loops. This is *what-to-run*.

### Eight confirmed, two refuted

| id | what | fixed |
|---|---|---|
| f1 | SQL compiler dropped WHERE / GROUP BY / HAVING / LIMIT / DISTINCT | ✅ fail closed |
| f2 | two registration paths, one wall-clock guard | ✅ guard at the shared funnel |
| f6 | distributed streaming dropped the job's sink | ✅ fail closed |
| f9 | flush verb complete and unreachable from Session | ✅ surfaced + bound in Python |
| f4 | `write_stream()` discarded the whole pipeline | ✅ writer carries it |
| f5 | `drop_duplicates` on output, and skipped under a side output | ✅ source-side, one helper |
| f7 | `temporal_join` accepted `join_keys` and ignored them | ✅ keyed via shared encoder |
| f3 | `key_column_type` hardcoded `"utf8"`, no setter anywhere | ❌ **open** |
| f8 | `output_rows` fabricated | REFUTED — inverted: `input_rows` is the liar |
| f10 | no SQL reaches the run-loop | REFUTED — a different compiler is its front door |

### Three things worth remembering

**f1's failure output is the argument.** With the guards off, the test prints
the spec that would have run: no trace of the predicate, and
`key_column: "region"` with `product` gone. `WHERE amount > 100` counted every
row. The engine reads the raw source directly and never executes the query
text, so a clause not rejected at compile time has no second chance.

**f7 was already fixed twenty lines away.** The interval join carries an
`H-1 (audit)` comment describing the identical unkeyed-state defect, repaired
then. The temporal join was the remaining instance, in the same file. Same
shape as dd47d50/8756b41.

**Two of my own tests passed against the bug before they caught it.** The f5
test deduped on the window key and asserted a row count — both the fixed and
broken code produce one row there. The f7 revert simulated the bug as "two
states, pick any", which HashMap order happened to answer correctly; the
accurate revert (one unkeyed state, second customer overwriting the first)
fails with `left: 99, right: 10`. A revert that does not reproduce the original
mechanism proves nothing.

### The corpus's missing axis — my own gap

`streaming_corpus.rs` varies event **timing** across arms over ONE fixed query:
a string key, no `WHERE`, one grouping column. Its axis is the loop. Every
finding above is on the orthogonal axis — query shape → spec — so the repo's
strongest test asset is **structurally blind to f1 and f3**, not by oversight
but because it has no second dimension.

Adding one (BIGINT key, top-level `WHERE`, multi-column `GROUP BY`, session
window) is cheap and is the highest-value remaining work, because it makes this
class visible to the arms that already exist.

### Open

- **f3** — the last confirmed finding. `key_column_type` is `"utf8"` at all
  three spec sites with no setter, and drives both the operator's output schema
  and the input cast: a `BIGINT` key emits a Utf8 column and a bigint-declared
  sink receives strings. Fix is to infer from the source schema with the
  declared value as an override.
- **The corpus query-shape axis**, above.
- **f6's better half.** The refusal shipped; forwarding the sink is the real
  answer, and the coordinator and executor have accepted registry sinks since
  #197 — only the client leg cannot express it.
- **`windowed()` as a partial constructor.** It takes 4 of 14 fields and leaves
  10 to optional setters, so both callers independently picked the same 4 and
  both omit `with_allowed_lateness_ms` (zero non-test callers repo-wide). A
  helper everyone reaches that still lets each caller be incomplete — the
  codebase's own defect shape, inverted.
- **Five user-facing loops outside the `StreamingLoop` gate**: three writer
  loops and two PyO3 loops written in the binding layer, choosing their policy
  implicitly by the order of two for-loops.

## §41 — The NEXMark harness, and the three bugs it found before its first number

The streaming benchmark was built to measure, and it did — but it earned its
keep before printing a single figure. Three defects fell out of it, all one
shape: **the engine had only ever been fed `Int64`.**

Every fixture in the tree, including my own conformance corpus, builds its
columns as `Int64` and `Utf8`. NEXMark's standard `Bid` schema does not: ids
and prices are `UInt64`. That single change of source type broke three separate
places in sequence, each surfacing only once the previous was fixed.

| # | Site | Symptom | Fix |
|---|---|---|---|
| 1 | `aggregate.rs::eval_compare` | `WHERE price > 5000` → "cannot compare column 'price' of type UInt64 against literal Int(5000)" | widened the numeric arm to all int/uint/float widths |
| 2 | `join.rs::extract_agg_key` | `GROUP BY auction` → "unsupported group key type: UInt64" | new `AggKey::UInt64` variant |
| 3 | `stream_driver.rs::is_numeric_agg_type` | `MAX(price)` → "unsupported column type for pre-downcast: UInt64" | removed unsigned from the "no coercion needed" set; coerce to `Int64` |

**#3 is the interesting one**, for two reasons.

First, it was an *internal contradiction*: `is_numeric_agg_type` said `UInt64`
was an acceptable aggregate input, and the operators' pre-downcast said it was
not. Two statements of the same fact, disagreeing — this register's most
frequent defect shape, found again in code I wrote three sections ago.

Second, the obvious repair is wrong. The default coercion target is `Float64`,
which is exact only below 2^53. A `u64` price above that would have been
**silently rounded** — and the cast does not error, because `cast_with_options`
considers u64→f64 a valid conversion regardless of precision loss. So unsigned
coerces to `Int64` (exact below 2^63) instead. The regression test asserts the
value `2^53 + 1` survives, not merely that the type changed; reverting the
target to `Float64` fails it with `left: Float64, right: Int64`, and reverting
the exactness check alone would have left the bug invisible.

The same reasoning applies to #2: casting unsigned keys to `Int64` would send
`u64::MAX` and `u64::MAX - 1` to distinct negative values (safe), but casting to
`Float64` would round **both to 2^64 — merging two groups into one.** Hence a
dedicated key variant rather than a cast. The test asserts non-aliasing.

**What this says about the corpus.** §40 recorded that my conformance corpus
varies event *timing* over one fixed query shape, so it was blind to query-shape
defects (f1, f3). This is the second blind axis: it also varies only over one
fixed *column type set*. Neither axis was chosen; both were inherited from the
first fixture written. A corpus fixed on two axes at once is a corpus that
proves the engine works on exactly one workload.

None of these three were found by 5,448 passing tests. They were found by the
first workload from outside the codebase.

### The harness itself

`crates/krishiv-bench/src/{nexmark.rs, bin/nexmark_stream.rs}`. Faithful
generator (splitmix64, standard schema, the canonical 1:3:46 person/auction/bid
proportions, configurable out-of-orderness). It drives through
`StreamDriver::on_input`, **not** `ContinuousWindowExecutor::drain` — the first
draft called `drain` directly and thereby skipped input typing entirely, which
is both how bug #3 hid and a reminder that `drain` alone is not the production
path.

Coverage is **4 of 22 queries** and the harness says so on every run rather than
implying completeness. First numbers, single-node, operator-level:

| query | events/sec | p50 µs | p99 µs | p99.9 µs |
|---|---|---|---|---|
| q2_filtered_bids | 9.9M | 64 | 219 | 728 |
| q5_hot_items | 9.7M | 37 | 392 | 804 |
| q7_highest_bid_keyed | 10.0M | 66 | 266 | 286 |
| q11_user_sessions | 15.1M | 48 | 111 | 200 |

**A completeness gate is not optional here** and runs on every query: this
engine's run-loop egress buffer drops its *oldest* batches at a cap, so a
throughput benchmark that ignores output would measure how fast the engine can
discard data and would score *better* the more it lost.

These numbers are not comparable to published Flink/Spark NEXMark results and
must not be quoted as if they were — those measure a distributed system end to
end through a source connector, sustainable-rate-searched per Karimov et al.
(ICDE 2018). This is one in-process operator chain fed from memory. What it is
good for is regression detection and relative comparison against itself.

## §42 — A3/f3: the key type nobody declared, and the benchmark that could not measure

### The fix (task #134)

`key_column_type` defaulted to `"utf8"` and the SQL compiler hardcoded
`"utf8"`, so **every SQL-planned streaming query emitted its key as a string.**
`GROUP BY auction` over a `BIGINT` produced a Utf8 column of digits: a sink
declared bigint received text, and anything sorting the key got lexicographic
order, where `100` precedes `20`.

The defect is representational, and it is this register's most frequent shape:
`"utf8"` meant both *"the user declared utf8"* and *"nobody said"*. One
spelling, two meanings, so the declaration could never be acted on. The fix
gives absence its own spelling — `"auto"` — and resolves it from the source.

**Where it resolves is the whole point.** `ContinuousWindowExecutor` builds its
operator lazily, on the first batch, and already inferred `agg_is_float` there
with this comment:

> *"so that `agg_is_float` reflects the actual aggregate input types instead of
> hardcoding `false` (which silently truncates Float64 to Int64)"*

The identical inference, for the identical reason, sitting three lines from a
key type that stayed hardcoded. The lesson has been learned once already in
this exact function and not generalised.

That block existed **twice** — in `drain` and in the checkpointing path,
near-identically. Rather than add the inference to one copy (this register's
recurring sibling-defect shape: dd47d50/8756b41, interval vs temporal join, S7,
both engine routing sites), both were replaced by one `ensure_operator`.

Resolution is conservative on purpose: an explicitly declared tag is an
override and is never touched, and an unresolvable column (absent, or a type
with no tag) stays `"auto"` and falls back to the historical `Utf8` — so no
query that ran before fails now.

Proven red by neutering the single line that adopts the source tag:
`left: Utf8, right: Int64`. Reverting the *default* instead only tripped the
test's premise assertion — a weaker proof, and worth recording as the
difference between reverting the fix and reverting near the fix.

### The benchmark could not have told me whether this helped

Rerunning NEXMark after the fix showed throughput apparently dropping. It had
not. Five back-to-back runs of the *identical binary* gave q7 anywhere from
6.7M to 11.7M events/sec — a **74% spread**. The harness reported one run and
no variance, so its numbers could not distinguish a regression from noise, and
the first before/after comparison I drew from them was meaningless.

This is the failure Karimov et al. (ICDE 2018) name directly, and it is the
same class as everything else in this register: **a measurement that cannot
fail is worth no more than a test that cannot fail.** A benchmark reporting one
run is asserting reproducibility it never checked.

The harness now runs 1 warm-up + 5 measured repetitions, reports the **median**
(a mean lets one descheduled run vanish into the average), prints the observed
**min–max spread on every row**, and pools per-batch latencies across
repetitions rather than averaging per-run percentiles — the average of five
p99s is not a p99 of anything. It also **asserts every repetition emits the
same row count**: identical input must give an identical answer, and if it does
not, no throughput number from the run means anything.

Measured spreads on this machine are 6%–68%, which is itself the finding: this
host runs other workloads, and any single-run number taken from it — including
the four I reported in §41 — should be read as indicative only.

| query | ev/sec (median of 5) | min–max | spread | p50 µs | p99 µs |
|---|---|---|---|---|---|
| q2_filtered_bids | 11.2M | 6.5M–12.2M | 51% | 79 | 297 |
| q5_hot_items | 6.1M | 4.9M–9.1M | 68% | 67 | 582 |
| q7_highest_bid_keyed | 9.6M | 8.9M–10.7M | 19% | 81 | 261 |
| q11_user_sessions | 11.5M | 10.9M–11.6M | 6% | 69 | 161 |

Row counts were **identical before and after the key-type fix** (898 / 7361 /
1481 / 1403), which is the real evidence that the change corrected output types
without disturbing grouping.

## §43 — Correction to §42, and two more dropped clauses

### Correction: §42 fixed one of two sites

§42 claimed the SQL-planned key type was now inferred. **It was not.**
`key_column_type` is built in two places, and I fixed the wrong one first:

| site | fixed in | reached by |
|---|---|---|
| `krishiv-plan/src/window.rs` (constructor + serde default) | §42 | hand-built specs, tests |
| `krishiv-sql/src/streaming_window_plan.rs:114` | **§43, here** | **every SQL query a user writes** |

The §42 test passed because it built its spec with
`WindowExecutionSpec::tumbling(...)` — the constructor — and never called
`compile_streaming_window_sql`. It proved the half I had fixed.

This is the sibling-defect shape this register has now recorded five times
(dd47d50/8756b41, interval vs temporal join, S7, both engine routing sites,
and the two lazy-init blocks in §42 itself). I wrote that paragraph in §42 and
then committed an instance of it in the same change. Worth stating plainly:
knowing the pattern does not detect it. Only a test that crosses the seam does.

**Why no unit test could have caught it.** `krishiv-sql` compiles specs but
never runs an operator; `krishiv-dataflow` runs operators but builds specs by
hand. The defect lives precisely in the seam between them, so the regression
test now lives in `krishiv-bench/tests/streaming_sql_key_type.rs`, which
depends on both: SQL text in, emitted Arrow type out. Proven red — the
end-to-end case fails `left: Utf8, right: Int64`.

### Two more clauses the compiler dropped (found by probing, not reading)

Rather than guess which NEXMark queries the engine supports, I compiled eleven
candidate shapes and looked at the resulting spec. Multi-key `GROUP BY` and
`HAVING` already failed closed with good messages (the A1 work). Two did not:

**1. `COUNT(DISTINCT bidder)` — a silent wrong answer.** It compiled to a plain
`Count` with the DISTINCT discarded. Verified against the running operator:
three bids from **one** bidder returned **3**. A user aggregating distinct
users got a total event count, in a column they had named for distinct users,
with no error. Refused now: real deduplication needs per-window key-set state
with its own memory bound and checkpoint format, and until that exists,
refusing is the only honest answer.

**2. `SUM(price * 908 / 1000)` — NEXMark Q1's currency conversion.** The
aggregate argument match had a `_ => {}` arm that swallowed any expression,
leaving `input_column` empty. It then failed at *operator construction* with
"Sum window aggregate requires a non-empty input_column" — a complaint about an
internal field, raised long after the compiler had seen the real problem and
thrown it away. Now refused at compile time, quoting the expression, and the
test asserts the message does **not** contain `input_column`: an error about
the user's SQL, not about our struct.

Both are the A1 class (§39: a clause the compiler cannot honour is an error,
never a silent omission). A1 fixed the clauses I had thought to look for;
these two needed the engine to be pointed at a workload from outside.

### The current, measured streaming SQL surface

| shape | status |
|---|---|
| `TUMBLE` / `HOP` / `SESSION` TVF | supported |
| single-column `GROUP BY` + window bounds | supported |
| multiple aggregates in one query | supported (2+ verified) |
| `COUNT` / `SUM` / `MIN` / `MAX` / `AVG` / `STDDEV` | supported |
| `WHERE col <op> literal`, `AND`/`OR` | supported |
| `AGG(x) FILTER (WHERE …)`, `CASE WHEN` idiom | supported |
| multi-column `GROUP BY` | refused, names the dropped columns |
| `HAVING` | refused |
| `COUNT(DISTINCT x)` | refused (**was a silent wrong answer**) |
| aggregate over an expression | refused (**was a late internal error**) |
| `WHERE MOD(auction, 123) = 0` (expression predicate) | refused |
| joins, top-N / rank, dedup | not supported |

That table is what bounds NEXMark coverage at 4 of 22 — the remainder need
joins (Q3/4/8/9/20), multi-key grouping (Q15/16/17), rank (Q19), or dedup
(Q18), each a real feature rather than a parser gap.

## §44 — Pin the CPUs, or measure the host

The §42/§43 numbers were unstable enough to be useless for comparison, and the
cause was not the engine. This host runs the Phase-62 soak and two clusters:
load average ~7 of 12 cores, three `krishiv` processes at ~50% CPU each.

The evidence that unpinned runs cannot be compared at all is that the variation
**between** invocations exceeded the spread **within** one:

| query | unpinned medians (2 invocations) | pinned to 4 cores (2 invocations) |
|---|---|---|
| q2_filtered_bids | 11.2M, 5.8M | 7.4M, 5.8M |
| q5_hot_items | 6.1M, 11.9M | 10.9M, 12.3M |
| q7_highest_bid_keyed | 9.6M, 3.5M | **4.9M, 4.6M** |
| q11_user_sessions | 11.5M, 16.2M | **22.7M, 22.7M** |

Unpinned, q7 moved 2.7× and q5 moved 2× between invocations of the identical
binary. Pinned, q11 reproduces exactly and q7 to within 6%. q11 is also 40%
*faster* pinned (22.7M vs 16.2M) — the contention was costing throughput, not
just adding variance.

`taskset -c 8-11` is now the documented invocation. The harness says so in its
own header, with the measurement behind it, because "run it pinned" without the
evidence is the kind of advice that gets dropped the first time someone is in a
hurry.

**Reference numbers (pinned, median of 5, 1 warm-up):**

| query | ev/sec | p50 µs | p99 µs |
|---|---|---|---|
| q2_filtered_bids | 5.8M–7.4M | 125–137 | 277–360 |
| q5_hot_items | 10.9M–12.3M | 25–26 | 334–373 |
| q7_highest_bid_keyed | 4.6M–4.9M | 170–188 | 446–502 |
| q11_user_sessions | 22.7M | 34 | 62 |

Ranges, not points: two pinned invocations, and the honest summary is the
interval they fall in.

### Open, deliberately not claimed

q7's p50 rose from ~81 µs (measured before the key-type fix, **unpinned**) to
~180 µs (after, **pinned**). Those two numbers are not comparable — different
method — so this is not evidence of a regression, and I am not recording one.

There is a plausible mechanism worth testing: the window operators key on a
`&str` internally, so a now-correctly-typed `UInt64` key must be formatted to a
string per row, where before it was cast once per batch by a vectorized Arrow
kernel and read as `&str`. Against that, q11 has the same key type and got
*faster*. Settling it needs a pinned A/B across the two commits, which is
task #135 — not a paragraph of reasoning.

## §45 — Aggregates over expressions (task #137, NEXMark Q1)

§43 refused `SUM(price * 908 / 1000)` because `WindowAgg::input_column` is a
column *name* and an expression has none. Refusing stopped the silent
omission; it did not make the query work. This implements it.

**The shape of the fix is a pre-window projection.** A new tiny, serializable
scalar IR (`WindowScalarExpr`: columns, int/float literals, `+ - * / %`) lives
in krishiv-plan beside `WindowAggFilter`, for the same stated reason — the
dataflow crate has no SQL parser and must not grow one. The SQL compiler lowers
an expression argument into a `DerivedColumn` named `__krishiv_expr_N`; the
operator materialises it before grouping; the aggregate names that column. The
spec's contract is unchanged: a window still only ever aggregates a named
column.

**Integer semantics are deliberate.** `common_type` promotes to `Float64` only
when an operand is already float; otherwise the computation is `Int64` and
division truncates. That is SQL's behaviour for integer operands and is what
Q1 expects — `2500 * 908 / 1000 = 2270`, not 2270.0.

**Where the sibling defect would have been.** `ensure_operator` is called from
both `drain` and `drain_transactional`. `drain` applies derived columns before
grouping; `drain_transactional` calls `ensure_operator` on the *raw* batch, so
an aggregate naming a derived column would have failed there while working in
`drain` — the same two-callers-drift shape as §42 and §43. Rather than patch
the second caller, `ensure_operator` now enriches its own probe batch, so a
caller passes the raw batch and *cannot* get the order wrong. Both call sites
are correct by construction, and a third would be too.

**The test that matters asserts a value, not a compile.** Q1 over prices
1000/2000/3000 must total **5448** (converted), where the old dropped-expression
behaviour totalled **6000** (raw). Proven red by simulating exactly that
regression — aggregate the expression's first column instead of the expression
— which fails `left: 6000, right: 5448`. A test that only checked "the query
compiles" or "a column came out" would have passed against the bug.

A second test asserts `__krishiv_expr_0` does **not** appear in the output
schema: an internal column that leaks is a wire-format change.

Only arithmetic lowers. `SUM(price || 'x')` is still refused by name — the A1
rule holds, and the surface grew by exactly what was implemented.

**Coverage is now 5 of 22** (q1, q2, q5, q7, q11), completeness gate PASS.
Pinned, Q1 runs at 4.1M events/sec (16% spread), the slowest of the five — it
computes two arithmetic kernels per batch before the window, which is real work
the others do not do.

## §46 — Why the last three features are not parser work

§45 landed #137 because it was genuinely a compiler change: lower an
expression, materialise a column, aggregate it. The remaining three refusals
look similar from the SQL surface and are not, and the difference is worth
recording so the next session does not mistake scope.

### #136 `COUNT(DISTINCT x)` — a checkpoint-format change

The obvious reading is "add a `WindowAggKind::CountDistinct` and a `HashSet`".
The obstruction is two levels down:

* `AggEntry` is `#[derive(Debug, Clone, Copy)]`. A `HashSet` removes `Copy`,
  which ripples through every operator that moves entries by value.
* `window/state_persistence.rs` encodes each `AggEntry` at a **fixed byte
  width** (`i64` + `u8` + `f64` + … , with the size named in a constant). A
  distinct-set is variable length, so this needs a format version and a restore
  path for checkpoints written by the current format.

That last point is the reason not to rush it: the Phase-62 soak (#41) is
running now with live checkpoints, and a format change that cannot restore them
turns a feature into an outage. The refusal shipped in §43 is correct in the
meantime — it returns an error rather than the wrong number it used to return.

Design when taken up: version the snapshot header; `distinct: Option<HashSet>`
behind `#[serde(default)]`; a per-window cardinality cap that **errors** rather
than silently under-counting; and a decision, recorded, on exact vs HLL.

### #138 multi-column `GROUP BY` — a key-representation change

`key_column: String` is singular through the spec, the operators, the output
schema, and the state key. The internal key is a `String`, so a composite needs
an unambiguous encoding (length-prefixed, not delimiter-joined — any separator
can occur in Utf8 data) plus `key_column_types: Vec<String>` to split it back
into N typed output columns.

The tempting shortcut — keep `key_column` and add `additional_key_columns` — is
the two-representations-of-one-fact shape this register keeps finding, and
should be refused. It is `key_columns: Vec<String>` or nothing.

Note also that ~39 spec literals would need updating. Three scripted sweeps
have mis-edited files in this session alone (§43's collateral, and twice here);
that tax is real and argues for a constructor rather than another sweep.

### #139 streaming joins — a wiring change, not a new operator

The operators already exist: `interval_join.rs`, `delta_join.rs`, and a
temporal join. What is missing is the streaming SQL surface reaching them —
`compile_streaming_window_sql` accepts a single `TABLE` source. **When wiring
these, check both siblings**: this register records interval-join and
temporal-join carrying the identical bug, fixed in one and left in the other,
with a comment in the second describing the defect it still had.

### Honest coverage statement

NEXMark coverage is **5 of 22** and the harness prints that on every run. The
17 remaining are not blocked on parsing:

| queries | blocked on |
|---|---|
| Q3, Q4, Q8, Q9, Q20 | #139 joins |
| Q15, Q16, Q17 | #138 multi-key |
| Q15 (also) | #136 COUNT(DISTINCT) |
| Q19 | top-N / rank operator |
| Q18 | dedup / row_number |
| Q0, Q10, Q12–Q14, Q21, Q22 | stateless or processing-time paths outside the window compiler |

## §47 — `COUNT(DISTINCT …)` implemented (task #136)

§43 refused it after proving it returned a wrong number; §46 recorded why it
was a checkpoint-format change rather than a parser change. Both were right,
and it is now implemented.

**The state lives beside `AggEntry`, not inside it.** `AggEntry` is `Copy` and
the hot loop depends on that. `AggState` gains `distinct: Vec<BTreeSet<String>>`
parallel to `entries` — empty for every other aggregate, so the cost is one
empty set per expression and the hot path for `COUNT`/`SUM`/`MIN`/`MAX`/`AVG`
is byte-for-byte what it was. `BTreeSet` rather than `HashSet` because the
encoder iterates it: ordered iteration makes two snapshots of identical state
byte-identical, which is what makes a snapshot comparable at all.

`entry.value` is kept in step with `set.len()` on every insert, so **every emit
path reads the distinct count with no change whatsoever** — no operator, schema
builder or array constructor needed touching.

**The format bumps only when it must.** v2 appends the sets after the
fixed-width entries and is written *only when a distinct set is non-empty*.
Every other query keeps producing byte-identical v1 checkpoints, so upgrading
does not rewrite state it did not change and a downgrade can still read it.
Three tests hold that line: v1 stays v1 (asserted on the exact payload length,
so a stray trailer would fail), v2 round-trips, and a truncated v2 trailer is
an **error** rather than a silently empty set.

**Merging is by union, never addition.** `fold_agg_states` combines partial
states; two contributions that each saw bidder 7 hold one distinct value
between them, not two. Adding the counts would over-count every value seen in
more than one contribution — silently. That arm is explicit, and the numeric
arm carries `unreachable!` for the same variant so a future edit cannot let it
fall through to addition.

**Cardinality is bounded and fails closed.** `COUNT(DISTINCT)` is the one
aggregate whose state grows with the *data* rather than the number of groups.
`MAX_DISTINCT_VALUES_PER_GROUP` errors on exceed. A count that quietly stops
counting is the defect class this register exists to remove; refusing to answer
is strictly better than answering wrongly.

Only `COUNT` takes `DISTINCT`. `SUM(DISTINCT x)` is a different aggregate over
a different multiset and is refused by name — inferring one from the other is
exactly the silent substitution this compiler does not make.

### The test that nearly passed against the bug

Four end-to-end tests assert the *number*, on fixtures where distinct-count and
row-count differ. Three failed correctly against the reverted mapping
(`left: 3, right: 1`). The fourth — distinctness across batches — **passed**,
and the reason is worth recording.

Its two batches reused the same timestamps. With `watermark_lag_ms = 0` the
watermark advances to the highest event time seen, so the second batch was
*late* and one row was dropped. A 4-row fixture silently became 3 rows, which
is exactly the number the test expected. Two unrelated bugs cancelled.

The fixture now gives later batches later timestamps, with the reason written
next to it, and fails against both the plain-count defect and a hypothetical
per-batch implementation (`left: 4, right: 3`).

This is the standing rule earning its place for the fourth time this session:
the test was written to catch a specific defect, ran green against that defect,
and only the revert exposed it.

## §48 — Multi-column `GROUP BY` (task #138, NEXMark Q15/16/17)

§46 said the shortcut of keeping `key_column` and adding
`additional_key_columns` is the two-representations-of-one-fact shape and must
be refused. It was, and this is the design that avoids it.

**There is still exactly one grouping key.** `GROUP BY auction, channel` lowers
to a *synthetic* key column, `__krishiv_key`, holding the encoded pair — built
by the derived-column machinery §45 added — plus `key_parts`, which says how to
expand it back on output. `key_parts` is a **presentation description**, not a
second key: every operator, state key and hash still sees one key column.
Single-column queries produce no composite and no derived column at all, so the
ordinary path is untouched.

**The encoding is length-prefixed, not delimiter-joined**, and that is the
difference between working and silently wrong. With a `:` separator,
`("a:b", "c")` and `("a", "b:c")` encode identically and two distinct groups
merge into one — with no error, which is this register's defining defect shape.
`"{byte_len}:{value}"` per part cannot collide, whatever the data contains.
There is a test for exactly that pair, and it is proven red by switching the
encoder to a delimiter join.

**Types resolve per part.** The synthetic key column is always `Utf8` (it holds
the encoding), so resolving only `key_column_type` would leave every part
`"auto"` and emit every grouping column as a string — the §42/§43 defect one
level down. Each part resolves from its own source column at operator build.

**Count windows fail closed.** They emit a single key column and cannot expand
a composite, so a composite key reaching one is an error rather than a window
publishing the encoded key nobody asked for.

### Proven red, twice, against two different defects

- Collapsing to the first key column (the original defect): `left: 2,
  right: 3` — `(auction, channel)` has three combinations where `auction` alone
  has two.
- Delimiter-joined encoding: the separator test fails, the length-prefixed one
  passes.

The first revert did **not** fail the separator test — grouping by `auction`
alone happens to give 2 rows there too. Each test needed the revert that
targets its own claim. That is the second time this session a test passed
against a revert aimed at a different line.

### Coverage

**8 of 22**, up from 5. Q16 and Q17 needed `COUNT(DISTINCT)` (§47) plus
min/max/avg in one query; Q15 needs the composite key. Pinned medians:

| query | ev/sec | rows out | p50 µs |
|---|---|---|---|
| q1_currency_conversion | 3.6M | 1481 | 238 |
| q2_filtered_bids | 6.1M | 898 | 142 |
| q5_hot_items | 11.7M | 7361 | 28 |
| q7_highest_bid_keyed | 4.1M | 1481 | 200 |
| q16_channel_statistics | 11.5M | 8 | 64 |
| q17_auction_statistics | 3.7M | 1481 | 200 |
| q15_bidding_statistics | 2.7M | 1777 | 272 |
| q11_user_sessions | 21.4M | 1403 | 35 |

q15 emitting 1777 rows against q17's 1481 is the composite key doing its job:
the same window split into more groups.

### A gap this surfaced

NEXMark Q15's canonical form groups by the reporting period *only* — no other
key. This compiler requires a grouping key in the SELECT list, so a
**window-only (global) aggregation** is not expressible; the variant above adds
`(auction, channel)`. The same gap blocks a global Q7. Recorded as its own item
rather than papered over.

## §49 — Streaming joins reach SQL (task #139)

§46 called this "wiring, not a new operator", and that was right. The operators
existed — `interval_join.rs`, `watermark_join.rs` wrapping it, and an executor
path in `aligned_join.rs`. **Nothing in `krishiv-sql` or `krishiv-plan` could
name one**, so the whole family was unreachable from a query.

**Syntax is the one users already write** — an equi-key plus an event-time band
in the `ON` clause, as in Flink and Spark:

```sql
FROM bid b JOIN auction a
  ON b.auction = a.id
 AND b.dateTime BETWEEN a.dateTime - 5000 AND a.dateTime + 5000
```

No new keyword was invented. `ON` terms are flattened, so their order does not
matter — a query that works should not stop working because two conjuncts were
swapped.

**`StreamingJoinSpec` lives in krishiv-plan**, beside `WindowExecutionSpec`, for
the reason that one lives there: `krishiv-sql` cannot depend on
`krishiv-dataflow`, and a spec is the only thing the two must agree on. The
`From` conversion to `WatermarkWindowJoinSpec` is the single place the
vocabularies meet, so they cannot drift into two descriptions of one join.

**What it refuses, and why each refusal is not a limitation being hidden:**

| shape | refused because |
|---|---|
| asymmetric band | the operator's window is symmetric; using one side runs a join the query did not describe, and only near the window edge |
| no time band | join state would be unbounded |
| no equi-key | every left row matches every right row in the window |
| `LEFT`/`RIGHT`/`FULL` | requires emitting rows whose partner may still arrive, which a bounded window cannot promise |
| 3+ streams | says how many it saw |
| `USING` / `NATURAL` | carries no time bound |

**Routing recognises the shape, not the validity.** `looks_like_streaming_join`
answers "is this a join" and deliberately not "is this join valid", so a
malformed join reaches *this* compiler and produces *its* error. Routing on
validity is precisely the f022220 defect, where a planner's reason was
discarded and replaced by a stateless-path parse error about something else.
There is a test asserting the router claims a query it will then refuse.

**Proven red** by making the conversion ignore the compiled window: the two
band-dependent tests fail `left: 1, right: 0`. The band test also asserts that
the *same* key *inside* the band does join — so a failure means the band, not a
broken key comparison.

### Not done, and not implied

The SQL surface, the plan spec, the conversion and the operator are wired and
tested end to end. **Job-level routing of a two-source streaming job through
`StreamingEngine` is not.** That engine builds one `ContinuousWindowExecutor`
from one source; a join needs the two-input path in `aligned_join.rs`, which is
a separate change with its own barrier-alignment and checkpoint questions. A
NEXMark Q3 harness arm also needs person/auction generators, which the current
generator does not produce — it emits bids only.

So: joins are expressible and executable, and are **not** yet runnable as a
submitted job or counted in NEXMark coverage, which stays at **8 of 22**.

## §50 — #135 settled: the typed key DID cost throughput, but the mechanism was a per-row cast kernel, not string formatting

The pinned A/B (one variable: `krishiv_dataflow_key_type_auto()` toggled to
`"utf8"`, the exact revert that proved §43's test red; identical harness,
taskset -c 8-11, median of 5) measured the typed key at **2–2.5x slower** on
every UInt64-keyed tumbling query: q7 4.1M vs 10.4M, q2 4.9M vs 10.8M, q1 3.7M
vs 7.2M, q17 4.0M vs 8.4M ev/sec — far outside the run spreads. The suspected
mechanism (per-row integer-to-string formatting) was wrong. The real one:
`extract_agg_key`'s unsigned arm called `arrow::compute::cast(col, UInt64)` —
on the WHOLE COLUMN, once PER ROW. Even arrow's same-type fast path allocates
a fresh ArrayData wrapper per call; ~135ns x 1000 rows/batch matched the
observed p50 delta (206us vs 71us) almost exactly. The old utf8 world never
hit that arm because the driver had already cast the key column once,
vectorized.

Fix: direct per-width downcasts (UInt8/16/32/64 each widening `as u64`), no
cast kernel in the per-row path. Re-measured: typed+fix is at parity or ahead
of the old utf8 world on every query — q1 10.8M (utf8: 7.2M), q2 15.7M
(10.8M), q7 10.0M (10.4M, within spread), q17 9.8M (8.4M), q11 20.5M (10.9M).
The register's answer to "should operators key on typed AggKey instead of
String?" is therefore NO for now — the string keying was never the cost.

Honesty note on the standing rule: this fix changes no observable semantics,
so no unit test can go red against the pre-fix line. The revert proof is the
A/B itself (reverting to the cast arm reproduces the 2.5x). The unsigned-key
test was extended to cover all four new match arms individually, since four
independent arms replaced one shared one and a broken UInt16 arm must not
hide behind a passing UInt32 case — that is coverage for the new shape, not a
regression proof, and is recorded as such.

Also recorded here, found by the same session's feature-mapping sweep and
filed as tasks rather than guessed at: (a) session-window COUNT(DISTINCT)
loses `AggState::distinct` across checkpoint/restore — session.rs has a
second, JSON persistence encoder that the V2 work in §47 never covered (the
sibling-defect shape, instance six; task #145); (b) `streaming_corpus::
QUERY_SHAPES` is dead — no assertion references it, and its
`multi_column_group_by` entry still claims `compiles: false`, stale since
§48 (task #133).

## §51 — Session-window COUNT(DISTINCT) lost its sets across checkpoint/restore (sibling-defect instance six)

Found by the feature-mapping sweep, not by a failing test — because no test
could fail: session windows persist through their own JSON encoder in
session.rs (`persist_to_state`/`restore_from_state`), a SECOND encoder that
the §47 V2 versioning in state_persistence.rs never touched. It serialized
the six AggEntry vectors and nothing else, so `AggState::distinct` restored
empty while the count restored as N. The failure is worse than under-counting:
the next already-seen value re-derived `value = set.len()` and RESET the
count to 1. The test feeds {x, y}, checkpoints, restores, feeds a duplicate
x, closes the session, and demands 2; against the reverted persist hunk it
fails `left: 1, right: 2` — the duplicate genuinely reset the count.

Fix mirrors the V2 rule in the JSON dialect: the `distinct` field is written
only when a set is non-empty, so non-DISTINCT session checkpoints stay
byte-identical (the running soak restores them); absent field = empty sets
(correct for old snapshots); present-but-malformed or wrong-cardinality =
CorruptEntry, failing loudly rather than degrading to empty sets.

Standing observation, now three encoders deep: state has TWO serialization
paths (binary state_persistence.rs for tumbling/sliding/count, JSON in
session.rs for sessions), and every field added to AggState must be added to
BOTH BY HAND. Any future auxiliary state (the top-N heap task #142 is the
next one) must touch both or repeat this defect exactly.
