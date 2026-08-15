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
| 1 | krishiv-sql | 46,632 | 61 | 38 | **34 live defects fixed**; 4 unreachable modules found; a crate-wide Unicode-folding bug class swept. Remaining are the big four: `distributed_plan` (8.5k), `spillable_join` (3.3k), `catalog/` (5.9k), `sql_tests` (2k) |
| 2 | krishiv-executor | 28,927 | 40 | **40 — COMPLETE 2026-08-02** | second crate fully read; 3 defects fixed |
| 3 | krishiv-shuffle | 14,329 | 36 | **36 — COMPLETE 2026-08-02** | first crate fully read; 4 defects fixed |
| 4 | krishiv-scheduler | **51,438** | **78** | **78 (COMPLETE)** | largest crate in the workspace; stage cutting, dispatch, single-task fallback, SC11 breaker |
| **Tier 2 — correctness blast radius** |
| 5 | krishiv-plan | 14,371 | 25 | **25 (COMPLETE)** | plan IR every surface depends on; 3 can't-fail tests fixed, 2 unreachable AQE rules with latent outer-join bugs documented, 4 dead pub surfaces recorded |
| 6 | krishiv-common | 7,966 | 23 | **23 (COMPLETE)** | 2 wrong-answer fixes (Float64 signed-zero shard split, UMM available `max`→`min`), a heartbeat env busy-loop, 3 declared-default drifts, registry `.rs.inc` scan blindness |
| 7 | krishiv-connectors | 39,930 | 97 | **97 (COMPLETE)** | ~60 defects fixed incl. avro silent corruption, kafka/cdc offset-before-delivery, NULL-predicate DELETE, orphan-cleanup deleting live MoR delete files, LanceDB fragment loss, 2PC later-epoch drops; streaming_unify deleted; ~120 tests added |
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
