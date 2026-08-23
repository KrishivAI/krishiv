# IncrementalDataFrame Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the delta/IVM mode the same DataFrame authoring surface as batch and streaming — `df.to_incremental()` → `IncrementalDataFrame` — and retire the overlapping Python `LiveTable` with zero functionality lost.

**Architecture:** `to_incremental()` unparses the DataFrame's logical plan to SQL (reusing the existing `krishiv_runtime::flight_client::plan_to_sql`), builds an `IncrementalViewSpec`, and registers it on a `Session`-created `IvmJob` (embedded or remote, mode-inherited). The returned `IncrementalDataFrame` is a thin feed/read handle delegating to the existing pyo3 `IvmJob`. View-DAG composition co-registers derived views into the base job (existing multi-view cascade).

**Tech Stack:** Rust, pyo3/maturin, `krishiv-api`, `krishiv-python`, `krishiv-ivm`/`krishiv-delta`, Arrow, pytest.

## Global Constraints

- Client wheel builds with `VIRTUAL_ENV="$PWD/.venv" PATH="$PWD/.venv/bin:$PATH" maturin develop --release --features "kafka,iceberg,cloud"` from `crates/krishiv-python` (~9–13 min). Verify "Installed krishiv" before testing; never `pkill maturin` mid-build.
- `krishiv-python` is excluded from CI clippy — run `cargo check -p krishiv-python` manually.
- NEVER `git add -A`; stage curated pathspecs; `git diff --cached --stat` before commit.
- Commit trailers: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>` + `Claude-Session: https://claude.ai/code/session_0131iye85m62YgxKqWZ7ANSh`.
- Retirement is scoped to the **Python client** `LiveTable`; do not touch engine `live_table.rs` internals used elsewhere or platform live-tables.
- k8s cluster: NodePort `http://213.199.60.184:31903` (flight) / `:31902` (http); connect with `http://` scheme (not `grpc://`). Distributed IVM needs `Session.connect(flight, http_url=http)`.

---

### Task 1: `IncrementalDataFrame` handle in the api layer (`DataFrame::to_incremental`)

**Files:**
- Modify: `crates/krishiv-api/src/dataframe.rs` (add `to_incremental`)
- Create: `crates/krishiv-api/src/compute/incremental_df.rs` (the `IncrementalDataFrame` struct)
- Modify: `crates/krishiv-api/src/compute/mod.rs` (module + re-export)
- Test: inline `#[cfg(test)]` in `incremental_df.rs`

**Interfaces:**
- Consumes: `krishiv_api::DataFrame::logical_plan()`, `DataFrame::schema()`, `Session::ivm(name)`, `krishiv_runtime::flight_client::plan_to_sql`, `IvmJob::register_view`, `krishiv_delta::IncrementalViewSpec`.
- Produces: `struct IncrementalDataFrame { job: IvmJob, view: String, sources: Vec<String> }` with `apply(&self, source: Option<&str>, delta: &DeltaBatch)`, `step()`, `snapshot()`, `last_output()` (the latest output `DeltaBatch`, or `None`), `checkpoint`/`restore`, `drop_view`, `name()`, `source_names()`.

- [ ] **Step 1: Write the failing test** — in `incremental_df.rs`, a test that builds a DataFrame `SELECT k, SUM(v) AS total FROM src GROUP BY k`, calls `df.to_incremental("v").await`, feeds a `DeltaBatch::from_inserts` of 3 rows to source `src`, and asserts `snapshot()` matches a full batch recompute.
- [ ] **Step 2: Run** `cargo test -p krishiv-api incremental_df -- --nocapture` → FAIL (no `to_incremental`).
- [ ] **Step 3: Implement** `IncrementalDataFrame`:
  - `DataFrame::to_incremental(self, name: &str) -> Result<IncrementalDataFrame>`: `let sql = plan_to_sql(&self.physical_plan()?);` (or `logical_plan` unparse — verify which `plan_to_sql` accepts and adapt); `let spec = IncrementalViewSpec { name: name.into(), body_sql: sql, output_schema: self.schema(), is_materialized: true, is_recursive: false, lateness: vec![] };` `let job = self.session().ivm(name).await?; job.register_view(spec).await?;` derive `sources` from the plan's table scans.
  - Delegate `apply/step/snapshot/checkpoint/restore` to `job`.
  - **If `DataFrame` has no `session()` accessor:** add `pub(crate) fn session(&self) -> &Session` (or thread the registry) — verify during impl; this is the one plumbing unknown.
- [ ] **Step 4: Run** the test → PASS.
- [ ] **Step 5: Commit** `git add crates/krishiv-api/src/dataframe.rs crates/krishiv-api/src/compute/incremental_df.rs crates/krishiv-api/src/compute/mod.rs && git commit` (feat: DataFrame::to_incremental in api layer).

### Task 2: Source-derivation + non-feedable-leaf guard

**Files:**
- Modify: `crates/krishiv-api/src/compute/incremental_df.rs`
- Test: inline

**Interfaces:**
- Produces: `IncrementalDataFrame::source_names() -> &[String]`; `to_incremental` errors on a plan leaf that is a one-shot file scan (no feedable name).

- [ ] **Step 1: Write failing tests** — (a) multi-source plan (`FROM orders JOIN returns …`) yields `source_names() == ["orders","returns"]`; (b) a `read_parquet(path)`-rooted plan returns an `Err` naming the offending leaf.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** table-ref extraction by walking the plan's scan nodes; reject leaves whose source is an anonymous file scan.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** (feat: source derivation + feedable-leaf guard).

### Task 3: pyo3 `IncrementalDataFrame` + `DataFrame.to_incremental()`

**Files:**
- Create: `crates/krishiv-python/src/incremental_dataframe.rs`
- Modify: `crates/krishiv-python/src/dataframe.rs` (add `to_incremental`, mirroring `to_streaming` at line 557)
- Modify: `crates/krishiv-python/src/lib.rs` (register the class)
- Test: `crates/krishiv-python/python/tests/test_incremental_dataframe.py`

**Interfaces:**
- Consumes: `krishiv_api::DataFrame::to_incremental`; existing `PyDeltaBatch` (`crate::incremental`), `PyBatch`.
- Produces (Python): `df.to_incremental(name=None) -> IncrementalDataFrame`; methods `apply(delta, source=None)`, `insert(batch)`, `delete(batch)`, `upsert(batch, keys)`, `apply_cdc(event)`, `snapshot() -> Batch|None`, `next_change() -> DeltaBatch|None` (each published delta at most once), `last_output() -> DeltaBatch|None` (non-consuming peek), `checkpoint()`, `restore(bytes)`, `drop()`, `name` (property), `source_names -> list[str]`, `transaction()` (context manager).

- [ ] **Step 1: Write the failing test** (`test_incremental_dataframe.py`): embedded session, register `src` with 3 rows, `iv = s.sql("SELECT k, SUM(v) AS total FROM src GROUP BY k").to_incremental()`, `iv.insert(batch); ` assert `iv.snapshot()` equals the batch `groupBy` oracle.
- [ ] **Step 2: Run** `.venv/bin/python -m pytest python/tests/test_incremental_dataframe.py -q` → FAIL (import/attr error).
- [ ] **Step 3: Implement** `PyIncrementalDataFrame` wrapping the api `IncrementalDataFrame` (or a `PyIvmJob` + view name). `apply` accepts a `PyDeltaBatch`; `insert/delete/upsert/apply_cdc` construct the `DeltaBatch` then `apply` + auto-step; `snapshot` returns a `QueryResult`; `changes` returns a `PyDataFrameStream`-style async iterator over output `DeltaBatch`es (reuse the `view_output_stream`/watch channel from `IncrementalView`). Add `PyDataFrame::to_incremental` calling the api method via `block_on_async`.
- [ ] **Step 4: maturin build** then run test → PASS.
- [ ] **Step 5: Commit** (feat: pyo3 IncrementalDataFrame + DataFrame.to_incremental).

### Task 4: `transaction()` context manager (atomic multi-source tick)

**Files:**
- Modify: `crates/krishiv-python/src/incremental_dataframe.rs`
- Modify: `crates/krishiv-python/python/krishiv/__init__.py` (pure-Python `_Transaction` ctx wrapper if simpler there)
- Test: `test_incremental_dataframe.py`

**Interfaces:**
- Produces: `iv.transaction()` returns a context manager; `apply` inside buffers (no step); `__exit__` issues one `step()`.

- [ ] **Step 1: Write failing test** — feed two sources inside `with iv.transaction():` then assert exactly one tick advanced (`StepSummary.tick` incremented by 1) and the combined effect is visible only after exit.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** a `defer_step` flag on the handle toggled by enter/exit; `__exit__` calls `step()` once. Expose as a Python context manager (`__enter__`/`__exit__`).
- [ ] **Step 4: Build + test** → PASS.
- [ ] **Step 5: Commit** (feat: IncrementalDataFrame.transaction).

### Task 5: View-DAG composition (`s.view(iv)` / `iv.as_source()`)

**Files:**
- Modify: `crates/krishiv-python/src/incremental_dataframe.rs` (`as_source`)
- Modify: `crates/krishiv-python/src/session.rs` (`view(iv)`)
- Modify: `crates/krishiv-api/src/compute/incremental_df.rs` (co-register into the same job when a leaf is another view)
- Test: `test_incremental_dataframe.py`

**Interfaces:**
- Produces: `s.view(iv) -> DataFrame` reading `iv`'s output as a named source; `iv.as_source() -> DataFrame`; downstream `to_incremental()` co-registers into `iv`'s job so feeds cascade.

- [ ] **Step 1: Write failing test** — `iv1 = df1.to_incremental()`; `iv2 = s.view(iv1).group_by("region").agg(n=F.count()).to_incremental()`; `iv1.insert(batch)`; assert `iv2.snapshot()` reflects the cascade (equals batch oracle of the two-level query).
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `as_source` (register `iv`'s view name as a readable table) + co-registration (detect the leaf view and register the derived view into the same `IvmJob`).
- [ ] **Step 4: Build + test** → PASS.
- [ ] **Step 5: Commit** (feat: incremental view-DAG composition).

### Task 6: Top-level exports + `.pyi` stubs

**Files:**
- Modify: `crates/krishiv-python/python/krishiv/__init__.py` (import + `__all__`)
- Modify: `crates/krishiv-python/python/krishiv/krishiv.pyi` (class + `to_incremental` + `Session.view`)
- Test: `test_incremental_dataframe.py` (export assertions)

- [ ] **Step 1: Write failing test** — assert `hasattr(krishiv, "IncrementalDataFrame")`, it's in `__all__`, and `from krishiv import *` exposes it.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** exports + stubs (mirror the streaming pattern; `next_change()`/`snapshot()` typed).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** (feat: export IncrementalDataFrame).

### Task 7: Retire the Python `LiveTable` (migrate to IncrementalDataFrame)

**Files:**
- Delete: `crates/krishiv-python/src/live_table.rs`
- Modify: `crates/krishiv-python/src/lib.rs` (unregister class), `crates/krishiv-python/src/session.rs` (remove `live_table`), `crates/krishiv-python/python/krishiv/__init__.py` + `krishiv.pyi` (drop exports/stubs)
- Rewrite: `crates/krishiv-python/python/tests/test_live_table.py`, `test_change_feed.py` → against `IncrementalDataFrame`
- Modify: any examples referencing `live_table`

**Interfaces:**
- Migration mapping enforced by rewritten tests: `s.live_table(name,sql)`→`s.sql(sql).to_incremental()`; `ingest_row(id,"insert"/"delete"/"update")`→`insert/delete/upsert`; `refresh()`→auto-step; `change_feed()`→`next_change()`; `drop()`→`drop()`.

- [ ] **Step 1: Rewrite** `test_live_table.py` + `test_change_feed.py` using `IncrementalDataFrame` per the mapping (these are the no-functionality-lost proof).
- [ ] **Step 2: Run** → FAIL (LiveTable still referenced / new API not wired in tests).
- [ ] **Step 3: Delete** `live_table.rs`, remove `Session.live_table` + registration + exports/stubs; `grep -rn "LiveTable\|live_table" crates/krishiv-python` returns nothing but the rewritten tests.
- [ ] **Step 4: Build + run** the rewritten tests + `import krishiv` clean; assert `not hasattr(krishiv, "LiveTable")`.
- [ ] **Step 5: Commit** (refactor: retire Python LiveTable, unify on IncrementalDataFrame).

### Task 8: Oracle / cross-mode / change-feed correctness suite

**Files:**
- Modify: `crates/krishiv-python/python/tests/test_incremental_dataframe.py`

- [ ] **Step 1: Write tests** — (a) oracle parity for agg/groupBy/join/filter/projection over a fed delta sequence vs full batch recompute; (b) change-feed: union of emitted output deltas reconstructs `snapshot()`; (c) update via `upsert` retracts correctly; (d) cross-mode: same `df` gives identical results via `collect()` and via `to_incremental()` snapshot after feeding the same rows.
- [ ] **Step 2: Run** → some FAIL if bugs exist.
- [ ] **Step 3: Fix any bug** found in the api/pyo3 layer (per "fix all bugs" mandate); if the bug is in the IVM engine, fix in `krishiv-ivm`/`krishiv-delta` and note it.
- [ ] **Step 4: Run** full `test_incremental_dataframe.py` → all PASS; run `test_ivm.py`, `test_pyspark_parity.py` → still green.
- [ ] **Step 5: Commit** (test: IncrementalDataFrame correctness suite + fixes).

### Task 9: Examples + docs to the unified triad

**Files:**
- Create: `crates/krishiv-python/examples/incremental_view.py`
- Modify: any streaming/batch example index/README that lists the three modes

- [ ] **Step 1:** Write a runnable `incremental_view.py` showing `df.collect()` / `df.to_streaming()` / `df.to_incremental()` off the same `df`, feed+snapshot+changes, and a 2-level view-DAG.
- [ ] **Step 2:** Run it embedded → correct output.
- [ ] **Step 3:** Update docs/examples listing to name all three conversions.
- [ ] **Step 4: Commit** (docs: incremental_view example + unified-triad docs).

### Task 10: Distributed validation on k8s with real datasets

**Files:**
- Create: `/tmp/claude-0/.../scratchpad/incremental_k8s.py` (validation, not committed)

- [ ] **Step 1:** If any change touches the engine/server IVM path (only if Task 8 fixed an engine bug), rebuild + redeploy the engine image (`scripts/build-fast-engine.sh`, distribute to 3 nodes, `kubectl set image`). Pure client-surface work needs no redeploy.
- [ ] **Step 2:** Connect `Session.connect("http://213.199.60.184:31903", http_url="http://213.199.60.184:31902")`; build a real-dataset incremental view (e.g. per-customer revenue over the 500k-row generator or an S3 parquet source via `register_remote_parquet`), feed deltas, assert `snapshot()` matches a distributed batch `execute_remote` ground truth.
- [ ] **Step 3:** Validate a distributed view-DAG cascade + a `transaction()` multi-source tick.
- [ ] **Step 4:** Record results; update memory `live-distributed-api-validation`.
- [ ] **Step 5: Commit** any harness/docs (scratchpad harness stays local).

## Self-Review

- **Spec coverage:** §4.1 unified surface → T1/T3; §4.2 feed/read/step → T3/T4; §4.2.1 view-DAG → T5; §4.3 plan→SQL plumbing → T1 (via existing `plan_to_sql`); §4.4 retire LiveTable → T7; §4.5 distributed → T10; §5 testing → T8/T10; cleanup/no-loss → T7/T8. All covered.
- **Placeholder scan:** the one genuine unknown (DataFrame→session accessor) is called out explicitly in T1 Step 3 with a fallback, not hidden.
- **Type consistency:** `IncrementalDataFrame` / `to_incremental` / `apply/insert/delete/upsert/snapshot/changes/transaction/as_source` used consistently T1→T10; `IncrementalViewSpec` fields match `krishiv-delta/src/view.rs`.

## Execution note

Because build cycles are ~10–15 min, group Rust-binding tasks (T3–T7) so maturin rebuilds are batched, running the growing pytest file after each rebuild rather than once per micro-step.
