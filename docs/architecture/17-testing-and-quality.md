# Testing and quality

Krishiv's quality bar is stated as rules that a build or a test enforces,
not as intentions. This document covers the CI tiers, the workspace lint
policy, the async/threading contract, generated documents and how they are
blessed, the property suites, the conformance and certification gates, and
the audit discipline for fixes.

## The standing rule

> Every fix ships with a test **proven red** against the reverted production
> line. A test that cannot distinguish correct from broken is worse than no
> test.

The register that enforces it is `../engineering-log/crate-audit-register.md`
(per-crate, per-fix, with commit hashes and the revert result). Items that
need a product decision are recorded as "not fixed — needs a decision", never
guessed at. The generalisable check for any test: *which single production
line would you delete to make this test fail?* If "none" or "an unrelated
line", it is not a regression test.

## CI tiers

The required-versus-optional split is a committed decision; any change to an
exclusion updates this table with a named rationale.

**Required on every PR and push (`ci.yml`):**

| Gate | Command | Proves |
|---|---|---|
| format | `just fmt` (`cargo fmt --all -- --check`) | rustfmt clean |
| lint | `just lint` | clippy `-D warnings` with the workspace lint policy below |
| feature graph | `just lint-features` | each optional feature builds alone (`15`) |
| lib tests | `just test` | every `--lib` unit test |
| integration tests | `just test-integration` | every `crates/*/tests/*.rs` suite, including the property suites |
| doctests | `just test-doc` | documentation examples compile and run |
| etcd tests | `just test-etcd` | `krishiv-scheduler --features etcd` — off by default, so `just test` runs none; guards the dedicated-runtime and snapshot-chunking fixes without a live etcd |
| external services | `just test-external` | the `#[ignore = "requires …"]` tests against provisioned Postgres, MinIO, Kafka, OTLP |
| Python | `test-python` job | maturin build + pytest — a non-compiling binding fails CI |
| security | `security.yml`, `codeql.yml` | cargo-deny advisories/licenses/bans; CodeQL |
| security/durability gate | `security-durability-gate.yml` | fail-closed behaviours against a built binary (`12`) |
| hygiene | `just project-check` | API surface inventories (`api/stable-api.toml`), Markdown links (`scripts/check_markdown_links.py`), release metadata, web content (`scripts/check_docs.sh`), compatibility phrasing (`scripts/compatibility_gate.py`) |

**Excluded from the required tier, with rationale:**

| Exclusion | Where | Why |
|---|---|---|
| `krishiv-python` from `cargo test`/clippy | `justfile` | needs a Python toolchain; covered by the required `test-python` job; Rust-side breakage caught by nightly `cargo check --all-targets` |
| `krishiv-chaos` | `justfile` | fault-injection suite runs minutes to hours; `nightly.yml` |
| `krishiv-bench` from `test-integration` | `justfile` | needs TPC-H/DS datasets (`KRISHIV_TPCH_DATA_DIR*`); `bench.yml` |
| live-cluster `#[ignore]` tests (`mode_conformance`, `api::tests` on fixed ports) | in-tree | need a running stack; the in-process placements are covered by `krishiv-conformance`; the multi-executor harness is `e2e.yml` |
| DFS `snapshot_round_trip` | `krishiv-state/src/dfs_backend.rs` | known limitation: snapshots store key hashes, not keys |

**Scheduled:** `coverage.yml` (nightly `just coverage`, cargo-llvm-cov over
the required scope, per-crate table), `nightly.yml` (chaos, python check,
long e2e), `bench.yml` (performance against baselines), `e2e.yml` (kind
failover + bare-metal distributed smoke), `phase58-chaos.yml` (HA chaos gate),
`release.yml` / `publish-main-image.yml` / `deploy-web.yml` (artefacts).

**Flakiness policy:** `.config/nextest.toml` profile `ci` retries twice, scoped
to `krishiv-scheduler`, `-executor`, `-api`, `-shuffle` — the crates with
sleep-based synchronisation. Retried-then-passed tests are reported FLAKY so
the budget stays visible; local runs never retry. This is a quarantine, not
the fix.

**Branch protection:** `main` requires the `ci.yml` jobs. Direct pushes run
the same gates locally: `just tidy && just test && just test-integration &&
just test-doc`.

## Workspace lint policy

`Cargo.toml [workspace.lints]`: `unsafe_code = "forbid"` (one audited
`#[allow]` at the CLI's `pre_exec`/`setpgid`); `unwrap_used`, `expect_used`,
`panic`, `todo`, `unimplemented`, `dbg_macro`, `print_*`, and indexing lints
denied in non-test code; `await_holding_lock` and `await_holding_refcell_ref`
denied. Every `#[allow]` in the tree is a reviewable site, which is why the
audit indexes them rather than raw counts.

## Async and threading contract

Policed by lint since 2026-07-10:

- `clippy::await_holding_lock` / `await_holding_refcell_ref` are **deny**
  workspace-wide: a std lock across `.await` cannot re-enter the tree.
- `krishiv_common::async_util::block_on` and `tokio::task::block_in_place`
  are clippy `disallowed-methods` (`clippy.toml`). Only deliberate
  sync-surface boundary modules carry a file-level
  `#![allow(clippy::disallowed_methods)]` with a justification comment.

| Tier | Crates | Rule |
|---|---|---|
| async-native core | scheduler, executor, shuffle, flight-sql, state, connectors (I/O), ui, mcp | `async fn` end to end; never `block_on`; filesystem I/O on hot paths via `tokio::fs` or `spawn_blocking` |
| sync surfaces (allow-listed bridges) | `krishiv-api` blocking wrappers (`blocking.rs`, `dataframe.rs`, `session.rs`, `io.rs`, `catalog.rs`), CLI command modules, `krishiv-python` (PyO3 is sync), `krishiv-runtime` `ExecutionBackend`, `etcd_metadata.rs`, CDC/delta lakehouse adapters, `iceberg_catalog_bridge.rs` | may call `async_util::block_on`, once at the surface, never per request on a hot path |
| program entries | `main.rs`, bench harnesses | own their runtime |

`async_util::block_on` is re-entrancy-safe: inside a Tokio runtime it routes
the future to a dedicated fallback runtime (`KRISHIV_FALLBACK_RUNTIME_THREADS`,
default 2) instead of panicking or starving a worker; raw `Handle::block_on`
from an async context panics. `spawn_blocking` (or `async_util::run_blocking`)
is mandatory for filesystem walks and large reads in async handlers,
RocksDB compaction-triggering operations, and any CPU-bound loop over ~10 ms.
To add a bridge: justify why the surface must be sync (FFI, public blocking
API, CLI), add the file-level allow with the two-line comment, never bridge
per request.

## Generated documents and blessing

These files are produced by tests from code and **must not be hand-edited**;
each has a `KRISHIV_BLESS_*` switch that regenerates it:

| Document | Source | Bless |
|---|---|---|
| `docs/reference/env-flags.md` | `krishiv_common::env_registry` | `KRISHIV_BLESS_ENV_FLAGS=1` |
| `docs/reference/sql-feature-matrix.md` | SQL feature registry | `KRISHIV_BLESS_SQL_MATRIX=1` |
| `docs/reference/pyspark-parity.md` | `krishiv_api::pyspark_parity` | `KRISHIV_BLESS_PYSPARK_PARITY=1` |
| `docs/reference/certification-matrix.md` | `krishiv_connectors::cert_matrix` | `KRISHIV_BLESS_CERT_MATRIX=1 cargo test -p krishiv-connectors cert_matrix` |
| `docs/reference/connector-reachability-matrix.md` | connector registry | its bless switch |
| `docs/reference/krishiv-vs-spark-sql.md` | conformance results | its bless switch |
| `api/stable-api.toml` inventories | `just api-inventory` | |

`docs/COMPATIBILITY.md` is hand-written but parsed by
`scripts/compatibility_gate.py`; keep its version phrasing. The web site
(`web/`) has its own content (`web/lib/docs-content/*.ts`,
`web/PRODUCT_FACTS.md`) checked by `scripts/check_docs.sh`.

## Property suites

Run inside `just test-integration`, for the crates where an example-based
miss is silent corruption:

- `krishiv-delta/tests/proptest_zset.rs` — Z-set laws: consolidation equals
  model addition, commutativity, additive inverse, idempotence, positive-part
  expansion, serialisation round-trip, `Trace` equals model under arbitrary
  chunking and merging.
- `krishiv-state/tests/proptest_checkpoint_kill.rs` — stop the checkpoint
  write sequence after any prefix and recovery lands on the last sealed
  epoch with every byte intact; flip any byte and the manifest fences it
  without bricking recovery.
- `krishiv-ivm/tests/proptest_ivm.rs` — incremental plan == diff-based
  fallback == one-shot recompute == plain-Rust model over random multi-tick
  insert/retract histories.

## Conformance and certification

- `krishiv-conformance` runs the same relational operations through SQL,
  Rust, and Python across the in-process placements and diffs results.
- The streaming corpus (`krishiv-dataflow::streaming_corpus`) runs identical
  inputs through every loop and fails on divergence (`08`).
- Connector certification (`cert_matrix`) records, per connector, the
  failure/recovery scenarios passed; `ConnectorMaturity::Certified` is
  earned there (`10`).
- TPC-DS 99/99 and the IVM 41/44 gates are re-run after optimizer and
  planner changes (`16`, `09`).
- The HA chaos gate and GA soak are the distributed certification
  (`14`).

## Coverage

`just coverage` (cargo-llvm-cov) over the required-gate scope; the nightly
job publishes a per-crate table and lcov. Coverage is measured, never
assumed: each audit section starts from the uncovered-region table for its
crate.

## Local workflow

`just tidy` (fmt + lint), `just test`, `just test-integration`, `just
test-doc`, `just project-check`; crate-scoped `cargo test -p <crate>`. Gate
on exit codes, not on scrolling output — a piped `cargo test` hides a red
result behind a green tail. Long gates run detached with a summary file.

## Related

- `../engineering-log/crate-audit-register.md`, `../engineering-log/ivm-audit-register.md`,
  `../engineering-log/status.md` (what is in flight), `CONTRIBUTING.md`.
