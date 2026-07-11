# CI Tiers — required vs optional (Phase 51)

The required-vs-optional split is a committed decision, not an accident of
one cargo flag. Any change to an exclusion below must update this file with
a named rationale.

## Required on every PR / push (`ci.yml`)

| Gate | Command | What it proves |
|---|---|---|
| Format | `just fmt` | rustfmt clean |
| Lint | `just lint` | clippy `-D warnings`, workspace lint policy (unwrap/panic/indexing/await-holding denies, `block_on` disallowed-method) |
| Feature graph | `just lint-features` | each optional feature builds alone |
| Lib tests | `just test` | all `--lib` unit tests |
| Integration tests | `just test-integration` | all `crates/*/tests/*.rs` suites (134+ fns, incl. the proptest suites below) |
| Doctests | `just test-doc` | documentation examples compile and run |
| External-service tests | `just test-external` job | the `#[ignore = "requires …"]` tests against real provisioned Postgres/MinIO/OTLP (audit §14 TEST-6) |
| Python bindings | `test-python` job | maturin build + pytest — a non-compiling binding fails CI (the `1694143` class) |
| Security | `security.yml` cargo-deny | advisories/licenses/bans |
| Hygiene | scripts job | API surface inventories, links, release metadata |

## Property-test suites (audit §14 TEST-3)

Run inside `just test-integration`; these are the algebraic/model tests for
the crates where an example-based miss is silent data corruption:

- `krishiv-delta/tests/proptest_zset.rs` — Z-set laws (consolidation = model
  addition, commutativity, additive inverse, idempotence, positive-part
  multiset expansion, serialization round-trip, `Trace` snapshot = model
  under arbitrary chunking/merging).
- `krishiv-state/tests/proptest_checkpoint_kill.rs` — checkpoint
  commit-or-abort across kill: stop the write sequence after any prefix and
  recovery must land on the last epoch whose manifest sealed, round-tripping
  every snapshot byte; flip any byte in a sealed epoch and the integrity
  manifest must fence it without bricking recovery.
- `krishiv-ivm/tests/proptest_ivm.rs` — incremental view == diff-based
  fallback == one-shot DataFusion recompute == plain-Rust model, over random
  multi-tick insert/retract histories (incl. group-emptying retractions).

## Flakiness policy (audit §14 TEST-5)

`.config/nextest.toml` defines a `ci` profile (selected via
`NEXTEST_PROFILE=ci` in the CI test job): retries=2 scoped to
`krishiv-scheduler` / `krishiv-executor` / `krishiv-api` / `krishiv-shuffle`
— the four crates with sleep-based synchronization (55 sites at audit time).
Retried-then-passed tests are reported FLAKY in the nextest summary, so the
flake budget stays observable instead of silently green. Local runs never
retry. **This is a quarantine, not the fix**: the sleep→event conversions
ride the subsystem rewrites (shuffle → Phase 52, scheduler → Phase 53,
streaming/api → Phase 55) — remove a crate from the override when its
rewrite lands.

Individually quarantined: `rocksdb_ephemeral_is_faster_than_file_backed…`
(`#[ignore]`, timing-sensitive benchmark-as-test; re-shape into `bench.yml`).

## Excluded from the required tier — named rationale per exclusion

| Exclusion | Where | Rationale |
|---|---|---|
| `krishiv-python` (from cargo test/clippy) | `justfile` | pyo3 needs a Python toolchain + venv; covered by the dedicated required `test-python` CI job instead. Rust-side breakage in the crate is caught by `cargo check --all-targets` in the nightly tier. |
| `krishiv-chaos` | `justfile` | long-running fault-injection suite; runs in `nightly.yml`, not per-PR (minutes-to-hours runtime). |
| `krishiv-bench` (from `test-integration`) | `justfile` | benchmark harnesses need TPC-H/DS datasets (`KRISHIV_TPCH_DATA_DIR*`); perf tier runs in `bench.yml`/`nightly.yml`. |
| Live-cluster `#[ignore]` tests (`mode_conformance` :9090, `api::tests` :50051) | in-tree | need a running coordinator/executor stack, not a container; they join Phase 58's real multi-executor harness (audit §14 TEST-2) — the in-process placements are covered by `krishiv-conformance` today. |
| DFS `snapshot_round_trip` | `krishiv-state/src/dfs_backend.rs` | known design limitation (snapshots store key hashes, not keys); unignore if the snapshot format changes. |
| Quarantined cargo features | `just lint-features` | pre-existing dependency-API rot in optional non-preset integrations; tracked in `docs/feature-graph.md`. |

## Scheduled tiers

- `coverage.yml` — nightly `just coverage` (cargo-llvm-cov over the exact
  required-gate scope), per-crate table in the job summary + lcov artifact
  (audit §14 TEST-4). The Phase 62 GA gate publishes this number.
- `nightly.yml` — chaos suite, python `cargo check --all-targets`, long e2e.
- `bench.yml` — performance tier against recorded baselines
  (`docs/BENCHMARKING.md`).
- `e2e.yml` — kind failover + bare-metal distributed smoke (three targets).

## Branch protection

`main` requires the `ci.yml` jobs above. When pushing directly to `main`
(maintainer flow), the same gates must be run locally first:
`just tidy && just test && just test-integration && just test-doc`.
