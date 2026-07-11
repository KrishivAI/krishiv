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
| Integration tests | `just test-integration` | all `crates/*/tests/*.rs` suites (134+ fns) |
| Doctests | `just test-doc` | documentation examples compile and run |
| Python bindings | `test-python` job | maturin build + pytest — a non-compiling binding fails CI (the `1694143` class) |
| Security | `security.yml` cargo-deny | advisories/licenses/bans |
| Hygiene | scripts job | API surface inventories, links, release metadata |

## Excluded from the required tier — named rationale per exclusion

| Exclusion | Where | Rationale |
|---|---|---|
| `krishiv-python` (from cargo test/clippy) | `justfile` | pyo3 needs a Python toolchain + venv; covered by the dedicated required `test-python` CI job instead. Rust-side breakage in the crate is caught by `cargo check --all-targets` in the nightly tier. |
| `krishiv-chaos` | `justfile` | long-running fault-injection suite; runs in `nightly.yml`, not per-PR (minutes-to-hours runtime). |
| `krishiv-bench` (from `test-integration`) | `justfile` | benchmark harnesses need TPC-H/DS datasets (`KRISHIV_TPCH_DATA_DIR*`); perf tier runs in `bench.yml`/`nightly.yml`. |
| `#[ignore = "requires …"]` external-service tests | in-tree | need live Postgres/S3/coordinator/cluster/OTLP/TCP; opt-in via `--ignored` where the service exists (standing them up with testcontainers is the audit §14(d) follow-up, tracked in phase-51). |
| Quarantined cargo features | `just lint-features` | pre-existing dependency-API rot in optional non-preset integrations; tracked in `docs/feature-graph.md`. |

## Scheduled tiers

- `nightly.yml` — chaos suite, python `cargo check --all-targets`, long e2e.
- `bench.yml` — performance tier against recorded baselines
  (`docs/BENCHMARKING.md`).
- `e2e.yml` — kind failover + bare-metal distributed smoke (three targets).

## Branch protection

`main` requires the `ci.yml` jobs above. When pushing directly to `main`
(maintainer flow), the same gates must be run locally first:
`just tidy && just test && just test-integration && just test-doc`.
