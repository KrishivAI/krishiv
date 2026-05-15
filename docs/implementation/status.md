# Krishiv Implementation Status

## Current Phase

R1 Foundation Alpha.

## Active Task

R1 Foundation Alpha local execution is complete. The next active task is R1
hardening/examples or the first R2 Kubernetes Distributed Alpha design slice.

## Completed

- Created `docs/architecture/krishiv-roadmap.md`.
- Created `AGENTS.md`.
- Created `docs/engineering/standards.md`.
- Created `docs/implementation/r1-foundation-alpha.md`.
- Created repo-local `codex/skills/krishiv-engine/SKILL.md`.
- Installed the `krishiv-engine` skill globally under `/Users/gopal/.agents/skills/krishiv-engine`.
- Added Codex rate-limit and resumability workflow documentation.
- Synced the updated `krishiv-engine` resume protocol into the global skill install.
- Added `docs/implementation/README.md` as the implementation tracker index.
- Added implementation trackers for R2 through R10.
- Synced the updated tracker-index guidance into the global `krishiv-engine` skill install.
- Created the root Rust workspace.
- Created R1 bootstrap crates: `krishiv-api`, `krishiv-cli`, `krishiv-sql`, `krishiv-plan`, `krishiv-exec`, and `krishiv-runtime`.
- Added public API stubs for `Session`, `SessionBuilder`, `DataFrame`, `Stream`, `ExecutionMode`, `QueryResult`, and `StreamBatch`.
- Added plan, runtime, SQL, execution, and CLI stubs.
- Added R1 bootstrap architecture docs, crate map, SQL compatibility placeholder, and example/test placeholders.
- Added `docs/architecture/file-guide.md` to explain each bootstrap file.
- Added `.gitignore` for local build artifacts.
- Added Arrow/DataFusion dependencies behind `krishiv-sql`.
- Implemented DataFusion-backed local SQL execution and `EXPLAIN`.
- Implemented local Parquet registration and direct Parquet reads.
- Replaced bootstrap result placeholders with Arrow `RecordBatch` results.
- Implemented bounded and unbounded local memory stream API shapes with bounded map/filter/collect support.
- Routed embedded and single-node local execution through the runtime backend seam.
- Implemented `krishiv sql`, `krishiv explain`, and `krishiv jobs`.
- Added embedded/single-node SQL-over-Parquet parity coverage.
- Added R1 CLI golden tests for `sql` and `explain`.
- Updated R1 SQL compatibility, crate map, file guide, and tracker docs.

## In Progress

- None.

## Next Steps

1. Add examples for embedded SQL over Parquet and bounded memory streams.
2. Add broader R1 SQL golden coverage for projection, filter, aggregate, and limit.
3. Decide whether to start R2 coordinator/executor skeletons or finish R1 examples first.
4. Keep R1 embedded and single-node behavior semantically aligned while R2 starts.

## Known Blockers

- None known.

## Last Validation

- `python3 /Users/gopal/.codex/skills/.system/skill-creator/scripts/quick_validate.py codex/skills/krishiv-engine` passed.
- `python3 /Users/gopal/.codex/skills/.system/skill-creator/scripts/quick_validate.py /Users/gopal/.agents/skills/krishiv-engine` passed.
- `find docs/implementation -maxdepth 1 -type f -print | sort` shows R1-R10 trackers, README, and status files.
- `wc -l docs/implementation/*.md` completed successfully.
- `python3 /Users/gopal/.codex/skills/.system/skill-creator/scripts/quick_validate.py /Users/gopal/.agents/skills/krishiv-engine` passed after tracker-index sync.
- `cargo fmt --all --check` passed.
- `cargo check --workspace` passed.
- `cargo test --workspace` passed.
- `cargo run -p krishiv-cli -- sql --query "select 1 as value"` passed.
- `cargo run -p krishiv-cli -- explain --query "select 1 as value"` passed.
- `cargo run -p krishiv-cli -- jobs` passed.
- `cargo run -p krishiv-cli -- --help` passed.
- `cargo run -p krishiv-cli -- explain --help` passed.
- `find . -path './target' -prune -o -type f -print | sort` confirmed the bootstrap file inventory.
- Placeholder scan across repo docs and crates returned no actionable markers.

## Resume Instructions

For a new Codex session:

1. Read `AGENTS.md`.
2. Read this file.
3. Read `docs/implementation/r1-foundation-alpha.md`.
4. Continue with R1 examples/hardening or begin the R2 tracker, depending on the user's next request.
