# Stateful corpus — embedded placement only (for now)

These files use DDL/DML (`CREATE TABLE` + `INSERT`) and currently run only
on the embedded placement. Over the Flight placements each statement plans
in a fresh context, so session table state does not persist across
statements — the "one SQL front door" gap (audit §8, Phase 60 / task #197).
When Phase 60 lands the shared front door, move these into `corpus/scalar/`
scope by extending the placement matrix in `tests/corpus.rs`.
