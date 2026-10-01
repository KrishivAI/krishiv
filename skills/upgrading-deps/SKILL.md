---
name: upgrading-deps
description: Use when bumping any Krishiv dependency, especially DataFusion, arrow, parquet, object_store, sqlparser, iceberg or the Rust toolchain/MSRV — and when a build breaks with "expected X, found a different X", "multiple versions of crate", or a deprecation warning after `cargo update`.
---

# Upgrading dependencies

Arrow/DataFusion is one graph. Two versions of `arrow-array` or
`object_store` in one binary compile until the first `RecordBatch` crosses a
crate boundary, then fail with "expected `arrow::array::RecordBatch`, found
`arrow_array::record_batch::RecordBatch`".

## Before changing a version

1. Research first, read-only (the `researcher` agent): latest versions, what
   arrow/object_store/sqlparser each pins, their `rust-version`, and what Sail
   pins (`lakehq/sail` Cargo.toml) — the coherent set is the one DataFusion
   itself depends on, not "latest" of each.
2. The one crate that lags (iceberg, historically) decides whether the move is
   possible: no release on the new arrow → git rev pin with an exit condition
   written in Cargo.toml, or wait. Both are the user's call; ask.
3. MSRV lives in four places: `[workspace.package] rust-version`,
   `rust-toolchain.toml`, `deploy/docker/Dockerfile.*` (`rust:X.Y-slim`),
   and CI. A git dep's `rust-version` can force it.

## After `cargo update`

```bash
grep -E '^name = "(arrow-array|object_store|sqlparser|parquet)"$' Cargo.lock | sort | uniq -c   # each exactly 1
cargo tree -e features -i <crate> | grep -B1 "<crate> feature"      # who turns on which features
```
Feature unification changes semantics: a feature another crate turns on
(DF 55 enables serde_json `preserve_order`, which reorders `from_json` MAP
entries) is now in your binary whether you asked or not. Run the second
command for every crate whose behaviour depends on features (serde_json,
tokio, parquet, object_store) and declare explicitly any feature you now
rely on.

## Deprecations: migrate, don't allow

`just lint` is `-D warnings`. Each deprecation names its replacement
(`partition_statistics` → `StatisticsContext::compute`,
`with_new_children` → `replace_children(…, ReplaceChildrenOptions)`); migrate
every call site. A rewrite by pattern must check the receiver type — a
builder's `with_new_children` is not the trait method. `#[allow(deprecated)]`
only with a comment naming why the replacement is a separate change.

## Behaviour changes hide behind green builds

Run the full gate (gating) and the SF100 digest comparison against the
previous run. On DF 54→55 the build was green and three things were not:
a cross-partition barrier that hung every broadcast-join fragment, a
per-partition sort reservation that overflowed a pool, and a map order flip.
Each is root-caused (debugging-krishiv) before it is fixed; a config switch
that makes a test pass is a hypothesis, not a fix, until the mechanism is
written down.

## Record

Register §85 pattern: what moved, what is held and why, every API that
changed, MSRV reasoning. `CHANGELOG.md` Unreleased for user-facing moves
(MSRV, majors). See logging-work.
