# Flag minimization & optimization plan (engine)

**Date:** 2026-07-14
**Motivation:** the DUR-2 live-cert attempt surfaced that S3 support is
compiled *out* of the shipped image (gated by the `cloud` cargo feature, which
the default build omits) with **no runtime signal**. This is the poster child
for the flag problem: capability determined at compile time, invisible at run
time. Extends audit §12 (#202) with a concrete minimization plan.

## The two flag classes (they must be kept distinct)

| Class | Mechanism | Count today | What it should control | What it must NOT do |
|---|---|---|---|---|
| **Deployment / compile-time** | Cargo `[features]` + `#[cfg(feature)]` | **51 features, 377 cfg-sites, 80 files** | *Only* optional dependency weight (binary size / build time) | Silently remove a runtime-selectable capability with no manifest |
| **Runtime / config** | `KRISHIV_*` env + CLI flags | **162 distinct env reads** | Deployment shape, limits, security posture, endpoints | Be read ad-hoc with no typed registry, no unknown-flag detection |

## Measured problems (this repo, today)

1. **Default build ships without core capabilities.** `krishiv` `default = ["local"]`
   = `[embedded, single-node, jemalloc, rest-catalog, iceberg]`. It has **no
   `kafka`** and **no `cloud`/`s3`**. `build-fast-engine.sh` builds `-p krishiv`
   with default features, so the shipped image cannot do Kafka streaming or
   object-store I/O — and nothing says so until a job fails.
2. **Dead & duplicate features.** `s3` gates **0** cfg-sites (the real gate is
   `cloud`, 3 sites) — a feature that does nothing. `bare-metal` and `cluster`
   are both aliases of `distributed`. `kafka`/`iceberg`/`jemalloc` are
   *re-declared* in 6+ crates and hand-propagated.
3. **High combinatorial surface, low coverage.** 51 features → 2⁵¹ nominal
   build configs; CI builds ~2 (default + workspace). Most `#[cfg]` combinations
   are never compiled (`krishiv-python` is even excluded from `just test`/`lint`
   — the [[krishiv-python-ci-blindspot]] class).
4. **No compile-time capability manifest.** The binary can't answer "was I built
   with kafka? cloud? iceberg?" — operators discover gaps by failure, not by
   inspection.
5. **Runtime flags: 162 ad-hoc reads, partial registry.** `env_registry.rs`
   exists but does not cover all reads; unknown-flag detection is weak (a
   misspelled `KRISHIV_*` silently no-ops — audit FLAG-4). Truthiness is mostly
   consolidated on `truthy_env` (FLAG-2 fixed) but endpoint names still have
   un-aliased variants (FLAG-3).

## The unifying principle

> **Compile-time features gate optional *dependency weight* only. Runtime
> capability is selected by env/config and validated against a typed registry.
> Any capability a feature can remove MUST be reported in a runtime manifest.**

The `cloud`-gates-S3 surprise violates this: a heavy-dep gate silently removed a
capability with no runtime signal. The fix is not "more flags" — it is making the
few flags that remain *visible* and *validated*.

## Plan

### A. Deployment (cargo) features — minimize & make visible

1. **Two-tier taxonomy, declared once.** Split features into (a) **profiles**
   that pick a deployment shape — `embedded | single-node | distributed | k8s`
   (keep these) — and (b) **capabilities** that pull optional dep families —
   `kafka | iceberg | delta | hudi | cloud | vector-sinks | state | etcd`.
   Declare each capability **once** at the `krishiv` crate and let it propagate
   via a single dependency edge; stop re-declaring `kafka`/`iceberg`/`jemalloc`
   per crate.
2. **Delete dead/duplicate features.** Remove `s3` (0 cfg-sites) or make it a
   documented alias of `cloud`. Collapse `bare-metal`/`cluster` to one alias.
   Fold `minimal` into `embedded`.
3. **Ship a real `prod` preset** = `distributed + kafka + iceberg + cloud +
   jemalloc`, and build **that** in `build-fast-engine.sh` (not bare default).
   This alone removes today's silent kafka/cloud gaps.
4. **Compile-time capability manifest (highest leverage).** A
   `krishiv::build_capabilities()` const assembled from `cfg!(feature=…)`,
   surfaced three ways: `krishiv --capabilities`, a one-line startup log
   (`capabilities: kafka=on iceberg=on cloud=off …`), and a `/healthz` /
   metrics field. Silent gaps become visible facts.
5. **CI gates the shipped preset(s).** Extend the `build-fast-engine`
   durable-CTAS marker check into a capability assertion (kafka+cloud+iceberg
   present for `prod`); add a `--features prod` build to CI so the shipped combo
   is actually compiled and smoke-tested.

### B. Runtime (env) flags — centralize & validate

1. **All reads through the typed registry.** Every `KRISHIV_*` is declared in
   `env_registry.rs` (name, type, default, scope, secret?). Ban raw
   `std::env::var("KRISHIV_…")` outside the registry (lint/grep gate).
2. **Unknown-flag detection at startup** (FLAG-4): warn on any `KRISHIV_*` in the
   environment not in the registry — kills typo-silent-no-op.
3. **Canonical names + deprecated aliases with a startup warning** (FLAG-3):
   e.g. the three coordinator-endpoint spellings collapse to one.
4. **Audited security-posture surface.** Group the escape-hatch runtime flags
   (`KRISHIV_ALLOW_ANONYMOUS`, `KRISHIV_FLIGHT_ALLOW_ALL_AUTHENTICATED`,
   `KRISHIV_ALLOW_LEGACY_FRAGMENTS`, `KRISHIV_ALLOW_ANONYMOUS_HTTP`) into one
   documented set that the SEC-7 boot banner already narrates.

## Sequencing (low-risk first)

1. Capability manifest + startup log (A4) — pure addition, immediate visibility.
2. `prod` preset + build-script/CI switch (A3, A5) — fixes the shipped-gap.
3. Delete dead features `s3`/`minimal`/`bare-metal` (A2) — mechanical.
4. Registry completion + unknown-flag warn (B1, B2) — incremental per crate.
5. Capability propagation cleanup (A1) — the largest, do last with the manifest
   as the regression guard.

## Relation to the DUR-2 S3-native sink work

Making `IcebergStreamingSink` object-store-native (the chosen Option B) belongs
behind the **same `cloud` capability**, must appear in the capability manifest,
and must be in the `prod` preset — otherwise it reintroduces the exact silent gap
this plan closes.
