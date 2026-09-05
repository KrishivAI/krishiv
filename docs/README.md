# Krishiv documentation

The Rust workspace is the source of truth; these documents explain it. Start
with the architecture set, which covers every feature and every crate, then
use the contracts, decisions, and references as needed.

## Architecture (read in order, or jump by topic)

| # | Document | Covers |
|---|---|---|
| 00 | [Overview](architecture/00-overview.md) | the three axes (engine × placement × surface), the ten invariants, component map, crate ownership, an end-to-end request, what Krishiv is not |
| 01 | [Execution modes](architecture/01-execution-modes.md) | `Embedded` / `SingleNode` / `Distributed`, placement, `SessionBuilder`, `from_env`, the fail-closed rule, the sync/async seam, CLI routing |
| 02 | [SQL engine](architecture/02-sql-engine.md) | `SqlEngine`, the `sql()` pipeline, pre-optimizer rewrites (CTE materialisation, grouping sets), intercepted statements, functions, optimizer rules, session config, catalogs, UDFs |
| 03 | [Planning and optimization](architecture/03-planning-and-optimization.md) | the two plan layers, `krishiv-plan` rules, DataFusion facts, plan → stages → tasks, adaptive execution, statistics, `EXPLAIN` |
| 04 | [Scheduler and coordinator](architecture/04-scheduler-and-coordinator.md) | CCP/JCP, leadership and fencing, job/task lifecycle, placement policies, failure handling, metadata stores, result spools, checkpoint coordination, control surfaces |
| 05 | [Executor and data plane](architecture/05-executor-and-data-plane.md) | task intake and execution models, batch tasks, the capacity model, streaming loops in-process, resident IVM, reporting |
| 06 | [Shuffle](architecture/06-shuffle.md) | store contract, backends and tiering, lease fencing, writers and indexes, reclamation, runtime filters, the shuffle service |
| 07 | [State, checkpoints, savepoints](architecture/07-state-checkpoints-savepoints.md) | state backends, key groups and rescaling, timers, migrations, checkpoint layout and integrity, the barrier protocol, exactly-once sinks, savepoints, restore |
| 08 | [Streaming](architecture/08-streaming.md) | front doors, watermarks, window operators, the operator library, the driver policy, the loops, dials, delivery guarantees |
| 09 | [Incremental view maintenance](architecture/09-incremental-view-maintenance.md) | Z-set algebra, planning and decomposition, the flow, partitioning, hosting and the resident protocol, surfaces, streaming versus incremental |
| 10 | [Connectors and lakehouse](architecture/10-connectors-and-lakehouse.md) | source/sink/2PC contracts, capabilities and guarantees, the registry and inventory, Iceberg/Delta/Hudi, CDC, data quality |
| 11 | [Public interfaces](architecture/11-public-interfaces.md) | Rust API, Python, CLI, Flight SQL, HTTP control plane, web console, MCP, Kubernetes CRDs |
| 12 | [Security](architecture/12-security.md) | production mode, authentication per surface, RBAC and policy hooks, TLS, fencing and integrity, input validation, supply chain |
| 13 | [Observability](architecture/13-observability.md) | metrics families, traces, logs, coordinator signals, per-query metrics, console, health |
| 14 | [Deployment and durability](architecture/14-deployment-and-durability.md) | durability profiles, placements (embedded, single node, bare metal, Kubernetes), HA, upgrades |
| 15 | [Configuration](architecture/15-configuration.md) | Cargo features and presets, the `KRISHIV_*` registry, session settings, precedence |
| 16 | [Performance](architecture/16-performance.md) | benchmarks, current numbers, the decisions measurements drove, measurement discipline, capacity guidance |
| 17 | [Testing and quality](architecture/17-testing-and-quality.md) | the standing rule, CI tiers, lint policy, the async contract, generated documents and blessing, property suites, conformance |
| 18 | [Compatibility and versioning](architecture/18-compatibility-and-versioning.md) | what is versioned, policy, dependency baseline, deprecation |

## Contracts and decisions

- [`contracts/engine-semantics.md`](contracts/engine-semantics.md) — batch and
  streaming semantics, delivery guarantees, the exactly-once matrix, operator
  identity, the Iceberg-first policy. Normative.
- [`contracts/connectors.md`](contracts/connectors.md) — source/sink
  obligations and maturity labels for every in-tree connector. Normative.
- [`decisions/`](decisions/README.md) — architecture decision records:
  [0001](decisions/0001-record-architecture-decisions.md) recording decisions,
  [0002](decisions/0002-public-api-shape-and-execution-semantics.md) public API
  shape and execution semantics, [0003](decisions/0003-task-fragment-encoding.md)
  task fragment encoding, [0004](decisions/0004-wire-protocol-front-door.md)
  Flight SQL as the wire front door.

## Reference (generated — do not edit by hand)

| Document | Generated from | Regenerate |
|---|---|---|
| [`reference/env-flags.md`](reference/env-flags.md) | `krishiv_common::env_registry` | `KRISHIV_BLESS_ENV_FLAGS=1` |
| [`reference/sql-feature-matrix.md`](reference/sql-feature-matrix.md) | SQL feature registry | `KRISHIV_BLESS_SQL_MATRIX=1` |
| [`reference/pyspark-parity.md`](reference/pyspark-parity.md) | `krishiv_api::pyspark_parity` | `KRISHIV_BLESS_PYSPARK_PARITY=1` |
| [`reference/certification-matrix.md`](reference/certification-matrix.md) | `krishiv_connectors::cert_matrix` | `KRISHIV_BLESS_CERT_MATRIX=1` |
| [`reference/connector-reachability-matrix.md`](reference/connector-reachability-matrix.md) | connector registry | bless switch in its test |
| [`reference/krishiv-vs-spark-sql.md`](reference/krishiv-vs-spark-sql.md) | conformance results | bless switch in its test |
| [`reference/jdbc-connectivity.md`](reference/jdbc-connectivity.md) | hand-written | — |

## Guides and operations

- [`guides/running-examples.md`](guides/running-examples.md) — running the
  examples in each mode.
- [`connector-sdk.md`](connector-sdk.md) — building a connector.
- [`BENCHMARKING.md`](BENCHMARKING.md) and
  [`benchmarks-tpcds.md`](benchmarks-tpcds.md) — how to benchmark and the
  TPC-DS record.
- [`grafana/`](grafana/README.md) — the dashboard.
- [`COMPATIBILITY.md`](COMPATIBILITY.md), [`RELEASE.md`](RELEASE.md),
  [`ROADMAP.md`](ROADMAP.md), [`GOVERNANCE.md`](GOVERNANCE.md).
- [`../deploy/k8s/README.md`](../deploy/k8s/README.md) — Kubernetes manifests.

## Engineering log (evidence, not reference)

`engineering-log/` holds the durable working records. They are kept because
the architecture documents cite them; they are not the place to learn the
system.

- [`status.md`](engineering-log/status.md) — the session handoff note (what
  is in flight, how it was validated, the next command).
- [`crate-audit-register.md`](engineering-log/crate-audit-register.md) — the
  read-every-file audit: per crate, per fix, with revert-proven tests and
  commit hashes; open items marked "needs a decision".
- [`ivm-audit-register.md`](engineering-log/ivm-audit-register.md) — the same
  for the incremental engine.
- Dated evidence: [production readiness audit](engineering-log/production-readiness-audit-2026-07.md),
  [honesty audit](engineering-log/honesty-audit-2026-07-25.md),
  [wire-or-delete review](engineering-log/wire-or-delete-2026-07.md),
  [distributed batch review](engineering-log/distributed-batch-review-2026-07-27.md),
  [SOTA practices survey](engineering-log/sota-distributed-engine-practices-2026-07-27.md),
  [HA chaos gate log](engineering-log/ha-chaos-gate-log.md),
  [GA soak report](engineering-log/ga-soak-report-2026-08-10.md).

Superseded phase plans and design notes were removed from the tree in the
2026-09 documentation rewrite; they remain in git history.

## Conventions

- Architecture documents describe the code as it is. When behaviour changes,
  the document changes in the same PR; when a document and the code disagree,
  the code is right and the document is a bug.
- Generated references are never hand-edited; run the bless switch.
- `scripts/check_markdown_links.py .` validates every repo-local link and runs
  in `just project-check`.
- `docs/engineering-log/status.md` receives a short handoff note per
  substantial session, not planning prose.
