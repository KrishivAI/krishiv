# Krishiv Public Roadmap

This roadmap communicates priorities, not release promises. Accepted work must
still follow the architecture and compatibility contracts.

## Current focus: production engine foundations

1. **Distributed batch reliability** — memory accounting, spillable operators,
   distributed sink commits, shuffle failure recovery, statistics, and adaptive
   execution.
2. **Authoritative streaming recovery** — repeated fault testing, state
   redistribution, savepoint upgrades, and certified source/sink combinations.
3. **User API completeness** — typed expressions, Python parity, prepared
   statements, query progress/cancellation, and SQL gateway interoperability.
4. **Iceberg-first lakehouse quality** — snapshot planning, schema/partition
   evolution, concurrent commits, row-level operations, and streaming writes.
5. **Open-source readiness** — reproducible releases, compatibility fixtures,
   contributor onboarding, benchmark history, and security response.

## Near-term acceptance criteria

- No blanket exactly-once claim; certified combinations are named explicitly.
- Distributed durable deployments pass coordinator/executor/object-store failure
  loops.
- Large batch queries spill instead of failing solely because inputs exceed RAM.
- Public APIs and durable metadata have documented compatibility windows.
- Benchmark changes include reproducible command, dataset, hardware, and commit
  metadata.

## Later engine work

- Unaligned checkpoints and regional recovery.
- Materialized-view and incremental-maintenance primitives.
- Runtime filters, skew mitigation, and broader adaptive execution.
- JDBC/ODBC through a versioned SQL gateway.
- Completed-job history service.
- Optional GPU resource/execution support.

## Explicitly outside the engine

Collaborative notebooks, workflow orchestration across jobs, billing, managed
SQL warehouses, enterprise governance administration, dashboards, model
registry/serving, and AI-agent products belong to a separate data platform.
