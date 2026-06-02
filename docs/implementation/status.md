# Krishiv Implementation Status

## Current Session (Completed)

### Runtime Mode Improvements

**`crates/krishiv-api/src/types.rs`**
- Added `DeploymentTarget` enum: `Embedded | SingleNode | BareMetal | Kubernetes`
  — orthogonal to `ExecutionMode` (HOW queries route vs WHERE the cluster runs).
  `From<ExecutionMode>` impl provides the default mapping.

**`crates/krishiv-api/src/session.rs`**
- `SessionBuilder::from_env()` — reads `KRISHIV_MODE`, `KRISHIV_COORDINATOR_URL`/`KRISHIV_COORDINATOR`, `KRISHIV_REMOTE_EXEC`. Handles all five mode values: `embedded`, `single-node`, `distributed`, `bare-metal`, `k8s`. Returns clear errors for unknown modes or missing coordinator URLs in distributed modes.
- `Session::from_env()` — convenience wrapper, recommended entry for k8s/bare-metal pods.
- `SessionBuilder::with_deployment_target()` — explicit override for telemetry.
- `Session::deployment_target()` — accessor; k8s deployments get `Kubernetes`, bare-metal get `BareMetal`.
- `deployment_target` stored in `Session` struct alongside `mode`.

**`crates/krishiv-api/src/lib.rs`**
- `DeploymentTarget` added to public re-exports.

**`crates/krishiv/src/lib.rs`**
- `DeploymentTarget` added to facade re-exports.

**`crates/krishiv-python/src/session.rs`**
- `PySession::from_env()` now delegates to `krishiv_api::Session::from_env()` — Python and Rust share identical env-var parsing. Previously Python re-implemented the logic with slight differences (`k8s` not handled, default was `SingleNode` not `Embedded`).

**`crates/krishiv/Cargo.toml`**
- Feature flag comments rewritten to accurately describe that features gate DEPENDENCIES not code paths, and that execution mode is always runtime-selected.

## What was NOT changed (correct as-is)
- `ExecutionMode → RuntimeMode + ExecutionPlacement` two-layer design — sound, keep it.
- Feature flags gating `flight-sql`, `shuffle`, `etcd`, `sqlite`, `k8s` optional deps — correct.
- All backends (Embedded, SingleNode, Distributed) always compiled — intentional; `#[cfg]` guards on backends would save marginal binary size at high complexity cost.
- `InProcessCluster::new()` for distributed sessions — it creates in-memory data structures only, no OS thread spawning; not worth a `new_minimal()`.

## Validation
```
cargo check -p krishiv-api -p krishiv-python -p krishiv   # clean
just check   # all 4 modes clean
```

## Next Steps
- Expose `SqlEngine::register_kafka_source` to Python via PyO3.
- Use `session.deployment_target()` in metrics labels (once OTLP plumbing is wired).
- True streaming windows for unbounded `GROUP BY` (DataFusion streaming exec).
