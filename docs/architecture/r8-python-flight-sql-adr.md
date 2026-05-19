# R8 Architectural Decision Record: Python/Tokio Thread Model and Flight SQL Routing

## Context

R8.1 adds Python bindings via PyO3, vectorized UDFs, and a Flight SQL endpoint. Two architectural decisions must be locked in before any code is written, as choosing incorrectly requires full rewrites.

---

## Decision 1: Python/Tokio Thread Model

### Problem

PyO3 Python bindings and Tokio async runtime both have exclusive ownership requirements:
- Tokio worker threads must never block waiting for I/O or CPU — they use cooperative yielding.
- Python holds the GIL (Global Interpreter Lock) for all CPython operations — holding it on a Tokio worker thread blocks the entire Tokio runtime while Python runs.
- A crashing Python UDF can take down the process if run on the same thread as a Tokio worker.

### Options Considered

**Option A — Inline execution on Tokio thread**: Call UDF directly from the operator loop. Simple, but holding the GIL on a Tokio worker thread can starve all other async tasks. A panicking UDF crashes the executor process.

**Option B — `spawn_blocking` per UDF call (CHOSEN)**: Route every Python UDF call through `tokio::task::spawn_blocking`, which runs it on Tokio's dedicated blocking thread pool (separate from async workers). The GIL is released by Tokio before the blocking thread runs, and the async continuation waits without holding a worker thread. A panicking UDF is caught at the `spawn_blocking` boundary.

**Option C — Subprocess per UDF execution**: Fork a Python subprocess and pass Arrow batches over a Unix socket (Arrow IPC). Maximum isolation — a crashing UDF cannot affect the executor. Chosen for streaming UDFs (post-GA) but not for batch UDFs in R8.1 because the per-invocation latency is too high for vectorized batch processing.

### Decision

**R8.1 batch UDFs use `spawn_blocking` (Option B).**

Rationale:
- Eliminates GIL-on-Tokio-thread deadlock with zero API surface change.
- `spawn_blocking` boundary catches panics and returns `Err(JoinError::Panicked)` — the executor can report task failure without crashing.
- Tokio's blocking thread pool defaults to 512 threads, sufficient for concurrent UDF batches.
- Arrow `RecordBatch` is `Send` so it can move across the blocking thread boundary safely.

**Streaming UDFs are explicitly out of scope for R8.1.** They will use Option C (subprocess, Arrow IPC over Unix socket) in a post-GA release, which is documented in the R8 tracker.

### Implementation Contract

```rust
// In krishiv-udf / krishiv-executor: every UDF invocation goes through this pattern:
pub async fn call_python_udf(
    udf: Arc<dyn PythonUdf>,
    batch: RecordBatch,
) -> Result<RecordBatch, UdfError> {
    tokio::task::spawn_blocking(move || {
        // GIL acquired here, on blocking thread, not on Tokio worker.
        Python::with_gil(|py| udf.call(py, &batch))
    })
    .await
    .map_err(|join_err| UdfError::Panic(join_err.to_string()))?
}
```

### asyncio Integration

`await session.sql_async()` from Python requires Tokio to run without blocking the Python event loop.

**Decision**: Krishiv owns its own Tokio runtime embedded in the PyO3 module (`once_cell::sync::Lazy<Runtime>`). Python callers do not provide a runtime. When called from an `asyncio` context, the PyO3 `#[pyo3(signature=...)]` function releases the GIL and blocks the calling Python thread on `runtime.block_on(future)`, which does not block any `asyncio` loop workers since it runs on the calling Python thread.

Callers using `asyncio` who want non-blocking behavior must call `await asyncio.get_event_loop().run_in_executor(None, session.sql, query)` — this is documented in the Python API reference.

---

## Decision 2: Flight SQL Query Routing

### Problem

Flight SQL adds a new network endpoint for SQL execution. If it is implemented as a separate query path it will immediately diverge from the CLI and Rust API paths, creating two surfaces that must be kept in sync forever.

### Options Considered

**Option A — Parallel query path**: Flight SQL service parses SQL and calls DataFusion directly, bypassing the Krishiv session/planner/runtime layer.

**Option B — Thin adapter over existing session (CHOSEN)**: The Flight SQL service creates a Krishiv `Session` (the same type used by the CLI), calls `session.sql(query)`, and maps the `QueryResult` (an Arrow `RecordBatch` list) onto the Flight SQL response stream.

**Option C — Route through coordinator gRPC**: Client sends SQL to coordinator via a new `SubmitSqlJob` RPC, coordinator dispatches it as a normal job, and the Flight SQL service polls for results. Provides full distributed execution but adds significant latency for interactive queries.

### Decision

**R8.1 uses Option B (thin adapter)** for the embedded/single-node execution model, which covers the R8.1 acceptance gate.

Rationale:
- Zero divergence: any fix or optimization to `session.sql()` automatically applies to Flight SQL.
- The CLI, Rust API, and Flight SQL all exercise the same code path — one set of tests covers all three.
- Option C (distributed via coordinator) is the natural R9/R10 extension: the Flight SQL service can switch to `SubmitSqlJob` routing for queries that exceed single-node capacity, without changing the Flight SQL protocol surface.

### Implementation Contract

```rust
// krishiv-ui or a new krishiv-flight-sql service:
impl FlightSqlService for KrishivFlightSqlService {
    async fn do_get_sql_info(&self, query: CommandStatementQuery) -> Result<FlightStream> {
        let session = Session::embedded(); // same as CLI
        let result = session.sql(&query.query).await?;
        // Map QueryResult -> RecordBatch stream
        Ok(result_to_flight_stream(result))
    }
}
```

---

## Decision 3: Beta API Stability Boundary

R8 marks Python and lakehouse APIs "beta." The Rust API must be frozen first.

### Decision

- All `pub` symbols in `krishiv-api`, `krishiv-sql`, and `krishiv-runtime` are considered **stable** as of R8.1. Any breaking change requires a semver bump.
- All symbols in `krishiv-python` and `krishiv-lakehouse` are **beta** — they may change between R8.x releases. This is communicated via:
  - `#[doc = "**Beta API**: may change between minor releases."]` on all public items in those crates.
  - A `STABILITY.md` at the crate root.
  - Python `__version_info__` carrying `(0, 8, 0, "beta")`.
- PyO3's `#[pyclass]` and `#[pyfunction]` macros do not distinguish stable/beta — the documentation is the stability contract.

---

## Summary Table

| Decision | Option Chosen | Key Constraint |
|---|---|---|
| UDF thread model | `spawn_blocking` | Never hold GIL on Tokio worker |
| Streaming UDF isolation | Subprocess (post-GA) | Crash isolation for long-running tasks |
| asyncio integration | Embedded Tokio runtime | Caller does not provide runtime |
| Flight SQL routing | Thin adapter over `Session` | No query path divergence |
| API stability | Beta annotation via docs | Rust API frozen; Python/lakehouse beta |
