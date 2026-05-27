# R13 Python-First Streaming API Implementation Tracker

## Goal

> **Status (2026-05-23):** R13 acceptance met on branch `cursor/implement-r13-7aa2` — Python package, transforms, bridges, CI, and runtime gap closures (GAP-RT-03/08, GAP-OB-01). Flight SQL client (GAP-RT-01) deferred to R14.


Expose the full Krishiv streaming compute engine to Python through a first-class
PyO3-backed package (`pip install krishiv`) with a schema-declared source API,
an asyncio-native streaming loop, a composable transformation chain, Pandas and
PyArrow interoperability, Jupyter display support, and a coherent error
hierarchy. The release targets data engineers who write Python and should never
need to know Rust exists. All blocking engine work from R12 must be complete
before Sprint 1 begins.

## Scope

In scope:

- `maturin` build pipeline producing `manylinux2014` and `macOS` (x86_64 and
  arm64) wheels and a source distribution.
- PyPI-ready `krishiv` package with optional extras: `krishiv[kafka]`,
  `krishiv[iceberg]`, `krishiv[arrow]`.
- `.pyi` type stub generation checked into the source tree.
- `ks.Schema` base class with Python type-annotation → Arrow `DataType`
  mapping at class-definition time.
- **Deployment-mode factory methods** covering all four modes — embedded,
  local single-node, remote cluster, and environment-driven:
  - `ks.Session.embedded()` — in-process DataFusion SQL, no streaming.
  - `ks.Session.local()` — in-process coordinator + executor via
    `InProcessCoordinator` (R12 ADR-12.4); full streaming semantics, no network.
  - `ks.Session.connect(url)` — remote coordinator via Flight SQL (R12 ADR-12.3);
    targets both bare-metal clusters and Kubernetes service endpoints.
  - `ks.Session.connect_async(url)` — asyncio-native variant of `connect()`;
    returns an awaitable (ADR-R13-01 Option C).
  - `ks.Session.from_env()` — reads `KRISHIV_COORDINATOR` env var; falls back
    to `local()` if unset; standard entry point for cloud/K8s deployments.
- Sources: `ks.read_kafka()`, `ks.read_parquet()`, `ks.read_iceberg()`.
- Transformation chain: `.with_watermark()`, `.key_by()`, `.window()`,
  `.agg()`.
- Asyncio-native: `async for batch in stream.window()`.
- Sinks: `ks.sinks.parquet()`, `ks.sinks.kafka()`, `ks.sinks.iceberg()`.
- Pandas bridge: `batch.to_pandas()`.
- PyArrow bridge: `batch.to_arrow()`.
- Jupyter `_repr_html_()` on `Batch`, `Stream`, `Schema`.
- Error hierarchy: `KrishivError`, `QueryError`, `SchemaError`,
  `ConnectorError`, `CheckpointError`, `AuthorizationError`.

Out of scope:

- `CREATE LIVE TABLE` SQL syntax (R14).
- `@ks.transform(memo=True)` memoization (R14).
- Multi-table CDC fan-out with schema evolution (R14).
- JVM (PySpark-compatible) API surface.
- Windows wheel builds (deferred; manylinux covers Linux CI; macOS covers
  developer laptops).

## Dependencies

- R12 **audit scope** (P0/P1 bugs) is closed on the R12 branch; **R12 carryover**
  maturity gaps must be closed or explicitly deferred before R13 Sprint 1.
  See [`docs/architecture/r12-maturity-gap-register.md`](../architecture/r12-maturity-gap-register.md).
- **R12 carryover prerequisites for R13** (minimum):
  - **GAP-RT-04**: real remote coordinator gRPC (CLI and Python `connect()` depend on this).
  - **GAP-RT-01 / ADR-12.3**: `DistributedBackend` Flight SQL transport — not log-only stub.
  - **GAP-RT-03**: `WindowedStream` → executor / plan lowering for asyncio window loops.
  - **GAP-CP-09**: executor binary task loop (or R13 documents library-only executor for dev).
  - **GAP-CN-01**: `kafka` feature compiles (`cargo build -p krishiv-connectors --features kafka`).
- **R13 Sprint 6 / deployment layer** (partially landed; gap-tracked):
  - `DistributedBackend` struct exists; full Flight client — **GAP-RT-01** (R13).
  - `SingleNodeBackend` in-process coordinator — **GAP-RT-01**, S6.2 (R13).
  - `EmbeddedBackend` streaming redirect — **GAP-RT-03**, S6.3 (R13).
  - Federation remains scheduler-owned for now (**GAP-FD-01** remote client is
    still an R19 concern; no standalone federation crate is present).
- `cargo test --workspace` passes clean on the baseline before any R13 edits.
- Python ≥ 3.10 in CI (for `match` statement in bridge code and PEP 604 union
  types in stubs).
- `maturin ≥ 1.4` installed in CI (`pip install maturin`).

## Deployment Mode Support

Every `ks.Session` factory method maps to one `ExecutionMode` in the Rust
engine. The mode determines which features are available and which Rust backend
handles execution. This table is the definitive contract for both the Python
and Rust APIs in R13.

### Python API (`ks.*`) — factory methods and capabilities

| Factory | Mode | Rust backend | SQL | Streaming | Keyed state | Multi-executor | Network |
|---------|------|-------------|-----|-----------|-------------|---------------|---------|
| `ks.Session.embedded()` | `Embedded` | `EmbeddedBackend` → DataFusion | ✅ | ❌ | ❌ | ❌ | none |
| `ks.Session.local()` | `SingleNode` | `SingleNodeBackend` → `InProcessCoordinator` | ✅ | ✅ | ✅ | ❌ | none |
| `ks.Session.connect(url)` | `Distributed` | `DistributedBackend` → Flight SQL | ✅ | ✅ | ✅ | ✅ | TCP to coordinator |
| `ks.Session.connect_async(url)` | `Distributed` | same, asyncio bridge | ✅ | ✅ | ✅ | ✅ | TCP to coordinator |
| `ks.Session.from_env()` | `SingleNode` or `Distributed` | reads `KRISHIV_COORDINATOR`; `local()` if unset | ✅ | ✅ | ✅ | if coordinator | optional |

Kubernetes is not a separate mode — it is `Distributed` with the coordinator
URL pointing at the K8s `coordinator-service` ClusterIP or LoadBalancer
endpoint. `from_env()` is the standard K8s entry point: set
`KRISHIV_COORDINATOR=http://krishiv-coordinator.krishiv-system.svc:50051`
in the pod spec.

### Rust API (`Session`, `DataFrame`, `Stream`) — equivalent constructors

| Rust constructor | Mode | Notes |
|-----------------|------|-------|
| `Session::new()` | `Embedded` | Default; DataFusion SQL only |
| `Session::builder().with_mode(SingleNode).build()` | `SingleNode` | Full streaming; no network |
| `Session::builder().with_coordinator(url).build()` | `Distributed` | Flight SQL to remote coordinator |
| `std::env::var("KRISHIV_COORDINATOR")` | `Distributed` | Use in server-side Rust code on K8s |

### Missing coverage entering R13 (to be closed in Sprint 1)

Before R13, none of these factory methods exist in Python. The only Python
entry point was `ks.Session.connect_async()` (a single stub). Sprint 1 task
S1.5 adds all five factory methods plus a deployment-mode guard that raises
`ModeError` with a clear message when a feature is called in an unsupported
mode (e.g., `.stream()` in `embedded()` mode before R12 S6 is complete).

---

## Architectural Decisions Required

### ADR-R13-01: Python Async / Tokio Bridge for ks.Session.connect_async()

**Problem**

PyO3 cannot hold the Python GIL across `.await` points in a Tokio future.
`ks.Session.connect_async()` must return a Python-native `Awaitable` that
integrates with the user's `asyncio` event loop, but the work runs on a
dedicated Tokio runtime. There is no single standard pattern for this in the
PyO3 ecosystem.

**Options**

- A. `pyo3-asyncio` crate: wraps a Tokio future as a Python coroutine.
  Maintained status is uncertain (last release 2023); may lag PyO3 API changes.
  Simplest call-site code.
- B. Manual `Future` bridging: spawn a Tokio task, return a Python object
  backed by `Arc<Mutex<Option<Result<…>>>>`, expose a `__await__` that polls
  the slot using `asyncio.run_coroutine_threadsafe` from a dedicated thread.
  No external dependency; more code to maintain.
- C. Dedicated thread with `asyncio.run_coroutine_threadsafe`: a Rust-side
  background thread owns the Tokio runtime and posts completed futures back
  into the user's asyncio loop via `pyo3::Python::attach` + the Python
  `asyncio` module. Requires careful lifetime management but avoids both the
  unmaintained crate and the complex manual slot.

**Recommendation**

Option C. The dedicated Tokio runtime thread approach avoids the pyo3-asyncio
maintenance risk and is well-understood in production PyO3 projects. The
`ks.Session` object holds a `Arc<TokioRuntimeThread>` that is started at
`connect_async()` and joined at `__del__`. Batch delivery crosses the boundary
via `Python::attach()` inside a `spawn_blocking` call on the Tokio side,
releasing the GIL between deliveries.

**Risk if deferred**

`ks.Session.connect_async()` and `async for batch in stream.window()` cannot
be implemented without resolving this bridge. Deferring blocks Sprint 5 and
leaves the asyncio API as stubs.

---

### ADR-R13-02: ks.Schema Python Type-Annotation to Arrow DataType Mapping

**Problem**

`ks.Schema` subclasses declare columns via Python class-level type annotations
(e.g., `name: str`, `value: float`, `ts: datetime`). These annotations must
be resolved to Arrow `DataType` values at class-definition time (not instance
creation time) so that schema validation can happen before any data flows.
Python's normal `__init__` runs too late.

**Options**

- A. `__init_subclass__` hook: inspect `cls.__annotations__` when a subclass is
  defined. Simple, no metaclass overhead, standard Python ≥ 3.6 pattern.
- B. Metaclass `SchemaMeta`: intercept class creation, validate and store the
  Arrow schema as a class attribute. More powerful (allows class-level
  validation errors), but metaclass conflicts with dataclass inheritance.
- C. Decorator `@ks.schema`: explicit opt-in annotation, avoids both hooks and
  metaclasses. Breaks the subclassing API proposed in the roadmap.

**Recommendation**

Option A. `__init_subclass__` is the cleanest pattern that matches the roadmap
API (`class MySchema(ks.Schema): …`). Conflicts with `@dataclass` are avoided
by not inheriting from `dataclass`. The mapping table (`str → Utf8`, `int →
Int64`, `float → Float64`, `bool → Boolean`, `datetime → TimestampMicrosecond`,
`bytes → LargeBinary`) is defined once in `krishiv-python/src/schema.rs` and
called from the PyO3 class `__init_subclass__` implementation.

**Risk if deferred**

Without schema resolution at class-definition time, `ks.read_kafka()` cannot
validate the schema against the Kafka message format until the first batch
arrives, making schema mismatches runtime errors instead of startup errors.

---

### ADR-R13-03: GIL Management for async for batch in stream.window()

**Problem**

Each batch delivered from the Tokio streaming engine must cross the Rust →
Python boundary. If the GIL is held on a Tokio worker thread while waiting for
the next batch, all other Python threads stall. If the GIL is released without
care, the Python batch object may be accessed without the GIL.

**Options**

- A. `spawn_blocking` on every batch delivery: move the GIL acquire +
  Python-object construction into a `spawn_blocking` closure so Tokio worker
  threads are never stalled waiting for the GIL.
- B. Dedicated GIL-holding thread: a single Python thread owns the GIL and
  receives batches from Tokio via a `tokio::sync::mpsc` channel. Serializes
  all Python object construction through one thread.
- C. `Python::attach()` with `allow_threads` guards: call
  `Python::attach()` only at the moment of object construction, then
  immediately release via `allow_threads`. Requires careful scoping per batch.

**Recommendation**

Option A. `spawn_blocking` is the idiomatic Tokio approach for GIL-bound work.
It keeps Tokio worker threads free for I/O while offloading GIL acquisition to
the blocking thread pool. Combined with ADR-R13-01 (Option C), each batch
arrives from the Tokio runtime thread, is dispatched to `spawn_blocking` for
Python construction, then posted to the asyncio loop.

**Risk if deferred**

Holding the GIL on a Tokio worker will cause throughput collapse under any
concurrent Python workload and may deadlock if the Python main thread calls back
into Krishiv while the GIL is held on a worker.

## Sprint 1 — maturin Pipeline & Package Infrastructure

### S1.1: krishiv-python crate scaffold — krishiv-python

- [ ] Create `krishiv-python/` crate with `[lib] crate-type = ["cdylib"]`.
- [ ] Add `pyo3 = { version = "0.21", features = ["extension-module"] }` and
      `maturin` build metadata to `Cargo.toml`.
- [ ] Add `krishiv-python` to the workspace `Cargo.toml` members list.
- [ ] Implement a minimal `#[pymodule]` named `_krishiv` that exposes a
      `__version__` string constant.

**Validation**: `maturin develop -m krishiv-python/Cargo.toml && python -c "import _krishiv; print(_krishiv.__version__)"`

### S1.2: Python package facade — krishiv (Python package)

- [ ] Create `python/krishiv/__init__.py` that imports from `_krishiv` and
      re-exports the public API. Users import as `import krishiv as ks` or
      `from krishiv import Session, Schema`.
- [ ] Add `python/krishiv/py.typed` marker file.
- [ ] Add `pyproject.toml` with `[tool.maturin]` pointing to the
      `krishiv-python` crate.

**Validation**: `pip install -e ".[dev]" && python -c "import krishiv as ks; print(ks.__version__)"`

### S1.3: .pyi stub generation

- [ ] Add `pyo3-stub-gen = "0.6"` (or equivalent) to `krishiv-python`.
- [ ] Run `cargo run --bin generate-stubs` to emit `python/krishiv/_krishiv.pyi`.
- [ ] Check the generated `.pyi` into source control.
- [ ] Add a CI step that re-generates stubs and asserts no diff (`git diff
      --exit-code`).

**Validation**: `python -c "import krishiv; help(krishiv.Session)"`

### S1.4: CI wheel build matrix

- [ ] Add a GitHub Actions job matrix: `manylinux2014_x86_64`,
      `manylinux2014_aarch64`, `macos-13` (x86_64), `macos-14` (arm64).
- [ ] Use `maturin build --release --out dist/` in each matrix leg.
- [ ] Upload wheels as CI artifacts; do not publish to PyPI until R13 acceptance
      gate is green.

**Validation**: CI matrix passes with wheel artifacts for all four targets.

### S1.5: Deployment-mode factory methods — krishiv-python

- [ ] Implement `Session.embedded()` classmethod: constructs `PySession` with
      `ExecutionMode::Embedded`; raises `ModeError` if `.stream()` is called
      (streaming requires `local()` or `connect()`).
- [ ] Implement `Session.local()` classmethod: constructs `PySession` with
      `ExecutionMode::SingleNode`; starts `InProcessCoordinator` on first
      streaming call (R12 ADR-12.4). SQL and streaming both work.
- [ ] Implement `Session.connect(url: str)` classmethod: validates `url` is a
      valid HTTP/HTTPS URL, constructs `PySession` with `ExecutionMode::Distributed`
      and a `DistributedBackend` pointing at the Flight SQL endpoint (R12 ADR-12.3).
      Returns a synchronous session; network errors raise `ConnectorError`.
- [ ] Implement `Session.connect_async(url: str)` classmethod: same as
      `connect()` but returns an asyncio-native awaitable (ADR-R13-01 Option C).
      Yields control to the asyncio loop during the Flight SQL handshake.
- [ ] Implement `Session.from_env()` classmethod: reads
      `KRISHIV_COORDINATOR` environment variable. If set, delegates to
      `Session.connect(os.environ["KRISHIV_COORDINATOR"])`. If unset, delegates
      to `Session.local()`. Intended as the canonical K8s/cloud entry point.
- [ ] Add `ModeError(KrishivError)` to the error hierarchy: raised when a
      feature is called in an unsupported mode with a message naming the required
      mode (e.g., `"streaming requires local() or connect() — embedded() mode
      does not support continuous operators"`).
- [ ] Add `.mode` property on `Session` returning a `str` (one of `"embedded"`,
      `"local"`, `"distributed"`).
- [ ] Add Python unit tests for each factory method:
  - `Session.embedded().mode == "embedded"`
  - `Session.local().mode == "local"`
  - `Session.connect("http://localhost:50051").mode == "distributed"`
  - `Session.from_env()` → `"local"` when `KRISHIV_COORDINATOR` is unset
  - `Session.from_env()` → `"distributed"` when `KRISHIV_COORDINATOR` is set
  - `Session.embedded().stream(q)` raises `ModeError`

**Validation**: `pytest python/tests/test_session_modes.py`

## Sprint 2 — Python Schema API & Sources

### S2.1: ks.Schema base class — krishiv-python

- [ ] Implement `PySchema` PyO3 class with `__init_subclass__` (ADR-R13-02,
      Option A).
- [ ] Build the annotation → Arrow DataType mapping table for `str`, `int`,
      `float`, `bool`, `datetime`, `bytes`, and `Optional[T]`.
- [ ] Expose `schema.arrow_schema() -> pyarrow.Schema` for interoperability.
- [ ] Add Python-level unit tests asserting correct Arrow type for each
      supported annotation.

**Validation**: `pytest python/tests/test_schema.py`

### S2.2: ks.read_parquet() — krishiv-python, krishiv-connectors

- [ ] Implement `ks.read_parquet(path, schema=MySchema)` that opens a Parquet
      file or directory via the existing `krishiv-exec` Parquet source operator.
- [ ] Return a `Stream` Python object wrapping a Rust-side `Arc<StreamPlan>`.
- [ ] Validate the Parquet file schema against `MySchema.arrow_schema()` at
      open time; raise `SchemaError` on mismatch.

**Validation**: `pytest python/tests/test_sources.py::test_read_parquet`

### S2.3: ks.read_kafka() — krishiv-python, krishiv-connectors

- [ ] Implement `ks.read_kafka(bootstrap_servers, topic, schema=MySchema,
      group_id=…)` gated on the `kafka` extra.
- [ ] Internally constructs a `KafkaSource` (R12 S3.3) and wraps it in a
      `Stream`.
- [ ] Raise `ConnectorError` if rdkafka is not available (feature not compiled).

**Validation**: `pytest python/tests/test_sources.py::test_read_kafka` (requires `krishiv[kafka]`)

### S2.4: ks.read_iceberg() — krishiv-python, krishiv-lakehouse

- [ ] Implement `ks.read_iceberg(catalog_uri, table_name, schema=MySchema)`
      gated on the `iceberg` extra.
- [ ] Wire into the existing `krishiv-lakehouse` Iceberg reader.
- [ ] Raise `ConnectorError` on catalog unreachable.

**Validation**: `pytest python/tests/test_sources.py::test_read_iceberg`

## Sprint 3 — Transformation Chain & Windowing

### S3.1: .with_watermark() — krishiv-python

- [ ] Implement `Stream.with_watermark(column: str, max_lateness_ms: int) ->
      Stream` that attaches a watermark strategy to the stream plan.
- [ ] Map to the Rust `WatermarkStrategy` struct in `krishiv-exec`.
- [ ] Raise `SchemaError` if `column` is not present in the stream schema.

**Validation**: `pytest python/tests/test_transforms.py::test_with_watermark`

### S3.2: .key_by() — krishiv-python

- [ ] Implement `Stream.key_by(*columns: str) -> KeyedStream` that sets the
      partition key for subsequent stateful operators.
- [ ] Return a `KeyedStream` Python object (distinct type from `Stream` for
      type-stub clarity).

**Validation**: `pytest python/tests/test_transforms.py::test_key_by`

### S3.3: .window() — krishiv-python

- [ ] Implement `KeyedStream.window(spec: WindowSpec) -> WindowedStream`.
- [ ] Provide factory functions: `ks.windows.tumbling(size_ms)`,
      `ks.windows.sliding(size_ms, slide_ms)`, `ks.windows.session(gap_ms)`.
- [ ] Raise `SchemaError` if the watermark column is not set on the parent
      stream before `.window()` is called.

**Validation**: `pytest python/tests/test_transforms.py::test_window`

### S3.4: .agg() — krishiv-python

- [ ] Implement `WindowedStream.agg(**agg_exprs) -> Stream` accepting
      aggregation expressions: `ks.agg.sum("col")`, `ks.agg.count()`,
      `ks.agg.max("col")`, `ks.agg.min("col")`, `ks.agg.mean("col")`.
- [ ] Map each expression to the corresponding DataFusion `AggregateExpr` via
      `krishiv-sql`.
- [ ] Raise `QueryError` if a referenced column is absent from the schema.

**Validation**: `pytest python/tests/test_transforms.py::test_agg`

## Sprint 4 — Sinks, Pandas Bridge, Jupyter Display

### S4.1: ks.sinks.parquet() — krishiv-python, krishiv-connectors

- [ ] Implement `stream.sink(ks.sinks.parquet(path, partition_by=None))`.
- [ ] Flush partition files on each watermark advance.
- [ ] Raise `ConnectorError` on write failure.

**Validation**: `pytest python/tests/test_sinks.py::test_parquet_sink`

### S4.2: ks.sinks.kafka() — krishiv-python, krishiv-connectors

- [ ] Implement `stream.sink(ks.sinks.kafka(bootstrap_servers, topic))` gated
      on the `kafka` extra.
- [ ] Serialize each batch row as JSON by default; accept a `serializer`
      callable for custom formats.

**Validation**: `pytest python/tests/test_sinks.py::test_kafka_sink`

### S4.3: ks.sinks.iceberg() — krishiv-python, krishiv-lakehouse

- [ ] Implement `stream.sink(ks.sinks.iceberg(catalog_uri, table_name))` gated
      on the `iceberg` extra.
- [ ] Append each batch as a new Iceberg data file; commit a snapshot per
      watermark advance.

**Validation**: `pytest python/tests/test_sinks.py::test_iceberg_sink`

### S4.4: Pandas and PyArrow bridge — krishiv-python

- [ ] Implement `Batch.to_arrow() -> pyarrow.RecordBatch` using
      `arrow2::ffi` or the `pyo3` Arrow PyCapsule interface.
- [ ] Implement `Batch.to_pandas() -> pandas.DataFrame` as
      `batch.to_arrow().to_pandas()`.
- [ ] Add `pyarrow` to the `[arrow]` optional extra; `pandas` to the `[arrow]`
      extra as well (pandas depends on Arrow at runtime).

**Validation**: `pytest python/tests/test_bridge.py`

### S4.5: Jupyter _repr_html_() — krishiv-python

- [ ] Implement `Batch._repr_html_()` rendering the first 20 rows as an HTML
      table with column types in the header.
- [ ] Implement `Stream._repr_html_()` displaying schema + source metadata.
- [ ] Implement `Schema._repr_html_()` displaying field names and Arrow types.

**Validation**: `pytest python/tests/test_repr.py`

## Sprint 5 — Asyncio-Native Streaming & Error Hierarchy

### S5.1: ks.Session and connect_async() — krishiv-python

- [ ] Implement `ks.Session` PyO3 class holding a `Arc<TokioRuntimeThread>`
      (ADR-R13-01, Option C).
- [ ] `Session.connect_async()` starts the background Tokio runtime thread and
      returns a Python `Awaitable` that resolves when the engine handshake
      completes.
- [ ] `Session.__del__` joins the background thread with a 5-second timeout,
      logging a warning if it does not terminate cleanly.

**Validation**: `pytest python/tests/test_async.py::test_connect_async`

### S5.2: async for batch in stream.window() — krishiv-python

- [ ] Implement `WindowedStream.__aiter__` and `__anext__` using the GIL
      bridge from ADR-R13-03 (Option A, `spawn_blocking` per batch).
- [ ] Each `__anext__` call posts a Tokio task; the result is delivered back to
      the asyncio loop via `asyncio.run_coroutine_threadsafe`.
- [ ] `StopAsyncIteration` is raised when the stream reaches its watermark
      upper bound or the session is closed.

**Validation**: `pytest python/tests/test_async.py::test_async_iteration`

### S5.3: Error hierarchy — krishiv-python

- [ ] Define `KrishivError(Exception)` as the root.
- [ ] Define `QueryError(KrishivError)`, `SchemaError(KrishivError)`,
      `ConnectorError(KrishivError)`, `CheckpointError(KrishivError)`,
      `AuthorizationError(KrishivError)`.
- [ ] Map Rust error variants to the corresponding Python class in the PyO3
      error converter.
- [ ] Add `.pyi` stubs for all six classes with docstrings.
- [ ] Add tests asserting each error type is raised in the appropriate failure
      scenario.

**Validation**: `pytest python/tests/test_errors.py`

### S5.4: Optional extras wiring — krishiv-python

- [ ] Ensure `pip install krishiv` installs the base package (no Kafka, Iceberg,
      or Arrow).
- [ ] `pip install krishiv[kafka]` compiles with `--features kafka` and installs
      `confluent-kafka` Python stub for type hints.
- [ ] `pip install krishiv[iceberg]` compiles with `--features iceberg`.
- [ ] `pip install krishiv[arrow]` installs `pyarrow` and `pandas`.
- [ ] Add a CI smoke-test for each extra combination.

**Validation**: `pytest python/tests/test_extras.py`

## Test Checklist

- [ ] `cargo clippy --workspace -- -D warnings` passes including `krishiv-python`.
- [ ] `cargo test -p krishiv-python` — Rust-side unit tests.
- [ ] `maturin develop` completes without error.
- [ ] `pytest python/tests/` — full Python test suite passes.
- [ ] `pytest python/tests/test_session_modes.py` — all five factory methods
      tested; `ModeError` raised correctly in unsupported modes.
- [ ] `pytest python/tests/test_async.py` — asyncio bridge tests pass.
- [ ] `pytest python/tests/test_bridge.py` — Pandas and PyArrow round-trip.
- [ ] CI wheel matrix produces valid wheels for all four targets.
- [ ] `.pyi` stubs are up to date (CI diff check passes).

## Acceptance Gate

R13 is complete when:

- [ ] `pip install krishiv` produces a working package from the CI-built wheel
      on manylinux2014 and macOS.
- [ ] All five deployment-mode factory methods work end-to-end:
  - `ks.Session.embedded().sql("SELECT 1")` returns a result without network.
  - `ks.Session.local().stream(q)` produces `StreamBatch` values via the
    in-process coordinator (requires R12 Sprint 6 to be complete).
  - `ks.Session.connect("http://localhost:50051").sql("SELECT 1")` connects
    to a running coordinator and returns a result.
  - `ks.Session.from_env()` selects `local()` or `connect()` based on the
    `KRISHIV_COORDINATOR` environment variable.
  - `ks.Session.embedded().stream(q)` raises `ModeError` with a clear message.
- [ ] A complete streaming pipeline from `ks.Session.local()` using
      `ks.read_kafka()` → `.key_by()` →
      `.window(ks.windows.tumbling(60_000))` → `.agg(total=ks.agg.sum("value"))`
      → `stream.sink(ks.sinks.parquet("out/"))` runs end-to-end in the
      integration test suite without error.
- [ ] The same pipeline runs end-to-end via `ks.Session.connect(url)` against
      a real coordinator process in the integration test suite.
- [ ] `async for batch in stream.window(…)` works in an `asyncio` event loop
      without GIL contention or deadlock (verified by the asyncio test suite
      running under `pytest-asyncio`).
- [ ] `batch.to_pandas()` and `batch.to_arrow()` return correct values for all
      supported Arrow column types.
- [ ] All exception classes in the error hierarchy (including `ModeError`) are
      raised in the correct scenarios (verified by `test_errors.py`).
- [ ] `.pyi` stubs are present, accurate, and pass `mypy --strict` on the
      `python/krishiv/` package.
- [ ] `cargo test --workspace` and `pytest python/tests/` both pass with zero
      failures.

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| pyo3 version mismatch between krishiv-python and existing crates | Pin pyo3 to a single version in the workspace root `Cargo.toml` using `[workspace.dependencies]` |
| manylinux2014 build fails due to rdkafka C toolchain in the wheel container | Build the `[kafka]` extra wheel separately using `manylinux2014` + cmake image; document in CI YAML |
| GIL contention causes throughput regression in asyncio tests | ADR-R13-03 (spawn_blocking) is validated by a throughput benchmark in Sprint 5; regression threshold is > 90% of single-threaded throughput |
| pyo3-stub-gen generates incorrect stubs for complex generic types | Fallback: maintain handwritten stubs for complex types; auto-generate only leaf classes |
| Python 3.10 min-version breaks users on 3.9 | Document the requirement in `pyproject.toml` `python_requires = ">=3.10"` with a clear error at import time on older versions |
