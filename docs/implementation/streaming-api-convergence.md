# Streaming API convergence (task #150)

## Decision

`StreamingDataFrame` becomes the ONE streaming pipeline core; everything
else is a facade over it. One `StreamingJob` handle owns the lifecycle.
The same `Session` that runs batch and Delta batch work runs streaming,
and the same pipeline produces identical results on all three deployment
modes (embedded / single-node / distributed) — refusals stay loud, but
the two output-mode gaps are MITIGATED, not refused:

- `update` mode runs distributed by promoting early-fire from the
  embedded loop into the run-loop as a PER-JOB registration option (the
  env flag stays embedded-only; a registration option is what a job can
  actually carry across the wire).
- `complete` mode is implemented at the SINK layer as a materialized
  result table: the append/update delta stream folds into a keyed
  `(key, window) -> row` table and each trigger rewrites the whole
  table. This is Spark's actual "complete" semantics (maintained state
  re-output) and it works identically in every mode because it never
  asks the engine to re-emit distributed operator state.

Why one core: this month's defects were overwhelmingly sibling drift —
two stacks that look alike and quietly diverge (embedded vs distributed
EOS, classed loops vs rloop, `write_stream()` running single-process on
a distributed session with no signal). One execution stack means every
engine fix (EOS barrier, split pipelines, checkpointing) exists
everywhere by construction.

## Phases

P1. **Unified `StreamingJob` handle (Rust, krishiv-api).** Merge
    `StreamingQuery` + job-id/push/drain into one handle: `id`,
    `state`, `progress`, `stop`, `await_termination`, `push`, `drain`.
    Backed by the in-process query state on embedded sessions and by
    coordinator HTTP state on single-node/distributed. Attach-by-id
    constructor replaces `RemoteStreamingJob`.

P2. **`write()` terminal on StreamingDataFrame (Rust).** `SinkSpec`
    { format, options, output_mode, trigger } carried through run-loop
    registration options; executors own kafka/iceberg/parquet sinks via
    the existing `rloop_connector_sinks` seam; console/memory/
    foreach_batch are documented CLIENT-side drains (Python callbacks
    cannot run on executors — saying otherwise would be dishonest).
    `trigger` maps onto the linger dial (`processing_time`), continuous
    (no linger), and bounded+EOS (`once`/`available_now`).

P3. **Update mode distributed.** Per-job `early_fire_ms` registration
    option → spec → fragment → run-loop window driver (embedded loop
    already has the machinery). Early-fired rows are provisional
    upserts; sinks that cannot upsert refuse update mode by name
    (Kafka keyed-compaction and the Tranche-B JDBC/Iceberg upsert
    writers can).

P4. **Complete mode at the sink.** `CompleteModeView`: fold deltas by
    (key, window), rewrite per trigger. Mode-agnostic by construction.

P5. **Python rewire.** PyStreamingDataFrame delegates to the Rust
    `write()` terminal + `StreamingJob`; `DataFrame.write_stream()` and
    `StreamingQuery` become thin PySpark-compat facades constructing
    the same objects; `RemoteStreamingJob` aliases the attach ctor.

P6. **Mode-matrix conformance.** The same pipeline + data through
    embedded, single-node, and distributed sessions asserting identical
    result multisets (extends the S1 cross-loop harness), including a
    sink round-trip and an update-mode and complete-mode case each.

P7. **Deprecation + docs.** Old surfaces marked deprecated with the
    replacement named; feature matrix updated to what the code DOES.

Discipline: every phase lands as its own commit with revert-proven
tests and full gates (fmt, clippy -D warnings, workspace tests).
