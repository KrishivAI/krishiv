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

P7. **Cleanup + docs (user decision 2026-08-21: REMOVE, don't
    deprecate).** The superseded surfaces are DELETED once the converged
    core covers them — `DataFrame::write_stream()`/`StreamingQuery` as a
    public API (the engine loop stays as the embedded executor behind
    the terminal), `RemoteStreamingJob` (folded into
    `StreamingJob::attach`), and the bare `submit_stream_job`/
    `push_stream_job_input`/`poll_stream_job` trio where the handle
    covers them. Python drops the parallel classes the same way. Docs
    and the feature matrix updated to what the code DOES.

Discipline: every phase lands as its own commit with revert-proven
tests and full gates (fmt, clippy -D warnings, workspace tests).

## Completion record (2026-08-21)

All seven phases landed, each on a green full gate with revert-proven
tests: P1 `b4e7489` (unified StreamingJob + the stop verb the trait
never had + the embedded flush-over-input fix), P2 `7233cbd` (write()
terminal + sink through the verified options echo), P3 `6fd4fa5`
(update mode on both engines via per-job early fire), P4 `62de713`
(complete mode as the sink-layer result table), P5 `65ea153` (Python as
a thin binding), P6 `213a51c` (terminal arm in the S1 conformance
matrix + embedded engine-sink bridge), P7 `147fbbb` (legacy Python
surfaces REMOVED — 650 lines deleted, nothing lost).

Follow-on: NEXMark benchmarked THROUGH the terminal on single-node and
the k3s rig (requires --flight-addr on the coordinators).

## Terminal benchmark record (2026-08-22)

Single-node durable stack (clusterd + 1 executor, RocksDB state,
1s checkpoints), NEXMark bids THROUGH `StreamingDataFrame.write()` +
the unified `StreamingJob` handle over Flight IPC — 100k rows/case,
median of 3 reps:

| case                | mode     | ev/s   | rows out |
|---------------------|----------|--------|----------|
| t1_count_per_bidder | append   | 62,891 | 93       |
| t2_count_per_auction| append   | 68,431 | 96       |
| t3_wide_window      | append   | 69,280 | 93       |
| t4_update_mode      | update   | 68,430 | 160      |
| t5_complete_mode    | complete | 62,190 | 93 (full table) |

Completeness gate PASS (5/5). These sit AT or ABOVE the raw
direct-registration harness (28–73K on the same box): the terminal adds
no measurable overhead, and Flight IPC beats the HTTP+base64 push path.

Two defects surfaced getting here, both fixed with revert-proven tests:
`10b1c35` (a run-loop launch whose dispatch is deduped against a prior
incarnation's leftover identity now fails registration loudly instead
of returning Ok on a job that can never run — the 2h silent wedge) and
`c132d5f` (complete-mode drain stability in the harness). Known gap
confirmed live and still open: a run-loop task returned to Pending has
no re-dispatcher (`launch_run_loop_job` runs only at registration), so
a failed-then-retried streaming task churns Assigned→Pending forever —
recorded in the KNOWN GAP comment at the launch call site.

## Full-corpus terminal benchmark (2026-08-22, later)

`nexmark_terminal` extended to ALL 22 NEXMark query classes through
`Session::stream_sql` -> `write()` -> `StreamingJob` (commit `e98d059`:
class-routed registration + side-tagged handle pushes), plus the
update/complete mode duo. Both legs PASS 24/24, durable (RocksDB + 1s
checkpoints):

- **Single-node** (attempt27): 60-140K ev/s. Two-source classes
  (q3/q4/q8/q9/q20) run 90-140K on 2x100k input.
- **k3s rig** (attempt31, 3 executors 1/node, image `fast-1024dce`):
  9.9-20.6K ev/s — consistent with the raw-harness rig band; s3's ~1.3
  free cores remain the straggler bound.

Getting the rig leg green surfaced two real distributed defects, both
fixed with revert-proven tests: `e4bad4e` (stream-exchange treated
receiver backpressure as fatal after ~150ms — a starved peer killed the
sending pipeline loop mid-stream) with `9ce147b` composing the EOS
deadline chain over the new 60s backpressure budget (exchange 60s <
executor quiesce 90s < coordinator EOS RPC 120s, ordering pinned by
test), and `1024dce` (a pre-split EOS flush emits ~1000 micro-batches
and a single exchange push above the 64-batch receiver cap can NEVER be
accepted — per-peer rows now coalesce into one batch before delivery;
single-node had masked this because loopback delivery skips the cap).
