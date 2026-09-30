# Engine certification matrix (Phase 62 GA gate)

Generated from `krishiv-connectors::cert_matrix` — do not edit by hand.
Regenerate with:
`KRISHIV_BLESS_CERT_MATRIX=1 cargo test -p krishiv-connectors cert_matrix`

Status is grounded in committed evidence: **no cell claims `certified`
without a linked benchmark / chaos test / live cert.** A partial launch
stays honest by construction — cells are cut (downgraded to `preview`),
never the gate.

## Compute × topology

| Compute | Topology | Status | Evidence | Notes |
|---|---|---|---|---|
| batch SQL | single-node | certified | krishiv-conformance corpus + docs/reference/sql-grammar.md coverage number; Phase 51 TPC-H yardstick in docs/BENCHMARKING.md |  |
| batch SQL | distributed | certified | krishiv-scheduler placement/failover/recovery chaos suite (sections/*.inc); live coordinator→executor dispatch proven on the 3-node k3s cert cluster 2026-07-22 (job batch-sql-*, task Succeeded on v2-exec-a) |  |
| parallel streaming | single-node | certified | benchmarks/results.jsonl streaming_latency_{embedded,single_node}_p50 (both inside budget); run_loop_v2 tumbling/session/cancel tests |  |
| parallel streaming | distributed | certified | run_loop_parallel_three_matches_parallel_one (Phase 55 exit gate: keyed exchange, parallelism-3 == parallelism-1); stream_exchange keyed-shuffle tests; Kafka→Iceberg exactly-once (G8) |  |
| IVM | single-node | certified | benchmarks/results.jsonl ivm_tick_p50_at_10m_rows (64.6ms vs 2000ms budget); ivm_vs_full_recompute bench; krishiv-ivm flow + partitioned tests; live IVM job proven on the k3s cert cluster 2026-07-22 | A submitted incremental job checkpoints its view state and source offsets together when a run completes, so a restarted or re-submitted job continues where the last checkpoint left it instead of re-reading its sources (IVM-AUD-INT-F13; a_durable_incremental_job_resumes_instead_of_starting_over). A run that dies part-way is redone from the previous checkpoint, so what it had written is written again: at-least-once. A source that reports no offset is not checkpointed and the job then starts over, as it does embedded. A session's `ivm` job at this placement is hosted by the daemon (IVM-AUD-API-A2). |
| IVM | distributed | preview | Phase 57 resident-IVM dispatch (submit_resident_ivm_step, O(Δ) wire); ivm_http dispatch-decision tests | Preview until a distributed-IVM chaos gate lands: an in-flight IVM tick is non-cancellable by design (#224, already-accepted deltas) and distributed executor-loss during a resident tick has lighter fault coverage than the batch/streaming paths. Both job shapes run on executors: a single-flow job as one resident flow pinned to one executor, and a key-partitioned job (first view a routable single-column GROUP BY) as one resident flow per shard, spread across the executors (IVM-AUD-DIST-A1 / DIST-A2). A shard whose dispatch fails is computed on the coordinator for that tick and re-attaches on the next. |

## Data-movement paths

| Source | Sink | Delivery | Status | Evidence |
|---|---|---|---|---|
| CDC / connector source (krishiv ivm run, submit of an Incremental job) | connector sink (per-tick consolidated changelog) | at-least-once | preview | IVM-AUD-INT-F4 / INT-F13. At a durable placement the job checkpoints view state and source offsets together when a run completes, and the next run resumes from there; a run that dies part-way is redone, so its changes are written again and nothing is skipped. Embedded, or with a source that reports no offset, a re-run starts over and rewrites the whole changelog. The sink is neither transactional nor keyed for idempotence, and `CompiledJob::delivery` is never read by any engine, so requesting a stronger contract has no effect |
| IVM view output (coordinator /views/{view}/snap and /output) | pull-only — the caller polls; there is no sink | at-least-once | preview | IVM-AUD-INT-F4 / INT-F5. `/output?since_tick=` returns every delta a view published after the caller's cursor, from a per-view log bounded by KRISHIV_IVM_OUTPUT_RETAIN_TICKS / _BYTES; a reader further behind than the bound, or across a restore, is told so (`missed`) and re-reads `/snap`. The cursor is the caller's, so a caller that crashes before recording it reads the same deltas again. Without a cursor `/output` returns only the newest delta. `/snap` is a whole snapshot and loses nothing, at O(view) per poll |
| Kafka | Iceberg | exactly-once | certified | G8 kill-loop certified on prod 2026-07-10 (image g8-9dd1fdf); DUR-2 recover-commit suite (append+upsert across executor crash, idempotent) |
| batch SQL / object-store files | object-store Parquet (staged, atomic publish) | effectively-once | preview | DUR-1 Committing-state demote/redrive (staged publish is idempotent, coordinator/mod.rs); sections/dur1.rs.inc regression tests |
| batch SQL | Iceberg (durable CTAS) | effectively-once | preview | durable CTAS (#162); overwrite_commit atomic version-hint flip (temp+fsync+rename, CONN-3); connectors iceberg suite |
| Kafka | Kafka (transactional) | best-effort | preview | two-phase transactional Kafka sink (kafka_transactional_sink, rdkafka); barrier-aligned prepare/commit gives read_committed consumers exactly-once output while the executor survives, but a crash after a checkpoint completes and before its transaction commits aborts that epoch's output with no recovery — so an epoch can be lost. Refused under durable profiles without KRISHIV_KAFKA_SINK_ALLOW_UNRECOVERABLE_TXN=1 |
| batch SQL | any registered connector sink (registry-sink batch export) | at-least-once | preview | #197 registry-sink output contract (executor fragment::batch::execute_registry_sink) — writes then flushes before reporting task success, so output is durable on success but a retried attempt re-delivers. Preview: reaches every registered sink driver (see the connector reachability matrix) but has no barrier-aligned commit, so it is not a checkpointed participant |
