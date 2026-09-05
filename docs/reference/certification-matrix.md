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
| IVM | single-node | certified | benchmarks/results.jsonl ivm_tick_p50_at_10m_rows (64.6ms vs 2000ms budget); ivm_vs_full_recompute bench; krishiv-ivm flow + partitioned tests; live IVM job proven on the k3s cert cluster 2026-07-22 | Certified for correctness and throughput, NOT for restart-resume. IVM-AUD-INT-F13: `IncrementalEngine::run` never touches the runtime's checkpoint service, so at single-node placement an incremental job restarts from zero and re-emits its whole changelog, exactly as it does embedded — the durable-checkpoint property that distinguishes single-node from embedded for the streaming engine does not exist for this one, and none of the evidence above restarts a job. Whether this row should be downgraded is an open decision recorded in docs/engineering-log/ivm-audit-register.md. |
| IVM | distributed | preview | Phase 57 resident-IVM dispatch (submit_resident_ivm_step, O(Δ) wire); ivm_http dispatch-decision tests | Preview until a distributed-IVM chaos gate lands: an in-flight IVM tick is non-cancellable by design (#224, already-accepted deltas) and distributed executor-loss during a resident tick has lighter fault coverage than the batch/streaming paths. Also read this row narrowly: executor-resident IVM requires an UNPARTITIONED job, and a job whose first view is a routable single-column GROUP BY is auto-partitioned on any coordinator with more than one shard (IvmJobRegistry::register_view), while a partitioned job always computes centrally. The canonical GROUP BY workload therefore runs on the coordinator's in-process shards and never reaches an executor; only jobs pinned single (Session::view / to_incremental view-DAGs) or with a non-shardable first view take the distributed path at all (IVM-AUD-DIST-A1). |

## Data-movement paths

| Source | Sink | Delivery | Status | Evidence |
|---|---|---|---|---|
| CDC / connector source (krishiv ivm run, submit of an Incremental job) | connector sink (per-tick consolidated changelog) | best-effort | preview | IVM-AUD-INT-F4 / INT-F13. Listed to state the guarantee, not to claim one: `IncrementalEngine::run` (krishiv-engines/src/lib.rs) takes no checkpoints and rewinds no source, so a restart re-runs the job from zero and rewrites the whole changelog, and a crash mid-drain leaves whatever the sink had already flushed. `CompiledJob::delivery` is never read by any engine, so requesting a stronger contract has no effect |
| IVM view output (coordinator /views/{view}/snap and /output) | pull-only — the caller polls; there is no sink | best-effort | preview | IVM-AUD-INT-F4 / INT-F5. `/output` peeks a coalescing watch that holds one delta per view, so a consumer polling slower than /step loses every delta but the newest; since INT-F5 the loss is measurable from `published_rows_total` but it is not recoverable. `/snap` is a whole snapshot and loses nothing, at O(view) per poll |
| Kafka | Iceberg | exactly-once | certified | G8 kill-loop certified on prod 2026-07-10 (image g8-9dd1fdf); DUR-2 recover-commit suite (append+upsert across executor crash, idempotent) |
| batch SQL / object-store files | object-store Parquet (staged, atomic publish) | effectively-once | preview | DUR-1 Committing-state demote/redrive (staged publish is idempotent, coordinator/mod.rs); sections/dur1.rs.inc regression tests |
| batch SQL | Iceberg (durable CTAS) | effectively-once | preview | durable CTAS (#162); overwrite_commit atomic version-hint flip (temp+fsync+rename, CONN-3); connectors iceberg suite |
| Kafka | Kafka (transactional) | exactly-once | preview | two-phase transactional Kafka sink (transactional_kafka); barrier-aligned prepare/commit — Preview: no prod kill-loop cert yet |
| batch SQL | any registered connector sink (registry-sink batch export) | at-least-once | preview | #197 registry-sink output contract (executor fragment::batch::execute_registry_sink) — writes then flushes before reporting task success, so output is durable on success but a retried attempt re-delivers. Preview: reaches every registered sink driver (see the connector reachability matrix) but has no barrier-aligned commit, so it is not a checkpointed participant |
