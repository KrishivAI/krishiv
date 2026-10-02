# TPC-H SF100 distributed on kind (3 executors, one box), 2026-10-02

Correctness run of the distributed path on `bd19cc6` (DataFusion 55.1, the fragment dynamic-filter fix, the selectivity join reorder): a 3-node kind cluster on the 12-core / 61 GB laptop, one executor per node (4 slots, derived shared pool ≈9.5 GiB each, 16 GiB pod limit), coordinator on the control-plane node, data in a host MinIO (`s3://krishiv-bench/tpch/sf100`, 41.5 GB) reached from the pods at the docker bridge gateway. Manifest: `deploy/k8s/bench/kind-sf100.yaml`; runner: `scripts/bench/tpch_cluster_run.py` (now records result digests from the inline Arrow IPC); diff: `scripts/bench/tpch_cluster_compare.py`.

**Timings are not a performance claim**: three executors share one box's cores and read through MinIO over loopback, so every query is contention-bound. What the run establishes is that every query distributes (no single-task fallback) and returns the embedded engine's answer.

| q | cluster s | embedded s | stages | tasks | answer |
|---|---|---|---|---|---|
| q1 | 49.5 | 21.2 | 2 | 25 | same |
| q2 | 29.3 | 3.0 | 11 | 241 | same |
| q3 | 72.6 | 11.5 | 6 | 121 | same |
| q4 | 40.3 | 6.3 | 4 | 73 | same |
| q5 | 168.4 | 17.6 | 7 | 145 | same |
| q6 | 19.2 | 7.3 | 2 | 25 | same |
| q7 | 124.1 | 31.2 | 7 | 145 | same |
| q8 | 51.4 | 18.3 | 8 | 169 | same |
| q9 | 135.2 | 34.1 | 8 | 169 | same |
| q10 | 54.5 | 14.8 | 8 | 169 | same |
| q11 | 16.1 | 4.0 | 3 | 49 | same rows, tie order |
| q12 | 38.2 | 9.9 | 4 | 73 | same |
| q13 | 40.5 | 16.2 | 4 | 73 | same |
| q14 | 24.3 | 6.9 | 4 | 73 | same |
| q15 | 43.4 | 6.9 | 4 | 73 | same |
| q16 | 26.3 | 2.8 | 6 | 121 | same |
| q17 | 30.3 | 17.2 | 6 | 121 | same |
| q18 | 216.7 | 25.9 | 8 | 169 | same |
| q19 | 40.4 | 10.7 | 4 | 73 | same |
| q20 | 49.6 | 12.2 | 7 | 145 | same |
| q21 | 180.8 | 52.7 | 10 | 217 | same |
| q22 | 42.5 | 2.5 | 4 | 73 | same |

**22/22 succeeded, 21 byte-identical, 1 same rows in a different tie order (q11), 0 different; total 1494 s.** Embedded column: median of the 2026-10-02 three-pass run.

## What the run found first

With `KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES=12e9` on the executors, q3 and q5 failed with `Resources exhausted … HashJoinInput … fair(pool_size: 32.0 MB)`: the first fragment of each stage reserved the *whole* process budget as a private pool and the other three slots got the 32 MiB floor (`unified executor memory exhausted; task granted minimum engine limit`, requested 12000000000, granted 33554432). The run above uses no explicit budget (the shared pool apportions live); the ladder is fixed in the same commit to request `budget / slots`.

