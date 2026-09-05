# krishiv-executor

The data-plane worker (`krishiv-executor` binary; `krishiv executor`):
receives typed task assignments, classifies them (`ExecutionModel::{Batch,
Streaming, DeltaBatch}`), runs DataFusion plan partitions with shuffle
read/write, hosts the long-lived streaming run-loops (`stream:rloop:` and the
classed loops) with key-group parallelism and credit-gated peer exchange,
holds resident IVM flows, spools large results, and reports heartbeats,
task status, and checkpoint acks. Capacity (slots, memory pool, parallelism)
derives from the cgroup via `krishiv_common::executor_capacity`.

```bash
krishiv executor --coordinator http://coordinator:50051 --slots 4
```

Documentation: `docs/architecture/05-executor-and-data-plane.md`,
`docs/architecture/08-streaming.md`, `docs/architecture/06-shuffle.md`.

License: Apache-2.0.
