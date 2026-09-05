# krishiv-scheduler

The control plane: `Coordinator` (cluster control plane) and
`JobCoordinator` (per-job), leader election with fencing tokens
(`SingleNodeLeader`, etcd lease), admission and queues, placement
(`StaticScheduler`, `SlotAwareScheduler`, `LocalityScheduler`,
`FairScheduler`), the executor registry and heartbeats, staged batch
planning and adaptive execution, checkpoint coordination, restore
directives, the `IvmJobRegistry`, metadata stores (memory, RocksDB, etcd),
result spools, and the gRPC + HTTP `/api/v1` control surfaces with bearer
auth.

Binaries: `krishiv-clusterd` (coordinator daemon; `krishiv clusterd`),
`krishiv-coordinator` (alias), `krishiv-job-coordinator`. Feature `etcd`
enables the etcd metadata store (`just test-etcd` runs its tests).

Documentation: `docs/architecture/04-scheduler-and-coordinator.md`,
`docs/architecture/12-security.md`, `docs/engineering-log/crate-audit-register.md`.

License: Apache-2.0.
