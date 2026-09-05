# krishiv-proto

Typed identifiers and the coordinator/executor wire contracts: `JobId`,
`StageId`, `TaskId`, `ExecutorId`, `AttemptId`, `LeaseGeneration`,
`FencingToken`, `KeyGroupRange`; `JobSpec`/`StageSpec`/`TaskSpec`,
`JobState`/`TaskState`/`StageKind`, `ExecutorDescriptor`/`ExecutorHeartbeat`,
`PlanFragment`, shuffle read/write configs, checkpoint ack and restore
messages; the prost/tonic-generated gRPC services (`wire`, `services`).
Feature `serde` derives `Serialize`/`Deserialize` on the types.

Documentation: `docs/architecture/04-scheduler-and-coordinator.md`,
`docs/architecture/18-compatibility-and-versioning.md`.

License: Apache-2.0.
