# Krishiv Implementation Trackers

This directory contains release-by-release implementation trackers for Krishiv. Use these files as the active execution layer beneath the roadmap in `docs/architecture/krishiv-roadmap.md`.

## How To Use These Trackers

1. Read `docs/implementation/status.md`.
2. Open the tracker for the current release phase.
3. Pick one small unchecked task.
4. Implement the task with tests.
5. Update the tracker and `status.md` before stopping.

Each tracker is intentionally checklist-oriented. R1-R3 are more concrete because they are near-term. R4-R10 stay decision-guiding where implementation details depend on earlier releases.

## Release Trackers

| Release | Tracker |
|---|---|
| R1 Foundation Alpha | [r1-foundation-alpha.md](r1-foundation-alpha.md) |
| R2 Kubernetes Distributed Alpha | [r2-kubernetes-distributed-alpha.md](r2-kubernetes-distributed-alpha.md) |
| R3 Connector Contracts | [r3-connector-contracts.md](r3-connector-contracts.md) |
| R4 Shuffle And Batch AQE | [r4-shuffle-and-batch-aqe.md](r4-shuffle-and-batch-aqe.md) |
| R5 Stateful Streaming Core | [r5-stateful-streaming-core.md](r5-stateful-streaming-core.md) |
| R6 Checkpoints And Savepoints | [r6-checkpoints-and-savepoints.md](r6-checkpoints-and-savepoints.md) |
| R7 Resource Governance And Adaptivity | [r7-resource-governance-and-adaptivity.md](r7-resource-governance-and-adaptivity.md) |
| R8 Lakehouse And Python Beta | [r8-lakehouse-and-python-beta.md](r8-lakehouse-and-python-beta.md) |
| R9 Governance And Operations | [r9-governance-and-operations.md](r9-governance-and-operations.md) |
| R10 GA Platform Release | [r10-ga-platform-release.md](r10-ga-platform-release.md) |

## Tracker Template

Use this structure for future release tracker revisions:

```md
# RX Release Name Implementation Tracker

## Goal
## Scope
## Out Of Scope
## Dependencies
## Architecture Deliverables
## API And Interface Deliverables
## Runtime Deliverables
## Test Checklist
## Acceptance Gate
## Risks And Mitigations
```

## Global Rules

- Keep embedded, single-node, and distributed behavior semantically aligned for supported features.
- Keep exactly one active `JobCoordinator` per job in distributed modes.
- Do not document exactly-once unless the source/sink/checkpoint combination is certified.
- Every connector must declare capability flags.
- Every substantial session must update `status.md`.
