# Krishiv Data Plane Transport Model

**Status:** Decision — approved for R4 implementation.
**Owner:** Architecture team.
**Linked releases:** R4 (shuffle implementation), R5 (streaming data movement), R10 (performance optimization).

---

## Decision

**R4 default:** Arrow IPC files for shuffle writes and Apache Arrow Flight
(IPC over gRPC) for shuffle reads and query result transfer.

**Control plane remains vanilla gRPC (tonic):** Registration, heartbeat, task assignment, task status updates, and cancellation RPCs stay on standard gRPC with Protobuf messages. These are low-volume, latency-tolerant messages that benefit from strongly-typed Protobuf schemas.

---

## Why Not vanilla gRPC + Protobuf for Data?

Every shuffle partition transferred over gRPC+Protobuf requires:

```
Arrow RecordBatch (zero-copy in memory)
  → serialize to Protobuf bytes       ← CPU: O(rows × columns)
  → transmit over HTTP/2
  → deserialize Protobuf bytes        ← CPU: O(rows × columns)
  → reconstruct Arrow RecordBatch     ← allocation + copy
```

For a 100M-row shuffle partition, this is ~400MB of unnecessary CPU work per partition. At TPC-H SF100 (the R4 usable product gate), this would dominate shuffle time.

---

## Why Arrow Flight

Apache Arrow Flight is Arrow IPC over gRPC. For local reads (same machine), it supports zero-copy via memory-mapped buffers. For network reads, it transmits Arrow IPC frames directly without intermediate Protobuf encoding:

```
Arrow RecordBatch (zero-copy in memory)
  → Arrow IPC frame (length-prefixed, direct buffer reference)
  → transmit over HTTP/2 (gRPC)
  → reconstruct Arrow RecordBatch (zero-copy for local; one copy for network)
```

Arrow Flight is already designed for this exact use case and is the data plane used by DataFusion, Ballista, and major cloud query engines.

---

## Separation of Control and Data Planes

```
Control Plane (tonic gRPC + Protobuf):
  Coordinator ──────────────────────────► Executor
  - RegisterExecutor / DeregisterExecutor
  - AssignTask / CancelTask
  - TaskStatusUpdate
  - Heartbeat

Data Plane:
  Executor local disk ────────────────► Executor (shuffle write finalization)
  Executor A ─────────────────────────► Executor B (local-mode shuffle reads via Flight)
  ShuffleStore backend ───────────────► Executor (object-store-mode reads via Flight)
  Executor ───────────────────────────► Coordinator (query results via Flight)
```

The coordinator itself is not on the data plane for bulk data movement. It receives only result metadata (row count, bytes written, error) — the actual data moves directly between executors and the shuffle store.

---

## R4 Shuffle Data Plane: Write Local, Read Through The Selected Backend

See `docs/architecture/shuffle-deployment-model.md` for the full shuffle model. The data plane role:

- **Write path:** Executor writes Arrow IPC frames to local disk (staging buffer). Arrow Flight is not used for the write path; local file I/O is faster.
- **Local durability read path:** When a partition is complete, the local file is atomically renamed to its final path and served by the producing executor over Arrow Flight.
- **Object-store durability read path:** When a partition is complete, the local file is uploaded to the configured object store. Stage N+1 reads through Arrow Flight from that object-store-backed shuffle location.
- **No Protobuf encoding:** In both read modes, partition data moves as Arrow IPC frames.

---

## What This Is Not

- **Not Arrow Flight SQL:** This is raw Arrow Flight for data movement, not SQL-over-Flight. Flight SQL is a separate feature (R8.1) for client-facing query access.
- **Not zero-copy for object-store reads:** Network reads from object storage require one copy into the executor's Arrow buffer pool. Zero-copy only applies to local reads within the same machine.
- **Not a true hybrid shuffle:** R4 supports either `local` durability or `object-store` durability for a job. It does not write both and then choose a local-read fast path with object-store fallback.
- **Not the final performance architecture:** A local shuffle service (similar to Spark's External Shuffle Service, running as a DaemonSet) is a future performance optimization for latency-sensitive workloads. R4 starts with executor-served local shuffle by default and optional object-store durability.

---

## Future Optimization Path (Post-R4)

| Phase | What changes | Why |
|---|---|---|
| R4 | Executor-served local shuffle by default; optional object-store durability | No external dependency by default, with an opt-in resilience path |
| R7/R10 | Local shuffle service (DaemonSet) with Arrow Flight | Reduces executor coupling and local read latency for high-fan-out shuffles at SF1000+ |
| Post-GA | Push-based shuffle with direct executor-to-executor Flight streams | Reduces staging cost for co-located stages after the baseline is proven |
