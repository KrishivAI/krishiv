# Streaming Compatibility Matrix (R16)

| Feature | Status | Notes |
|---------|--------|-------|
| gRPC checkpoint barriers | Supported | `BarrierService` bidirectional stream |
| Barrier alignment (multi-input) | Supported | `BarrierAligner` with timeout |
| CEP `begin().followed_by().within()` | Supported | `krishiv-cep` sequential matcher |
| CEP quantifiers (`one+`, `not_followed_by`) | Unsupported | `CepCompileError::UnsupportedCombinator` |
| Temporal as-of join | Supported | `TemporalJoinSpec` + `VersionedTableState` |
| Stream-stream interval join | Supported | `IntervalJoinSpec` |
| Late-data side output | Supported | `with_side_output("late", ms)` |
| Key-group rescaling | Supported | 32768 key groups; `KeyGroupRescaler` |
| State schema migration | Supported | `StateMigrationRegistry` |
| RocksDB incremental checkpoint | Supported | `IncrementalCheckpointWriter` (segment manifests) |
| Watermark propagation (multi-source) | Supported | `min(input watermarks)` |
