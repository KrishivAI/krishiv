# krishiv-state

Keyed operator state and its durability: the `StateBackend` trait with
in-memory, RocksDB (per-write or per-epoch fsync), TTL, disaggregated
(DFS-primary with local cache), broadcast, and queryable backends; key
groups (32 768) and rescaling; event- and processing-time timers; state
schema migrations and `OperatorStateDescriptor`; checkpoint storage
(ephemeral, local filesystem, object store) with SHA-256 integrity
manifests and incremental SST checkpoints; savepoints. Property-tested
kill/restore (`tests/proptest_checkpoint_kill.rs`).

Documentation: `docs/architecture/07-state-checkpoints-savepoints.md`.

License: Apache-2.0.
