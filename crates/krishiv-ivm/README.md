# krishiv-ivm

The incremental view maintenance driver over `krishiv-delta`:
`IncrementalFlow` (sources, views in topological order, dirty-bit
scheduling, dedup, lateness, delta checkpoints, watch subscribers),
`plan` (O(Δ) plans for aggregate / join / distinct / top-N with a
`DiffBased` fallback), `decompose` (single-source multi-operator queries cut
into single-operator hops), `PartitionedIncrementalFlow` (key-sharded flows,
`KRISHIV_IVM_SHARDS`), the resident-executor wire codec (`IVMD1`/`IVMD2`),
and the memory budget. Property-tested against a diff-based oracle and a
plain-Rust model (`tests/proptest_ivm.rs`).

Documentation: `docs/architecture/09-incremental-view-maintenance.md`,
`docs/engineering-log/ivm-audit-register.md`.

License: Apache-2.0.
