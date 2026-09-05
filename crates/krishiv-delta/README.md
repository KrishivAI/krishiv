# krishiv-delta

The Z-set algebra for incremental computing (DBSP-style), **not** Delta Lake
(that lives in `krishiv-connectors::lakehouse`): `DeltaBatch` (weighted
Arrow batch), `Trace` (8-level Spine with per-batch key index),
`SourceState` (materialised relation plus deficit), `LatenessSpec` /
`WatermarkTracker`, `LogicFingerprint`, `CoalescingMap`, and the operators —
map/project, filter, consolidate (linear); join (bilinear); aggregate,
distinct, top-N (stateful). Z-set laws are property-tested
(`tests/proptest_zset.rs`).

Documentation: `docs/architecture/09-incremental-view-maintenance.md`.

License: Apache-2.0.
