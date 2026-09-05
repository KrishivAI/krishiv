# krishiv-sql

DataFusion integration: `SqlEngine` and its registries, the `sql()`
pipeline with pre-optimizer rewrites (CTE materialisation, ROLLUP/CUBE/
GROUPING SETS), intercepted statements (DDL, DML, `ANALYZE`, `EXPLAIN`,
streaming and incremental view DDL, `SET`), Krishiv's optimizer rules
(`JoinReorder`, semi-join pushdown, `SpillableJoinSelection`,
`CooperativeAmplifiers`, `LateMaterializeTopKAggregate`,
`AnnTopKPrefilter`), session configuration, distributed plan encoding,
catalogs (local, REST, Postgres, Glue, Unity, Hive, Nessie, Polaris),
information schema, Spark SQL extensions, streaming window compilation,
Python UDF hosting.

Features: `iceberg`, `iceberg-datafusion`, `local-catalog`,
`postgres-catalog`, `rest-catalog`, plus connector forwarders. `default = []`
so an embedded build stays lean.

Documentation: `docs/architecture/02-sql-engine.md`,
`docs/architecture/03-planning-and-optimization.md`,
`docs/reference/sql-feature-matrix.md` (generated).

License: Apache-2.0.
