# Golden Tests

SQL, plan, and CLI snapshot fixtures live here when Krishiv behavior is stable
enough for golden files.

R1 includes golden fixtures for minimal `krishiv sql`, minimal
`krishiv explain`, and a Parquet-backed projection/filter/aggregate/limit query.
Broader SQL compatibility coverage should add focused fixtures here before
changing public output formatting.
