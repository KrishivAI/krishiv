#!/usr/bin/env python3
"""Generate TPC-DS SF1 as Parquet via DuckDB's tpcds extension (dsdgen).

The nightly dataset tier (`dataset-tier` job in .github/workflows/bench.yml,
running scripts/bench-datasets-tier.sh) provisions its own data because CI
has no pre-generated datasets. DuckDB's dsdgen is deterministic for a given
scale factor and duckdb version, so night-over-night history rows measure
the engine, not the data. All generated tables are exported (one
`<table>.parquet` per table, the layout `krishiv_bench::tpcds::tables_exist`
expects), not just the ones today's bundled queries read, so adding a query
to `tpcds.rs` never requires a dataset-cache bump.

Usage: python3 scripts/bench/gen_tpcds_sf1.py <output-dir>

Requires: `pip install duckdb` (the tpcds extension is fetched by INSTALL).
"""

import sys
from pathlib import Path

import duckdb


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL tpcds; LOAD tpcds;")
    con.execute("CALL dsdgen(sf=1)")
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    for table in tables:
        dest = out / f"{table}.parquet"
        con.execute(f"COPY {table} TO '{dest}' (FORMAT PARQUET)")
        print(f"wrote {dest}")
    print(f"generated {len(tables)} TPC-DS SF1 tables in {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
