#!/usr/bin/env python3
"""TPC-H SF100 on DuckDB — the single-process reference for the embedded tier.

Companion to `tpch_cluster_run.py` (distributed), `tpch_spark_run.py` (external
distributed baseline) and `tpch_embedded_run.py` (Krishiv in-process). Writes
the same JSON shape as all of them so the four can be diffed directly.

# Why DuckDB and not Spark for this tier

The 3-node and 1-node tiers measure a *distributed* engine, where Spark is the
right comparison. The embedded tier is one process with no scheduler, no
shuffle service and no network — the regime DuckDB was built for and where it,
not Spark, is the engine to beat. Running Spark here too is still useful
(`--master local[*]`), but a Krishiv-vs-Spark-only embedded number would be
choosing a weak reference.

# What "same resources" means here

Whatever budget the tier is given, all three engines get it:

    threads        matched to the CPU limit of the other tiers' executor
    memory_limit   matched to that executor's memory limit

DuckDB spills to disk past `memory_limit`, so `temp_directory` must have room;
it is set explicitly rather than left to default into a container layer that
may be much smaller than it looks.

# Reading the same bytes

The same Parquet in the same MinIO, through httpfs against the
`internalTrafficPolicy: Local` service, so DuckDB reads its own node's MinIO —
the identical locality the Krishiv and Spark runs get. A cross-node read was
measured at 7.6 MB/s on this cluster and would measure the network, not the
engine.

# What is deliberately NOT tuned

Nothing beyond threads, memory_limit and temp_directory, all of which exist
only to match the other engines' resources. No pragma tweaks, no statistics
hints, no query rewrites. Note the asymmetry that does exist and is not
DuckDB's fault: Krishiv runs with its optimisations on (late materialisation,
declared primary keys). Parquet carries no key and DuckDB has no equivalent
declaration for a view over files, so it simply does not get that information.
State it with the results rather than letting it pass as an engine difference.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from repeat_stats import print_spread, summarize  # noqa: E402 - needs the path set above

TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]

# nation and region are single files at the SF100 root; the rest are directories
# of parts. Getting this wrong reads zero rows and still "succeeds".
SINGLE_FILE = {"nation", "region"}


def table_glob(root: str, table: str) -> str:
    root = root.rstrip("/")
    if table in SINGLE_FILE:
        return f"{root}/{table}.parquet"
    return f"{root}/{table}/*.parquet"


def connect(args):
    """A DuckDB connection configured for the tier's resource budget."""
    import duckdb

    con = duckdb.connect(database=":memory:")
    con.execute(f"SET threads TO {args.threads}")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    con.execute(f"SET temp_directory = '{args.temp_dir}'")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    endpoint = os.environ["AWS_ENDPOINT_URL"]
    # DuckDB wants host:port without a scheme, plus an explicit ssl flag.
    scheme, _, hostport = endpoint.partition("://")
    con.execute(f"SET s3_endpoint = '{hostport}'")
    con.execute(f"SET s3_use_ssl = {'true' if scheme == 'https' else 'false'}")
    con.execute("SET s3_url_style = 'path'")
    con.execute(f"SET s3_region = '{os.environ.get('AWS_REGION', 'us-east-1')}'")
    con.execute(f"SET s3_access_key_id = '{os.environ['AWS_ACCESS_KEY_ID']}'")
    con.execute(f"SET s3_secret_access_key = '{os.environ['AWS_SECRET_ACCESS_KEY']}'")
    return con


def register(con, root: str) -> None:
    """Expose each table as a view over its Parquet, like the other runners.

    Views, not `CREATE TABLE AS`: loading SF100 into memory first would measure
    an ingest DuckDB is not being asked to do, and neither Krishiv nor Spark is
    given that head start.
    """
    for table in TABLES:
        con.execute(
            f"CREATE OR REPLACE VIEW {table} AS "
            f"SELECT * FROM read_parquet('{table_glob(root, table)}')"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True, help="s3://bucket/prefix root")
    parser.add_argument("--corpus-json", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--out")
    parser.add_argument("--only", help="comma-separated query ids")
    parser.add_argument("--scale", type=int, default=100)
    parser.add_argument("--threads", type=int, required=True)
    parser.add_argument("--memory-limit", required=True, help="e.g. 4500MB")
    parser.add_argument("--temp-dir", default="/tmp/duckdb-spill")
    parser.add_argument("--timeout", type=float, default=10800.0)
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="run the whole corpus N times; report per-query median and spread",
    )
    args = parser.parse_args()

    with open(args.corpus_json, encoding="utf-8") as handle:
        corpus = json.load(handle)
    queries = corpus["queries"] if isinstance(corpus, dict) else corpus
    if args.only:
        wanted = [q.strip() for q in args.only.split(",") if q.strip()]
        by_id = {q["id"]: q for q in queries}
        missing = sorted({q for q in wanted if q not in by_id})
        if missing:
            raise SystemExit(f"unknown query ids: {missing}")
        queries = [by_id[q] for q in wanted]

    os.makedirs(args.temp_dir, exist_ok=True)
    con = connect(args)
    register(con, args.data)
    print(
        f"# {args.label}: {len(queries)} queries at SF{args.scale} on DuckDB\n"
        f"# data {args.data}  threads={args.threads} memory_limit={args.memory_limit}",
        flush=True,
    )

    runs: list[dict] = []
    for pass_index in range(args.repeat):
        if args.repeat > 1:
            print(f"\n# pass {pass_index + 1} of {args.repeat}", flush=True)
        for query in queries:
            started = time.monotonic()
            try:
                rows = con.execute(query["sql"]).fetchall()
                outcome = {
                    "id": query["id"],
                    "name": query.get("name", ""),
                    "status": "ok",
                    "elapsed_s": round(time.monotonic() - started, 2),
                    "rows": len(rows),
                }
                print(
                    f"ok   {outcome['id']:>3} {outcome['name']:<34} "
                    f"{outcome['elapsed_s']:>9.2f} s  rows={outcome['rows']}",
                    flush=True,
                )
            except Exception as error:  # noqa: BLE001 - the message is the finding
                outcome = {
                    "id": query["id"],
                    "name": query.get("name", ""),
                    "status": "failed",
                    "elapsed_s": round(time.monotonic() - started, 2),
                    # Keep the tail: DuckDB puts the cause last.
                    "error": str(error)[-400:],
                }
                print(
                    f"FAIL {outcome['id']:>3} {outcome['name']:<34} {outcome['error'][:120]}",
                    flush=True,
                )
            outcome["pass_index"] = pass_index
            runs.append(outcome)

    results = summarize(runs, args.repeat)

    ok = [r for r in results if r["status"] == "ok"]
    total = sum(r["elapsed_s"] for r in ok)
    print(f"\n{len(ok)} / {len(results)} queries succeeded; total {total:.1f} s")
    if args.repeat > 1:
        print_spread(results)

    record = {
        "label": args.label,
        "engine": "duckdb",
        "scale_factor": args.scale,
        "data_root": args.data,
        "threads": args.threads,
        "memory_limit": args.memory_limit,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "succeeded": len(ok),
        "attempted": len(results),
        "total_elapsed_s": total,
        "queries": results,
    }
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            json.dump(record, handle, indent=2)
        print(f"wrote {args.out}")
    return 0 if len(ok) == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
