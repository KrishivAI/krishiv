#!/usr/bin/env python3
"""TPC-H SF100 on Apache Spark — the external baseline for the Krishiv sweep.

Runs the **same 22 queries** from the same `corpus.json`, against the **same
Parquet files** in the same MinIO bucket, on the **same three nodes**, with
executor CPU and memory matched to the Krishiv bench deployment. It writes the
same JSON shape as `tpch_cluster_run.py` so the two runs can be diffed directly
rather than eyeballed.

# What "same resources" means here, precisely

Krishiv's bench deployment on this cluster is:

    coordinator   cpu 2      memory 2Gi
    executor x3   cpu 3800m  memory 5000Mi     (one per node, nodes are 4 CPU / 7.75Gi)

Spark is given the same shape: a driver matched to the coordinator and three
executors matched to the Krishiv executors, one per node. `spark.executor.cores`
is 3 rather than 3.8 because Spark's task slots are integral — 4 would let the
executor oversubscribe its own 3800m cgroup limit and get throttled, which
would understate Spark. This is the one place the two are not identical, and it
is documented rather than hidden.

# What is deliberately NOT tuned

No broadcast-threshold tweaks, no AQE knobs beyond the 3.5 defaults, no
statistics collection, no query rewrites. The Krishiv numbers were produced by
submitting the corpus SQL to a default engine; this submits the same SQL to a
default Spark. Tuning one side and not the other is how baselines become
marketing.
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

# nation and region are single files at the SF100 root; the rest are directories.
SINGLE_FILE = {"nation", "region"}


def table_path(root: str, table: str) -> str:
    root = root.rstrip("/")
    return f"{root}/{table}.parquet" if table in SINGLE_FILE else f"{root}/{table}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="s3a://bucket/prefix or a local path")
    ap.add_argument("--corpus-json", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", default="spark")
    ap.add_argument("--only", default="", help="comma-separated query ids")
    ap.add_argument("--timeout", type=float, default=7200.0)
    args = ap.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(args.label).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"# {args.label}: Spark {spark.version} against {args.data}", flush=True)
    print(
        f"# executors: {spark.conf.get('spark.executor.instances', '?')}"
        f" x {spark.conf.get('spark.executor.cores', '?')} cores"
        f" / {spark.conf.get('spark.executor.memory', '?')}",
        flush=True,
    )

    with open(args.corpus_json, encoding="utf-8") as handle:
        corpus = json.load(handle)
    # The corpus file is `{"count": N, "queries": [...]}`; a bare list is also
    # accepted. Iterating the dict directly yielded its *keys* — the strings
    # "count" and "queries" — and died on `query["sql"]` with "string indices
    # must be integers", 28 s into the run, after every table had registered.
    if isinstance(corpus, dict):
        corpus = corpus["queries"]
    wanted = [q.strip() for q in args.only.split(",") if q.strip()]
    if wanted:
        by_id = {q["id"]: q for q in corpus}
        missing = [q for q in wanted if q not in by_id]
        if missing:
            raise SystemExit(f"unknown query ids: {missing}")
        corpus = [by_id[q] for q in wanted]

    # Register every table once, up front. The Krishiv harness passes table
    # paths per query and the coordinator resolves them at plan time; doing the
    # equivalent here (a temp view per table) keeps per-query timing to
    # planning + execution on both sides rather than charging Spark for
    # metadata discovery it would normally amortise.
    register_started = time.monotonic()
    for table in TABLES:
        spark.read.parquet(table_path(args.data, table)).createOrReplaceTempView(table)
    register_s = time.monotonic() - register_started
    print(f"# registered {len(TABLES)} tables in {register_s:.1f}s", flush=True)

    results = []
    runs: list[dict] = []
    for pass_index in range(args.repeat):
        if args.repeat > 1:
            print(f"\n# pass {pass_index + 1} of {args.repeat}", flush=True)
        for query in corpus:
            started = time.monotonic()
            try:
                rows = spark.sql(query["sql"]).collect()
                elapsed = time.monotonic() - started
                runs.append(
                    {
                        "id": query["id"],
                        "name": query["name"],
                        "status": "ok",
                        "elapsed_s": elapsed,
                        "row_count": len(rows),
                    }
                )
                print(
                    f"ok   {query['id']:>4} {query['name']:<38} {elapsed:9.2f} s"
                    f"  rows={len(rows)}",
                    flush=True,
                )
            except Exception as error:  # noqa: BLE001 - the message is the finding
                elapsed = time.monotonic() - started
                runs.append(
                    {
                        "id": query["id"],
                        "name": query["name"],
                        "status": "failed",
                        "elapsed_s": elapsed,
                        "error": str(error)[:600],
                    }
                )
                print(
                    f"FAIL {query['id']:>4} {query['name']:<38} {elapsed:9.2f} s"
                    f"  {str(error)[:100]}",
                    flush=True,
                )

    for row in runs:
        row.setdefault("pass_index", 0)
    results = summarize(runs, args.repeat)
    if args.repeat > 1:
        print_spread(results)

    ok = [r for r in results if r["status"] == "ok"]
    total = sum(r["elapsed_s"] for r in ok)
    record = {
        "label": args.label,
        "engine": f"spark-{spark.version}",
        "data_root": args.data,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "table_register_s": register_s,
        "executor_instances": spark.conf.get("spark.executor.instances", None),
        "executor_cores": spark.conf.get("spark.executor.cores", None),
        "executor_memory": spark.conf.get("spark.executor.memory", None),
        "succeeded": len(ok),
        "attempted": len(results),
        "total_elapsed_s": total,
        "queries": results,
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2)
    print(f"\n{len(ok)} / {len(results)} queries succeeded; total {total:.1f} s")
    print(f"wrote {args.out}")

    # In cluster mode the driver is a pod whose filesystem dies with it, so the
    # file above is unreachable. Emit the same record to stdout between markers
    # and recover it from `kubectl logs`. Cheap, and it means the run does not
    # depend on the driver having anywhere durable to write.
    print("---KRISHIV-SPARK-RESULT-BEGIN---", flush=True)
    print(json.dumps(record), flush=True)
    print("---KRISHIV-SPARK-RESULT-END---", flush=True)

    spark.stop()
    # Non-zero if any query failed: a partial run must not read as a pass.
    return 0 if len(ok) == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
