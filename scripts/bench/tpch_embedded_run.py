#!/usr/bin/env python3
"""Run the TPC-H corpus through the embedded (in-process) engine and time it.

Companion to `tpch_cluster_run.py`, which drives a real coordinator over HTTP.
This one invokes `krishiv sql --local`, so the same queries execute with no
control plane, no shuffle over the network, and no object store — the data is
local parquet.

That difference is the point. A distributed run over a single-disk object store
measures the storage topology far more than the engine: on the 3-node k3s
cluster the executors sat at 17-24% CPU while every byte funnelled through one
MinIO pod on one virtualised disk. Running the identical corpus against local
storage says what the engine does when it is not waiting for bytes.

The queries come from the same `tpch_corpus` JSON both runners read, so the SQL
is byte-identical across topologies. Comparing anything else would be comparing
two different benchmarks.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from benchlock import machine_lock  # noqa: E402 - needs the path set above
from repeat_stats import print_spread, summarize  # noqa: E402 - same

# nation and region generate as a single file; the rest are directories of
# parts. `--parquet name=path` accepts either, but the path has to be right.
SINGLE_FILE_TABLES = {"nation", "region"}


def table_path(data_root: str, table: str) -> str:
    """Path for `table` under `data_root`, local or object store.

    The `exists()` probe is a *local* disambiguation: on disk, a single-file
    table may have been generated either as `nation.parquet` or as a directory,
    so the layout is checked rather than assumed.

    An object-store URI has no filesystem to probe — `os.path.exists` is always
    False for `s3://...` — and taking the fallback there silently resolves
    `nation` to a DIRECTORY that does not exist, for a table that is a single
    file. So a URI root uses the same fixed convention `tpch_spark_run` and
    `tpch_duckdb_run` use, which is what the generator actually writes.

    (Identical in kind to the CLI's `--parquet` guard, where the same
    `Path::exists` question about a URI produced "parquet file not found" for
    data that was there.)
    """
    if "://" in data_root:
        root = data_root.rstrip("/")
        return f"{root}/{table}.parquet" if table in SINGLE_FILE_TABLES else f"{root}/{table}"
    if table in SINGLE_FILE_TABLES:
        single = os.path.join(data_root, f"{table}.parquet")
        if os.path.exists(single):
            return single
    return os.path.join(data_root, table)


def run_query(
    binary: str,
    data_root: str,
    query: dict,
    timeout_s: int,
    primary_keys: dict[str, list[str]] | None = None,
) -> dict:
    """Execute one query, returning its outcome and wall-clock elapsed time.

    The declared primary keys are passed through for the same reason the
    cluster runner passes them: they are part of the TPC-H schema, and an
    embedded run that omits them plans a *different query* from the
    distributed one. That difference would show up as an embedded-vs-cluster
    performance gap and be misread as the cost of distribution.
    """
    keys = primary_keys or {}
    argv = [binary, "sql", "--local"]
    for table in query["tables"]:
        argv += ["--parquet", f"{table}={table_path(data_root, table)}"]
        if keys.get(table):
            argv += ["--primary-key", f"{table}={','.join(keys[table])}"]
    argv += ["--query", query["sql"]]

    started = time.monotonic()
    try:
        proc = subprocess.run(
            argv, capture_output=True, text=True, timeout=timeout_s, check=False
        )
    except subprocess.TimeoutExpired:
        return {
            "status": "timeout",
            "elapsed_s": round(time.monotonic() - started, 2),
            "error": f"exceeded {timeout_s}s",
        }
    elapsed = round(time.monotonic() - started, 2)

    if proc.returncode != 0:
        # Keep the tail: an engine failure puts the plan first and the cause
        # last, so truncating from the front discards the only useful part.
        detail = (proc.stderr or proc.stdout or "").strip()
        return {"status": "failed", "elapsed_s": elapsed, "error": detail[-400:]}
    return {"status": "ok", "elapsed_s": elapsed}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, help="path to the krishiv binary")
    parser.add_argument("--data", required=True, help="local SF100 parquet root")
    parser.add_argument("--corpus-json", required=True)
    parser.add_argument("--scale", type=int, default=100)
    parser.add_argument("--label", default="embedded-local")
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--out", help="write results JSON here")
    parser.add_argument(
        "--only",
        help=(
            "comma-separated query ids, in the order given. Present on every "
            "other runner; its absence here meant a targeted rerun or smoke "
            "test had to run all 22."
        ),
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="run the whole corpus N times; report per-query median and spread",
    )
    args = parser.parse_args()

    with open(args.corpus_json, encoding="utf-8") as handle:
        corpus = json.load(handle)
    queries = corpus["queries"]
    # An older corpus file has no `primary_keys`; defaulting to {} keeps the
    # previous behaviour rather than silently declaring an empty key.
    primary_keys = corpus.get("primary_keys", {})
    if args.only:
        # A LIST, not a set intersection: the requested order is the run order,
        # which is how a sweep front-loads the queries under investigation.
        wanted = [q.strip() for q in args.only.split(",") if q.strip()]
        by_id = {q["id"]: q for q in queries}
        missing = sorted({q for q in wanted if q not in by_id})
        if missing:
            raise SystemExit(f"unknown query ids: {missing}")
        seen: set[str] = set()
        queries = [by_id[q] for q in wanted if not (q in seen or seen.add(q))]

    print(
        f"# {args.label}: {len(queries)} queries at SF{args.scale} "
        f"via {args.binary} --local",
        flush=True,
    )
    print(f"# data root {args.data}", flush=True)

    # Same machine-wide lock the comparison runner takes: an embedded sweep and
    # a three-engine comparison are equally capable of ruining each other's
    # numbers, and they are launched by different scripts that have no reason
    # to know about one another.
    runs: list[dict] = []
    with machine_lock(f"tpch_embedded_run {args.label}"):
        for pass_index in range(args.repeat):
            if args.repeat > 1:
                print(f"\n# pass {pass_index + 1} of {args.repeat}", flush=True)
            for query in queries:
                # The corpus id (`q1`), NOT an enumeration index. This used to
                # record `id: 1`, which meant an embedded result could not be
                # joined to a cluster, Spark or DuckDB result by query — every
                # cross-topology comparison would silently match nothing.
                query_id = query["id"]
                label = f"{query_id} {query['name']}"
                outcome = run_query(
                    args.binary, args.data, query, args.timeout, primary_keys
                )
                outcome = {
                    "id": query_id,
                    "name": query["name"],
                    "pass_index": pass_index,
                    **outcome,
                }
                runs.append(outcome)
                if outcome["status"] == "ok":
                    print(f"ok    {label:<45} {outcome['elapsed_s']:>8.2f} s", flush=True)
                else:
                    print(
                        f"FAIL  {label:<45} {outcome['status']}: {outcome.get('error', '')}",
                        flush=True,
                    )

    results = summarize(runs, args.repeat)
    ok = sum(1 for r in results if r["status"] == "ok")
    total = sum(r["elapsed_s"] for r in results if r["status"] == "ok")
    print(f"\n{ok}/{len(results)} queries ran; total {total:.1f} s", flush=True)
    if args.repeat > 1:
        print_spread(results)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "label": args.label,
                    "scale": args.scale,
                    "topology": "embedded-local-parquet",
                    "queries": results,
                },
                handle,
                indent=2,
            )
        print(f"wrote {args.out}", flush=True)

    return 0 if ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
