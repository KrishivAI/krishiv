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

# nation and region generate as a single file; the rest are directories of
# parts. `--parquet name=path` accepts either, but the path has to be right.
SINGLE_FILE_TABLES = {"nation", "region"}


def table_path(data_root: str, table: str) -> str:
    """Filesystem path for `table` under `data_root`."""
    if table in SINGLE_FILE_TABLES:
        single = os.path.join(data_root, f"{table}.parquet")
        if os.path.exists(single):
            return single
    return os.path.join(data_root, table)


def run_query(binary: str, data_root: str, query: dict, timeout_s: int) -> dict:
    """Execute one query, returning its outcome and wall-clock elapsed time."""
    argv = [binary, "sql", "--local"]
    for table in query["tables"]:
        argv += ["--parquet", f"{table}={table_path(data_root, table)}"]
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
    args = parser.parse_args()

    with open(args.corpus_json, encoding="utf-8") as handle:
        queries = json.load(handle)["queries"]

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
    results = []
    with machine_lock(f"tpch_embedded_run {args.label}"):
        for index, query in enumerate(queries, start=1):
            name = f"q{index} {query['name']}"
            outcome = run_query(args.binary, args.data, query, args.timeout)
            results.append({"id": index, "name": query["name"], **outcome})
            if outcome["status"] == "ok":
                print(f"ok    {name:<45} {outcome['elapsed_s']:>8.2f} s", flush=True)
            else:
                print(
                    f"FAIL  {name:<45} {outcome['status']}: {outcome.get('error', '')}",
                    flush=True,
                )

    ok = sum(1 for r in results if r["status"] == "ok")
    total = sum(r["elapsed_s"] for r in results if r["status"] == "ok")
    print(f"\n{ok}/{len(results)} queries ran; total {total:.1f} s", flush=True)

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
