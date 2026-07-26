#!/usr/bin/env python3
"""Run the TPC-H corpus through Krishiv, DuckDB and Spark on one machine.

The only comparison that means anything is one where the hardware, the data
and the SQL are identical and the runs do not overlap. Everything else in this
directory measures Krishiv against itself across topologies; this measures it
against other engines under conditions none of them can blame.

Three rules the runner enforces, because breaking any of them silently
produces a number that looks fine and means nothing:

1. **Runs are serial.** Two engines sharing eight cores measure contention,
   not throughput. Each engine gets the machine to itself.
2. **The SQL is the same text.** All three read the same corpus JSON. Dialect
   differences are recorded as failures rather than papered over with
   per-engine rewrites, because a rewritten query is a different query.
3. **Results are compared, not just timed.** A wrong answer returned quickly
   is not a win. Row counts and the first row of each result are captured per
   engine and cross-checked; a disagreement is reported as loudly as a crash.

Usage:
  scripts/bench/tpch_compare_engines.py --data /data/tpch-sf100 \
      --corpus-json corpus.json --krishiv target/release/krishiv \
      --out benchmarks/tpch-sf100-engine-comparison.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time

SINGLE_FILE_TABLES = {"nation", "region"}

# Stop before the disk is full rather than after. SF100 spills: a 39 GB input
# on a box with ~24 GB free means any of the three engines can run it dry on
# q9/q18/q21. Filling a disk is not a benchmark result — it corrupts every
# later query on the box, and on the cluster this exact failure mode evicted an
# executor and invalidated a whole sweep before anyone noticed it was a disk
# problem at all.
MIN_FREE_BYTES = 12 * 1024**3


def free_bytes(path: str) -> int:
    stats = os.statvfs(path)
    return stats.f_bavail * stats.f_frsize


def disk_headroom_ok(path: str) -> bool:
    return free_bytes(path) >= MIN_FREE_BYTES


TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


def table_path(data_root: str, table: str) -> str:
    if table in SINGLE_FILE_TABLES:
        single = os.path.join(data_root, f"{table}.parquet")
        if os.path.exists(single):
            return single
    return os.path.join(data_root, table)


def parquet_glob(data_root: str, table: str) -> str:
    """A path DuckDB/Spark can read: a file, or every parquet under a dir."""
    path = table_path(data_root, table)
    return path if path.endswith(".parquet") else os.path.join(path, "**", "*.parquet")


def fingerprint(rows: list[tuple]) -> dict:
    """Summarise a result set so two engines can be compared without storing it.

    Row count alone would pass a query that returned the right number of wrong
    rows, so this also hashes the fully-ordered, stringified contents. Floats
    are rounded to 6 significant digits first: TPC-H sums over 600M rows
    legitimately differ in the last bits between engines depending on summation
    order, and treating that as a correctness failure would be a false alarm.
    """
    digest = hashlib.sha256()
    for row in rows:
        for value in row:
            if isinstance(value, float):
                digest.update(f"{value:.6g}".encode())
            else:
                digest.update(str(value).encode())
            digest.update(b"\x1f")
        digest.update(b"\x1e")
    return {"rows": len(rows), "digest": digest.hexdigest()[:16]}


# ── engines ──────────────────────────────────────────────────────────────

def run_duckdb(data_root: str, queries: list[dict], timeout_s: int) -> list[dict]:
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET threads TO {os.cpu_count() or 8}")
    for table in TABLES:
        con.execute(
            f"CREATE OR REPLACE VIEW {table} AS "
            f"SELECT * FROM read_parquet('{parquet_glob(data_root, table)}')"
        )
    out = []
    for index, query in enumerate(queries, start=1):
        if not disk_headroom_ok(data_root):
            out.append({"id": index, "name": query["name"], "status": "skipped",
                        "elapsed_s": 0.0,
                        "error": f"disk below {MIN_FREE_BYTES // 1024**3} GiB free"})
            print(f"  duckdb q{index:<3} skipped (low disk)", flush=True)
            continue
        started = time.monotonic()
        try:
            rows = con.execute(query["sql"]).fetchall()
            out.append({
                "id": index, "name": query["name"], "status": "ok",
                "elapsed_s": round(time.monotonic() - started, 2),
                **fingerprint(rows),
            })
        except Exception as err:  # noqa: BLE001 - any engine error is a result
            out.append({
                "id": index, "name": query["name"], "status": "failed",
                "elapsed_s": round(time.monotonic() - started, 2),
                "error": str(err)[-300:],
            })
        print(f"  duckdb q{index:<3} {out[-1]['status']:<7} "
              f"{out[-1]['elapsed_s']:>8.2f} s", flush=True)
    return out


def run_spark(data_root: str, queries: list[dict], timeout_s: int) -> list[dict]:
    from pyspark.sql import SparkSession

    cores = os.cpu_count() or 8
    # Size the heap from the machine, not from a number typed once and never
    # rechecked. This box has 23 GB total with ~8 GB already resident, so the
    # 16g this used to request would have had the JVM and the page cache
    # fighting over the same pages — and it was also *more* than Krishiv's own
    # query pool (0.6 of RAM), which would have made the comparison flattering
    # to Spark in one direction and starved by swapping in the other.
    total_gib = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") / 1024**3
    driver_gib = max(4, int(total_gib * 0.6))
    spark = (
        SparkSession.builder.appName("tpch-sf100")
        .master(f"local[{cores}]")
        .config("spark.driver.memory", f"{driver_gib}g")
        .config("spark.sql.shuffle.partitions", str(cores * 2))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    print(f"  spark driver heap {driver_gib}g of {total_gib:.0f} GiB", flush=True)
    spark.sparkContext.setLogLevel("ERROR")
    for table in TABLES:
        spark.read.parquet(table_path(data_root, table)).createOrReplaceTempView(table)

    out = []
    for index, query in enumerate(queries, start=1):
        if not disk_headroom_ok(data_root):
            out.append({"id": index, "name": query["name"], "status": "skipped",
                        "elapsed_s": 0.0,
                        "error": f"disk below {MIN_FREE_BYTES // 1024**3} GiB free"})
            print(f"  spark q{index:<3} skipped (low disk)", flush=True)
            continue
        started = time.monotonic()
        try:
            rows = [tuple(r) for r in spark.sql(query["sql"]).collect()]
            out.append({
                "id": index, "name": query["name"], "status": "ok",
                "elapsed_s": round(time.monotonic() - started, 2),
                **fingerprint(rows),
            })
        except Exception as err:  # noqa: BLE001
            out.append({
                "id": index, "name": query["name"], "status": "failed",
                "elapsed_s": round(time.monotonic() - started, 2),
                "error": str(err)[-300:],
            })
        print(f"  spark  q{index:<3} {out[-1]['status']:<7} "
              f"{out[-1]['elapsed_s']:>8.2f} s", flush=True)
    spark.stop()
    return out


def run_krishiv(binary: str, data_root: str, queries: list[dict],
                timeout_s: int) -> list[dict]:
    out = []
    for index, query in enumerate(queries, start=1):
        if not disk_headroom_ok(data_root):
            out.append({"id": index, "name": query["name"], "status": "skipped",
                        "elapsed_s": 0.0,
                        "error": f"disk below {MIN_FREE_BYTES // 1024**3} GiB free"})
            print(f"  krishiv q{index:<3} skipped (low disk)", flush=True)
            continue
        # NDJSON, not the ASCII table: counting lines in a rendered table
        # counts borders and padding, which would let a wrong-but-fast result
        # pass as correct. This makes Krishiv's digest comparable to the other
        # two engines' rather than a row count taken on trust.
        argv = [binary, "sql", "--local", "--format", "json"]
        for table in query["tables"]:
            argv += ["--parquet", f"{table}={table_path(data_root, table)}"]
        argv += ["--query", query["sql"]]
        started = time.monotonic()
        try:
            proc = subprocess.run(argv, capture_output=True, text=True,
                                  timeout=timeout_s, check=False)
        except subprocess.TimeoutExpired:
            out.append({"id": index, "name": query["name"], "status": "timeout",
                        "elapsed_s": round(time.monotonic() - started, 2)})
            continue
        elapsed = round(time.monotonic() - started, 2)
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            out.append({"id": index, "name": query["name"], "status": "failed",
                        "elapsed_s": elapsed, "error": detail[-300:]})
        else:
            try:
                rows = [
                    tuple(json.loads(line).values())
                    for line in proc.stdout.splitlines()
                    if line.strip()
                ]
            except json.JSONDecodeError as err:
                out.append({"id": index, "name": query["name"], "status": "failed",
                            "elapsed_s": elapsed,
                            "error": f"result was not NDJSON: {err}"})
                print(f"  krishiv q{index:<2} failed  {elapsed:>8.2f} s", flush=True)
                continue
            out.append({"id": index, "name": query["name"], "status": "ok",
                        "elapsed_s": elapsed, **fingerprint(rows)})
        print(f"  krishiv q{index:<2} {out[-1]['status']:<7} "
              f"{out[-1]['elapsed_s']:>8.2f} s", flush=True)
    return out


def median(values: list[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def merge_passes(passes: list[list[dict]]) -> list[dict]:
    """Collapse repeated passes over the corpus into one median-timed result.

    Repeating the *whole corpus* rather than each query back-to-back is
    deliberate: running one query three times in a row measures how warm its
    own working set is by the third go, which flatters later repetitions and
    tells you nothing about a cold plan.

    Two single runs of this corpus on the same box differed by -64% to +68%
    per query, so a single sample cannot support any claim finer than an order
    of magnitude. The median of N passes can.

    Digests are compared across passes as well: an engine that returns
    different answers to the same query on the same data is a correctness bug,
    and it is invisible to any harness that only keeps the last result.
    """
    if len(passes) == 1:
        return passes[0]
    merged = []
    for index in range(len(passes[0])):
        samples = [p[index] for p in passes]
        ok = [s for s in samples if s["status"] == "ok"]
        base = dict(ok[0] if ok else samples[0])
        if ok:
            times = [s["elapsed_s"] for s in ok]
            base["elapsed_s"] = round(median(times), 2)
            base["samples_s"] = times
            base["runs_ok"] = f"{len(ok)}/{len(samples)}"
            digests = {s.get("digest") for s in ok}
            if len(digests) > 1:
                base["status"] = "nondeterministic"
                base["error"] = f"differing digests across passes: {sorted(digests)}"
        merged.append(base)
    return merged


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True)
    parser.add_argument("--corpus-json", required=True)
    parser.add_argument("--krishiv", help="path to the krishiv binary")
    parser.add_argument("--engines", default="krishiv,duckdb,spark")
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--repeat", type=int, default=1,
                        help="passes over the whole corpus per engine; "
                             "reported time is the median (default 1)")
    parser.add_argument("--out")
    args = parser.parse_args()

    with open(args.corpus_json, encoding="utf-8") as handle:
        queries = json.load(handle)["queries"]

    wanted = [e.strip() for e in args.engines.split(",") if e.strip()]
    results: dict[str, list[dict]] = {}

    # Serial by construction: one engine finishes before the next starts.
    # Every engine gets the same number of passes — giving one of them a median
    # and the others a single sample would bias the comparison toward whichever
    # got to discard its worst run.
    for engine in wanted:
        if engine == "krishiv" and not args.krishiv:
            print("skipping krishiv: --krishiv not given", flush=True)
            continue
        if engine not in ("duckdb", "spark", "krishiv"):
            print(f"unknown engine {engine}", flush=True)
            continue
        passes = []
        for attempt in range(1, args.repeat + 1):
            print(f"\n=== {engine} ({len(queries)} queries) "
                  f"pass {attempt}/{args.repeat} ===", flush=True)
            if engine == "duckdb":
                passes.append(run_duckdb(args.data, queries, args.timeout))
            elif engine == "spark":
                passes.append(run_spark(args.data, queries, args.timeout))
            else:
                passes.append(run_krishiv(args.krishiv, args.data, queries, args.timeout))
        results[engine] = merge_passes(passes)

    print("\n=== summary ===", flush=True)
    for engine, rows in results.items():
        ok = [r for r in rows if r["status"] == "ok"]
        total = sum(r["elapsed_s"] for r in ok)
        print(f"{engine:<8} {len(ok)}/{len(rows)} ok  total {total:8.1f} s", flush=True)

    # Cross-engine correctness. Identical SQL over identical data must agree,
    # so a disagreement is a bug in one of them — and a fast wrong answer is
    # not a win. Every pair is checked, including Krishiv against the two
    # reference engines, which is the check the row-count-only version of this
    # runner could not make.
    names = [e for e in results if any(r.get("digest") for r in results[e])]
    mismatches = 0
    for i, left in enumerate(names):
        for right in names[i + 1:]:
            print(f"\n=== {left} vs {right} result agreement ===", flush=True)
            agreed = 0
            for a, b in zip(results[left], results[right]):
                if a["status"] != "ok" or b["status"] != "ok":
                    continue
                if a.get("digest") == b.get("digest"):
                    agreed += 1
                else:
                    mismatches += 1
                    print(f"  MISMATCH q{a['id']} {a['name']}: "
                          f"{left} rows={a.get('rows')} digest={a.get('digest')} vs "
                          f"{right} rows={b.get('rows')} digest={b.get('digest')}",
                          flush=True)
            print(f"  {agreed} queries agreed", flush=True)
    if mismatches:
        print(f"\n{mismatches} result mismatches — timings below are not "
              f"comparable until these are explained", flush=True)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            json.dump({"scale": 100, "data_root": args.data,
                       "cores": os.cpu_count(), "repeat": args.repeat,
                       "engines": results},
                      handle, indent=2)
        print(f"\nwrote {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
