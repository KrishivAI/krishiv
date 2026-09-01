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
import datetime
import decimal
import hashlib
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from benchlock import machine_lock  # noqa: E402 - needs the path set above

SINGLE_FILE_TABLES = {"nation", "region"}

# Stop before the disk is full rather than after. SF100 spills: a 39 GB input
# on a box with ~24 GB free means any of the three engines can run it dry on
# q9/q18/q21. Filling a disk is not a benchmark result — it corrupts every
# later query on the box, and on the cluster this exact failure mode evicted an
# executor and invalidated a whole sweep before anyone noticed it was a disk
# problem at all.
MIN_FREE_BYTES = 12 * 1024**3

# Where the engines spill. This is NOT the data root, and conflating the two is
# how a whole comparison was lost: the guard below measured free space on the
# filesystem holding the parquet input, while Spark spilled shuffle blocks to
# its default `spark.local.dir` of /tmp — a 12 GiB *tmpfs*, i.e. RAM. SF100
# shuffle does not fit in 12 GiB, so Spark died with `DiskBlockObjectWriter:
# Exception occurred while manually close the output stream` after five of six
# passes had already run, and no results were written at all.
#
# Pointing it at real disk and checking *this* path is the fix. A guard that
# measures a filesystem the workload never writes to is worse than no guard,
# because it reports healthy right up to the failure.
SPILL_DIR = os.environ.get("KRISHIV_BENCH_SPILL_DIR", "/data/bench-spill")


def free_bytes(path: str) -> int:
    stats = os.statvfs(path)
    return stats.f_bavail * stats.f_frsize


def disk_headroom_ok(path: str) -> bool:
    """True when both the data path and the spill path have headroom.

    Checked together because a run needs both, and whichever is tighter is the
    one that ends it.
    """
    os.makedirs(SPILL_DIR, exist_ok=True)
    return (free_bytes(path) >= MIN_FREE_BYTES
            and free_bytes(SPILL_DIR) >= MIN_FREE_BYTES)


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


SIGNIFICANT_DIGITS = 6

# Floor on how many decimal places a rounded value keeps. Significant digits
# alone are not enough: engines return the same average at different
# *precisions*, and for a small magnitude 6 significant digits reaches deeper
# than the coarser engine has digits at all. TPC-H q1's `avg_disc` is ~0.05, so
# DuckDB's double rounds to 0.0499853 while Spark's and DataFusion's DECIMAL(_,6)
# hold only 0.049985 — the same number, reported as a disagreement on every row.
# Rounding to whichever is COARSER (fewer digits) normalises that without
# blunting large values: a 12-digit sum still keeps 6 significant digits, so an
# error of billions cannot hide inside the tolerance.
DECIMAL_PLACES = 6

# Bump whenever `canonical_value` or `fingerprint` changes what they hash.
#
# Digests are only comparable within one scheme. This is not hypothetical: the
# 2026-08-30 09:43 baseline was written two hours *before* 9d62731 fixed
# `canonical_value`, so a later run diffed against it reported q8 and q11 as
# answer changes when both engines in fact agreed. That cost a real
# investigation, and the file gave no way to know it was from another scheme.
# `scale`/`cores`/`repeat` describe the machine; nothing described the ruler.
#
#   1 = pre-9d62731: str(value), type-sensitive
#   2 = 9d62731 onward: numeric comparison, DECIMAL_PLACES rounding
DIGEST_SCHEME = 2


def canonical_value(value) -> str:
    """Render a cell so that equal values compare equal across engines.

    The three engines return the same number as three different Python types.
    A TPC-H money column comes back from Spark as `Decimal('0.000000')`, from
    DuckDB as `0.0`, and from Krishiv — which arrives over JSON — as `0.0`.
    Hashing `str(value)` therefore hashed the type as much as the number, and
    reported disagreements that did not exist: it flagged 17 of 22 queries,
    including a DuckDB-versus-Spark difference on q1, which is a plain
    four-row aggregate that both engines get right.

    So numbers are compared as numbers. Exact integers keep every digit, which
    matters because `count(*)` at SF100 runs to nine digits and rounding one
    would hide precisely the kind of bug this check exists to catch. Anything
    with a fractional part is rounded to `SIGNIFICANT_DIGITS`, because sums
    over 600M rows legitimately differ in their low bits with summation order
    and that is not a correctness failure.
    """
    if value is None:
        return "\x00null"
    if isinstance(value, bool):  # bool is an int subclass; check it first
        return "true" if value else "false"
    if isinstance(value, (int, float, decimal.Decimal)):
        try:
            number = value if isinstance(value, decimal.Decimal) \
                else decimal.Decimal(str(value))
        except (decimal.InvalidOperation, ValueError):
            return str(value)
        if not number.is_finite():
            return str(number)
        if number == number.to_integral_value():
            return str(int(number))
        rounded = decimal.Context(prec=SIGNIFICANT_DIGITS).create_decimal(number)
        # Take the coarser of the two roundings. `exponent` is the power of ten
        # of the last kept digit, so the larger exponent is the blunter value.
        exponent = rounded.as_tuple().exponent
        if isinstance(exponent, int) and exponent < -DECIMAL_PLACES:
            rounded = rounded.quantize(
                decimal.Decimal(1).scaleb(-DECIMAL_PLACES),
                context=decimal.Context(prec=99),
            )
        return format(rounded.normalize(), "f")
    if isinstance(value, datetime.datetime):
        return value.isoformat(sep=" ").replace(".000000", "")
    if isinstance(value, datetime.date):
        return value.isoformat()
    return str(value)


def fingerprint(rows: list[tuple]) -> dict:
    """Summarise a result set so two engines can be compared without storing it.

    Row count alone would pass a query that returned the right number of wrong
    rows, so the contents are hashed too — twice. `digest` preserves row order;
    `digest_unordered` sorts the canonicalised rows first. TPC-H queries whose
    ORDER BY does not fully determine a total order may return tied rows in any
    sequence, and two engines permuting a tie are not disagreeing about the
    answer. Keeping both lets a real difference in content be reported as a
    failure while a difference in tie order is reported as what it is.
    """
    canonical = [[canonical_value(cell) for cell in row] for row in rows]

    def digest_of(sequence) -> str:
        digest = hashlib.sha256()
        for row in sequence:
            for cell in row:
                digest.update(cell.encode())
                digest.update(b"\x1f")
            digest.update(b"\x1e")
        return digest.hexdigest()[:16]

    return {
        "rows": len(rows),
        "digest": digest_of(canonical),
        "digest_unordered": digest_of(sorted(canonical)),
    }


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
        # Spill to real disk. The default is /tmp, which on this box is a
        # 12 GiB tmpfs — so "spilling" meant moving shuffle blocks from heap
        # into RAM, and SF100 exhausted it mid-run.
        .config("spark.local.dir", SPILL_DIR)
        # Reclaim shuffle files between queries instead of at session end.
        # All 22 queries share one SparkSession, and the ContextCleaner only
        # frees a query's shuffle blocks once its RDDs are garbage-collected —
        # which, with a large heap and no memory pressure, may not happen for
        # a long time. The spill therefore accumulated across the corpus until
        # the disk guard tripped and skipped 17 of 22 queries. Nothing leaked:
        # the directory was empty afterwards. It was peak, not growth, and
        # collecting more eagerly flattens the peak.
        .config("spark.cleaner.periodicGC.interval", "1min")
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
            # Drop this query's cached blocks before the next one starts, so
            # peak spill is one query's worth rather than the corpus's.
            spark.catalog.clearCache()
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
            # Judge determinism on content, not on the order of tied rows —
            # the same distinction the cross-engine check makes. An engine
            # that returns different *rows* run to run is a correctness bug;
            # one that permutes a tie is not.
            contents = {s.get("digest_unordered") or s.get("digest") for s in ok}
            if len(contents) > 1:
                base["status"] = "nondeterministic"
                base["error"] = f"differing results across passes: {sorted(contents)}"
            elif len({s.get("digest") for s in ok}) > 1:
                base["tie_order_varies"] = True
        merged.append(base)
    return merged


def compare_to_baseline(baseline_path: str, current: dict) -> int:
    """Diff this run's per-query digests against a stored baseline.

    Refuses to compare across canonicalisation schemes. A digest is only
    meaningful relative to the `canonical_value` that produced it, so diffing a
    scheme-1 file against a scheme-2 run reports disagreements that do not
    exist — which is exactly what happened with the 2026-08-30 baseline, on q8
    and q11, and it read like an engine regression.
    """
    with open(baseline_path, encoding="utf-8") as handle:
        baseline = json.load(handle)

    got = baseline.get("digest_scheme")
    if got != DIGEST_SCHEME:
        where = f"scheme {got}" if got is not None else "no recorded scheme"
        print(f"REFUSING to compare: baseline has {where}, this run is scheme "
              f"{DIGEST_SCHEME}. Digests across schemes are not comparable; "
              f"re-run the baseline with this harness.", flush=True)
        return 2

    problems = 0
    for engine, rows in current.items():
        base = {q["id"]: q for q in baseline.get("engines", {}).get(engine, [])}
        for row in rows:
            prior = base.get(row["id"])
            if prior is None:
                continue
            if row.get("status") != "ok" or prior.get("status") != "ok":
                continue
            if row["digest"] == prior["digest"]:
                continue
            # Same rows in a different order is not a wrong answer: TPC-H
            # ORDER BY clauses that do not fully determine a total order leave
            # ties free to permute.
            if row["digest_unordered"] == prior["digest_unordered"]:
                print(f"  {engine} q{row['id']}: same rows, different tie order",
                      flush=True)
                continue
            problems += 1
            print(f"  {engine} q{row['id']}: ANSWER CHANGED "
                  f"({prior['digest']} -> {row['digest']})", flush=True)
    print(f"\nanswer changes vs baseline: {problems}", flush=True)
    return 1 if problems else 0


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
    parser.add_argument("--compare-to",
                        help="baseline JSON to diff digests against; "
                             "refuses across canonicalisation schemes")
    args = parser.parse_args()

    with open(args.corpus_json, encoding="utf-8") as handle:
        queries = json.load(handle)["queries"]

    wanted = [e.strip() for e in args.engines.split(",") if e.strip()]
    results: dict[str, list[dict]] = {}

    # Serial by construction: one engine finishes before the next starts, and
    # the machine lock extends that guarantee across processes. Serialising the
    # engines inside one run is worthless if a second run of this same script
    # is executing beside it — which is exactly what happened, undetected,
    # because each caller script had its own name-matching guard that did not
    # know about the others. Every engine also gets the same number of passes;
    # giving one a median and the others a single sample would bias the
    # comparison toward whichever got to discard its worst run.
    with machine_lock(f"tpch_compare_engines {args.engines} repeat={args.repeat}"):
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
                    passes.append(
                        run_krishiv(args.krishiv, args.data, queries, args.timeout))
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
            reordered = 0
            for a, b in zip(results[left], results[right]):
                if a["status"] != "ok" or b["status"] != "ok":
                    continue
                if a.get("digest") == b.get("digest"):
                    agreed += 1
                elif a.get("digest_unordered") == b.get("digest_unordered"):
                    # Same rows, different sequence. TPC-H does not always
                    # impose a total order, so tied rows may come back in
                    # either order. That is not a wrong answer, and calling it
                    # one would bury the disagreements that matter.
                    reordered += 1
                    agreed += 1
                    print(f"  tie-order q{a['id']} {a['name']}: same "
                          f"{a.get('rows')} rows, different order", flush=True)
                else:
                    mismatches += 1
                    print(f"  MISMATCH q{a['id']} {a['name']}: "
                          f"{left} rows={a.get('rows')} digest={a.get('digest')} vs "
                          f"{right} rows={b.get('rows')} digest={b.get('digest')}",
                          flush=True)
            print(f"  {agreed} queries agreed"
                  + (f" ({reordered} only up to tie order)" if reordered else ""),
                  flush=True)
    if mismatches:
        print(f"\n{mismatches} result mismatches — timings below are not "
              f"comparable until these are explained", flush=True)
    else:
        print("\nall engines agreed on every comparable query", flush=True)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            json.dump({"scale": 100, "data_root": args.data,
                       "cores": os.cpu_count(), "repeat": args.repeat,
                       # Stamped so a cross-scheme comparison is detectable
                       # instead of silently reporting false disagreements.
                       "digest_scheme": DIGEST_SCHEME,
                       "engines": results},
                      handle, indent=2)
        print(f"\nwrote {args.out}", flush=True)

    if args.compare_to:
        return compare_to_baseline(args.compare_to, results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
