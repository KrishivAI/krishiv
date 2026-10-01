#!/usr/bin/env python3
"""Paired, interleaved A/B of two Krishiv binaries over a query corpus.

    python3 skills/benchmarking/ab_krishiv.py OLD_BIN NEW_BIN CORPUS.json DATA_DIR \
        --rounds 3 --timeout 300 [--queries q5,q7] [--env KRISHIV_JOIN_REORDER=on]

CORPUS.json is the tpch_corpus shape: {"queries": [{"name", "tables", "sql"}]}.
One process per (query, binary, round); order alternates every round so neither
binary always runs second. Prints one flushed line per run (checkpoint), then a
per-query table of best-of and median, the result digest of each binary, peak
RSS, and the suite sums. A digest mismatch is reported before any timing.
"""
import argparse, hashlib, json, os, resource, statistics, subprocess, sys, time

ap = argparse.ArgumentParser()
ap.add_argument("old"); ap.add_argument("new"); ap.add_argument("corpus"); ap.add_argument("data")
ap.add_argument("--rounds", type=int, default=3); ap.add_argument("--timeout", type=int, default=600)
ap.add_argument("--queries", default=""); ap.add_argument("--env", action="append", default=[])
a = ap.parse_args()
env = dict(os.environ, **dict(kv.split("=", 1) for kv in a.env))
queries = json.load(open(a.corpus))["queries"]
want = set(a.queries.split(",")) if a.queries else None
queries = [q for i, q in enumerate(queries, 1) if not want or f"q{i}" in want or q["name"] in want]

def table_path(t):
    d = os.path.join(a.data, t); return d if os.path.isdir(d) else d + ".parquet"

def run(binary, q):
    argv = [binary, "sql", "--local", "--format", "json"]
    for t in q["tables"]: argv += ["--parquet", f"{t}={table_path(t)}"]
    argv += ["--query", q["sql"]]
    before = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    t0 = time.monotonic()
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=a.timeout, env=env)
        status = "ok" if p.returncode == 0 else "fail"
        rows = sorted(l for l in p.stdout.splitlines() if l.strip())
        digest = hashlib.sha256("\n".join(rows).encode()).hexdigest()[:12]
    except subprocess.TimeoutExpired:
        status, digest = "timeout", "-"
    el = time.monotonic() - t0
    rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1e6
    return el, status, digest, max(rss, before / 1e6)

samples = {}; digests = {}; rss = {}
bins = [("old", a.old), ("new", a.new)]
for r in range(a.rounds):
    for i, q in enumerate(queries):
        order = bins if (r + i) % 2 == 0 else bins[::-1]
        for label, binary in order:
            el, st, dg, mx = run(binary, q)
            samples.setdefault((q["name"], label), []).append(el if st == "ok" else float("inf"))
            digests.setdefault((q["name"], label), dg); rss[(q["name"], label)] = max(rss.get((q["name"], label), 0), mx)
            print(f"round {r+1} {label:3} {q['name']:<24} {st:7} {el:8.2f} s digest={dg} maxrss_gb={mx:.1f}", flush=True)

tot = {"old": 0.0, "new": 0.0}; mism = 0
print("\n| query | old best | new best | old med | new med | new/old | digests | rss old/new GB |\n|---|---|---|---|---|---|---|---|")
for q in queries:
    n = q["name"]; o, w = samples[(n, "old")], samples[(n, "new")]
    same = digests[(n, "old")] == digests[(n, "new")]; mism += not same
    ratio = statistics.median(w) / statistics.median(o) if statistics.median(o) else float("nan")
    tot["old"] += statistics.median(o); tot["new"] += statistics.median(w)
    print(f"| {n} | {min(o):.2f} | {min(w):.2f} | {statistics.median(o):.2f} | {statistics.median(w):.2f} | {ratio:.2f} | {'same' if same else 'DIFF'} | {rss[(n,'old')]:.1f}/{rss[(n,'new')]:.1f} |")
print(f"\nsum of medians: old {tot['old']:.1f} s, new {tot['new']:.1f} s ({tot['old']/tot['new']:.2f}x); digest mismatches: {mism}")
sys.exit(1 if mism else 0)
