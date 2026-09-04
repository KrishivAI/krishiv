#!/usr/bin/env python3
"""Run all 99 official TPC-DS queries through the krishiv CLI and verify
each answer against DuckDB over the identical Parquet dataset."""
import json, os, subprocess, sys, time
from decimal import Decimal
from pathlib import Path

S = "/tmp/claude-1000/-home-gopal-Desktop-code/7be33609-c075-4cdc-86ef-d43b7116eed2/scratchpad"
ROOT = "/home/gopal/Desktop/code/krishiv"
DATA = Path(ROOT) / "target" / "tpcds-sf1"
BIN = Path(ROOT) / "target" / "release" / "krishiv"
TIMEOUT = int(os.environ.get("Q_TIMEOUT", "300"))

queries = json.load(open(f"{S}/tpcds-queries.json"))
tables = sorted(p.stem for p in DATA.glob("*.parquet"))
pq_args = []
for t in tables:
    pq_args += ["--parquet", f"{t}={DATA}/{t}.parquet"]

import duckdb
oracle = duckdb.connect()
for t in tables:
    oracle.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{DATA}/{t}.parquet')")

def canon(v):
    if v is None: return None
    if isinstance(v, bool): return v
    if isinstance(v, (int,)): return float(v)
    if isinstance(v, (float, Decimal)): return round(float(v), 2)
    return str(v).strip()

def canon_rows(rows):
    return sorted([tuple(canon(c) for c in r) for r in rows], key=lambda r: [(x is None, str(x)) for x in r])

results = []
for q in queries:
    nr, sql = q["nr"], q["sql"]
    rec = {"query": nr}
    # oracle
    try:
        t0 = time.time()
        ref = oracle.execute(sql).fetchall()
        rec["duckdb_ms"] = round((time.time()-t0)*1000, 1)
        rec["duckdb_rows"] = len(ref)
    except Exception as e:
        ref = None
        rec["duckdb_error"] = str(e).split("\n")[0][:200]
    # engine
    cmd = [str(BIN), "sql", "--local", "--format", "json", "--timeout", str(TIMEOUT), *pq_args, "-q", sql]
    t0 = time.time()
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT + 30)
        rec["krishiv_ms"] = round((time.time()-t0)*1000, 1)
        rec["rc"] = p.returncode
        if p.returncode != 0:
            rec["status"] = "error"
            err = (p.stderr or p.stdout).strip().split("\n")
            rec["error"] = (err[-1] if err else "")[:300]
        else:
            got = [json.loads(l) for l in p.stdout.splitlines() if l.strip().startswith("{")]
            rec["krishiv_rows"] = len(got)
            if ref is None:
                rec["status"] = "ran_no_oracle"
            else:
                a = canon_rows([list(r.values()) for r in got])
                b = canon_rows([list(r) for r in ref])
                rec["status"] = "match" if a == b else "mismatch"
                if a != b:
                    rec["detail"] = f"rows {len(a)} vs {len(b)}"
    except subprocess.TimeoutExpired:
        rec["status"] = "timeout"
        rec["krishiv_ms"] = TIMEOUT * 1000
    results.append(rec)
    print(json.dumps(rec), flush=True)

json.dump(results, open(f"{S}/ds99/results.json", "w"), indent=1)
from collections import Counter
c = Counter(r["status"] for r in results)
print("SUMMARY " + json.dumps(dict(c)), flush=True)
