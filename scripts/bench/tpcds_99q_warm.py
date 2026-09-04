#!/usr/bin/env python3
"""Warm-cache, best-of-3 TPC-DS SF1 run: krishiv baseline, krishiv with
parquet pushdown_filters, and DuckDB, all over the same Parquet files."""
import json,subprocess,time,os
S="/tmp/claude-1000/-home-gopal-Desktop-code/7be33609-c075-4cdc-86ef-d43b7116eed2/scratchpad"
ROOT="/home/gopal/Desktop/code/krishiv"; D=f"{ROOT}/target/tpcds-sf1"
BIN=f"{ROOT}/target/release/krishiv"
tables=sorted(f[:-8] for f in os.listdir(D) if f.endswith(".parquet"))
pq=[]
for t in tables: pq += ["--parquet", f"{t}={D}/{t}.parquet"]
qs=json.load(open(f"{S}/tpcds-queries.json"))
import duckdb
con=duckdb.connect()
for t in tables: con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{D}/{t}.parquet')")
PUSH="SET datafusion.execution.parquet.pushdown_filters = true; "
def kr(sql,reps):
    best=None
    for _ in range(reps):
        t0=time.time()
        p=subprocess.run([BIN,"sql","--local","--format","json","--timeout","600",*pq,"-q",sql],
                         capture_output=True,text=True,timeout=700)
        ms=(time.time()-t0)*1000
        if p.returncode!=0: return None,(p.stderr or "").strip()[-200:]
        best=ms if best is None else min(best,ms)
    return best,None
def dk(sql,reps):
    best=None
    for _ in range(reps):
        t0=time.time(); con.execute(sql).fetchall(); ms=(time.time()-t0)*1000
        best=ms if best is None else min(best,ms)
    return best
out=[]
for q in qs:
    nr,sql=q["nr"],q["sql"]
    kr(sql,1); dk(sql,1)                     # warm
    b,e1=kr(sql,3)
    p,e2=kr(PUSH+sql,3)
    d=dk(sql,3)
    rec={"query":nr,"krishiv_ms":b,"krishiv_pushdown_ms":p,"duckdb_ms":round(d,1),
         "err":e1 or e2}
    if b: rec["krishiv_ms"]=round(b,1)
    if p: rec["krishiv_pushdown_ms"]=round(p,1)
    out.append(rec); print(json.dumps(rec),flush=True)
json.dump(out,open(f"{S}/ds99/results-warm.json","w"),indent=1)
print("DONE")
