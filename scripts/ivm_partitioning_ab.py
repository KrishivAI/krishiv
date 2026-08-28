import base64, json, os, statistics, time, urllib.request
import pyarrow as pa
BASE=os.environ["BASE"]; TOK=os.environ["TOK"]; SEED=20000; DELTA=5000; TICKS=3
def req(m,p,b=None):
    r=urllib.request.Request(BASE+p,method=m,data=json.dumps(b).encode() if b is not None else None,
        headers={"Authorization":"Bearer "+TOK,"Content-Type":"application/json"})
    with urllib.request.urlopen(r,timeout=180) as f:
        raw=f.read(); return json.loads(raw) if raw else {}
S=pa.schema([pa.field("region",pa.string()),pa.field("amount",pa.int64())])
def ipc(start,n):
    a=[pa.array([f"r{(start+i)%8}" for i in range(n)]),pa.array([(start+i)%1000 for i in range(n)]),pa.array([1]*n,pa.int64())]
    fs=pa.schema(list(S)+[pa.field("_weight",pa.int64())]); sink=pa.BufferOutputStream()
    with pa.ipc.new_stream(sink,fs) as w: w.write(pa.record_batch(a,schema=fs))
    return base64.b64encode(sink.getvalue().to_pybytes()).decode()

SQL="SELECT region, SUM(amount) AS total FROM orders GROUP BY region"
FIELDS=[{"name":"region","data_type":"Utf8","nullable":False},{"name":"total","data_type":"Int64","nullable":True}]
print(f"\nA/B: identical shardable query, partitioning forced on vs off\n")
print(f"{'partitioned=':<16}{'reported':>10}{'median tick':>16}")
print("-"*44)
for flag in (True, False):
    job=f"ab-{flag}"
    try: req("DELETE",f"/api/v1/ivm/jobs/{job}")
    except Exception: pass
    resp=req("POST","/api/v1/ivm/jobs",{"job_id":job,"partitioned":flag})
    reported=resp.get("partitioned","?")
    req("POST",f"/api/v1/ivm/jobs/{job}/views",{"name":"v","body_sql":SQL,
        "output_schema":{"fields":FIELDS},"is_materialized":True})
    off=0
    req("POST",f"/api/v1/ivm/jobs/{job}/sources/orders/feed",{"delta_ipc_b64":ipc(off,SEED)}); off+=SEED
    req("POST",f"/api/v1/ivm/jobs/{job}/step")
    s=[]
    for _ in range(TICKS):
        req("POST",f"/api/v1/ivm/jobs/{job}/sources/orders/feed",{"delta_ipc_b64":ipc(off,DELTA)}); off+=DELTA
        t=time.perf_counter(); req("POST",f"/api/v1/ivm/jobs/{job}/step"); s.append(time.perf_counter()-t)
    print(f"{str(flag):<16}{str(reported):>10}{statistics.median(s)*1e3:>13.2f}ms")
    req("DELETE",f"/api/v1/ivm/jobs/{job}")
