#!/usr/bin/env python3
"""Distributed IVM corpus benchmark: drives views through the coordinator's HTTP API.

Reports its OWN axis. Every tick here carries Arrow-IPC-over-base64 encode/decode
plus request latency, so this measures the DISPATCH path, not incremental
maintenance. It must not be set against the in-process single-node table.
"""
import base64, io, json, os, statistics, sys, time, urllib.request
import pyarrow as pa

BASE = os.environ["BASE"]; TOK = os.environ["TOK"]
SEED = int(os.environ.get("SEED", "20000")); DELTA = int(os.environ.get("DELTA", "5000"))
TICKS = int(os.environ.get("TICKS", "3"))

def req(method, path, body=None):
    r = urllib.request.Request(BASE + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "Bearer " + TOK, "Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=120) as f:
        raw = f.read()
        return json.loads(raw) if raw else {}

def ipc_b64(schema, cols, n):
    arrays = [pa.array(c) for c in cols] + [pa.array([1] * n, pa.int64())]
    full = pa.schema(list(schema) + [pa.field("_weight", pa.int64())])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, full) as w:
        w.write(pa.record_batch(arrays, schema=full))
    return base64.b64encode(sink.getvalue().to_pybytes()).decode()

# One source shape, three query shapes covering the plan classes that matter:
# a global aggregate, a grouped aggregate, and a filtered projection (stateless).
SCHEMA = pa.schema([pa.field("region", pa.string()), pa.field("amount", pa.int64())])
QUERIES = [
    ("global_sum", "SELECT SUM(amount) AS total FROM orders",
     [{"name": "total", "data_type": "Int64", "nullable": True}]),
    ("grouped_sum", "SELECT region, SUM(amount) AS total FROM orders GROUP BY region",
     [{"name": "region", "data_type": "Utf8", "nullable": False},
      {"name": "total", "data_type": "Int64", "nullable": True}]),
    ("filtered", "SELECT region, amount FROM orders WHERE amount > 10",
     [{"name": "region", "data_type": "Utf8", "nullable": False},
      {"name": "amount", "data_type": "Int64", "nullable": False}]),
]

def rows(start, n):
    return [[f"r{(start + i) % 8}" for i in range(n)], [(start + i) % 1000 for i in range(n)]]

print(f"\nDistributed IVM corpus tick — seed {SEED}, delta {DELTA}, median of {TICKS} ticks")
print(f"coordinator: {BASE}\n")
print(f"{'query':<16}{'plan':>8}{'tick (dispatch+compute)':>26}{'rows out':>12}")
print("-" * 64)

for name, sql, fields in QUERIES:
    job = f"bench-{name}"
    try: req("DELETE", f"/api/v1/ivm/jobs/{job}")
    except Exception: pass
    req("POST", "/api/v1/ivm/jobs", {"job_id": job})
    req("POST", f"/api/v1/ivm/jobs/{job}/views", {
        "name": "v", "body_sql": sql,
        "output_schema": {"fields": fields}, "is_materialized": True})
    off = 0
    req("POST", f"/api/v1/ivm/jobs/{job}/sources/orders/feed",
        {"delta_ipc_b64": ipc_b64(SCHEMA, rows(off, SEED), SEED)})
    off += SEED
    req("POST", f"/api/v1/ivm/jobs/{job}/step")

    samples, out_rows = [], 0
    for _ in range(TICKS):
        req("POST", f"/api/v1/ivm/jobs/{job}/sources/orders/feed",
            {"delta_ipc_b64": ipc_b64(SCHEMA, rows(off, DELTA), DELTA)})
        off += DELTA
        t0 = time.perf_counter()
        s = req("POST", f"/api/v1/ivm/jobs/{job}/step")
        samples.append(time.perf_counter() - t0)
        out_rows = s.get("total_output_rows", out_rows)
    med = statistics.median(samples) * 1e3
    print(f"{name:<16}{'incr':>8}{med:>23.2f}ms{out_rows:>12}")
    req("DELETE", f"/api/v1/ivm/jobs/{job}")

print("-" * 64)
print("NOTE: this axis includes Arrow-IPC-base64 encode/decode and HTTP round-trip")
print("per tick. It measures the DISPATCH path, not incremental maintenance, and")
print("is NOT comparable to the in-process single-node numbers.")
