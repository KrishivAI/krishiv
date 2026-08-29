#!/usr/bin/env python3
"""Distributed delta-batch vs batch (recompute) A/B, per query, on the cluster.

Both arms run the SAME ROUTE: `partitioned=false` pins each job to a single
flow, so both dispatch to an executor as a resident flow. Only the MODE varies
— the recompute arm sets `force_diff_based`, which IVM-AUD-DIST-4 added to the
attach wire. This is deliberate: the earlier `partitioned` A/B varied the route
(central vs dispatched) and its 28.5x was retracted for exactly that reason
(register §68). Vary one thing.

Reports its OWN axis. Every tick carries Arrow-IPC-over-base64 encode/decode
plus HTTP round-trip, so this measures the DISPATCH path. It is NOT comparable
to the in-process single-node corpus table.
"""
import base64, json, os, statistics, time, urllib.request
import pyarrow as pa

BASE = os.environ["BASE"]; TOK = os.environ["TOK"]
SEED = int(os.environ.get("SEED", "20000")); DELTA = int(os.environ.get("DELTA", "5000"))
TICKS = int(os.environ.get("TICKS", "3"))

def req(method, path, body=None):
    r = urllib.request.Request(BASE + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "Bearer " + TOK, "Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=600) as f:
        raw = f.read()
        return json.loads(raw) if raw else {}

def ipc_b64(schema, cols, n):
    arrays = [pa.array(c) for c in cols] + [pa.array([1] * n, pa.int64())]
    full = pa.schema(list(schema) + [pa.field("_weight", pa.int64())])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, full) as w:
        w.write(pa.record_batch(arrays, schema=full))
    return base64.b64encode(sink.getvalue().to_pybytes()).decode()

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

def run(name, sql, fields, force_diff_based):
    job = f"modeab-{name}-{'batch' if force_diff_based else 'delta'}"
    try: req("DELETE", f"/api/v1/ivm/jobs/{job}")
    except Exception: pass
    # partitioned=False on BOTH arms: same route, only the mode differs.
    req("POST", "/api/v1/ivm/jobs",
        {"job_id": job, "partitioned": False, "force_diff_based": force_diff_based})
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
        # IVM-AUD-DIST-5: compare the LOGICAL multiset, not the physical
        # encoding. `total_output_rows` counts physical delta rows, and the
        # incremental arm consolidates duplicates into weighted rows while a
        # full recompute does not — so this harness previously reported the
        # `filtered` view's arms disagreeing 989 vs 4945 (a clean 5x) when both
        # held the same Z-set. `total_inserted_rows` sums positive weights and
        # is invariant across encodings.
        out_rows = s.get("total_inserted_rows", out_rows)
    req("DELETE", f"/api/v1/ivm/jobs/{job}")
    return statistics.median(samples) * 1e3, out_rows

print(f"\nDistributed delta-batch vs batch — seed {SEED}, delta {DELTA}, median of {TICKS} ticks")
print(f"coordinator: {BASE}   (both arms partitioned=false: SAME route, mode varies)\n")
print(f"{'query':<14}{'delta-batch':>14}{'batch':>12}{'speedup':>10}{'rows out':>11}{'agree':>8}")
print("-" * 69)
for name, sql, fields in QUERIES:
    try:
        d_ms, d_rows = run(name, sql, fields, False)
        b_ms, b_rows = run(name, sql, fields, True)
        agree = "yes" if d_rows == b_rows else f"NO {d_rows}/{b_rows}"
        print(f"{name:<14}{d_ms:>12.2f}ms{b_ms:>10.2f}ms{b_ms/d_ms:>9.2f}x{d_rows:>11}{agree:>8}")
    except Exception as e:
        print(f"{name:<14}{'ERROR':>12}  {e}")
print("-" * 69)
print("Own axis: includes Arrow-IPC-base64 encode/decode + HTTP per tick. Measures")
print("the DISPATCH path, not incremental maintenance. NOT comparable to the")
print("in-process single-node corpus numbers.")
print("'agree' compares LOGICAL inserted-row counts (sum of positive Z-set")
print("weights) across arms; a NO means the two arms computed different answers")
print("and the speedup on that row is meaningless. It deliberately does NOT")
print("compare total_output_rows: that counts physical delta rows, and the")
print("incremental arm consolidates duplicates while a recompute does not, so")
print("identical answers can differ there by the duplicate factor.")
