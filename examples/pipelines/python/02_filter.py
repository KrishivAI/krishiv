"""02 · Filter: keep large orders only."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))
import krishiv as ks
import pyarrow as pa

def B(d):
    return ks.Batch(pa.record_batch(d))

s = ks.Session(); pl = s.pipeline("filter")
pl.source_memory("orders", [B({"id": [1,2,3], "amount": [100,50,250]})])
pl.view("big", "SELECT id, amount FROM orders WHERE amount >= 100", materialized=True)
sink = pl.sink_memory("big"); pl.mode("ivm"); pl.run("once")
n = sum(b.num_rows for b in sink.collect())
print(f"[02] big orders = {n}"); assert n == 2
