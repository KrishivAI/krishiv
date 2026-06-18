"""03 · Group-by: revenue per region."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))
import krishiv as ks
import pyarrow as pa

def B(d):
    return ks.Batch(pa.record_batch(d))

s = ks.Session(); pl = s.pipeline("by_region")
pl.source_memory("orders", [B({"region": ["US","EU","US","EU"], "amount": [100,50,25,75]})])
pl.view("rev", "SELECT region, SUM(amount) AS total FROM orders GROUP BY region", materialized=True)
sink = pl.sink_memory("rev"); pl.mode("ivm"); pl.run("once")
df = sink.collect()[0].to_pandas()
print(f"[03] regions:\n{df}"); assert len(df) == 2
