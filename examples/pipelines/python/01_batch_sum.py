"""01 · Batch: in-memory source -> SUM view -> sink."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))
import krishiv as ks
import pyarrow as pa

def B(d):
    return ks.Batch(pa.record_batch(d))

s = ks.Session(); pl = s.pipeline("batch_sum")
pl.source_memory("orders", [B({"amount": [100, 50, 25, 75]})])
pl.view("revenue", "SELECT SUM(amount) AS total FROM orders", materialized=True)
sink = pl.sink_memory("revenue"); pl.mode("ivm"); pl.run("once")
total = sink.collect()[0].to_pandas()["total"][0]
print(f"[01] total revenue = {total}"); assert total == 250
