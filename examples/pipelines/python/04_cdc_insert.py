"""04 · IVM/CDC: insert change events -> incremental SUM."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))
import krishiv as ks
import pyarrow as pa

def B(d):
    return ks.Batch(pa.record_batch(d))

s = ks.Session(); pl = s.pipeline("cdc")
pl.source_cdc("orders", [(None, B({"amount":[100]})), (None, B({"amount":[50]})), (None, B({"amount":[25]}))])
pl.view("revenue", "SELECT SUM(amount) AS total FROM orders", materialized=True)
sink = pl.sink_memory("revenue"); pl.run("once")
total = sink.collect()[0].to_arrow().column("total")[0].as_py()
print(f"[04] cdc revenue = {total}"); assert total == 175
