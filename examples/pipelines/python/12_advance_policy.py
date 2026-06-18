"""12 · Advance policy: step per change (streaming-like)."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))
import krishiv as ks
import pyarrow as pa

def B(d):
    return ks.Batch(pa.record_batch(d))

s = ks.Session(); pl = s.pipeline("stream_like")
pl.source_cdc("events", [(None, B({"amount":[10]})), (None, B({"amount":[20]})), (None, B({"amount":[30]}))])
pl.view("running_total", "SELECT SUM(amount) AS total FROM events", materialized=True)
sink = pl.sink_memory("running_total"); pl.mode("stream"); pl.run("on_change")
total = sink.collect()[0].to_arrow().column("total")[0].as_py()
print(f"[12] running total (on_change) = {total}"); assert total == 60
