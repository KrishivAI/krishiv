"""09 · Temporary view: intermediate transform."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))
import krishiv as ks
import pyarrow as pa

def B(d):
    return ks.Batch(pa.record_batch(d))

s = ks.Session(); pl = s.pipeline("tv")
pl.source_memory("raw", [B({"amount": [100, 50, 300, 20]})])
pl.temp_view("big", "SELECT amount FROM raw WHERE amount >= 100")
pl.view("big_count", "SELECT COUNT(*) AS n FROM big", materialized=True)
sink = pl.sink_memory("big_count"); pl.mode("ivm"); pl.run("once")
n = sink.collect()[0].to_arrow().column("n")[0].as_py()
print(f"[09] big via temp view = {n}"); assert n == 2
