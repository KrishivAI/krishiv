"""11 · Persistent incremental runs + full refresh."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))
import krishiv as ks
import pyarrow as pa

def B(d):
    return ks.Batch(pa.record_batch(d))

s = ks.Session()
def run(vals, refresh=False):
    pl = s.pipeline("acc")
    pl.source_memory("raw", [B({"amount": vals})])
    pl.view("total", "SELECT SUM(amount) AS s FROM raw", materialized=True)
    sink = pl.sink_memory("total"); pl.mode("ivm")
    (pl.refresh if refresh else pl.run)("once")
    return sink.collect()[0].to_arrow().column("s")[0].as_py()
a = run([10]); b = run([5]); c = run([100], refresh=True)
print(f"[11] run1={a} run2(incremental)={b} run3(refresh)={c}")
assert (a, b, c) == (10, 15, 100)
