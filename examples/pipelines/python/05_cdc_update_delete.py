"""05 · IVM/CDC: update + delete with Z-set retraction."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))
import krishiv as ks
import pyarrow as pa

def B(d):
    return ks.Batch(pa.record_batch(d))

s = ks.Session(); pl = s.pipeline("cdc_mut")
def row(i, a): return B({"id":[i], "amount":[a]})
pl.source_cdc("orders", [
    (None, row(1,100)), (None, row(2,50)),
    (row(2,50), row(2,200)),   # update
    (row(1,100), None),        # delete
])
pl.view("revenue", "SELECT SUM(amount) AS total FROM orders", materialized=True)
sink = pl.sink_memory("revenue"); pl.run("once")
total = sink.collect()[0].to_pandas()["total"][0]
print(f"[05] revenue after update+delete = {total}"); assert total == 200
