"""06 · Expectation DROP: filter rows failing a predicate."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))
import krishiv as ks
import pyarrow as pa

def B(d):
    return ks.Batch(pa.record_batch(d))

s = ks.Session(); pl = s.pipeline("dq")
pl.source_memory("raw", [B({"amount": [10, -5, 20]})])
pl.view("clean", "SELECT amount FROM raw", materialized=True)
pl.expect("clean", "positive", "amount > 0", "drop")
sink = pl.sink_memory("clean"); pl.mode("ivm"); pl.run("once")
n = sum(b.num_rows for b in sink.collect())
print(f"[06] kept {n} of 3 rows"); assert n == 2
