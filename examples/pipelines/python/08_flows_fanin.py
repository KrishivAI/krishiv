"""08 · Fan-in: multiple sources appended into one view (flows)."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))
import krishiv as ks
import pyarrow as pa

def B(d):
    return ks.Batch(pa.record_batch(d))

s = ks.Session(); pl = s.pipeline("fanin")
pl.source_memory("topic_a", [B({"id": [1,2]})])
pl.source_memory("topic_b", [B({"id": [3,4,5]})])
pl.flow("all_events", "SELECT id FROM topic_a")
pl.flow("all_events", "SELECT id FROM topic_b")
sink = pl.sink_memory("all_events"); pl.mode("ivm"); pl.run("once")
n = sum(b.num_rows for b in sink.collect())
print(f"[08] fan-in rows = {n}"); assert n == 5
