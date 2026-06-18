"""10 · Dry-run validation: catch a bad pipeline before running."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))
import krishiv as ks
import pyarrow as pa

def B(d):
    return ks.Batch(pa.record_batch(d))

s = ks.Session()
good = s.pipeline("good")
good.source_memory("raw", [B({"amount":[1,2,3]})])
good.view("total", "SELECT SUM(amount) AS s FROM raw", materialized=True)
good.sink_memory("total"); good.mode("ivm")
good.validate(); print("[10] well-formed pipeline: VALID")
bad = s.pipeline("bad")
bad.source_memory("raw", [B({"amount":[1]})])
bad.view("total", "SELECT SUM(amount) AS s FROM raw", materialized=True)
bad.sink_memory("missing"); bad.mode("ivm")
try:
    bad.validate(); print("[10] ERROR: bad pipeline validated"); assert False
except Exception as e:
    print(f"[10] bad pipeline rejected: {type(e).__name__}")
