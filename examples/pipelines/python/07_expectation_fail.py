"""07 · Expectation FAIL: abort on a violation."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))
import krishiv as ks
import pyarrow as pa

def B(d):
    return ks.Batch(pa.record_batch(d))

s = ks.Session(); pl = s.pipeline("dq_fail")
pl.source_memory("raw", [B({"amount": [10, -5]})])
pl.view("clean", "SELECT amount FROM raw", materialized=True)
pl.expect("clean", "positive", "amount > 0", "fail")
pl.sink_memory("clean"); pl.mode("ivm")
try:
    pl.run("once"); print("[07] ERROR: should have failed"); assert False
except Exception as e:
    print(f"[07] failed as expected: {type(e).__name__}")
