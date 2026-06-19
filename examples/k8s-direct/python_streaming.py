import pyarrow as pa, krishiv as ks
session = ks.Session.from_env()
batch = pa.RecordBatch.from_pydict({"val": [1, 2]})
results = session.memory_stream_collect("stream", [ks.Batch(batch)])
print("Direct mode stream processed!")
