from krishiv import Session, Schema
import pyarrow as pa
session = Session()
schema = pa.schema([("user_id", pa.int64()), ("amount", pa.int64())])
session.register_unbounded("events", schema)
df = session.sql("SELECT user_id, amount * 2 AS double_amount FROM events")
print(df.explain())
