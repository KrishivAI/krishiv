import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

df = pd.DataFrame({
    'payer': ['A', 'A', 'B', 'B', 'C'],
    'diagnosis_group': ['G1', 'G2', 'G1', 'G3', 'G1'],
    'service_year': [2026, 2026, 2026, 2025, 2026],
    'allowed_amount_cents': [10000, 20000, 15000, 30000, 25000]
})
table = pa.Table.from_pandas(df)
pq.write_table(table, 'claims.parquet')
