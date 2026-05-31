#!/usr/bin/env python3
"""Standard batch SQL dataframe query example in Python."""

import os
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import krishiv as ks

def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = os.path.join(temp_dir, "people.parquet")
        write_people_parquet(parquet_path)

        session = ks.Session.from_env()
        session.register_parquet("people", parquet_path)

        result = (
            session.sql("select city, count(*) as count from people group by city order by city")
            .collect()
        )
        print(result.pretty())

def write_people_parquet(path):
    schema = pa.schema([
        ("id", pa.int64()),
        ("city", pa.string()),
    ])
    table = pa.Table.from_pydict({
        "id": [1, 2, 3],
        "city": ["London", "Paris", "London"],
    }, schema=schema)
    pq.write_table(table, path)

if __name__ == "__main__":
    main()
