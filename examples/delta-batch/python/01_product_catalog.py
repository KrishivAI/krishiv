"""Example 1: Product Catalog — basic Delta Lake CRUD.

Creates a Delta table with product data, appends more products,
then reads back different versions (time-travel).
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))

import tempfile, shutil
import krishiv as ks
import pyarrow as pa

def main():
    tmpdir = tempfile.mkdtemp(prefix="delta_example_")
    delta_path = os.path.join(tmpdir, "products")
    try:
        session = ks.Session()

        # Version 0: initial product catalog
        schema = pa.schema([
            pa.field("product_id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("category", pa.string()),
            pa.field("price", pa.float64()),
        ])
        batch_v0 = pa.record_batch(
            [[1, 2, 3],
             ["Laptop", "Mouse", "Keyboard"],
             ["Electronics", "Electronics", "Electronics"],
             [999.99, 29.99, 79.99]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(batch_v0)], mode="overwrite")
        print("=== Version 0: Initial catalog (3 products) ===")

        # Version 1: append accessories
        batch_v1 = pa.record_batch(
            [[4, 5],
             ["USB Hub", "Monitor Stand"],
             ["Accessories", "Accessories"],
             [45.00, 129.99]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(batch_v1)], mode="append")
        print("=== Version 1: Appended accessories (+2 products) ===")

        # Version 2: overwrite with updated prices
        batch_v2 = pa.record_batch(
            [[1, 2, 3, 4, 5],
             ["Laptop", "Mouse", "Keyboard", "USB Hub", "Monitor Stand"],
             ["Electronics", "Electronics", "Electronics", "Accessories", "Accessories"],
             [899.99, 24.99, 69.99, 45.00, 129.99]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(batch_v2)], mode="overwrite")
        print("=== Version 2: Overwrite with updated prices ===")

        # Read latest version
        df = ks.read_delta(session, delta_path)
        print("\n--- Latest version (v2) ---")
        print(df.collect_pretty())

        # Time-travel to version 0
        df_v0 = ks.read_delta(session, delta_path, version=0)
        print("\n--- Time-travel to version 0 ---")
        print(df_v0.collect_pretty())

        # Time-travel to version 1
        df_v1 = ks.read_delta(session, delta_path, version=1)
        print("\n--- Time-travel to version 1 ---")
        print(df_v1.collect_pretty())

        print("\nAll product catalog operations completed successfully!")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == "__main__":
    main()
