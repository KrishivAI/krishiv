"""Example 6: ETL Pipeline — Delta table as staging area in an ETL workflow.

Simulates an ETL pipeline that:
1. Loads raw data into a Delta staging table
2. Transforms and cleans the data
3. Writes the cleaned result to a final Delta table
4. Validates the pipeline with SQL queries
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))

import tempfile, shutil
import krishiv as ks
import pyarrow as pa

def main():
    tmpdir = tempfile.mkdtemp(prefix="delta_etl_")
    raw_path = os.path.join(tmpdir, "raw_orders")
    clean_path = os.path.join(tmpdir, "clean_orders")
    try:
        session = ks.Session()

        raw_schema = pa.schema([
            pa.field("order_id", pa.string()),
            pa.field("customer", pa.string()),
            pa.field("item", pa.string()),
            pa.field("qty_str", pa.string()),
            pa.field("price_str", pa.string()),
            pa.field("date", pa.string()),
        ])

        # Stage 1: Raw data (messy, needs cleaning)
        raw = pa.record_batch(
            [["ORD-001", "ORD-002", "ORD-003", "ORD-004", "ORD-005"],
             ["Alice", "BOB", "carol", "Dave ", "Eve"],
             ["Widget", "Gadget", "widget", "GADGET", "Widget Pro"],
             ["5", "2", "abc", "3", "1"],
             ["29.99", "49.99", "19.99", "NOT_A_NUMBER", "99.99"],
             ["2025-01-15", "2025-01-15", "2025-01-16", "2025-01-16", "2025-01-17"]],
            schema=raw_schema,
        )
        ks.write_delta(raw_path, [ks.Batch(raw)], mode="overwrite")
        print("Stage 1: Raw data loaded (5 records, some dirty)")

        # Register raw table and transform
        raw_arrow = ks.read_delta(session, raw_path).collect().to_arrow()
        session.register_record_batches("raw_orders", [
            ks.Batch(b) for b in raw_arrow.to_batches()
        ])

        # Clean and transform
        clean_df = session.sql("""
            SELECT
                order_id,
                TRIM(LOWER(customer)) as customer,
                LOWER(item) as item,
                CAST(qty_str AS BIGINT) as quantity,
                CAST(price_str AS DOUBLE) as unit_price,
                date as order_date
            FROM raw_orders
            WHERE qty_str ~ '^[0-9]+$'
              AND price_str ~ '^[0-9]+\\.?[0-9]*$'
        """)
        clean_result = clean_df.collect().to_arrow()
        clean_batches = [ks.Batch(b) for b in clean_result.to_batches()]

        # Stage 2: Write cleaned data
        ks.write_delta(clean_path, clean_batches, mode="overwrite")
        print("Stage 2: Cleaned data written")

        # Validate
        session.register_record_batches("clean_orders", [
            ks.Batch(b) for b in clean_result.to_batches()
        ])
        validation = session.sql("SELECT COUNT(*) as valid_rows FROM clean_orders")
        print("\n--- Validation ---")
        print(validation.collect_pretty())

        # rejected records
        rejected = session.sql("""
            SELECT order_id, customer, qty_str, price_str
            FROM raw_orders
            WHERE qty_str !~ '^[0-9]+$' OR price_str !~ '^[0-9]+\\.?[0-9]*$'
        """)
        print("\n--- Rejected Records ---")
        print(rejected.collect_pretty())

        print("\nETL pipeline example completed successfully!")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == "__main__":
    main()
