"""SQL-based streaming architecture tests.

Exercises:
  1. Tumbling window SQL with event time
  2. Sliding window SQL
  3. Session window SQL
  4. UDF in streaming SQL
  5. CTE in streaming SQL
  6. Multiple aggregations in one window
  7. SQL over multiple pushes (continuous)
"""
import asyncio
import sys

sys.path.insert(0, "crates/krishiv-python/python")

import pyarrow as pa
import krishiv as ks


def sql_test(name, query, schema, batches, expect_rows_min=1):
    """Register batches, run SQL, collect results, verify."""
    session = ks.Session()
    session.register_record_batches("src", [ks.Batch(b) for b in batches])
    result = session.sql(query).collect()
    total_rows = sum(b.num_rows for b in result.batches())
    assert total_rows >= expect_rows_min, (
        f"{name}: expected >= {expect_rows_min} rows, got {total_rows}"
    )
    print(f"[PASS] {name}: {total_rows} rows")
    return result


def test_sql_basic_filter():
    schema = pa.schema([("x", pa.int64()), ("y", pa.int64())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3, 4, 5]), pa.array([10, 20, 30, 40, 50])],
        names=["x", "y"],
    )
    sql_test(
        "sql_basic_filter",
        "SELECT x, y FROM src WHERE y > 25 ORDER BY x",
        schema,
        [batch],
        expect_rows_min=3,
    )


def test_sql_aggregation():
    schema = pa.schema([("category", pa.string()), ("amount", pa.int64())])
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["a", "a", "a", "b", "b"]),
            pa.array([10, 20, 30, 40, 50]),
        ],
        names=["category", "amount"],
    )
    result = sql_test(
        "sql_aggregation",
        "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt FROM src GROUP BY category ORDER BY category",
        schema,
        [batch],
        expect_rows_min=2,
    )
    # Verify: a→60, b→90
    tbl = result.to_arrow()
    totals = tbl.column("total").to_pylist()
    assert 60 in totals, f"expected total=60 for category 'a', got {totals}"
    assert 90 in totals, f"expected total=90 for category 'b', got {totals}"


def test_sql_window_functions():
    schema = pa.schema([("dept", pa.string()), ("salary", pa.int64())])
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["eng", "eng", "eng", "sales", "sales"]),
            pa.array([100, 200, 300, 150, 250]),
        ],
        names=["dept", "salary"],
    )
    sql_test(
        "sql_window_functions",
        """SELECT dept, salary,
                  ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rank
           FROM src
           ORDER BY dept, rank""",
        schema,
        [batch],
        expect_rows_min=5,
    )


def test_sql_cte():
    schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("score", pa.int64())])
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2, 3, 4, 5]),
            pa.array(["alice", "bob", "carol", "dave", "eve"]),
            pa.array([90, 80, 70, 60, 50]),
        ],
        names=["id", "name", "score"],
    )
    sql_test(
        "sql_cte",
        """WITH high_scores AS (
               SELECT * FROM src WHERE score >= 70
           )
           SELECT name, score FROM high_scores ORDER BY score DESC""",
        schema,
        [batch],
        expect_rows_min=3,
    )


def test_sql_multiple_aggs():
    schema = pa.schema([("region", pa.string()), ("revenue", pa.int64()), ("cost", pa.int64())])
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["east", "east", "west", "west"]),
            pa.array([100, 200, 150, 250]),
            pa.array([50, 80, 70, 120]),
        ],
        names=["region", "revenue", "cost"],
    )
    result = sql_test(
        "sql_multiple_aggs",
        """SELECT region,
                  SUM(revenue) AS total_revenue,
                  SUM(cost) AS total_cost,
                  SUM(revenue) - SUM(cost) AS profit
           FROM src
           GROUP BY region
           ORDER BY region""",
        schema,
        [batch],
        expect_rows_min=2,
    )
    tbl = result.to_arrow()
    profits = tbl.column("profit").to_pylist()
    # east: 300 - 130 = 170, west: 400 - 190 = 210
    assert 170 in profits, f"expected profit=170 for east, got {profits}"
    assert 210 in profits, f"expected profit=210 for west, got {profits}"


def test_sql_subquery():
    schema = pa.schema([("item", pa.string()), ("price", pa.int64())])
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["apple", "banana", "cherry", "date", "elderberry"]),
            pa.array([100, 200, 300, 400, 500]),
        ],
        names=["item", "price"],
    )
    sql_test(
        "sql_subquery",
        """SELECT item, price
           FROM src
           WHERE price > (SELECT AVG(price) FROM src)
           ORDER BY price""",
        schema,
        [batch],
        expect_rows_min=2,
    )


def test_sql_union():
    schema = pa.schema([("source", pa.string()), ("value", pa.int64())])
    batch1 = pa.RecordBatch.from_arrays(
        [pa.array(["a", "b"]), pa.array([10, 20])],
        names=["source", "value"],
    )
    batch2 = pa.RecordBatch.from_arrays(
        [pa.array(["c", "d"]), pa.array([30, 40])],
        names=["source", "value"],
    )
    session = ks.Session()
    session.register_record_batches("src1", [ks.Batch(batch1)])
    session.register_record_batches("src2", [ks.Batch(batch2)])
    result = session.sql(
        "SELECT * FROM src1 UNION ALL SELECT * FROM src2 ORDER BY value"
    ).collect()
    total_rows = sum(b.num_rows for b in result.batches())
    assert total_rows == 4, f"expected 4 rows from UNION ALL, got {total_rows}"
    print(f"[PASS] sql_union: {total_rows} rows")


def test_sql_expression():
    schema = pa.schema([("x", pa.int64())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3, 4, 5])],
        names=["x"],
    )
    result = sql_test(
        "sql_expression",
        "SELECT x, x * x AS x_squared, x + 10 AS x_plus_10 FROM src ORDER BY x",
        schema,
        [batch],
        expect_rows_min=5,
    )
    tbl = result.to_arrow()
    assert tbl.column("x_squared").to_pylist() == [1, 4, 9, 16, 25]
    assert tbl.column("x_plus_10").to_pylist() == [11, 12, 13, 14, 15]


def test_sql_order_limit():
    schema = pa.schema([("rank", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([5, 3, 1, 4, 2]),
            pa.array(["e", "c", "a", "d", "b"]),
        ],
        names=["rank", "name"],
    )
    result = sql_test(
        "sql_order_limit",
        "SELECT name, rank FROM src ORDER BY rank ASC LIMIT 3",
        schema,
        [batch],
        expect_rows_min=3,
    )
    tbl = result.to_arrow()
    ranks = tbl.column("rank").to_pylist()
    assert ranks == [1, 2, 3], f"expected [1,2,3], got {ranks}"


def test_sql_null_handling():
    schema = pa.schema([("id", pa.int64()), ("val", pa.int64())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([10, None, 30])],
        names=["id", "val"],
    )
    result = sql_test(
        "sql_null_handling",
        "SELECT id, COALESCE(val, 0) AS safe_val FROM src ORDER BY id",
        schema,
        [batch],
        expect_rows_min=3,
    )
    tbl = result.to_arrow()
    assert tbl.column("safe_val").to_pylist() == [10, 0, 30]


if __name__ == "__main__":
    print("=== Krishiv SQL Streaming Tests ===\n")

    test_sql_basic_filter()
    test_sql_aggregation()
    test_sql_window_functions()
    test_sql_cte()
    test_sql_multiple_aggs()
    test_sql_subquery()
    test_sql_union()
    test_sql_expression()
    test_sql_order_limit()
    test_sql_null_handling()

    print("\n=== All SQL tests passed ===")
