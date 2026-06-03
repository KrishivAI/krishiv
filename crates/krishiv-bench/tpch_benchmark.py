import time
import os
import argparse
from pyspark.sql import SparkSession
import krishiv as ks

Q1 = """
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM
    lineitem
WHERE
    l_shipdate <= CAST('1998-09-02' AS DATE)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
"""

Q3 = """
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS DATE)
    AND l_shipdate > CAST('1995-03-15' AS DATE)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue desc,
    o_orderdate
LIMIT 10;
"""

Q6 = """
SELECT
    sum(l_extendedprice * l_discount) as revenue
FROM
    lineitem
WHERE
    l_shipdate >= CAST('1994-01-01' AS DATE)
    AND l_shipdate < CAST('1995-01-01' AS DATE)
    AND l_discount between 0.05 and 0.07
    AND l_quantity < 24;
"""

Q12 = """
SELECT
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) as low_line_count
FROM
    orders,
    lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode in ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= CAST('1994-01-01' AS DATE)
    AND l_receiptdate < CAST('1995-01-01' AS DATE)
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;
"""

Q14 = """
SELECT
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= CAST('1995-09-01' AS DATE)
    AND l_shipdate < CAST('1995-10-01' AS DATE);
"""

QUERIES = {"Q1": Q1, "Q3": Q3, "Q6": Q6, "Q12": Q12, "Q14": Q14}
TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

def run_spark(data_path: str):
    print("\n--- Running PySpark Benchmark ---")
    spark = SparkSession.builder \
        .appName("TPCH-Benchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    for table in TABLES:
        df = spark.read.parquet(f"{data_path}/{table}.parquet")
        df.createOrReplaceTempView(table)
        
    for name, query in QUERIES.items():
        print(f"Executing {name}...")
        start = time.time()
        spark.sql(query).collect()
        end = time.time()
        print(f"PySpark {name} Time: {end - start:.4f} seconds")
        
    spark.stop()

def run_krishiv(data_path: str):
    print("\n--- Running Krishiv Benchmark ---")
    session = ks.Session()
    
    for table in TABLES:
        session.register_parquet(table, f"{data_path}/{table}.parquet")
        
    for name, query in QUERIES.items():
        print(f"Executing {name}...")
        start = time.time()
        session.sql(query).collect()
        end = time.time()
        print(f"Krishiv {name} Time: {end - start:.4f} seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TPC-H Benchmark Harness")
    parser.add_argument("--data-path", type=str, required=True, help="Path to TPC-H parquet files")
    args = parser.parse_args()
    
    run_krishiv(args.data_path)
    run_spark(args.data_path)
