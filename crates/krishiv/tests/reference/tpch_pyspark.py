"""Reference PySpark script for TPC-H migration analyzer (R15 S5.2)."""

from krishiv.compat.spark import SparkSession, col, avg, sum, explode

spark = SparkSession.builder.remote("sc://coordinator:7070").appName("tpch").getOrCreate()
df = spark.sql("SELECT l_returnflag FROM lineitem")
df = df.filter(col("l_returnflag") == "N")
df = df.groupBy("l_linestatus").agg(avg("l_quantity"))
df.show()
df.collect()
