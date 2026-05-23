from krishiv.compat.spark import SparkSession, col


def test_dataframe_sql_builder_filter():
    spark = SparkSession(remote=None)
    df = spark.sql("SELECT 1 AS x").filter(col("x") > 0)
    assert "WHERE" in df._query
    assert "x" in df._query


def test_groupby_agg_sql():
    spark = SparkSession(remote=None)
    df = spark.sql("SELECT 1 AS k, 2 AS v").groupBy("k").agg("sum(v)")
    assert "GROUP BY" in df._query
    assert "sum" in df._query
