"""Parity coverage for the PySpark-shaped surface grafted onto Krishiv.

These tests exercise the additive PySpark-compatibility layer: native
``Column`` predicates/operators, ``F.when`` and the expanded ``F.*`` function
set, the ``DataFrame`` camelCase/convenience methods, ``Session``
``createDataFrame``/``range``/``catalog``/``read``, the ``Row`` helper, and the
``krishiv.types`` module. Krishiv's own richer APIs (lazy ``QueryResult``,
snake_case methods) remain and are covered elsewhere; nothing here replaces them.
"""

import pytest

import krishiv as ks
from krishiv import Row, Session, col, lit, when
from krishiv import functions as F
from krishiv import types as T


@pytest.fixture
def session():
    return Session.embedded()


@pytest.fixture
def df(session):
    return session.createDataFrame(
        [(1, "alice", 10.0), (2, "bob", 20.0), (3, "carol", 30.0), (1, "alice", 40.0)],
        ["id", "name", "amount"],
    )


# ── Column predicates & operators (native) ───────────────────────────────────


def test_column_predicates_render_sql():
    c = col("x")
    assert c.between(1, 5).sql() == '("x" BETWEEN 1 AND 5)'
    assert c.isin(1, 2, 3).sql() == '("x" IN (1, 2, 3))'
    assert c.isin([1, 2, 3]).sql() == '("x" IN (1, 2, 3))'
    assert c.like("a%").sql() == '("x" LIKE \'a%\')'
    assert c.ilike("A%").sql() == '("x" ILIKE \'A%\')'
    assert c.isNull().sql() == '("x" IS NULL)'
    assert c.isNotNull().sql() == '("x" IS NOT NULL)'
    assert "IS NOT DISTINCT FROM" in c.eqNullSafe(lit(1)).sql()


def test_column_operators_render_sql():
    c = col("x")
    assert (~(c > lit(1))).sql() == '(NOT ("x" > 1))'
    assert (c % lit(3)).sql() == '("x" % 3)'
    assert (-c).sql() == '(- "x")'
    assert (c ** lit(2)).sql() == 'power("x", 2)'
    assert (1 + c).sql() == '(1 + "x")'
    assert (10 - c).sql() == '(10 - "x")'
    assert (2 * c).sql() == '(2 * "x")'


def test_column_predicates_execute(df):
    assert df.filter(col("id").between(2, 3)).count() == 2
    assert df.filter(col("name").isin(["alice", "bob"])).count() == 3
    assert df.filter(col("name").like("a%")).count() == 2
    assert df.filter(col("name").startswith("a")).count() == 2
    assert df.filter(col("name").contains("li")).count() == 2
    assert df.filter(col("name").substr(1, 1) == lit("b")).count() == 1


# ── when / otherwise ─────────────────────────────────────────────────────────


def test_when_otherwise(df):
    labelled = df.select(
        col("id"),
        when(col("amount") < lit(15), "low")
        .when(col("amount") < lit(35), "mid")
        .otherwise("high")
        .alias("bucket"),
    )
    buckets = sorted(r["bucket"] for r in labelled.collect_rows())
    assert buckets == ["high", "low", "mid", "mid"]


def test_when_value_is_literal_not_column(df):
    # PySpark: a bare string value is a literal, not a column reference.
    out = df.select(when(col("id") == lit(1), "one").otherwise("other").alias("w"))
    assert sorted({r["w"] for r in out.collect_rows()}) == ["one", "other"]


# ── F.* functions ────────────────────────────────────────────────────────────


def test_scalar_functions(df):
    out = df.select(
        F.upper("name").alias("u"),
        F.length("name").alias("n"),
        F.round(col("amount") / lit(3.0), 2).alias("r"),
        F.concat_ws("-", "name", F.cast(col("id"), "string")).alias("c"),
    ).collect_rows()
    assert out[0]["u"] == "ALICE"
    assert out[0]["n"] == 5


def test_aggregate_functions(df):
    row = df.agg(
        F.sum("amount").alias("total"),
        F.count_distinct("id").alias("uniq"),
        F.avg("amount").alias("mean"),
    ).collect_rows()[0]
    assert row["total"] == 100.0
    assert row["uniq"] == 3


def test_grouped_agg(df):
    result = {
        r["id"]: r["s"]
        for r in df.groupBy("id").agg(F.sum("amount").alias("s")).collect_rows()
    }
    assert result == {1: 50.0, 2: 20.0, 3: 30.0}


def test_rollup_and_cube(df):
    # rollup over id adds the grand-total (id = None) super-aggregate row.
    rolled = {
        r["id"]: r["s"]
        for r in df.rollup("id").agg(F.sum("amount").alias("s")).collect_rows()
    }
    assert rolled[1] == 50.0 and rolled[2] == 20.0 and rolled[3] == 30.0
    assert rolled[None] == 100.0  # grand total
    assert df.cube("id").count().count() == 4  # 3 groups + grand total


def test_hash_functions_return_hex(session):
    import hashlib  # noqa: PLC0415

    row = session.createDataFrame([(1,)], ["z"]).select(
        F.md5(lit("abc")).alias("m"),
        F.sha256(lit("abc")).alias("s256"),
        F.sha512(lit("abc")).alias("s512"),
    ).collect_rows()[0]
    assert row["m"] == hashlib.md5(b"abc").hexdigest()
    assert row["s256"] == hashlib.sha256(b"abc").hexdigest()
    assert row["s512"] == hashlib.sha512(b"abc").hexdigest()


def test_explode(session):
    df = session.sql(
        "SELECT 1 AS id, make_array(10, 20, 30) AS a "
        "UNION ALL SELECT 2 AS id, make_array(40, 50) AS a"
    )
    rows = df.select(col("id"), F.explode(col("a")).alias("e")).collect_rows()
    assert sorted((r["id"], r["e"]) for r in rows) == [
        (1, 10), (1, 20), (1, 30), (2, 40), (2, 50)
    ]
    # withColumn form keeps existing columns
    wc = df.withColumn("e", F.explode(col("a"))).collect_rows()
    assert len(wc) == 5 and {"id", "a", "e"} <= set(wc[0].keys())


def test_posexplode(session):
    df = session.sql("SELECT make_array(10, 20, 30) AS a")
    rows = df.select(F.posexplode(col("a"))).collect_rows()
    assert sorted((r["pos"], r["col"]) for r in rows) == [(0, 10), (1, 20), (2, 30)]


def test_dataframe_unnest_primitive(session):
    df = session.sql("SELECT 1 AS id, make_array(7, 8) AS a")
    assert df.unnest(["a"]).count() == 2


def test_higher_order_functions(session):
    df = session.sql("SELECT make_array(1, 2, 3, 4) AS a")
    row = df.select(
        F.transform(col("a"), lambda x: x * lit(10)).alias("t"),
        F.filter(col("a"), lambda x: x > lit(2)).alias("f"),
        F.exists(col("a"), lambda x: x > lit(3)).alias("e"),
        F.forall(col("a"), lambda x: x > lit(0)).alias("g"),
    ).collect_rows()[0]
    assert row["t"] == [10, 20, 30, 40]
    assert row["f"] == [3, 4]
    assert row["e"] is True
    assert row["g"] is True


def test_zip_with_and_indexed_lambdas(session):
    df = session.sql("SELECT make_array(10, 20, 30) AS a, make_array(1, 2, 3) AS b")
    row = df.select(
        F.zip_with(col("a"), col("b"), lambda x, y: x + y).alias("z"),
        F.transform(col("a"), lambda x, i: x + i).alias("ti"),
        F.filter(col("a"), lambda x, i: i > lit(0)).alias("fi"),
    ).collect_rows()[0]
    assert row["z"] == [11, 22, 33]
    assert row["ti"] == [10, 21, 32]   # element + 0-based index
    assert row["fi"] == [20, 30]       # keep index > 0
    # unequal lengths pad the shorter with NULL (Spark semantics)
    pad = session.sql("SELECT make_array(1, 2, 3) AS a, make_array(10) AS b").select(
        F.zip_with(col("a"), col("b"), lambda x, y: F.coalesce(y, lit(0)) + x).alias("z")
    ).collect_rows()[0]
    assert pad["z"] == [11, 2, 3]


def test_nested_higher_order(session):
    df = session.sql("SELECT make_array(make_array(1, 2), make_array(3)) AS a")
    out = df.select(
        F.transform(col("a"), lambda inner: F.transform(inner, lambda y: y + lit(100))).alias("n")
    ).collect_rows()[0]
    assert out["n"] == [[101, 102], [103]]


def test_aggregate(session):
    df = session.sql(
        "SELECT make_array(1, 2, 3, 4) AS a "
        "UNION ALL SELECT make_array(10, 20) AS a "
        "UNION ALL SELECT CAST(make_array() AS BIGINT[]) AS a"
    )
    rows = df.select(
        F.aggregate(col("a"), lit(0), lambda acc, x: acc + x).alias("sum"),
        F.aggregate(col("a"), lit(1), lambda acc, x: acc * x).alias("prod"),
        F.aggregate(
            col("a"), lit(0), lambda acc, x: acc + x, lambda acc: acc * lit(10)
        ).alias("sum_x10"),
    ).collect_rows()
    sums = sorted(r["sum"] for r in rows)
    assert sums == [0, 10, 30]          # [], [10,20], [1,2,3,4]
    prods = sorted(r["prod"] for r in rows)
    assert prods == [1, 24, 200]        # empty→1 (identity)
    finished = sorted(r["sum_x10"] for r in rows)
    assert finished == [0, 100, 300]    # finish: acc*10


def test_reduce_alias(session):
    df = session.sql("SELECT make_array(2, 3, 4) AS a")
    val = df.select(F.reduce(col("a"), lit(0), lambda acc, x: acc + x).alias("r")).collect_rows()[0]["r"]
    assert val == 9


def test_array_functions(session):
    adf = session.createDataFrame([(1,)], ["z"]).select(
        F.array(lit(3), lit(1), lit(2)).alias("a")
    )
    out = adf.select(
        F.array_contains("a", lit(2)).alias("has2"),
        F.array_distinct("a").alias("d"),
        F.cardinality("a").alias("len"),
    ).collect_rows()[0]
    assert out["has2"] is True
    assert out["len"] == 3


def test_datetime_extractions(session):
    ddf = session.createDataFrame([("2021-06-15",)], ["d"]).select(
        F.to_date(col("d")).alias("dt")
    )
    out = ddf.select(
        F.year("dt").alias("y"), F.month("dt").alias("m"), F.day("dt").alias("day")
    ).collect_rows()[0]
    assert (out["y"], out["m"], out["day"]) == (2021, 6, 15)


# ── DataFrame convenience & camelCase ────────────────────────────────────────


def test_dataframe_actions(df):
    assert df.count() == 4
    assert isinstance(df.first(), Row)
    assert len(df.take(2)) == 2
    assert not df.isEmpty()
    assert df.limit(0).isEmpty()
    assert df.dtypes[0] == ("id", "Int64")


def test_dataframe_transforms(df):
    assert df.withColumn("x2", col("id") * 2).columns() == ["id", "name", "amount", "x2"]
    assert df.withColumnRenamed("id", "ident").columns()[0] == "ident"
    assert df.select("id", col("name").alias("nm")).columns() == ["id", "nm"]
    assert df.selectExpr("id + 1 AS inc").collect_rows()[0]["inc"] == 2
    assert df.where(col("id") > lit(1)).count() == 2
    assert df.drop("amount").columns() == ["id", "name"]
    assert df.dropDuplicates(["id", "name"]).count() == 3
    assert df.orderBy(col("amount").desc()).first()["amount"] == 40.0


def test_union_by_name(session):
    a = session.createDataFrame([(1, "x")], ["id", "name"])
    b = session.createDataFrame([("y", 2)], ["name", "id"])  # different order
    assert a.unionByName(b).count() == 2
    assert a.crossJoin(session.createDataFrame([(9,)], ["k"])).count() == 1


def test_na_and_stat(session):
    ndf = session.createDataFrame([(1, 2.0), (2, 4.0), (3, 6.0)], ["i", "f"])
    assert ndf.na.fill(0).count() == 3
    assert abs(ndf.stat.corr("i", "f") - 1.0) < 1e-9


# ── Session parity ───────────────────────────────────────────────────────────


def test_create_dataframe_variants(session):
    from_tuples = session.createDataFrame([(1, "a")], ["id", "name"])
    assert from_tuples.count() == 1
    from_dicts = session.createDataFrame([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    assert from_dicts.count() == 2
    typed = session.createDataFrame(
        [(1, "a")],
        T.StructType([T.StructField("id", T.LongType()), T.StructField("name", T.StringType())]),
    )
    assert typed.columns() == ["id", "name"]


def test_range(session):
    assert session.range(5).count() == 5
    assert session.range(2, 10, 2).count() == 4


def test_catalog(session):
    session.createDataFrame([(1,)], ["z"]).create_or_replace_temp_view("v1")
    assert "v1" in session.catalog.listTables()
    assert session.catalog.tableExists("v1")
    assert session.catalog.dropTempView("v1")
    assert not session.catalog.tableExists("v1")


def test_reader_fluent(session, tmp_path):
    src = session.sql("SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2 AS id, 'b' AS name")
    path = str(tmp_path / "out.parquet")
    src.write.mode("overwrite").format("parquet").save(path)
    loaded = session.read.format("parquet").load(path)
    assert loaded.count() == 2


def test_reader_csv_with_options(session, tmp_path):
    src = session.sql("SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2 AS id, 'b' AS name")
    path = str(tmp_path / "out.csv")
    src.write.mode("overwrite").option("header", True).csv(path)
    loaded = session.read.format("csv").option("header", True).load(path)
    assert loaded.count() == 2


def test_persist_unpersist_return_self(df):
    # PySpark cache/persist/unpersist all return the DataFrame for chaining.
    assert df.persist().count() == 4
    assert df.cache().unpersist().count() == 4


def test_write_created_dataframe(session, tmp_path):
    # A createDataFrame result is MemTable-backed; the parquet write path must
    # fall back to client-side collect-then-write (the distributed sink cannot
    # ship an in-memory table to a fresh fragment engine).
    df = session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "name"])
    path = str(tmp_path / "created.parquet")
    df.write.mode("overwrite").parquet(path)
    assert session.read.parquet(path).count() == 3


def test_spark_session_builder():
    spark = ks.SparkSession.builder.appName("t").getOrCreate()
    assert spark.sql("SELECT 1 AS n").collect_rows()[0]["n"] == 1


def test_udf_register(session):
    square = session.udf.register("square", lambda x: x * x, "int")
    combine = session.udf.register(
        "combine", lambda a, b: f"{a}={b}", "string", argTypes=["int", "string"]
    )
    tolen = session.udf.register("tolen", len, T.IntegerType(), argTypes=[T.StringType()])
    df = session.sql("SELECT 3 AS n, 'xy' AS s")
    row = df.select_columns(
        [square(col("n")).alias("sq"), combine(col("n"), col("s")).alias("c"),
         tolen(col("s")).alias("l")]
    ).collect_rows()[0]
    assert row["sq"] == 9
    assert row["c"] == "3=xy"
    assert row["l"] == 2


# ── types module ─────────────────────────────────────────────────────────────


def test_types_simple_string():
    assert T.IntegerType().simpleString() == "int"
    assert T.LongType().simpleString() == "bigint"
    assert T.ArrayType(T.StringType()).simpleString() == "array<string>"
    st = T.StructType([T.StructField("a", T.IntegerType()), T.StructField("b", T.StringType())])
    assert st.fieldNames() == ["a", "b"]
    assert st.simpleString() == "struct<a:int,b:string>"


def test_row_helper():
    r = Row(id=1, name="a")
    assert r.id == 1
    assert r["name"] == "a"
    assert r.asDict() == {"id": 1, "name": "a"}


# ── native API surface parity (Rust methods now exposed on the bindings) ─────


def test_native_union_by_name(session):
    a = session.sql("SELECT 1 AS id, 'x' AS name")
    b = session.sql("SELECT 'y' AS name, 2 AS id")  # reordered
    # native union_by_name aligns by column name, not position
    assert sorted(r["id"] for r in a.union_by_name(b).collect_rows()) == [1, 2]


def test_native_with_columns_renamed(session):
    df = session.sql("SELECT 1 AS a, 2 AS b, 3 AS c")
    out = df.with_columns_renamed([("a", "x"), ("c", "z")])
    assert out.columns() == ["x", "b", "z"]


def test_native_column_not_like(session):
    df = session.sql("SELECT 'alice' AS n UNION ALL SELECT 'bob'")
    assert df.filter(col("n").not_like("a%")).collect_rows()[0]["n"] == "bob"


def test_native_collect_as_delta_batch(session):
    import krishiv  # noqa: PLC0415

    db = session.sql("SELECT 1 AS x UNION ALL SELECT 2").collect_as_delta_batch()
    assert isinstance(db, krishiv.DeltaBatch)
    assert db.num_rows == 2
