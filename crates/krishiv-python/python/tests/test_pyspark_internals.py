"""Cover the branches of krishiv._pyspark not hit by the parity tests:
na/stat, fluent reader/writer, catalog, UDF type coercion, Row edges,
grouping-set builder, SparkSession builder, and error paths."""
import pytest

import krishiv as ks
from krishiv import Row, Session, col, lit
from krishiv import functions as F
from krishiv import types as T


@pytest.fixture
def session():
    return Session.embedded()


@pytest.fixture
def df(session):
    return session.createDataFrame(
        [(1, "a", 10.0), (2, "b", None), (3, "c", 30.0)], ["id", "name", "val"]
    )


# ── Row ──────────────────────────────────────────────────────────────────────


def test_row_attr_error_and_repr():
    r = Row(id=1, name="x")
    assert "Row(" in repr(r)
    assert r.asDict(recursive=True) == {"id": 1, "name": "x"}
    with pytest.raises(AttributeError):
        _ = r.nonexistent


# ── na / stat ────────────────────────────────────────────────────────────────


def test_na_fill_variants(df):
    assert df.na.fill(0.0).na.fill(0.0).count() == 3            # scalar, all cols
    assert df.na.fill({"val": 0.0}).filter(col("id") == lit(2)).collect_rows()[0]["val"] == 0.0
    assert df.na.fill(0.0, subset=["val"]).count() == 3


def test_na_drop_and_unsupported(df):
    assert df.na.drop(subset=["val"]).count() == 2             # row id=2 has null val
    with pytest.raises(NotImplementedError):
        df.na.drop(how="all")
    with pytest.raises(NotImplementedError):
        df.na.drop(thresh=2)
    with pytest.raises(ValueError):
        df.na.drop(how="bogus")


def test_stat_corr_cov(session):
    s = session.createDataFrame([(1, 2.0), (2, 4.0), (3, 6.0)], ["i", "f"])
    assert abs(s.stat.corr("i", "f") - 1.0) < 1e-9
    assert isinstance(s.stat.cov("i", "f"), float)
    with pytest.raises(NotImplementedError):
        s.stat.corr("i", "f", method="spearman")


# ── fluent reader / writer ───────────────────────────────────────────────────


def test_writer_reader_all_formats(session, tmp_path):
    src = session.sql("SELECT 1 AS id, 'x' AS name UNION ALL SELECT 2, 'y'")
    pq = str(tmp_path / "d.parquet")
    src.write.format("parquet").mode("overwrite").option("k", "v").options(a=1).partitionBy().save(pq)
    assert session.read.format("parquet").option("k", "v").options(a=1).schema(None).load(pq).count() == 2
    csv = str(tmp_path / "d.csv")
    src.write.mode("overwrite").csv(csv)
    assert session.read.csv(csv, header=True, sep=",").count() == 2
    js = str(tmp_path / "d.json")
    src.write.mode("overwrite").json(js)
    assert session.read.json(js).count() == 2
    # reader.table + reader.parquet shortcut
    session.sql("SELECT 1 AS n").create_or_replace_temp_view("rv")
    assert session.read.table("rv").count() == 1
    assert session.read.parquet(pq).count() == 2


def test_writer_saveAsTable_and_load_errors(session, tmp_path):
    src = session.sql("SELECT 1 AS id")
    with pytest.raises(NotImplementedError):
        src.write.saveAsTable("t")
    with pytest.raises(ValueError):
        session.read.format("orc").load(str(tmp_path / "x.orc"))


# ── catalog ──────────────────────────────────────────────────────────────────


def test_catalog(session):
    session.sql("SELECT 1 AS a, 2 AS b").create_or_replace_temp_view("cv")
    cat = session.catalog
    assert "cv" in cat.listTables()
    assert cat.tableExists("cv")
    assert set(cat.listColumns("cv")) == {"a", "b"}
    assert cat.dropTempView("cv") is True
    assert cat.dropTempView("cv") is False        # already gone
    assert cat.dropGlobalTempView("nope") is False


# ── UDF registration type coercion ───────────────────────────────────────────


def test_udf_type_forms(session):
    a = session.udf.register("u_int", lambda x: x + 1, "int")
    b = session.udf.register("u_long", lambda x: x + 1, T.LongType())
    c = session.udf.register("u_str", lambda x: x.upper(), "string", argTypes=["string"])
    row = session.sql("SELECT 5 AS n, 'hi' AS t").select_columns(
        [a(col("n")).alias("a"), b(col("n")).alias("b"), c(col("t")).alias("c")]
    ).collect_rows()[0]
    assert (row["a"], row["b"], row["c"]) == (6, 6, "HI")


# ── grouping-set builder (rollup / cube) ─────────────────────────────────────


def test_rollup_cube(df):
    rolled = {r["name"]: r["s"] for r in df.rollup("name").agg(F.count("id").alias("s")).collect_rows()}
    assert rolled[None] == 3                       # grand total
    assert df.cube("name").count().count() == 4    # 3 groups + total


# ── SparkSession builder ─────────────────────────────────────────────────────


def test_spark_session_builder_options():
    b = ks.SparkSession.builder.appName("x").master("local[*]").config("k", "v").config(spark_x=1)
    spark = b.getOrCreate()
    assert spark.sql("SELECT 1 AS n").collect_rows()[0]["n"] == 1


# ── createDataFrame variants + range ─────────────────────────────────────────


def test_create_dataframe_all_forms(session):
    assert session.createDataFrame([(1, "a")], ["x", "y"]).count() == 1        # tuples + names
    assert session.createDataFrame([{"x": 1}, {"x": 2}]).count() == 2          # dicts
    assert session.createDataFrame([(1,)]).columns() == ["_1"]                 # inferred names
    typed = session.createDataFrame(
        [(1, "a")], T.StructType([T.StructField("i", T.LongType()), T.StructField("n", T.StringType())])
    )
    assert typed.columns() == ["i", "n"]
    import pandas as pd  # noqa: PLC0415
    assert session.createDataFrame(pd.DataFrame({"a": [1, 2, 3]})).count() == 3
    assert session.range(0, 10, 2).count() == 5


# ── select/withColumn/orderBy branch coverage ────────────────────────────────


def test_dataframe_branch_coverage(df):
    assert df.select("id", col("name").alias("nm")).columns() == ["id", "nm"]  # mixed
    assert df.select("id", "name").columns() == ["id", "name"]                 # all names
    assert df.orderBy("id", ascending=False).first()["id"] == 3
    assert df.orderBy(["id"], ascending=[True]).first()["id"] == 1
    assert df.sort(["id"], [True]).first()["id"] == 3                          # native desc form
    assert df.where("id >= 2").count() == 2
    assert df.filter(col("id") >= lit(2)).count() == 2
    assert df.toDF("a", "b", "c").columns() == ["a", "b", "c"]
    with pytest.raises(ValueError):
        df.toDF("only_one")
    with pytest.raises(ValueError):
        df.select(F.explode(col("name")), F.explode(col("id")))               # >1 explode


def test_union_by_name_allow_missing(session):
    a = session.sql("SELECT 1 AS id, 'x' AS name")
    b = session.sql("SELECT 2 AS id, 99 AS extra")
    out = a.unionByName(b, allowMissingColumns=True)
    assert out.count() == 2
    assert set(out.columns()) == {"id", "name", "extra"}


# ── set / union / subtract / dedup ───────────────────────────────────────────


def test_set_ops(session):
    a = session.createDataFrame([(1,), (2,), (2,), (3,)], ["x"])
    b = session.createDataFrame([(2,), (3,), (4,)], ["x"])
    assert a.dropDuplicates().count() == 3           # subset=None -> distinct
    assert a.drop_duplicates().count() == 3          # snake alias
    assert a.unionAll(b).count() == 7                # keeps dups
    assert set(r["x"] for r in a.subtract(b).collect_rows()) == {1}
    assert a.crossJoin(b).count() == 4 * 3           # cartesian


# ── take / tail / iterators / toPandas / printSchema ─────────────────────────


def test_row_access_forms(df, capsys):
    assert [r["id"] for r in df.take(2)] == [1, 2]        # take(n)
    assert [r["id"] for r in df.head(2)] == [1, 2]        # head(n) list form
    assert df.tail(1)[0]["id"] == 3                        # tail
    assert [r["id"] for r in df.toLocalIterator()] == [1, 2, 3]
    pdf = df.toPandas()
    assert list(pdf["id"]) == [1, 2, 3]
    assert df.isEmpty() is False
    assert df.where("id > 100").isEmpty() is True
    df.printSchema()
    out = capsys.readouterr().out
    assert out.startswith("root") and "id:" in out


# ── withColumns (plural) / withColumnsRenamed / groupBy(Column) ───────────────


def test_with_columns_plural_and_grouped_column(df):
    out = df.withColumns({"a": lit(1), "b": col("id") + lit(10)})
    row = out.filter(col("id") == lit(1)).collect_rows()[0]
    assert (row["a"], row["b"]) == (1, 11)
    ren = df.withColumnsRenamed({"id": "ident", "name": "nm"})
    assert set(ren.columns()) >= {"ident", "nm"}
    # groupBy given a Column (not a string name) routes through group_by_columns
    grouped = df.groupBy(col("name")).agg(F.count("id").alias("c"))
    assert grouped.count() == 3


# ── posexplode two-name alias ────────────────────────────────────────────────


def test_posexplode_alias_two_names(session):
    src = session.sql("SELECT 1 AS id")
    out = src.select(col("id"), F.posexplode(F.array(lit(10), lit(20))).alias("p", "v"))
    rows = {r["p"]: r["v"] for r in out.collect_rows()}
    assert rows == {0: 10, 1: 20}


# ── na type-match helper (bool / str columns) ────────────────────────────────


def test_na_fill_type_aware(session):
    d = session.createDataFrame([(1, "a", True), (2, None, None)], ["i", "s", "b"])
    # filling with a string only touches the string column, leaving bool/int alone
    filled = d.na.fill("Z")
    row = filled.filter(col("i") == lit(2)).collect_rows()[0]
    assert row["s"] == "Z"
    # a bool value fills only the bool column (exercises the bool type-match branch)
    bfilled = d.na.fill(False)
    brow = bfilled.filter(col("i") == lit(2)).collect_rows()[0]
    assert brow["b"] is False


def test_agg_named_kwargs(df):
    # named Column kwarg (Column branch) + named plain-literal kwarg (_as_column -> lit)
    out = df.agg(total=F.sum("id"), label=7)
    row = out.collect_rows()[0]
    assert row["total"] == 6 and row["label"] == 7


def test_na_type_matches_helper():
    from krishiv._pyspark import _na_type_matches  # noqa: PLC0415
    assert _na_type_matches(True, "boolean") is True
    assert _na_type_matches(True, "int") is False
    assert _na_type_matches(5, "int64") is True
    assert _na_type_matches("x", "utf8") is True
    assert _na_type_matches("x", "int") is False
    # a value of an unmodelled type (bytes) defaults to fillable
    assert _na_type_matches(b"x", "binary") is True


# ── UDF register on a builtin (no introspectable signature) ──────────────────


def test_udf_register_builtin_arity_fallback(session):
    # ``max`` has no inspectable signature -> arity falls back to len(argTypes)
    fn = session.udf.register("u_max2", max, "int", argTypes=["int", "int"])
    row = session.sql("SELECT 3 AS a, 7 AS b").select_columns(
        [fn(col("a"), col("b")).alias("m")]
    ).collect_rows()[0]
    assert row["m"] == 7


# ── reader.csv with integer delimiter + reader.json ──────────────────────────


def test_reader_csv_int_delimiter_and_json(session, tmp_path):
    src = session.sql("SELECT 1 AS id, 'x' AS name UNION ALL SELECT 2, 'y'")
    csv = str(tmp_path / "d.csv")
    src.write.mode("overwrite").csv(csv)
    # sep given as an ordinal int is coerced via chr()
    assert session.read.option("sep", ord(",")).csv(csv, header=True).count() == 2
    js = str(tmp_path / "d.json")
    src.write.mode("overwrite").json(js)
    assert session.read.format("json").load(js).count() == 2
