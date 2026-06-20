import os
import tempfile

import krishiv as ks
import pytest


@pytest.fixture
def session():
    return ks.Session.local()


@pytest.fixture
def sample_df(session):
    return session.sql("SELECT 1 AS id, 'alice' AS name, 3.14 AS value")


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


def test_write_csv_and_read_back(session, sample_df, tmp_dir):
    path = os.path.join(tmp_dir, "out.csv")
    sample_df.write_csv(path)
    assert os.path.exists(path)
    result = session.read_csv(path)
    rows = result.collect()
    assert rows.row_count == 1


def test_write_json_and_read_back(session, sample_df, tmp_dir):
    path = os.path.join(tmp_dir, "out.json")
    sample_df.write_json(path)
    assert os.path.exists(path)
    result = session.read_json(path)
    rows = result.collect()
    assert rows.row_count == 1


def test_write_parquet_and_read_back(session, sample_df, tmp_dir):
    path = os.path.join(tmp_dir, "out.parquet")
    sample_df.write_parquet(path)
    assert os.path.exists(path)
    result = session.read_parquet(path)
    rows = result.collect()
    assert rows.row_count == 1


def test_read_csv_with_options(session, tmp_dir):
    path = os.path.join(tmp_dir, "noheader.csv")
    with open(path, "w") as f:
        f.write("99|bob|2.71\n")
    result = session.read_csv_with_options(path, has_header=False, delimiter="|")
    rows = result.collect()
    assert rows.row_count == 1
    pretty = result.collect().pretty()
    assert "99" in pretty


def test_read_parquet_with_options(session, sample_df, tmp_dir):
    write_path = os.path.join(tmp_dir, "opts.parquet")
    sample_df.write_parquet(write_path)
    result = session.read_parquet_with_options(write_path)
    rows = result.collect()
    assert rows.row_count == 1


def test_csv_round_trip_integrity(session, sample_df, tmp_dir):
    path = os.path.join(tmp_dir, "roundtrip.csv")
    sample_df.write_csv(path)
    result = session.read_csv(path)
    pretty = result.collect().pretty()
    assert "alice" in pretty
    assert "3.14" in pretty


def test_json_round_trip_integrity(session, sample_df, tmp_dir):
    path = os.path.join(tmp_dir, "roundtrip.json")
    sample_df.write_json(path)
    result = session.read_json(path)
    pretty = result.collect().pretty()
    assert "alice" in pretty


def test_parquet_round_trip_integrity(session, sample_df, tmp_dir):
    path = os.path.join(tmp_dir, "roundtrip.parquet")
    sample_df.write_parquet(path)
    result = session.read_parquet(path)
    pretty = result.collect().pretty()
    assert "alice" in pretty
    assert "3.14" in pretty


def test_multiple_formats_same_session(session, sample_df, tmp_dir):
    csv_path = os.path.join(tmp_dir, "multi.csv")
    json_path = os.path.join(tmp_dir, "multi.json")
    parquet_path = os.path.join(tmp_dir, "multi.parquet")
    sample_df.write_csv(csv_path)
    sample_df.write_json(json_path)
    sample_df.write_parquet(parquet_path)
    csv_result = session.read_csv(csv_path).collect()
    json_result = session.read_json(json_path).collect()
    parquet_result = session.read_parquet(parquet_path).collect()
    assert csv_result.row_count == 1
    assert json_result.row_count == 1
    assert parquet_result.row_count == 1


def test_read_nonexistent_csv_does_not_crash(session):
    result = session.read_csv("/nonexistent/path/data.csv")
    assert result is not None


def test_read_nonexistent_json_does_not_crash(session):
    result = session.read_json("/nonexistent/path/data.json")
    assert result is not None


def test_read_nonexistent_parquet_does_not_crash(session):
    result = session.read_parquet("/nonexistent/path/data.parquet")
    assert result is not None


def test_write_csv_with_options(session, sample_df, tmp_dir):
    path = os.path.join(tmp_dir, "opts.csv")
    sample_df.write_csv_with_options(path, delimiter="|", has_header=True)
    assert os.path.exists(path)
    with open(path) as f:
        content = f.read()
    assert "|" in content


def test_write_json_with_options(session, sample_df, tmp_dir):
    path = os.path.join(tmp_dir, "opts.json")
    sample_df.write_json(path)
    assert os.path.exists(path)


def test_write_parquet_with_options(session, sample_df, tmp_dir):
    path = os.path.join(tmp_dir, "opts.parquet")
    sample_df.write_parquet_with_options(path)
    assert os.path.exists(path)


def test_read_file_auto_detect(session, sample_df, tmp_dir):
    path = os.path.join(tmp_dir, "auto.csv")
    sample_df.write_csv(path)
    result = session.read_file(path, format="csv")
    rows = result.collect()
    assert rows.row_count == 1


def test_write_file_with_format(session, sample_df, tmp_dir):
    path = os.path.join(tmp_dir, "manual.json")
    sample_df.write_file(path, format="json")
    assert os.path.exists(path)


def test_read_csv_empty_file(session, tmp_dir):
    path = os.path.join(tmp_dir, "empty.csv")
    with open(path, "w") as f:
        f.write("")
    result = session.read_csv(path)
    rows = result.collect()
    assert rows.row_count == 0


def test_write_to_nested_tmp_directory(session, sample_df):
    with tempfile.TemporaryDirectory() as base:
        nested = os.path.join(base, "a", "b", "c")
        os.makedirs(nested)
        path = os.path.join(nested, "deep.csv")
        sample_df.write_csv(path)
        assert os.path.exists(path)
        result = session.read_csv(path)
        assert result.collect().row_count == 1
