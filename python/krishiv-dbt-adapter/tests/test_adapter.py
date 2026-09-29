from krishiv_dbt_adapter.impl import KrishivAdapter, KrishivCredentials, KrishivConnection


def test_compile_table_model():
    adapter = KrishivAdapter(type("C", (), {"credentials": KrishivCredentials()})())
    sql = adapter.compile_model({"name": "orders", "config": {"materialized": "table"}, "compiled_code": "SELECT 1"})
    assert "CREATE TABLE orders" in sql


def test_incremental_and_view():
    adapter = KrishivAdapter(type("C", (), {"credentials": KrishivCredentials()})())
    inc = adapter.compile_model({"name": "x", "config": {"materialized": "incremental"}, "raw_code": "SELECT 2"})
    view = adapter.compile_model({"name": "y", "config": {"materialized": "view"}, "raw_code": "SELECT 3"})
    assert "INSERT INTO" in inc
    assert "CREATE OR REPLACE VIEW" in view


def test_connection_without_flightsql_fails_instead_of_doing_nothing(monkeypatch):
    # With the driver missing, every statement used to be recorded and never
    # run, and `dbt run` reported success over an untouched warehouse.
    import builtins

    real_import = builtins.__import__

    def no_flightsql(name, *args, **kwargs):
        if name.startswith("flightsql"):
            raise ImportError("no flightsql")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_flightsql)
    import pytest

    with pytest.raises(RuntimeError, match="flightsql"):
        KrishivConnection(KrishivCredentials())


def test_dry_run_connection_records_sql():
    conn = KrishivConnection(KrishivCredentials(), dry_run=True)
    conn.execute("SELECT 1")
    assert conn.last_query == "SELECT 1"
