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


def test_connection_records_sql_without_flightsql():
    conn = KrishivConnection(KrishivCredentials())
    conn.execute("SELECT 1")
    assert conn.last_query == "SELECT 1"
