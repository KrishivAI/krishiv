import pyarrow as pa
from krishiv_ge.datasource import KrishivDatasource, KrishivSQLAlchemyDataConnector


def test_validate_not_null():
    batch = pa.record_batch([pa.array([1, 2, 3])], names=["id"])
    ds = KrishivDatasource()
    result = ds.validate_not_null(batch, "id")
    assert result["success"] is True


def test_sql_connector_query():
    c = KrishivSQLAlchemyDataConnector("localhost")
    assert "events" in c.get_batch_data(table_name="events")
