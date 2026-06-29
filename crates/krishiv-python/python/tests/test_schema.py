"""Schema annotation → Arrow type mapping."""

from datetime import datetime

import krishiv as ks


class EventSchema(ks.Schema):
    name: str
    value: float
    active: bool
    ts: datetime
    payload: bytes


def test_schema_column_names():
    assert EventSchema.column_names() == ["active", "name", "payload", "ts", "value"]


def test_schema_repr_html_contains_columns():
    html = EventSchema._repr_html_()
    assert "name" in html
    assert "Float64" in html or "float" in html.lower()


def test_arrow_schema_requires_pyarrow():
    try:
        import pyarrow  # noqa: F401

        schema = EventSchema.arrow_schema()
        assert schema.field("name").type == pyarrow.string()
    except ImportError:
        import pytest

        pytest.skip("pyarrow not installed")
