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
    # Declaration order is preserved (not sorted) — an IVM view binds this schema
    # to its SELECT list positionally, so alphabetical sorting silently mismatched
    # columns. Order here must match the annotation order above.
    assert EventSchema.column_names() == ["name", "value", "active", "ts", "payload"]


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
