"""Schema annotation → Arrow type mapping."""

import pytest

pyarrow = pytest.importorskip("pyarrow")


def test_schema_annotations_resolve():
    import krishiv as ks

    class EventSchema(ks.Schema):
        user_id: str
        value: float
        active: bool

    fields = {f.name: str(f.type) for f in EventSchema.arrow_schema()}
    assert "user_id" in fields
    assert "value" in fields
    assert "active" in fields
