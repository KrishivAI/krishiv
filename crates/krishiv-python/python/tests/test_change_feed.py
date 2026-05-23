"""R14 change feed tests."""

import krishiv as ks


def test_change_feed_emits_ops_in_order():
    session = ks.Session.embedded()
    table = session.live_table("orders", "SELECT * FROM orders")
    table.ingest_row(10, "insert")
    table.ingest_row(10, "update")
    table.ingest_row(10, "delete")
    ops = [op for op, _batch in table.change_feed()]
    assert ops == ["insert", "update", "delete"]
