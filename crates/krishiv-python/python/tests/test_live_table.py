"""R14 live table API tests."""

import krishiv as ks


def test_create_live_table():
    session = ks.Session.embedded()
    table = session.live_table(
        "orders_summary",
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
    )
    assert table.name == "orders_summary"
    table.ingest_row(1, "insert")
    table.refresh()
    table.drop()


def test_change_feed_order():
    session = ks.Session.embedded()
    table = session.live_table("orders", "SELECT * FROM orders")
    table.ingest_row(1, "insert")
    table.ingest_row(1, "update")
    table.ingest_row(1, "delete")
    feed = table.change_feed()
    assert feed is not None
