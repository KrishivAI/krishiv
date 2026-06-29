import pytest

import krishiv as ks


@pytest.fixture
def registry():
    session = ks.Session.local()
    try:
        return session.operation_registry()
    except (RuntimeError, ks.ModeError) as e:
        pytest.skip(str(e))


def test_operation_registry_from_session(registry):
    assert registry is not None


def test_repr(registry):
    result = repr(registry)
    assert isinstance(result, str)
    assert "OperationRegistry" in result


def test_cancelled_ids_returns_collection(registry):
    ids = registry.cancelled_ids()
    assert isinstance(ids, (list, tuple))


def test_is_cancelled_unknown_id(registry):
    assert registry.is_cancelled(999_999_999) is False


def test_progress_unknown_id(registry):
    result = registry.progress(999_999_999)
    assert result is None


def test_cancel_nonexistent_id(registry):
    registry.cancel(999_999_998)


def test_remove_nonexistent_id(registry):
    registry.remove(999_999_997)


def test_cancel_and_is_cancelled(registry):
    op_id = 42
    assert registry.is_cancelled(op_id) is False
    registry.cancel(op_id)
    assert registry.is_cancelled(op_id) is True
    assert op_id in registry.cancelled_ids()


def test_remove_after_cancel(registry):
    op_id = 77
    registry.cancel(op_id)
    assert registry.is_cancelled(op_id) is True
    registry.remove(op_id)
    assert registry.is_cancelled(op_id) is False
    assert op_id not in registry.cancelled_ids()


def test_cancel_multiple_ids(registry):
    ids = [10, 20, 30]
    for oid in ids:
        registry.cancel(oid)
    cancelled = registry.cancelled_ids()
    for oid in ids:
        assert oid in cancelled
    for oid in ids:
        registry.remove(oid)
