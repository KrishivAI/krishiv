"""State schema migration decorator."""

import krishiv as ks


def test_state_migration_decorator_registers_and_applies():
    @ks.state_migration(from_version=1, to_version=2)
    def migrate_v1_to_v2(old: bytes) -> bytes:
        return old + b"->2"

    out = ks.apply_state_migration(1, 2, b"v1")
    assert out == b"v1->2"


def test_register_state_migration_explicit():
    def migrate(old: bytes) -> bytes:
        return old + b"->3"

    ks.register_state_migration(2, 3, migrate)
    assert ks.apply_state_migration(2, 3, b"x") == b"x->3"
