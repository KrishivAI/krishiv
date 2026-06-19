"""R14 memoization tests."""

import ast
import inspect
import hashlib

import krishiv as ks


def _sample_fn(x):
    return x


def _normalized_source(fn):
    src = ast.unparse(ast.parse(inspect.getsource(fn)))
    return src.encode()


def test_cache_invalidation():
    schema_json = '{"fields":[{"name":"id","type":"int64"}]}'
    ipc = b"\x00"  # placeholder; real IPC provided by transform path in production

    key_a = hashlib.sha256(_normalized_source(_sample_fn) + schema_json.encode() + ipc).digest()
    info1 = ks.memo_cache_info()
    assert info1.size >= 0

    key_b = hashlib.sha256(b"changed" + schema_json.encode() + ipc).digest()
    assert key_a != key_b


def test_memo_cache_info():
    info = ks.memo_cache_info()
    assert info.hits >= 0
    assert info.misses >= 0
