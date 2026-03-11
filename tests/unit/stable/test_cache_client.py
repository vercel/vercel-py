from __future__ import annotations

import re

from hypothesis import given, strategies as st

from vercel._internal.stable.cache.client import (
    _DEFAULT_NAMESPACE_SEPARATOR,
    create_key_transformer,
    default_key_hash_function,
)


@given(key=st.text(min_size=1, max_size=200))
def test_prop_djb2_hash_is_deterministic_and_hex(key: str) -> None:
    h1 = default_key_hash_function(key)
    h2 = default_key_hash_function(key)
    assert h1 == h2
    assert re.fullmatch(r"[0-9a-f]+", h1), f"non-hex output: {h1!r}"


@given(
    key=st.text(min_size=1, max_size=200),
    namespace=st.text(min_size=1, max_size=50),
)
def test_prop_cache_key_with_namespace_starts_with_prefix(key: str, namespace: str) -> None:
    transform = create_key_transformer(None, namespace, None)
    result = transform(key)
    sep = _DEFAULT_NAMESPACE_SEPARATOR
    assert result.startswith(f"{namespace}{sep}")


@given(key=st.text(min_size=1, max_size=200))
def test_prop_cache_key_without_namespace_is_bare_hash(key: str) -> None:
    transform = create_key_transformer(None, None, None)
    result = transform(key)
    expected = default_key_hash_function(key)
    assert result == expected
