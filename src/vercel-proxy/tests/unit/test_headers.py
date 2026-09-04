import pytest

from vercel.proxy import Headers

# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_from_asgi_basic() -> None:
    h = Headers.from_asgi([(b"content-type", b"application/json"), (b"x-foo", b"baz")])
    assert h["content-type"] == "application/json"
    assert h["x-foo"] == "baz"


def test_from_asgi_normalises_keys() -> None:
    h = Headers.from_asgi([(b"Content-Type", b"text/plain")])
    assert h["content-type"] == "text/plain"


# ---------------------------------------------------------------------------
# Case-insensitive access
# ---------------------------------------------------------------------------


def test_getitem_case_insensitive() -> None:
    h = Headers((("content-type", "text/html"),))
    assert h["Content-Type"] == "text/html"
    assert h["content-type"] == "text/html"
    assert h["CONTENT-TYPE"] == "text/html"


def test_getitem_missing_raises_key_error() -> None:
    h = Headers((("x-foo", "bar"),))
    with pytest.raises(KeyError):
        _ = h["x-missing"]


# ---------------------------------------------------------------------------
# get() — first value
# ---------------------------------------------------------------------------


def test_get_returns_first_value_for_duplicate_keys() -> None:
    h = Headers((("set-cookie", "a=1"), ("set-cookie", "b=2")))
    assert h.get("set-cookie") == "a=1"


def test_get_returns_default_when_absent() -> None:
    h = Headers(())
    assert h.get("x-missing") is None
    assert h.get("x-missing", "fallback") == "fallback"


# ---------------------------------------------------------------------------
# get_all() — multi-value
# ---------------------------------------------------------------------------


def test_get_all_returns_all_values() -> None:
    h = Headers((("set-cookie", "a=1"), ("set-cookie", "b=2")))
    assert h.get_all("set-cookie") == ["a=1", "b=2"]


def test_get_all_returns_empty_list_when_absent() -> None:
    h = Headers(())
    assert h.get_all("x-missing") == []


def test_get_all_case_insensitive() -> None:
    h = Headers.from_asgi([(b"Set-Cookie", b"a=1"), (b"set-cookie", b"b=2")])
    assert h.get_all("set-cookie") == ["a=1", "b=2"]


# ---------------------------------------------------------------------------
# Mapping protocol
# ---------------------------------------------------------------------------


def test_iter_yields_unique_keys() -> None:
    h = Headers((("set-cookie", "a=1"), ("set-cookie", "b=2"), ("x-foo", "bar")))
    assert list(h) == ["set-cookie", "x-foo"]


def test_len_counts_unique_keys() -> None:
    h = Headers((("set-cookie", "a=1"), ("set-cookie", "b=2"), ("x-foo", "bar")))
    assert len(h) == 2


def test_contains() -> None:
    h = Headers((("x-foo", "bar"),))
    assert "x-foo" in h
    assert "X-Foo" in h
    assert "x-missing" not in h


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


def test_setattr_raises() -> None:
    h = Headers((("x-foo", "bar"),))
    with pytest.raises(AttributeError):
        h.x_foo = "baz"  # type: ignore[attr-defined]


def test_delattr_raises() -> None:
    h = Headers((("x-foo", "bar"),))
    with pytest.raises(AttributeError):
        del h.x_foo  # type: ignore[attr-defined]


def test_internal_store_is_tuple() -> None:
    h = Headers((("x-foo", "bar"),))
    assert isinstance(h._store, tuple)  # noqa: SLF001


def test_internal_store_entries_are_tuples() -> None:
    h = Headers((("x-foo", "bar"),))
    assert all(isinstance(entry, tuple) for entry in h._store)  # noqa: SLF001


# ---------------------------------------------------------------------------
# repr
# ---------------------------------------------------------------------------


def test_repr_single_value() -> None:
    h = Headers((("content-type", "text/html"),))
    assert repr(h) == "Headers({'content-type': 'text/html'})"


def test_repr_multi_value() -> None:
    h = Headers((("set-cookie", "a=1"), ("set-cookie", "b=2")))
    assert repr(h) == "Headers({'set-cookie': ['a=1', 'b=2']})"


def test_repr_mixed() -> None:
    h = Headers((("host", "example.com"), ("set-cookie", "a=1"), ("set-cookie", "b=2")))
    assert repr(h) == "Headers({'host': 'example.com', 'set-cookie': ['a=1', 'b=2']})"


# ---------------------------------------------------------------------------
# Equality and hashing
# ---------------------------------------------------------------------------


def test_equality_same_store() -> None:
    h1 = Headers((("x-foo", "bar"),))
    h2 = Headers((("x-foo", "bar"),))
    assert h1 == h2


def test_equality_different_values() -> None:
    h1 = Headers((("x-foo", "bar"),))
    h2 = Headers((("x-foo", "baz"),))
    assert h1 != h2


def test_hashable() -> None:
    h = Headers((("x-foo", "bar"),))
    assert isinstance(hash(h), int)
    s = {h}
    assert h in s
