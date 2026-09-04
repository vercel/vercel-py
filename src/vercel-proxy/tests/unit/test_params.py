import pytest

from vercel.proxy import Params

# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_basic() -> None:
    p = Params((("id", "42"), ("name", "alice")))
    assert p["id"] == "42"
    assert p["name"] == "alice"


def test_empty() -> None:
    p = Params(())
    assert len(p) == 0


# ---------------------------------------------------------------------------
# Access
# ---------------------------------------------------------------------------


def test_getitem_missing_raises() -> None:
    p = Params((("id", "1"),))
    with pytest.raises(KeyError):
        _ = p["missing"]


def test_keys_are_case_sensitive() -> None:
    p = Params((("Id", "1"),))
    assert p["Id"] == "1"
    with pytest.raises(KeyError):
        _ = p["id"]


def test_get_returns_none_when_absent() -> None:
    p = Params(())
    assert p.get("missing") is None


def test_get_returns_default_when_absent() -> None:
    p = Params(())
    assert p.get("missing", "fallback") == "fallback"


def test_get_returns_value_when_present() -> None:
    p = Params((("id", "42"),))
    assert p.get("id") == "42"


# ---------------------------------------------------------------------------
# Mapping protocol
# ---------------------------------------------------------------------------


def test_iter() -> None:
    p = Params((("a", "1"), ("b", "2")))
    assert list(p) == ["a", "b"]


def test_len() -> None:
    p = Params((("a", "1"), ("b", "2")))
    assert len(p) == 2


def test_contains() -> None:
    p = Params((("id", "1"),))
    assert "id" in p
    assert "missing" not in p


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


def test_setattr_raises() -> None:
    p = Params((("id", "1"),))
    with pytest.raises(AttributeError):
        p.id = "2"  # type: ignore[attr-defined]


def test_internal_store_is_tuple() -> None:
    p = Params((("id", "1"),))
    assert isinstance(p._store, tuple)  # noqa: SLF001


# ---------------------------------------------------------------------------
# Equality and hashing
# ---------------------------------------------------------------------------


def test_equality() -> None:
    assert Params((("id", "1"),)) == Params((("id", "1"),))


def test_inequality() -> None:
    assert Params((("id", "1"),)) != Params((("id", "2"),))


def test_hashable() -> None:
    p = Params((("id", "1"),))
    assert isinstance(hash(p), int)
    assert p in {p}
