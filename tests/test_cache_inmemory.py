"""
Tests for vercel.cache.cache_in_memory — InMemoryCache and AsyncInMemoryCache.

Covers set/get/delete, TTL expiry, tag-based invalidation, __contains__,
__getitem__, the new __contains__ on AsyncInMemoryCache, and the key
transformer utilities.
"""

from __future__ import annotations

import time

import pytest

from vercel.cache.cache_in_memory import AsyncInMemoryCache, InMemoryCache
from vercel.cache.utils import create_key_transformer, default_key_hash_function

# ---------------------------------------------------------------------------
# InMemoryCache (sync)
# ---------------------------------------------------------------------------


class TestInMemoryCache:
    def setup_method(self) -> None:
        self.cache = InMemoryCache()

    # Basic set / get
    def test_set_and_get(self) -> None:
        self.cache.set("key1", {"hello": "world"})
        assert self.cache.get("key1") == {"hello": "world"}

    def test_get_missing_key_returns_none(self) -> None:
        assert self.cache.get("nonexistent") is None

    def test_overwrite_value(self) -> None:
        self.cache.set("k", "first")
        self.cache.set("k", "second")
        assert self.cache.get("k") == "second"

    # Delete
    def test_delete_removes_key(self) -> None:
        self.cache.set("del_me", 42)
        self.cache.delete("del_me")
        assert self.cache.get("del_me") is None

    def test_delete_nonexistent_key_is_no_op(self) -> None:
        self.cache.delete("ghost")  # should not raise

    # __contains__
    def test_contains_existing_key(self) -> None:
        self.cache.set("present", True)
        assert "present" in self.cache

    def test_not_contains_missing_key(self) -> None:
        assert "absent" not in self.cache

    # __getitem__
    def test_getitem_returns_value(self) -> None:
        self.cache.set("item", [1, 2, 3])
        assert self.cache["item"] == [1, 2, 3]

    def test_getitem_raises_key_error_for_missing(self) -> None:
        with pytest.raises(KeyError):
            _ = self.cache["no_such_key"]

    # TTL
    def test_get_returns_none_after_ttl_expires(self) -> None:
        self.cache.set("ttl_key", "val", {"ttl": 0})
        # TTL of 0 seconds — should be considered expired immediately
        time.sleep(0.01)
        assert self.cache.get("ttl_key") is None

    def test_contains_false_after_ttl_expires(self) -> None:
        self.cache.set("ttl_check", "v", {"ttl": 0})
        time.sleep(0.01)
        assert "ttl_check" not in self.cache

    def test_get_returns_value_before_ttl_expires(self) -> None:
        self.cache.set("long_ttl", "valid", {"ttl": 3600})
        assert self.cache.get("long_ttl") == "valid"

    # Tag invalidation
    def test_expire_tag_removes_tagged_entries(self) -> None:
        self.cache.set("a", 1, {"tags": ["tag1"]})
        self.cache.set("b", 2, {"tags": ["tag2"]})
        self.cache.expire_tag("tag1")
        assert self.cache.get("a") is None
        assert self.cache.get("b") == 2

    def test_expire_multiple_tags_at_once(self) -> None:
        self.cache.set("x", 10, {"tags": ["alpha"]})
        self.cache.set("y", 20, {"tags": ["beta"]})
        self.cache.set("z", 30, {"tags": ["gamma"]})
        self.cache.expire_tag(["alpha", "gamma"])
        assert self.cache.get("x") is None
        assert self.cache.get("y") == 20
        assert self.cache.get("z") is None

    def test_expire_tag_no_match_is_no_op(self) -> None:
        self.cache.set("safe", 99, {"tags": ["keep"]})
        self.cache.expire_tag("other_tag")
        assert self.cache.get("safe") == 99

    def test_entry_with_no_tags_not_removed_by_expire_tag(self) -> None:
        self.cache.set("untagged", "data")
        self.cache.expire_tag("any_tag")
        assert self.cache.get("untagged") == "data"


# ---------------------------------------------------------------------------
# AsyncInMemoryCache
# ---------------------------------------------------------------------------


class TestAsyncInMemoryCache:
    def setup_method(self) -> None:
        self.cache = AsyncInMemoryCache()

    async def test_set_and_get(self) -> None:
        await self.cache.set("k", "v")
        assert await self.cache.get("k") == "v"

    async def test_get_missing_returns_none(self) -> None:
        assert await self.cache.get("missing") is None

    async def test_delete_removes_key(self) -> None:
        await self.cache.set("d", 123)
        await self.cache.delete("d")
        assert await self.cache.get("d") is None

    async def test_expire_tag_removes_entries(self) -> None:
        await self.cache.set("p", 1, {"tags": ["t"]})
        await self.cache.set("q", 2, {"tags": ["other"]})
        await self.cache.expire_tag("t")
        assert await self.cache.get("p") is None
        assert await self.cache.get("q") == 2

    # contains() coroutine
    async def test_contains_returns_true_for_existing(self) -> None:
        await self.cache.set("here", True)
        assert await self.cache.contains("here") is True

    async def test_contains_returns_false_for_missing(self) -> None:
        assert await self.cache.contains("nowhere") is False

    # __contains__ dunder (new — sync sugar over the delegate)
    async def test_dunder_contains_returns_true(self) -> None:
        await self.cache.set("chk", "yes")
        assert "chk" in self.cache  # uses __contains__

    async def test_dunder_contains_returns_false(self) -> None:
        assert "nope" not in self.cache

    # Shared delegate
    def test_shares_delegate_with_sync_cache(self) -> None:
        sync_cache = InMemoryCache()
        sync_cache.set("shared", "val")
        async_cache = AsyncInMemoryCache(delegate=sync_cache)
        assert "shared" in async_cache  # __contains__ via delegate

    async def test_ttl_expiry_via_async(self) -> None:
        await self.cache.set("ttl_async", "data", {"ttl": 0})
        time.sleep(0.01)
        assert await self.cache.get("ttl_async") is None


# ---------------------------------------------------------------------------
# Key transformer utilities
# ---------------------------------------------------------------------------


class TestDefaultKeyHashFunction:
    def test_deterministic(self) -> None:
        h1 = default_key_hash_function("hello")
        h2 = default_key_hash_function("hello")
        assert h1 == h2

    def test_different_inputs_produce_different_hashes(self) -> None:
        assert default_key_hash_function("a") != default_key_hash_function("b")

    def test_returns_hex_string(self) -> None:
        h = default_key_hash_function("test")
        assert isinstance(h, str)
        int(h, 16)  # Should not raise — must be valid hex


class TestCreateKeyTransformer:
    def test_no_namespace_hashes_key(self) -> None:
        transform = create_key_transformer(None, None, None)
        key = transform("greeting")
        assert key == default_key_hash_function("greeting")

    def test_with_namespace_prefixes(self) -> None:
        transform = create_key_transformer(None, "myns", None)
        key = transform("greeting")
        assert key.startswith("myns$")
        assert key == f"myns${default_key_hash_function('greeting')}"

    def test_custom_separator(self) -> None:
        transform = create_key_transformer(None, "ns", "::")
        key = transform("k")
        assert key.startswith("ns::")

    def test_custom_hash_function(self) -> None:
        identity = lambda k: k  # noqa: E731
        transform = create_key_transformer(identity, None, None)
        assert transform("raw_key") == "raw_key"

    def test_custom_hash_with_namespace(self) -> None:
        identity = lambda k: k  # noqa: E731
        transform = create_key_transformer(identity, "ns", "-")
        assert transform("raw_key") == "ns-raw_key"
