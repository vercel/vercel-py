"""Integration tests for Vercel Cache API.

Tests RuntimeCache and AsyncRuntimeCache using the in-memory fallback
when cache environment variables are not set.
"""

import pytest


class TestRuntimeCacheInMemory:
    """Test RuntimeCache with in-memory fallback."""

    def test_get_cache_returns_runtime_cache(self, mock_env_clear):
        """Test get_cache returns a RuntimeCache instance."""
        from vercel.cache import RuntimeCache, get_cache

        cache = get_cache()
        assert isinstance(cache, RuntimeCache)

    def test_get_set_delete_sync(self, mock_env_clear):
        """Test basic get/set/delete operations (sync)."""
        from vercel.cache import get_cache

        cache = get_cache()

        # Set a value
        cache.set("test_key", "test_value")

        # Get the value
        result = cache.get("test_key")
        assert result == "test_value"

        # Delete the value
        cache.delete("test_key")

        # Get should return None after delete
        result = cache.get("test_key")
        assert result is None

    def test_set_with_dict_value(self, mock_env_clear):
        """Test setting dict values."""
        from vercel.cache import get_cache

        cache = get_cache()

        test_data = {"name": "test", "count": 42, "nested": {"key": "value"}}
        cache.set("dict_key", test_data)

        result = cache.get("dict_key")
        assert result == test_data

        cache.delete("dict_key")

    def test_set_with_list_value(self, mock_env_clear):
        """Test setting list values."""
        from vercel.cache import get_cache

        cache = get_cache()

        test_list = [1, 2, 3, "four", {"five": 5}]
        cache.set("list_key", test_list)

        result = cache.get("list_key")
        assert result == test_list

        cache.delete("list_key")

    def test_contains_operator(self, mock_env_clear):
        """Test __contains__ for 'in' operator."""
        from vercel.cache import get_cache

        cache = get_cache()

        cache.set("exists_key", "value")

        assert "exists_key" in cache
        assert "nonexistent_key" not in cache

        cache.delete("exists_key")

    def test_getitem_operator(self, mock_env_clear):
        """Test __getitem__ for bracket notation."""
        from vercel.cache import get_cache

        cache = get_cache()

        cache.set("bracket_key", "bracket_value")

        assert cache["bracket_key"] == "bracket_value"

        # Should raise KeyError for missing keys
        with pytest.raises(KeyError):
            _ = cache["missing_key"]

        cache.delete("bracket_key")

    def test_namespace_option(self, mock_env_clear):
        """Test namespace option for key prefixing."""
        from vercel.cache import get_cache

        cache1 = get_cache(namespace="ns1")
        cache2 = get_cache(namespace="ns2")

        # Set same key in different namespaces
        cache1.set("shared_key", "value1")
        cache2.set("shared_key", "value2")

        # Each should get their own value
        assert cache1.get("shared_key") == "value1"
        assert cache2.get("shared_key") == "value2"

        # Cleanup
        cache1.delete("shared_key")
        cache2.delete("shared_key")

    def test_namespace_separator_option(self, mock_env_clear):
        """Test custom namespace separator."""
        from vercel.cache import get_cache

        cache = get_cache(namespace="myns", namespace_separator="::")

        cache.set("key", "value")
        # The key transformation should use the custom separator
        # The value should still be retrievable
        assert cache.get("key") == "value"

        cache.delete("key")


class TestAsyncRuntimeCacheInMemory:
    """Test AsyncRuntimeCache with in-memory fallback."""

    @pytest.mark.asyncio
    async def test_get_set_delete_async(self, mock_env_clear):
        """Test basic get/set/delete operations (async)."""
        from vercel.cache import AsyncRuntimeCache

        cache = AsyncRuntimeCache()

        # Set a value
        await cache.set("async_key", "async_value")

        # Get the value
        result = await cache.get("async_key")
        assert result == "async_value"

        # Delete the value
        await cache.delete("async_key")

        # Get should return None after delete
        result = await cache.get("async_key")
        assert result is None

    @pytest.mark.asyncio
    async def test_async_dict_value(self, mock_env_clear):
        """Test async setting dict values."""
        from vercel.cache import AsyncRuntimeCache

        cache = AsyncRuntimeCache()

        test_data = {"async": True, "data": [1, 2, 3]}
        await cache.set("async_dict", test_data)

        result = await cache.get("async_dict")
        assert result == test_data

        await cache.delete("async_dict")

    @pytest.mark.asyncio
    async def test_async_contains(self, mock_env_clear):
        """Test async contains method."""
        from vercel.cache import AsyncRuntimeCache

        cache = AsyncRuntimeCache()

        await cache.set("async_exists", "value")

        assert await cache.contains("async_exists") is True
        assert await cache.contains("async_missing") is False

        await cache.delete("async_exists")

    @pytest.mark.asyncio
    async def test_async_namespace(self, mock_env_clear):
        """Test async cache with namespace."""
        from vercel.cache import AsyncRuntimeCache

        cache1 = AsyncRuntimeCache(namespace="async_ns1")
        cache2 = AsyncRuntimeCache(namespace="async_ns2")

        await cache1.set("key", "value1")
        await cache2.set("key", "value2")

        assert await cache1.get("key") == "value1"
        assert await cache2.get("key") == "value2"

        await cache1.delete("key")
        await cache2.delete("key")


class TestSyncAsyncParity:
    """Test that sync and async caches produce consistent results."""

    @pytest.mark.asyncio
    async def test_sync_async_share_in_memory_store(self, mock_env_clear):
        """Test that sync and async caches share the same in-memory store."""
        from vercel.cache import AsyncRuntimeCache, get_cache

        sync_cache = get_cache()
        async_cache = AsyncRuntimeCache()

        # Set with sync
        sync_cache.set("shared_store_key", "sync_value")

        # Get with async - should see the value
        result = await async_cache.get("shared_store_key")
        assert result == "sync_value"

        # Delete with async
        await async_cache.delete("shared_store_key")

        # Sync should see it's gone
        assert sync_cache.get("shared_store_key") is None


class TestCacheTagOperations:
    """Test cache tag-based operations."""

    def test_expire_tag_sync(self, mock_env_clear):
        """Test expiring cache entries by tag (sync)."""
        from vercel.cache import get_cache

        cache = get_cache()

        # Set values with tags
        cache.set("tagged1", "value1", {"tags": ["tag1", "tag2"]})
        cache.set("tagged2", "value2", {"tags": ["tag1"]})
        cache.set("untagged", "value3")  # No tag1

        # Verify entries exist before expiring
        assert cache.get("tagged1") == "value1"
        assert cache.get("tagged2") == "value2"
        assert cache.get("untagged") == "value3"

        # Expire by tag - should remove entries with tag1
        cache.expire_tag("tag1")

        # Entries with tag1 should be expired
        assert cache.get("tagged1") is None
        assert cache.get("tagged2") is None
        # Entry without tag1 should still exist
        assert cache.get("untagged") == "value3"

    def test_expire_tag_list(self, mock_env_clear):
        """Test expiring cache entries by multiple tags."""
        from vercel.cache import get_cache

        cache = get_cache()

        cache.set("tagged_a", "value_a", {"tags": ["tag_a"]})
        cache.set("tagged_b", "value_b", {"tags": ["tag_b"]})
        cache.set("tagged_both", "value_both", {"tags": ["tag_a", "tag_b"]})

        # Verify entries exist
        assert cache.get("tagged_a") == "value_a"
        assert cache.get("tagged_b") == "value_b"
        assert cache.get("tagged_both") == "value_both"

        # Expire by multiple tags
        cache.expire_tag(["tag_a", "tag_b"])

        # All tagged entries should be expired
        assert cache.get("tagged_a") is None
        assert cache.get("tagged_b") is None
        assert cache.get("tagged_both") is None

    @pytest.mark.asyncio
    async def test_expire_tag_async(self, mock_env_clear):
        """Test expiring cache entries by tag (async)."""
        from vercel.cache import AsyncRuntimeCache

        cache = AsyncRuntimeCache()

        await cache.set("async_tagged", "value", {"tags": ["async_tag"]})
        await cache.set("async_untagged", "other_value")

        # Verify entry exists
        assert await cache.get("async_tagged") == "value"
        assert await cache.get("async_untagged") == "other_value"

        await cache.expire_tag("async_tag")

        # Tagged entry should be expired
        assert await cache.get("async_tagged") is None
        # Untagged entry should still exist
        assert await cache.get("async_untagged") == "other_value"


class TestCacheWithOptions:
    """Test cache operations with options."""

    def test_set_with_ttl_option(self, mock_env_clear):
        """Test setting cache with TTL option."""
        from vercel.cache import get_cache

        cache = get_cache()

        # Set with TTL (time to live)
        cache.set("ttl_key", "ttl_value", {"ttl": 60})  # 60 seconds

        # Value should be retrievable immediately
        result = cache.get("ttl_key")
        assert result == "ttl_value"

        cache.delete("ttl_key")

    def test_set_with_tags_option(self, mock_env_clear):
        """Test setting cache with tags option."""
        from vercel.cache import get_cache

        cache = get_cache()

        # Set with tags
        cache.set("tags_key", "tags_value", {"tags": ["category:test", "type:demo"]})

        result = cache.get("tags_key")
        assert result == "tags_value"

        cache.delete("tags_key")


class TestCacheKeyTransformation:
    """Test cache key transformation with hash function."""

    def test_custom_hash_function(self, mock_env_clear):
        """Test custom key hash function."""
        import hashlib

        from vercel.cache import get_cache

        def custom_hash(key: str) -> str:
            return hashlib.md5(key.encode()).hexdigest()

        cache = get_cache(key_hash_function=custom_hash)

        cache.set("original_key", "value")
        result = cache.get("original_key")
        assert result == "value"

        cache.delete("original_key")

    def test_namespace_with_hash_function(self, mock_env_clear):
        """Test namespace combined with hash function."""
        from vercel.cache import get_cache

        def simple_hash(key: str) -> str:
            return f"hashed_{key}"

        cache = get_cache(namespace="ns", key_hash_function=simple_hash)

        cache.set("key", "value")
        result = cache.get("key")
        assert result == "value"

        cache.delete("key")
