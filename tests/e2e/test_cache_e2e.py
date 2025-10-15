"""
E2E tests for Vercel Cache functionality.

These tests verify the cache workflow including:
- Setting and getting values
- TTL expiration
- Tag-based invalidation
- Namespace isolation
- In-memory cache implementation

Note: Vercel uses HTTP caching headers and Data Cache for production caching.
This SDK provides an in-memory cache implementation for development and testing.
"""

import asyncio
import pytest
import time

from vercel.cache.aio import get_cache


class TestCacheE2E:
    """End-to-end tests for cache functionality with in-memory implementation."""

    @pytest.fixture
    def cache(self):
        """Get a cache instance for testing."""
        return get_cache(namespace="e2e-test")

    @pytest.fixture
    def test_data(self):
        """Sample test data."""
        return {
            "user": {"id": 123, "name": "Test User", "email": "test@example.com"},
            "post": {"id": 456, "title": "Test Post", "content": "This is a test post"},
            "settings": {"theme": "dark", "notifications": True},
        }

    @pytest.mark.asyncio
    async def test_cache_set_get_basic(self, cache, test_data):
        """Test basic cache set and get operations."""
        key = "test:basic"

        # Clean up any existing data
        await cache.delete(key)

        # Verify key doesn't exist initially
        result = await cache.get(key)
        assert result is None

        # Set a value
        await cache.set(key, test_data["user"], {"ttl": 60})

        # Get the value back
        result = await cache.get(key)
        assert result is not None
        assert isinstance(result, dict)
        assert result["id"] == 123
        assert result["name"] == "Test User"
        assert result["email"] == "test@example.com"

    @pytest.mark.asyncio
    async def test_cache_ttl_expiration(self, cache, test_data):
        """Test TTL expiration functionality."""
        key = "test:ttl"

        # Clean up any existing data
        await cache.delete(key)

        # Set a value with short TTL
        await cache.set(key, test_data["post"], {"ttl": 2})

        # Verify value exists immediately
        result = await cache.get(key)
        assert result is not None
        assert result["title"] == "Test Post"

        # Wait for TTL to expire
        time.sleep(3)

        # Verify value is expired
        result = await cache.get(key)
        assert result is None

    @pytest.mark.asyncio
    async def test_cache_tag_invalidation(self, cache, test_data):
        """Test tag-based cache invalidation."""
        # Set multiple values with different tags
        await cache.set("test:tag1:item1", test_data["user"], {"tags": ["users", "test"]})
        await cache.set("test:tag1:item2", test_data["post"], {"tags": ["posts", "test"]})
        await cache.set("test:tag1:item3", test_data["settings"], {"tags": ["settings"]})

        # Verify all items exist
        assert await cache.get("test:tag1:item1") is not None
        assert await cache.get("test:tag1:item2") is not None
        assert await cache.get("test:tag1:item3") is not None

        # Invalidate by tag
        await cache.expire_tag("test")

        # Verify tagged items are gone, untagged item remains
        assert await cache.get("test:tag1:item1") is None
        assert await cache.get("test:tag1:item2") is None
        assert await cache.get("test:tag1:item3") is not None  # Only has "settings" tag

        # Clean up
        await cache.delete("test:tag1:item3")

    @pytest.mark.asyncio
    async def test_cache_multiple_tags(self, cache, test_data):
        """Test cache operations with multiple tags."""
        key = "test:multi-tag"

        # Set value with multiple tags
        await cache.set(key, test_data["user"], {"tags": ["users", "active", "premium"]})

        # Verify value exists
        result = await cache.get(key)
        assert result is not None

        # Invalidate by one tag
        await cache.expire_tag("active")

        # Verify value is gone (any tag invalidation removes the item)
        result = await cache.get(key)
        assert result is None

    @pytest.mark.asyncio
    async def test_cache_delete_operation(self, cache, test_data):
        """Test explicit cache deletion."""
        key = "test:delete"

        # Set a value
        await cache.set(key, test_data["settings"], {"ttl": 60})

        # Verify value exists
        result = await cache.get(key)
        assert result is not None

        # Delete the value
        await cache.delete(key)

        # Verify value is gone
        result = await cache.get(key)
        assert result is None

    @pytest.mark.asyncio
    async def test_cache_namespace_isolation(self, cache, test_data):
        """Test that different namespaces are isolated."""
        # Create another cache instance with different namespace
        other_cache = get_cache(namespace="e2e-test-other")

        key = "test:namespace"

        # Set value in first namespace
        await cache.set(key, test_data["user"], {"ttl": 60})

        # Verify value exists in first namespace
        result = await cache.get(key)
        assert result is not None

        # Verify value doesn't exist in other namespace
        result = await other_cache.get(key)
        assert result is None

        # Clean up
        await cache.delete(key)

    @pytest.mark.asyncio
    async def test_cache_in_memory_behavior(self, cache, test_data):
        """Test in-memory cache behavior."""
        # This test verifies that the cache works with the in-memory implementation
        # The cache uses in-memory storage for development and testing

        key = "test:in-memory"

        # Set a value
        await cache.set(key, test_data["post"], {"ttl": 60})

        # Get the value back
        result = await cache.get(key)
        assert result is not None
        assert result["title"] == "Test Post"

        # Clean up
        await cache.delete(key)

    @pytest.mark.asyncio
    async def test_cache_complex_data_types(self, cache):
        """Test cache with complex data types."""
        key = "test:complex"

        complex_data = {
            "string": "hello world",
            "number": 42,
            "float": 3.14,
            "boolean": True,
            "list": [1, 2, 3, "four"],
            "nested": {"inner": {"value": "nested value"}},
            "null_value": None,
        }

        # Set complex data
        await cache.set(key, complex_data, {"ttl": 60})

        # Get it back
        result = await cache.get(key)
        assert result is not None
        assert result == complex_data

        # Clean up
        await cache.delete(key)

    @pytest.mark.asyncio
    async def test_cache_concurrent_operations(self, cache, test_data):
        """Test concurrent cache operations."""

        async def set_value(i: int):
            key = f"test:concurrent:{i}"
            await cache.set(key, {"index": i, "data": test_data["user"]}, {"ttl": 60})
            return key

        async def get_value(key: str):
            return await cache.get(key)

        # Set multiple values concurrently
        keys = await asyncio.gather(*[set_value(i) for i in range(5)])

        # Get all values concurrently
        results = await asyncio.gather(*[get_value(key) for key in keys])

        # Verify all values were set and retrieved correctly
        for i, result in enumerate(results):
            assert result is not None
            assert result["index"] == i

        # Clean up
        await asyncio.gather(*[cache.delete(key) for key in keys])
