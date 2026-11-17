"""
Unit tests for client class instantiation.

These tests verify that client classes can be instantiated without errors.
"""


class TestClientInstantiation:
    """Test that all client classes can be instantiated."""

    def test_build_cache_instantiation(self):
        """Test BuildCache can be instantiated."""
        from vercel.cache.cache_build import BuildCache

        client = BuildCache(
            endpoint="https://cache.example.com",
            headers={"Authorization": "Bearer test"},
        )
        assert client is not None
        assert hasattr(client, "_endpoint")
        assert hasattr(client, "_headers")
        assert hasattr(client, "_client")

    def test_async_build_cache_instantiation(self):
        """Test AsyncBuildCache can be instantiated."""
        from vercel.cache.cache_build import AsyncBuildCache

        client = AsyncBuildCache(
            endpoint="https://cache.example.com",
            headers={"Authorization": "Bearer test"},
        )
        assert client is not None
        assert hasattr(client, "_endpoint")
        assert hasattr(client, "_headers")
