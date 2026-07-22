"""Integration tests for Vercel Functions module.

Tests environment access, IP/geo extraction, and header context management.
"""

import pytest


class TestGetEnv:
    """Test get_env and Env dataclass."""

    def test_get_env_from_os_environ(self, mock_env_clear, monkeypatch):
        """Test get_env reads from os.environ."""
        from vercel.functions import Env, get_env

        monkeypatch.setenv("VERCEL", "1")
        monkeypatch.setenv("VERCEL_ENV", "production")
        monkeypatch.setenv("VERCEL_URL", "my-app.vercel.app")
        monkeypatch.setenv("VERCEL_REGION", "iad1")

        env = get_env()

        assert isinstance(env, Env)
        assert env.VERCEL == "1"
        assert env.VERCEL_ENV == "production"
        assert env.VERCEL_URL == "my-app.vercel.app"
        assert env.VERCEL_REGION == "iad1"

    def test_get_env_from_custom_mapping(self, mock_env_clear):
        """Test get_env reads from custom mapping."""
        from vercel.functions import get_env

        custom_env = {
            "VERCEL": "1",
            "VERCEL_ENV": "preview",
            "VERCEL_DEPLOYMENT_ID": "dpl_test123",
        }

        env = get_env(custom_env)

        assert env.VERCEL == "1"
        assert env.VERCEL_ENV == "preview"
        assert env.VERCEL_DEPLOYMENT_ID == "dpl_test123"

    def test_get_env_normalizes_empty_strings(self, mock_env_clear, monkeypatch):
        """Test that empty strings are normalized to None."""
        from vercel.functions import get_env

        monkeypatch.setenv("VERCEL", "1")
        monkeypatch.setenv("VERCEL_URL", "")  # Empty string

        env = get_env()

        assert env.VERCEL == "1"
        assert env.VERCEL_URL is None

    def test_env_to_dict(self, mock_env_clear, monkeypatch):
        """Test Env.to_dict method."""
        from vercel.functions import get_env

        monkeypatch.setenv("VERCEL", "1")
        monkeypatch.setenv("CI", "true")

        env = get_env()
        env_dict = env.to_dict()

        assert isinstance(env_dict, dict)
        assert env_dict["VERCEL"] == "1"
        assert env_dict["CI"] == "true"

    def test_env_getitem(self, mock_env_clear, monkeypatch):
        """Test Env bracket notation access."""
        from vercel.functions import get_env

        monkeypatch.setenv("VERCEL_ENV", "development")

        env = get_env()

        assert env["VERCEL_ENV"] == "development"

        with pytest.raises(KeyError):
            _ = env["NONEXISTENT_KEY"]

    def test_env_get_with_default(self, mock_env_clear, monkeypatch):
        """Test Env.get method with default."""
        from vercel.functions import get_env

        monkeypatch.setenv("VERCEL", "1")

        env = get_env()

        assert env.get("VERCEL") == "1"
        assert env.get("NONEXISTENT", "default") == "default"
        assert env.get("VERCEL_URL") is None

    def test_env_git_fields(self, mock_env_clear, monkeypatch):
        """Test Git-related environment fields."""
        from vercel.functions import get_env

        monkeypatch.setenv("VERCEL_GIT_PROVIDER", "github")
        monkeypatch.setenv("VERCEL_GIT_REPO_SLUG", "my-repo")
        monkeypatch.setenv("VERCEL_GIT_REPO_OWNER", "my-org")
        monkeypatch.setenv("VERCEL_GIT_COMMIT_REF", "main")
        monkeypatch.setenv("VERCEL_GIT_COMMIT_SHA", "abc123")

        env = get_env()

        assert env.VERCEL_GIT_PROVIDER == "github"
        assert env.VERCEL_GIT_REPO_SLUG == "my-repo"
        assert env.VERCEL_GIT_REPO_OWNER == "my-org"
        assert env.VERCEL_GIT_COMMIT_REF == "main"
        assert env.VERCEL_GIT_COMMIT_SHA == "abc123"


class TestIpAddress:
    """Test ip_address function."""

    def test_ip_address_from_request_object(self, mock_env_clear):
        """Test extracting IP from request-like object."""
        from vercel.functions import ip_address

        class MockHeaders:
            def get(self, name):
                if name == "x-real-ip":
                    return "203.0.113.42"
                return None

        class MockRequest:
            headers = MockHeaders()

        ip = ip_address(MockRequest())
        assert ip == "203.0.113.42"

    def test_ip_address_from_headers_object(self, mock_env_clear):
        """Test extracting IP from headers-like object."""
        from vercel.functions import ip_address

        class MockHeaders:
            def get(self, name):
                if name == "x-real-ip":
                    return "192.168.1.100"
                return None

        ip = ip_address(MockHeaders())
        assert ip == "192.168.1.100"

    def test_ip_address_missing(self, mock_env_clear):
        """Test IP is None when header missing."""
        from vercel.functions import ip_address

        class MockHeaders:
            def get(self, name):
                return None

        class MockRequest:
            headers = MockHeaders()

        ip = ip_address(MockRequest())
        assert ip is None


class TestGeolocation:
    """Test geolocation function."""

    def test_geolocation_full(self, mock_env_clear):
        """Test extracting full geolocation data."""
        from vercel.functions import geolocation

        headers_data = {
            "x-vercel-ip-city": "San%20Francisco",  # URL encoded
            "x-vercel-ip-country": "US",
            "x-vercel-ip-country-region": "CA",
            "x-vercel-ip-latitude": "37.7749",
            "x-vercel-ip-longitude": "-122.4194",
            "x-vercel-ip-postal-code": "94103",
            "x-vercel-id": "iad1::12345",
        }

        class MockHeaders:
            def get(self, name):
                return headers_data.get(name)

        class MockRequest:
            headers = MockHeaders()

        geo = geolocation(MockRequest())

        assert geo["city"] == "San Francisco"  # Decoded
        assert geo["country"] == "US"
        assert geo["countryRegion"] == "CA"
        assert geo["latitude"] == "37.7749"
        assert geo["longitude"] == "-122.4194"
        assert geo["postalCode"] == "94103"
        assert geo["region"] == "iad1"

    def test_geolocation_flag_generation(self, mock_env_clear):
        """Test country flag emoji generation."""
        from vercel.functions import geolocation

        class MockHeaders:
            def get(self, name):
                if name == "x-vercel-ip-country":
                    return "US"
                return None

        class MockRequest:
            headers = MockHeaders()

        geo = geolocation(MockRequest())

        # US flag emoji
        assert geo["flag"] is not None
        # The flag should be the US flag emoji (two regional indicator symbols)

    def test_geolocation_empty(self, mock_env_clear):
        """Test geolocation with no headers."""
        from vercel.functions import geolocation

        class MockHeaders:
            def get(self, name):
                return None

        class MockRequest:
            headers = MockHeaders()

        geo = geolocation(MockRequest())

        assert geo["city"] is None
        assert geo["country"] is None
        assert geo["region"] == "dev1"  # Default when no request ID

    def test_geolocation_region_from_request_id(self, mock_env_clear):
        """Test region extraction from request ID."""
        from vercel.functions import geolocation

        class MockHeaders:
            def get(self, name):
                if name == "x-vercel-id":
                    return "sfo1::request-123"
                return None

        class MockRequest:
            headers = MockHeaders()

        geo = geolocation(MockRequest())
        assert geo["region"] == "sfo1"


class TestSetGetHeaders:
    """Test header context management."""

    def test_set_and_get_headers(self, mock_env_clear):
        """Test setting and getting headers in context."""
        from vercel.functions import get_headers, set_headers

        # Initially None
        assert get_headers() is None

        # Set headers
        set_headers({"Content-Type": "application/json", "X-Custom": "value"})

        headers = get_headers()
        assert headers is not None
        assert headers["Content-Type"] == "application/json"
        assert headers["X-Custom"] == "value"

        # Clear headers
        set_headers(None)
        assert get_headers() is None

    def test_headers_overwrite(self, mock_env_clear):
        """Test that set_headers overwrites previous values."""
        from vercel.functions import get_headers, set_headers

        set_headers({"First": "value1"})
        assert get_headers()["First"] == "value1"

        set_headers({"Second": "value2"})
        headers = get_headers()
        assert headers.get("First") is None
        assert headers["Second"] == "value2"

        set_headers(None)


class TestRuntimeCacheExport:
    """Test that cache classes are properly exported from functions."""

    def test_get_cache_export(self, mock_env_clear):
        """Test get_cache is accessible from functions module."""
        from vercel.functions import RuntimeCache, get_cache

        cache = get_cache()
        assert isinstance(cache, RuntimeCache)

    def test_async_runtime_cache_export(self, mock_env_clear):
        """Test AsyncRuntimeCache is accessible from functions module."""
        from vercel.functions import AsyncRuntimeCache

        cache = AsyncRuntimeCache()
        assert cache is not None


class TestEnvImmutability:
    """Test Env dataclass immutability."""

    def test_env_is_frozen(self, mock_env_clear, monkeypatch):
        """Test that Env instances are immutable."""
        from vercel.functions import get_env

        monkeypatch.setenv("VERCEL", "1")

        env = get_env()

        with pytest.raises(AttributeError):  # Frozen dataclass
            env.VERCEL = "2"


class TestGeoTypedDict:
    """Test Geo TypedDict structure."""

    def test_geo_fields(self, mock_env_clear):
        """Test Geo TypedDict has expected fields."""
        from vercel.functions import Geo

        # Create a Geo dict manually
        geo: Geo = {
            "city": "New York",
            "country": "US",
            "flag": None,
            "region": "iad1",
            "countryRegion": "NY",
            "latitude": "40.7128",
            "longitude": "-74.0060",
            "postalCode": "10001",
        }

        assert geo["city"] == "New York"
        assert geo["country"] == "US"
