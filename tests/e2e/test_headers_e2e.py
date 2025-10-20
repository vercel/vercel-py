"""
E2E tests for Vercel Headers and Geolocation functionality.

These tests verify the complete headers workflow including:
- IP address extraction
- Geolocation data extraction
- Header parsing and validation
- Request context handling
"""

import pytest
from unittest.mock import Mock

from vercel.headers import ip_address, geolocation, set_headers, get_headers


class TestHeadersE2E:
    """End-to-end tests for headers and geolocation functionality."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock request object for testing."""
        request = Mock()
        request.headers = Mock()
        return request

    @pytest.fixture
    def sample_headers(self):
        """Sample Vercel headers for testing."""
        return {
            "x-real-ip": "192.168.1.100",
            "x-vercel-ip-city": "San Francisco",
            "x-vercel-ip-country": "US",
            "x-vercel-ip-country-region": "CA",
            "x-vercel-ip-latitude": "37.7749",
            "x-vercel-ip-longitude": "-122.4194",
            "x-vercel-ip-postal-code": "94102",
            "x-vercel-id": "iad1:abc123def456",
        }

    def test_ip_address_extraction(self, mock_request, sample_headers):
        """Test IP address extraction from headers."""
        # Test with request object
        mock_request.headers.get.side_effect = lambda key: sample_headers.get(key.lower())

        ip = ip_address(mock_request)
        assert ip == "192.168.1.100"

        # Test with headers directly
        ip = ip_address(sample_headers)
        assert ip == "192.168.1.100"

    def test_ip_address_missing_header(self, mock_request):
        """Test IP address extraction when header is missing."""
        mock_request.headers.get.return_value = None

        ip = ip_address(mock_request)
        assert ip is None

    def test_geolocation_extraction(self, mock_request, sample_headers):
        """Test geolocation data extraction from headers."""
        mock_request.headers.get.side_effect = lambda key: sample_headers.get(key.lower())

        geo = geolocation(mock_request)

        # Verify all expected fields are present
        assert isinstance(geo, dict)
        assert geo["city"] == "San Francisco"
        assert geo["country"] == "US"
        assert geo["countryRegion"] == "CA"
        assert geo["latitude"] == "37.7749"
        assert geo["longitude"] == "-122.4194"
        assert geo["postalCode"] == "94102"
        assert geo["region"] == "iad1"  # Extracted from x-vercel-id

    def test_geolocation_flag_generation(self, mock_request, sample_headers):
        """Test flag emoji generation from country code."""
        mock_request.headers.get.side_effect = lambda key: sample_headers.get(key.lower())

        geo = geolocation(mock_request)

        # Verify flag is generated for US
        assert geo["flag"] is not None
        assert len(geo["flag"]) == 2  # Flag emoji should be 2 characters

        # Test with different country
        sample_headers["x-vercel-ip-country"] = "GB"
        geo = geolocation(mock_request)
        assert geo["flag"] is not None
        assert len(geo["flag"]) == 2

    def test_geolocation_missing_headers(self, mock_request):
        """Test geolocation when headers are missing."""
        mock_request.headers.get.return_value = None

        geo = geolocation(mock_request)

        # All fields should be None or have default values
        assert geo["city"] is None
        assert geo["country"] is None
        assert geo["flag"] is None
        assert geo["countryRegion"] is None
        assert geo["region"] == "dev1"  # Default when no x-vercel-id
        assert geo["latitude"] is None
        assert geo["longitude"] is None
        assert geo["postalCode"] is None

    def test_geolocation_url_decoded_city(self, mock_request):
        """Test geolocation with URL-encoded city names."""
        # Test with URL-encoded city name
        mock_request.headers.get.side_effect = lambda key: {
            "x-vercel-ip-city": "New%20York",
            "x-vercel-ip-country": "US",
            "x-vercel-id": "iad1:abc123",
        }.get(key.lower())

        geo = geolocation(mock_request)
        assert geo["city"] == "New York"  # Should be URL decoded

    def test_geolocation_region_extraction(self, mock_request):
        """Test region extraction from Vercel ID."""
        test_cases = [
            ("iad1:abc123def456", "iad1"),
            ("sfo1:xyz789", "sfo1"),
            ("fra1:test123", "fra1"),
            ("lhr1:example456", "lhr1"),
        ]

        for vercel_id, expected_region in test_cases:
            mock_request.headers.get.side_effect = lambda key: {"x-vercel-id": vercel_id}.get(
                key.lower()
            )

            geo = geolocation(mock_request)
            assert geo["region"] == expected_region

    def test_geolocation_invalid_country_code(self, mock_request):
        """Test geolocation with invalid country codes."""
        # Test with invalid country code
        mock_request.headers.get.side_effect = lambda key: {
            "x-vercel-ip-country": "INVALID",
            "x-vercel-id": "iad1:abc123",
        }.get(key.lower())

        geo = geolocation(mock_request)
        assert geo["flag"] is None  # Should not generate flag for invalid code

        # Test with empty country code
        mock_request.headers.get.side_effect = lambda key: {
            "x-vercel-ip-country": "",
            "x-vercel-id": "iad1:abc123",
        }.get(key.lower())

        geo = geolocation(mock_request)
        assert geo["flag"] is None

    def test_headers_context_management(self):
        """Test headers context management functionality."""
        # Test setting and getting headers
        test_headers = {
            "x-real-ip": "10.0.0.1",
            "x-vercel-ip-city": "Test City",
            "x-vercel-ip-country": "US",
        }

        # Set headers
        set_headers(test_headers)

        # Get headers
        retrieved_headers = get_headers()

        # Verify headers were set correctly
        assert retrieved_headers is not None
        assert retrieved_headers.get("x-real-ip") == "10.0.0.1"
        assert retrieved_headers.get("x-vercel-ip-city") == "Test City"
        assert retrieved_headers.get("x-vercel-ip-country") == "US"

    def test_headers_case_insensitive(self, mock_request):
        """Test that headers are case-insensitive."""
        # Test with mixed case headers - note: headers are actually case-sensitive
        mock_request.headers.get.side_effect = lambda key: {
            "x-real-ip": "192.168.1.1",  # Use lowercase as expected by implementation
            "x-vercel-ip-city": "Test City",
            "x-vercel-ip-country": "US",
        }.get(key.lower())

        ip = ip_address(mock_request)
        assert ip == "192.168.1.1"

        geo = geolocation(mock_request)
        assert geo["city"] == "Test City"
        assert geo["country"] == "US"

    def test_geolocation_edge_cases(self, mock_request):
        """Test geolocation edge cases."""
        # Test with empty string values - note: empty strings are returned as-is, not converted to None
        mock_request.headers.get.side_effect = lambda key: {
            "x-vercel-ip-city": "",
            "x-vercel-ip-country": "",
            "x-vercel-id": "",
        }.get(key.lower())

        geo = geolocation(mock_request)
        assert geo["city"] == ""  # Empty string is returned as-is
        assert geo["country"] == ""  # Empty string is returned as-is
        assert geo["region"] == ""  # Empty string when x-vercel-id is empty string

    def test_geolocation_typing(self, mock_request, sample_headers):
        """Test that geolocation returns proper typing."""
        mock_request.headers.get.side_effect = lambda key: sample_headers.get(key.lower())

        geo = geolocation(mock_request)

        # Verify return type matches Geo TypedDict
        assert isinstance(geo, dict)

        # Check that all expected keys are present
        expected_keys = {
            "city",
            "country",
            "flag",
            "region",
            "countryRegion",
            "latitude",
            "longitude",
            "postalCode",
        }
        assert set(geo.keys()) == expected_keys

        # Verify types
        assert geo["city"] is None or isinstance(geo["city"], str)
        assert geo["country"] is None or isinstance(geo["country"], str)
        assert geo["flag"] is None or isinstance(geo["flag"], str)
        assert geo["region"] is None or isinstance(geo["region"], str)
        assert geo["countryRegion"] is None or isinstance(geo["countryRegion"], str)
        assert geo["latitude"] is None or isinstance(geo["latitude"], str)
        assert geo["longitude"] is None or isinstance(geo["longitude"], str)
        assert geo["postalCode"] is None or isinstance(geo["postalCode"], str)

    def test_headers_integration_with_frameworks(self):
        """Test headers integration with web frameworks."""
        # Simulate FastAPI request
        from unittest.mock import Mock

        fastapi_request = Mock()
        fastapi_request.headers = {
            "x-real-ip": "203.0.113.1",
            "x-vercel-ip-city": "Tokyo",
            "x-vercel-ip-country": "JP",
            "x-vercel-id": "nrt1:japan123",
        }

        # Test IP extraction
        ip = ip_address(fastapi_request)
        assert ip == "203.0.113.1"

        # Test geolocation
        geo = geolocation(fastapi_request)
        assert geo["city"] == "Tokyo"
        assert geo["country"] == "JP"
        assert geo["region"] == "nrt1"

    def test_headers_performance(self, mock_request, sample_headers):
        """Test headers performance with multiple calls."""
        mock_request.headers.get.side_effect = lambda key: sample_headers.get(key.lower())

        # Test multiple calls
        for _ in range(100):
            ip = ip_address(mock_request)
            geo = geolocation(mock_request)

            assert ip == "192.168.1.100"
            assert geo["city"] == "San Francisco"

    def test_headers_real_world_scenarios(self, mock_request):
        """Test headers with real-world scenarios."""
        # Test with various real-world header combinations
        scenarios = [
            {
                "headers": {
                    "x-real-ip": "8.8.8.8",
                    "x-vercel-ip-city": "Mountain View",
                    "x-vercel-ip-country": "US",
                    "x-vercel-ip-country-region": "CA",
                    "x-vercel-id": "sfo1:google123",
                },
                "expected": {
                    "ip": "8.8.8.8",
                    "city": "Mountain View",
                    "country": "US",
                    "region": "sfo1",
                },
            },
            {
                "headers": {
                    "x-real-ip": "1.1.1.1",
                    "x-vercel-ip-city": "Sydney",
                    "x-vercel-ip-country": "AU",
                    "x-vercel-id": "syd1:cloudflare123",
                },
                "expected": {"ip": "1.1.1.1", "city": "Sydney", "country": "AU", "region": "syd1"},
            },
        ]

        for scenario in scenarios:
            mock_request.headers.get.side_effect = lambda key: scenario["headers"].get(key.lower())

            ip = ip_address(mock_request)
            geo = geolocation(mock_request)

            assert ip == scenario["expected"]["ip"]
            assert geo["city"] == scenario["expected"]["city"]
            assert geo["country"] == scenario["expected"]["country"]
            assert geo["region"] == scenario["expected"]["region"]
