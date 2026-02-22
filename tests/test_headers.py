"""
Tests for vercel.headers — geolocation, ip_address, flag helpers.

These tests use lightweight stub objects (no HTTP framework dependency)
to cover all header-parsing paths including the newly added ``requestId``.
"""

from __future__ import annotations

from vercel.headers import (
    CITY_HEADER_NAME,
    COUNTRY_HEADER_NAME,
    IP_HEADER_NAME,
    LATITUDE_HEADER_NAME,
    LONGITUDE_HEADER_NAME,
    POSTAL_CODE_HEADER_NAME,
    REGION_HEADER_NAME,
    REQUEST_ID_HEADER_NAME,
    _get_flag,
    _region_from_request_id,
    geolocation,
    ip_address,
)

# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _Headers:
    """Minimal dict-backed headers stub that satisfies _HeadersLike."""

    def __init__(self, data: dict[str, str]) -> None:
        self._data = data

    def get(self, name: str) -> str | None:
        return self._data.get(name)


class _Request:
    """Minimal request stub that satisfies _RequestLike."""

    def __init__(self, data: dict[str, str]) -> None:
        self.headers = _Headers(data)


# ---------------------------------------------------------------------------
# ip_address()
# ---------------------------------------------------------------------------


class TestIpAddress:
    def test_returns_ip_from_request(self) -> None:
        req = _Request({IP_HEADER_NAME: "1.2.3.4"})
        assert ip_address(req) == "1.2.3.4"

    def test_accepts_headers_directly(self) -> None:
        headers = _Headers({IP_HEADER_NAME: "10.0.0.1"})
        assert ip_address(headers) == "10.0.0.1"

    def test_returns_none_when_header_missing(self) -> None:
        req = _Request({})
        assert ip_address(req) is None

    def test_accepts_headers_without_ip(self) -> None:
        headers = _Headers({})
        assert ip_address(headers) is None


# ---------------------------------------------------------------------------
# _get_flag()
# ---------------------------------------------------------------------------


class TestGetFlag:
    def test_us_flag(self) -> None:
        flag = _get_flag("US")
        assert flag == "🇺🇸"

    def test_gb_flag(self) -> None:
        flag = _get_flag("GB")
        assert flag == "🇬🇧"

    def test_lowercase_country_code(self) -> None:
        # Should be case-insensitive (uppercased internally)
        flag = _get_flag("in")
        assert flag is not None
        assert len(flag) == 2  # two regional-indicator symbols

    def test_none_input(self) -> None:
        assert _get_flag(None) is None

    def test_empty_string(self) -> None:
        assert _get_flag("") is None

    def test_too_long_code(self) -> None:
        assert _get_flag("USA") is None

    def test_single_char_code(self) -> None:
        assert _get_flag("U") is None

    def test_non_alpha_code(self) -> None:
        assert _get_flag("1A") is None


# ---------------------------------------------------------------------------
# _region_from_request_id()
# ---------------------------------------------------------------------------


class TestRegionFromRequestId:
    def test_extracts_region_prefix(self) -> None:
        assert _region_from_request_id("iad1:abc123:xyz") == "iad1"

    def test_no_colon_returns_full_string(self) -> None:
        assert _region_from_request_id("iad1") == "iad1"

    def test_none_returns_dev1(self) -> None:
        assert _region_from_request_id(None) == "dev1"


# ---------------------------------------------------------------------------
# geolocation()
# ---------------------------------------------------------------------------


class TestGeolocation:
    def _make_request(self, overrides: dict[str, str] | None = None) -> _Request:
        defaults: dict[str, str] = {
            CITY_HEADER_NAME: "New%20York",
            COUNTRY_HEADER_NAME: "US",
            REGION_HEADER_NAME: "NY",
            LATITUDE_HEADER_NAME: "40.7128",
            LONGITUDE_HEADER_NAME: "-74.0060",
            POSTAL_CODE_HEADER_NAME: "10001",
            REQUEST_ID_HEADER_NAME: "iad1:abc:def",
        }
        if overrides:
            defaults.update(overrides)
        return _Request(defaults)

    def test_returns_geo_typeddict(self) -> None:
        geo = geolocation(self._make_request())
        # TypedDict is just a dict at runtime
        assert isinstance(geo, dict)

    def test_city_is_url_decoded(self) -> None:
        geo = geolocation(self._make_request())
        assert geo["city"] == "New York"

    def test_country(self) -> None:
        geo = geolocation(self._make_request())
        assert geo["country"] == "US"

    def test_flag_derived_from_country(self) -> None:
        geo = geolocation(self._make_request())
        assert geo["flag"] == "🇺🇸"

    def test_country_region(self) -> None:
        geo = geolocation(self._make_request())
        assert geo["countryRegion"] == "NY"

    def test_region_derived_from_request_id(self) -> None:
        geo = geolocation(self._make_request())
        assert geo["region"] == "iad1"

    def test_latitude_and_longitude(self) -> None:
        geo = geolocation(self._make_request())
        assert geo["latitude"] == "40.7128"
        assert geo["longitude"] == "-74.0060"

    def test_postal_code(self) -> None:
        geo = geolocation(self._make_request())
        assert geo["postalCode"] == "10001"

    def test_request_id_exposed(self) -> None:
        """requestId should be the raw x-vercel-id header value."""
        geo = geolocation(self._make_request())
        assert geo["requestId"] == "iad1:abc:def"

    def test_request_id_none_when_header_absent(self) -> None:
        req = _Request({})
        geo = geolocation(req)
        assert geo["requestId"] is None

    def test_all_fields_none_when_no_headers(self) -> None:
        req = _Request({})
        geo = geolocation(req)
        assert geo["city"] is None
        assert geo["country"] is None
        assert geo["flag"] is None
        assert geo["countryRegion"] is None
        assert geo["latitude"] is None
        assert geo["longitude"] is None
        assert geo["postalCode"] is None
        assert geo["requestId"] is None

    def test_region_falls_back_to_dev1_without_request_id(self) -> None:
        req = _Request({COUNTRY_HEADER_NAME: "US"})
        geo = geolocation(req)
        assert geo["region"] == "dev1"

    def test_geo_keys_are_complete(self) -> None:
        """Ensure the returned dict has exactly the expected top-level keys."""
        geo = geolocation(self._make_request())
        expected_keys = {
            "city",
            "country",
            "flag",
            "region",
            "countryRegion",
            "latitude",
            "longitude",
            "postalCode",
            "requestId",
        }
        assert set(geo.keys()) == expected_keys
