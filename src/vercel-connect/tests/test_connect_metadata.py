"""Connector metadata, including forward-compatible passthrough.

The declared schema is a documented subset, so unknown top-level fields must
survive into `extra` rather than being dropped or silently widening the type.
"""

from typing import Any

import httpx
import pytest
import respx
from conftest import TEST_BASE_URL, session_options

from vercel.api import session
from vercel.connect import ConnectApiError, get_connector_metadata, sync as connect_sync

METADATA_URL = f"{TEST_BASE_URL}/v1/connect/connectors/oauth%2Flinear"


def metadata_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": "scl_123",
        "uid": "oauth/linear",
        "type": "oauth",
        "name": "Linear",
        "service": "linear",
        "clientUrl": "https://linear.app",
        "createdAt": 1_700_000_000_000,
        "updatedAt": 1_700_000_100_000,
        "data": {"clientId": "abc", "clientSecret": {"encrypted": True}},
    }
    payload.update(overrides)
    return payload


@respx.mock
async def test_get_connector_metadata_parses_declared_fields_async(mock_env_clear: None) -> None:
    route = respx.get(METADATA_URL).mock(return_value=httpx.Response(200, json=metadata_payload()))

    async with session(service_options=session_options()):
        metadata = await get_connector_metadata("oauth/linear")

    assert route.calls.last.request.method == "GET"
    assert route.calls.last.request.headers["authorization"] == "Bearer oidc-token"
    assert metadata.id == "scl_123"
    assert metadata.uid == "oauth/linear"
    assert metadata.type == "oauth"
    assert metadata.name == "Linear"
    assert metadata.service == "linear"
    assert metadata.client_url == "https://linear.app"
    assert metadata.created_at is not None
    assert metadata.created_at.tzinfo is not None
    assert metadata.updated_at is not None


@respx.mock
def test_get_connector_metadata_parses_declared_fields_sync(mock_env_clear: None) -> None:
    respx.get(METADATA_URL).mock(return_value=httpx.Response(200, json=metadata_payload()))

    with session(service_options=session_options()):
        metadata = connect_sync.get_connector_metadata("oauth/linear")

    assert metadata.uid == "oauth/linear"
    assert metadata.client_url == "https://linear.app"


@respx.mock
async def test_wire_data_becomes_vendor(mock_env_clear: None) -> None:
    respx.get(METADATA_URL).mock(return_value=httpx.Response(200, json=metadata_payload()))

    async with session(service_options=session_options()):
        metadata = await get_connector_metadata("oauth/linear")

    assert metadata.vendor == {"clientId": "abc", "clientSecret": {"encrypted": True}}
    assert "data" not in metadata.extra


@respx.mock
async def test_absent_data_yields_empty_vendor(mock_env_clear: None) -> None:
    payload = metadata_payload()
    del payload["data"]
    respx.get(METADATA_URL).mock(return_value=httpx.Response(200, json=payload))

    async with session(service_options=session_options()):
        metadata = await get_connector_metadata("oauth/linear")

    assert metadata.vendor == {}


@respx.mock
async def test_unknown_top_level_fields_survive_in_extra(mock_env_clear: None) -> None:
    """Connector capabilities arrive untyped today; they must not be dropped."""
    respx.get(METADATA_URL).mock(
        return_value=httpx.Response(
            200,
            json=metadata_payload(
                supportedSubjectTypes=["app", "user"],
                supportsTriggers=True,
                grantedScopes=["read", "write"],
                defaultInstallationId="T123",
                triggerEvents=["issue.created"],
            ),
        )
    )

    async with session(service_options=session_options()):
        metadata = await get_connector_metadata("oauth/linear")

    assert metadata.extra["supportedSubjectTypes"] == ["app", "user"]
    assert metadata.extra["supportsTriggers"] is True
    assert metadata.extra["grantedScopes"] == ["read", "write"]
    assert metadata.extra["defaultInstallationId"] == "T123"
    assert metadata.extra["triggerEvents"] == ["issue.created"]


@respx.mock
async def test_declared_fields_are_not_duplicated_into_extra(mock_env_clear: None) -> None:
    respx.get(METADATA_URL).mock(return_value=httpx.Response(200, json=metadata_payload()))

    async with session(service_options=session_options()):
        metadata = await get_connector_metadata("oauth/linear")

    for name in ("id", "uid", "type", "name", "service", "clientUrl", "createdAt", "updatedAt"):
        assert name not in metadata.extra


@respx.mock
async def test_optional_declared_fields_may_be_absent(mock_env_clear: None) -> None:
    respx.get(METADATA_URL).mock(
        return_value=httpx.Response(
            200, json={"id": "scl_123", "uid": "oauth/linear", "type": "oauth"}
        )
    )

    async with session(service_options=session_options()):
        metadata = await get_connector_metadata("oauth/linear")

    assert metadata.name is None
    assert metadata.client_url is None
    assert metadata.created_at is None
    assert metadata.vendor == {}
    assert metadata.extra == {}


@respx.mock
async def test_get_connector_metadata_maps_errors(mock_env_clear: None) -> None:
    respx.get(METADATA_URL).mock(
        return_value=httpx.Response(
            404, json={"error": {"code": "not_found", "message": "no such connector"}}
        )
    )

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_connector_metadata("oauth/linear")

    assert exc_info.value.code == "not_found"
