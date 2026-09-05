"""Unit tests for BlobRequestClient.resolve_token and request-layer auth."""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import httpx as legacy_httpx
import httpx2 as httpx
import pytest

from vercel._internal.blob.core import BlobRequestClient, _add_authorization_header
from vercel._internal.blob.errors import BlobUnknownError

TOKEN = "test_token_123"
PROVIDER_TOKEN = "provider_token_456"


def _make_request_client(
    *,
    token_provider: AsyncMock | None = None,
) -> tuple[BlobRequestClient, AsyncMock, AsyncMock]:
    send = AsyncMock(return_value=httpx.Response(200, json={"pathname": "test.txt"}))
    mock_transport = Mock()
    mock_transport.send = send
    provider = token_provider or AsyncMock(return_value=PROVIDER_TOKEN)
    client = BlobRequestClient(
        transport=mock_transport,
        retry=MagicMock(
            retries=0,
            backoff_base=0,
            backoff_max=0,
            retry_on_response=None,
            retry_on_network_error=False,
        ),
        sleep_fn=lambda _: None,
        token_provider=provider,
    )
    return client, provider, send


@pytest.mark.asyncio
async def test_resolve_token_with_explicit_token() -> None:
    client, provider, _ = _make_request_client()
    result = await client.resolve_token(TOKEN)
    assert result == TOKEN
    provider.assert_not_awaited()


@pytest.mark.asyncio
async def test_resolve_token_without_token_uses_provider() -> None:
    client, provider, _ = _make_request_client()
    result = await client.resolve_token(None)
    assert result == PROVIDER_TOKEN
    provider.assert_awaited_once()


@pytest.mark.asyncio
async def test_request_api_sends_override_authorization_header() -> None:
    client, _, send = _make_request_client()
    with patch("vercel._internal.blob.core.get_api_url", return_value="https://api.example.com/"):
        with patch("vercel._internal.blob.core.make_request_id", return_value="req-1"):
            with patch("vercel._internal.blob.core.get_api_version", return_value="1"):
                with patch(
                    "vercel._internal.blob.core.get_proxy_through_alternative_api_header_from_env",
                    return_value={},
                ):
                    with patch(
                        "vercel._internal.blob.core.should_use_x_content_length",
                        return_value=False,
                    ):
                        await client.request_api("", "PUT", token=TOKEN, body=b"data")

    assert send.await_args is not None
    call_kwargs = send.await_args.kwargs
    headers = call_kwargs["headers"]
    assert headers["authorization"] == f"Bearer {TOKEN}"


@pytest.mark.asyncio
async def test_request_api_sends_provider_token_authorization_header() -> None:
    client, _, send = _make_request_client()
    with patch("vercel._internal.blob.core.get_api_url", return_value="https://api.example.com/"):
        with patch("vercel._internal.blob.core.make_request_id", return_value="req-1"):
            with patch("vercel._internal.blob.core.get_api_version", return_value="1"):
                with patch(
                    "vercel._internal.blob.core.get_proxy_through_alternative_api_header_from_env",
                    return_value={},
                ):
                    with patch(
                        "vercel._internal.blob.core.should_use_x_content_length",
                        return_value=False,
                    ):
                        await client.request_api("", "PUT", body=b"data")

    assert send.await_args is not None
    call_kwargs = send.await_args.kwargs
    headers = call_kwargs["headers"]
    assert headers["authorization"] == f"Bearer {PROVIDER_TOKEN}"


@pytest.mark.asyncio
async def test_send_retries_legacy_httpx_transport_errors() -> None:
    request = legacy_httpx.Request("GET", "https://api.example.com/data")
    send = AsyncMock(
        side_effect=[
            legacy_httpx.ConnectError("connection reset", request=request),
            legacy_httpx.Response(200, request=request),
        ]
    )
    transport = Mock()
    transport.send = send
    client = BlobRequestClient(
        transport=cast(Any, transport),
        retry=MagicMock(
            retries=1,
            backoff_base=0,
            backoff_max=0,
            retry_on_response=None,
            retry_on_network_error=True,
        ),
        sleep_fn=lambda _: None,
        token_provider=AsyncMock(return_value=PROVIDER_TOKEN),
    )

    response = await client.send("GET", str(request.url), token=TOKEN)

    assert isinstance(response, legacy_httpx.Response)
    assert send.await_count == 2


@pytest.mark.asyncio
async def test_request_api_wraps_legacy_httpx_errors() -> None:
    request = legacy_httpx.Request("PUT", "https://api.example.com/")
    send = AsyncMock(side_effect=legacy_httpx.ConnectError("network down", request=request))
    transport = Mock()
    transport.send = send
    client = BlobRequestClient(
        transport=cast(Any, transport),
        retry=MagicMock(
            retries=0,
            backoff_base=0,
            backoff_max=0,
            retry_on_response=None,
            retry_on_network_error=False,
        ),
        sleep_fn=lambda _: None,
        token_provider=AsyncMock(return_value=PROVIDER_TOKEN),
    )

    with (
        patch("vercel._internal.blob.core.get_api_url", return_value=str(request.url)),
        patch("vercel._internal.blob.core.make_request_id", return_value="req-1"),
        patch("vercel._internal.blob.core.get_api_version", return_value="1"),
        patch(
            "vercel._internal.blob.core.get_proxy_through_alternative_api_header_from_env",
            return_value={},
        ),
        patch("vercel._internal.blob.core.should_use_x_content_length", return_value=False),
        pytest.raises(BlobUnknownError) as exc_info,
    ):
        await client.request_api("", "PUT", token=TOKEN, body=b"data")

    assert isinstance(exc_info.value.__cause__, legacy_httpx.ConnectError)


class TestAddAuthorizationHeader:
    def test_adds_bearer_token(self) -> None:
        result = _add_authorization_header("mytoken", {"x-custom": "1"})
        assert result == {"x-custom": "1", "authorization": "Bearer mytoken"}

    def test_preserves_existing_headers(self) -> None:
        result = _add_authorization_header("tok", {"a": "b", "c": "d"})
        assert result["a"] == "b"
        assert result["c"] == "d"
        assert result["authorization"] == "Bearer tok"
