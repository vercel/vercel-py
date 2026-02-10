"""Tests for RawBody support in HTTP transports."""

import pytest
import respx
from httpx import Response

from vercel._http import (
    AsyncTransport,
    BlockingTransport,
    RawBody,
    create_base_async_client,
    create_base_client,
)
from vercel._iter_coroutine import iter_coroutine


class TestRawBodySupport:
    """Test that RawBody content is passed through transport unchanged."""

    @respx.mock
    def test_sync_raw_body_iterable(self):
        """BlockingTransport should forward iterable bodies without JSON encoding."""
        base_url = "https://upload.example.com"
        expected = b"chunk-1chunk-2"

        def handler(request):
            payload = b"".join(request.stream)
            assert payload == expected
            return Response(200, json={"ok": True})

        route = respx.post(f"{base_url}/upload").mock(side_effect=handler)

        client = create_base_client(timeout=30.0, base_url=base_url)
        transport = BlockingTransport(client)
        try:
            body = RawBody(iter([b"chunk-1", b"chunk-2"]))
            response = iter_coroutine(transport.send("POST", "/upload", body=body))
            assert response.status_code == 200
            assert route.called
        finally:
            transport.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_async_raw_body_async_iterable(self):
        """AsyncTransport should forward async iterable bodies without JSON encoding."""
        base_url = "https://upload.example.com"
        expected = b"part-apart-b"

        async def chunks():
            yield b"part-a"
            yield b"part-b"

        async def handler(request):
            body = b""
            async for chunk in request.stream:
                body += chunk
            assert body == expected
            return Response(200, json={"ok": True})

        route = respx.post(f"{base_url}/upload").mock(side_effect=handler)

        client = create_base_async_client(timeout=30.0, base_url=base_url)
        transport = AsyncTransport(client)
        try:
            response = await transport.send("POST", "upload", body=RawBody(chunks()))
            assert response.status_code == 200
            assert route.called
        finally:
            await transport.aclose()
