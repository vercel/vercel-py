from __future__ import annotations

from typing import Any, cast

import httpx
import pytest
from hypothesis import given, strategies as st

from vercel._internal.http import sync_sleep
from vercel._internal.stable.errors import ErrorDetails, error_for_status
from vercel._internal.stable.sdk.request_client import SdkClientLineage, VercelRequestClient
from vercel.stable.errors import APIResponseError, ConflictError
from vercel.stable.options import SdkOptions


class _Runtime:
    def __init__(self, transport: _Transport) -> None:
        self._transport = transport

    async def get_transport(self, *, timeout: float | None = None) -> _Transport:
        assert timeout == 5.0
        return self._transport


class _Transport:
    def __init__(self, response: httpx.Response) -> None:
        self._response = response

    async def send(self, *args: object, **kwargs: object) -> httpx.Response:
        return self._response


@pytest.mark.asyncio
async def test_http_errors_include_structured_metadata() -> None:
    response = httpx.Response(
        409,
        json={
            "error": {
                "message": "Project already exists.",
                "code": "project_conflict",
            },
            "requestId": "req_123",
            "traceId": "trace_456",
        },
    )
    client = VercelRequestClient(
        _lineage=SdkClientLineage(
            runtime=cast(Any, _Runtime(_Transport(response))),
            root_timeout=5.0,
            env={},
        ),
        _options=SdkOptions(token="token"),
        _sleep_fn=sync_sleep,
    )

    with pytest.raises(ConflictError) as excinfo:
        await client.send_json("POST", "/v1/projects")

    error = excinfo.value
    assert str(error) == "Project already exists."
    assert error.status_code == 409
    assert error.error_code == "project_conflict"
    assert error.request_id == "req_123"
    assert error.trace_id == "trace_456"
    assert error.payload == {
        "error": {
            "message": "Project already exists.",
            "code": "project_conflict",
        },
        "requestId": "req_123",
        "traceId": "trace_456",
    }


@pytest.mark.asyncio
async def test_http_error_fallback_uses_headers_for_ids() -> None:
    response = httpx.Response(
        500,
        text="server exploded",
        headers={
            "x-request-id": "req_from_header",
            "x-vercel-trace-id": "trace_from_header",
        },
    )
    client = VercelRequestClient(
        _lineage=SdkClientLineage(
            runtime=cast(Any, _Runtime(_Transport(response))),
            root_timeout=5.0,
            env={},
        ),
        _options=SdkOptions(token="token"),
        _sleep_fn=sync_sleep,
    )

    with pytest.raises(APIResponseError) as excinfo:
        await client.send("GET", "/v1/projects")

    error = excinfo.value
    assert str(error) == "500 Internal Server Error"
    assert error.status_code == 500
    assert error.error_code is None
    assert error.request_id == "req_from_header"
    assert error.trace_id == "trace_from_header"
    assert error.payload is None


# ---------------------------------------------------------------------------
# Property-based tests
# ---------------------------------------------------------------------------


@given(status=st.integers(min_value=400, max_value=599))
def test_prop_error_status_mapping_is_total(status: int) -> None:
    details = ErrorDetails(message=f"status {status}")
    err = error_for_status(status, details)
    assert isinstance(err, APIResponseError)
