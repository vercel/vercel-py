"""Unit tests for sandbox error hierarchy and classification."""

from __future__ import annotations

from datetime import timedelta

import httpx
import pytest

from vercel._internal.http import BaseTransport, ReadResponsePolicy
from vercel._internal.http.transport import HeaderTypes, QueryParamTypes, RequestBody
from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.sandbox.core import SandboxCredentials, SandboxRequestClient
from vercel._internal.sandbox.errors import (
    APIError,
    SandboxAuthError,
    SandboxError,
    SandboxNotFoundError,
    SandboxPermissionError,
    SandboxRateLimitError,
    SandboxServerError,
)
from vercel.sandbox import (
    APIError as PublicAPIError,
    SandboxAuthError as PublicSandboxAuthError,
    SandboxError as PublicSandboxError,
    SandboxNotFoundError as PublicSandboxNotFoundError,
    SandboxPermissionError as PublicSandboxPermissionError,
    SandboxRateLimitError as PublicSandboxRateLimitError,
    SandboxServerError as PublicSandboxServerError,
)


class StaticTransport(BaseTransport):
    def __init__(self, response: httpx.Response) -> None:
        self.response = response

    async def send(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        body: RequestBody = None,
        headers: HeaderTypes | None = None,
        timeout: timedelta | None = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NEVER,
    ) -> httpx.Response:
        return self.response


def _make_response(
    status_code: int,
    *,
    code: str,
    message: str,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return httpx.Response(
        status_code,
        headers=headers,
        json={"error": {"code": code, "message": message}},
    )


async def _fake_credentials_factory() -> SandboxCredentials:
    return SandboxCredentials(token="test-token", project_id=None, team_id=None)


def _make_request_client(response: httpx.Response) -> SandboxRequestClient:
    return SandboxRequestClient(
        transport=StaticTransport(response),
        credentials_factory=_fake_credentials_factory,
    )


def test_public_error_hierarchy_is_exposed() -> None:
    assert issubclass(SandboxNotFoundError, APIError)
    assert issubclass(PublicAPIError, PublicSandboxError)
    assert issubclass(PublicSandboxAuthError, PublicAPIError)
    assert issubclass(PublicSandboxNotFoundError, PublicAPIError)
    assert issubclass(PublicSandboxPermissionError, PublicAPIError)
    assert issubclass(PublicSandboxRateLimitError, PublicAPIError)
    assert issubclass(PublicSandboxServerError, PublicAPIError)


@pytest.mark.parametrize(
    "status_code,error_type,code,message,headers,retry_after",
    [
        (401, SandboxAuthError, "unauthorized", "Authentication required.", None, None),
        (403, SandboxPermissionError, "forbidden", "Access denied.", None, None),
        (429, SandboxRateLimitError, "rate_limited", "Slow down.", {"retry-after": "120"}, 120),
        (500, SandboxServerError, "internal_server_error", "Something broke.", None, None),
        (404, SandboxNotFoundError, "not_found", "Missing file.", None, None),
    ],
)
def test_request_classifies_sandbox_http_errors(
    status_code: int,
    error_type: type[APIError],
    code: str,
    message: str,
    headers: dict[str, str] | None,
    retry_after: int | None,
) -> None:
    response = _make_response(status_code, code=code, message=message, headers=headers)
    client = _make_request_client(response)

    with pytest.raises(error_type) as exc_info:
        iter_coroutine(client.request("GET", "/v1/sandboxes/test"))

    error = exc_info.value
    assert type(error) is error_type
    assert isinstance(error, SandboxError)
    assert isinstance(error, APIError)
    assert error.response is response
    assert error.status_code == status_code
    assert error.data == {"error": {"code": code, "message": message}}
    assert f"HTTP {status_code}" in str(error)
    assert message in str(error)
    if retry_after is not None:
        assert getattr(error, "retry_after", None) == retry_after
