"""Reusable fake Sandbox API harness for unstable SDK tests."""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx

from vercel._internal.http.transport import BytesBody, JSONBody, RawBody, RequestBody


@dataclass(frozen=True, slots=True)
class RecordedSandboxRequest:
    method: str
    path: str
    headers: Mapping[str, str]
    query: Mapping[str, Any]
    body: Any


@dataclass(frozen=True, slots=True)
class ScriptedSandboxResponse:
    status_code: int = 200
    json: Mapping[str, Any] | None = None
    headers: Mapping[str, str] = field(default_factory=dict)


class FakeSandboxAPI:
    def __init__(self) -> None:
        self.requests: list[RecordedSandboxRequest] = []
        self._responses: deque[ScriptedSandboxResponse] = deque()

    def script_response(
        self,
        *,
        status_code: int = 200,
        json: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self._responses.append(
            ScriptedSandboxResponse(
                status_code=status_code,
                json=json,
                headers=headers or {},
            )
        )

    async def send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: RequestBody = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
    ) -> httpx.Response:
        _ = (timeout, follow_redirects, stream)
        self.requests.append(
            RecordedSandboxRequest(
                method=method.upper(),
                path=path,
                headers=headers or {},
                query=params or {},
                body=_record_body(body),
            )
        )
        response = self._responses.popleft() if self._responses else ScriptedSandboxResponse()
        request = httpx.Request(method, f"https://sandbox.vercel.com/{path.lstrip('/')}")
        return httpx.Response(
            response.status_code,
            json=response.json,
            headers=response.headers,
            request=request,
        )


def _record_body(body: RequestBody) -> Any:
    if isinstance(body, JSONBody):
        return body.data
    if isinstance(body, BytesBody):
        return body.data
    if isinstance(body, RawBody):
        return body.data
    return None


__all__ = ["FakeSandboxAPI", "RecordedSandboxRequest", "ScriptedSandboxResponse"]
