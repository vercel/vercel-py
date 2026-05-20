"""Reusable fake Sandbox API harness for unstable SDK tests."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx

from vercel._internal.http.transport import BaseTransport, BytesBody, JSONBody, RawBody, RequestBody


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
    delay: float | None = None


class FakeSandboxAPI(BaseTransport):
    def __init__(self) -> None:
        self.requests: list[RecordedSandboxRequest] = []
        self._responses: deque[ScriptedSandboxResponse] = deque()
        self._path_responses: dict[str, deque[ScriptedSandboxResponse]] = {}

    def script_response(
        self,
        *,
        status_code: int = 200,
        json: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        delay: float | None = None,
    ) -> None:
        self._responses.append(
            ScriptedSandboxResponse(
                status_code=status_code,
                json=json,
                headers=headers or {},
                delay=delay,
            )
        )

    def script_response_for_path(
        self,
        path: str,
        *,
        status_code: int = 200,
        json: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        delay: float | None = None,
    ) -> None:
        key = path.lstrip("/")
        if key not in self._path_responses:
            self._path_responses[key] = deque()
        self._path_responses[key].append(
            ScriptedSandboxResponse(
                status_code=status_code,
                json=json,
                headers=headers or {},
                delay=delay,
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
        lookup_key = path.lstrip("/")
        path_deque = self._path_responses.get(lookup_key)
        if path_deque:
            response = path_deque.popleft()
        else:
            response = self._responses.popleft() if self._responses else ScriptedSandboxResponse()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if response.delay is not None and loop is not None:
            await asyncio.sleep(response.delay)
        request = httpx.Request(method, f"https://sandbox.vercel.com/{path.lstrip('/')}")
        return httpx.Response(
            response.status_code,
            json=response.json,
            headers=response.headers,
            request=request,
        )

    async def aclose(self) -> None:
        pass


def _record_body(body: RequestBody) -> Any:
    if isinstance(body, JSONBody):
        return body.data
    if isinstance(body, BytesBody):
        return body.data
    if isinstance(body, RawBody):
        return body.data
    return None


__all__ = ["FakeSandboxAPI", "RecordedSandboxRequest", "ScriptedSandboxResponse"]
