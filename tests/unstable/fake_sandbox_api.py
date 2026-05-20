"""Reusable fake Sandbox API harness for unstable SDK tests."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

import httpx

from vercel._internal.http.transport import (
    BaseTransport,
    BytesBody,
    HeaderTypes,
    JSONBody,
    QueryParamTypes,
    RawBody,
    RequestBody,
)

if TYPE_CHECKING:
    from vercel._internal.http.transport import AsyncTransport, SyncTransport
    from vercel._internal.unstable.session import Session, SyncSession


@dataclass(frozen=True, slots=True)
class RecordedSandboxRequest:
    method: str
    path: str
    headers: Mapping[str, str]
    query: Mapping[str, Any]
    body: Any
    timeout: timedelta | None = None


@dataclass(frozen=True, slots=True)
class ScriptedSandboxResponse:
    status_code: int = 200
    json: Mapping[str, Any] | None = None
    headers: Mapping[str, str] = field(default_factory=dict)
    delay: float | None = None


def _normalize_path_for_lookup(path: str) -> str:
    if path.startswith(("http://", "https://")):
        from urllib.parse import urlparse

        parsed = urlparse(path)
        return parsed.path or "/"
    return path


def _record_headers(
    headers: HeaderTypes | None,
    *,
    token: str | None,
    body: RequestBody,
) -> Mapping[str, str]:
    request_headers = httpx.Headers(headers)
    if token is not None:
        request_headers.setdefault("authorization", f"Bearer {token}")
    if isinstance(body, JSONBody):
        request_headers.setdefault("content-type", "application/json")
    elif isinstance(body, BytesBody):
        request_headers.setdefault("content-type", body.content_type)
    return dict(request_headers.multi_items())


def _record_query(params: QueryParamTypes | None) -> Mapping[str, Any]:
    if params is None:
        return {}
    return dict(httpx.QueryParams(params).multi_items())


class FakeSandboxAPI(BaseTransport):
    def __init__(self) -> None:
        self.requests: list[RecordedSandboxRequest] = []
        self._responses: deque[ScriptedSandboxResponse] = deque()
        self._path_responses: dict[str, deque[ScriptedSandboxResponse]] = {}

    def install(self, session: Session | SyncSession) -> None:
        from vercel._internal.unstable.session import Session

        if isinstance(session, Session):
            session._transport = cast("AsyncTransport", self)
        else:
            session._transport = cast("SyncTransport", self)

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
        token: str | None = None,
        params: QueryParamTypes | None = None,
        body: RequestBody = None,
        headers: HeaderTypes | None = None,
        timeout: timedelta | None = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
    ) -> httpx.Response:
        _ = (timeout, follow_redirects, stream)
        normalized = _normalize_path_for_lookup(path)
        self.requests.append(
            RecordedSandboxRequest(
                method=method.upper(),
                path=normalized,
                headers=_record_headers(headers, token=token, body=body),
                query=_record_query(params),
                body=_record_body(body),
                timeout=timeout,
            )
        )
        lookup_key = normalized.lstrip("/")
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
        request = httpx.Request(method, f"https://sandbox.vercel.com/{normalized.lstrip('/')}")
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
