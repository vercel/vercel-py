"""Lazy REST request helper for the clean-room stable SDK surface."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx

from vercel._internal.http import (
    DEFAULT_API_BASE_URL,
    JSONBody,
    RawBody,
    RequestBody,
    RequestClient,
    sync_sleep,
)
from vercel._internal.http.request_client import SleepFn
from vercel._internal.stable.errors import ErrorDetails, error_for_status
from vercel._internal.stable.runtime import AsyncRuntime, SyncRuntime
from vercel.stable.errors import APIResponseError
from vercel.stable.options import SdkOptions


@dataclass(slots=True)
class SdkRequestState:
    request_client: RequestClient | None = None


@dataclass(slots=True)
class SdkClientLineage:
    runtime: SyncRuntime | AsyncRuntime
    root_timeout: float | None
    env: Mapping[str, str]
    request_state: SdkRequestState = field(default_factory=SdkRequestState)


@dataclass(slots=True)
class VercelRequestClient:
    _lineage: SdkClientLineage
    _options: SdkOptions
    _sleep_fn: SleepFn

    async def send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | RequestBody = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        request_client = await self._get_request_client()
        response = await request_client.send(
            method,
            self._build_url(path),
            params=params,
            body=_coerce_request_body(body),
            headers=headers,
            timeout=self._lineage.root_timeout if timeout is None else timeout,
        )
        self._raise_for_status(response)
        return response

    async def send_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | RequestBody = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        response = await self.send(
            method,
            path,
            params=params,
            body=body,
            headers=headers,
            timeout=timeout,
        )
        if not response.content:
            return {}
        payload = response.json()
        if not isinstance(payload, dict):
            raise APIResponseError(
                "Expected a JSON object response from the Vercel API.",
                status_code=response.status_code,
            )
        return payload

    async def _get_request_client(self) -> RequestClient:
        request_client = self._lineage.request_state.request_client
        if request_client is None:
            transport = await self._lineage.runtime.get_transport(
                timeout=self._lineage.root_timeout,
            )
            request_client = RequestClient(
                transport=transport,
                base_headers=self._base_headers(),
                base_params=self._base_params(),
                sleep_fn=self._sleep_fn,
            )
            self._lineage.request_state.request_client = request_client
        return request_client

    def _base_headers(self) -> dict[str, str]:
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self._resolve_token()}",
        }
        headers.update(self._options.headers)
        return headers

    def _base_params(self) -> dict[str, Any] | None:
        params: dict[str, Any] = {}
        if self._options.team_id is not None:
            params["teamId"] = self._options.team_id
        if self._options.team_slug is not None:
            params["slug"] = self._options.team_slug
        return params or None

    def _build_url(self, path: str) -> str:
        base_url = (self._options.base_url or DEFAULT_API_BASE_URL).rstrip("/")
        normalized_path = path.lstrip("/")
        if not normalized_path:
            return f"{base_url}/"
        return f"{base_url}/{normalized_path}"

    def _raise_for_status(self, response: httpx.Response) -> None:
        if response.status_code < 400:
            return

        raise error_for_status(response.status_code, _extract_error_details(response))

    def _resolve_token(self) -> str:
        if self._options.token:
            return self._options.token

        value = self._lineage.env.get("VERCEL_TOKEN")
        if value:
            return value

        raise RuntimeError("Missing API token. Pass token=... or set one of: VERCEL_TOKEN.")


def _coerce_request_body(body: dict[str, Any] | RequestBody) -> RequestBody:
    if body is None:
        return None
    if isinstance(body, (JSONBody, RawBody)):
        return body
    return JSONBody(body)


def create_sync_request_client(
    *,
    lineage: SdkClientLineage,
    options: SdkOptions,
) -> VercelRequestClient:
    return VercelRequestClient(
        _lineage=lineage,
        _options=options,
        _sleep_fn=sync_sleep,
    )


def create_async_request_client(
    *,
    lineage: SdkClientLineage,
    options: SdkOptions,
) -> VercelRequestClient:
    return VercelRequestClient(
        _lineage=lineage,
        _options=options,
        _sleep_fn=asyncio.sleep,
    )


def _extract_error_details(response: httpx.Response) -> ErrorDetails:
    try:
        payload = response.json()
    except ValueError:
        payload = None

    if isinstance(payload, dict):
        candidates = [payload.get("message"), payload.get("error")]
        error_payload = payload.get("error")
        if isinstance(error_payload, dict):
            candidates.extend([error_payload.get("message"), error_payload.get("code")])
        for candidate in candidates:
            if isinstance(candidate, str) and candidate:
                return ErrorDetails(
                    message=candidate,
                    error_code=_extract_error_code(payload),
                    request_id=_extract_request_id(payload, response),
                    trace_id=_extract_trace_id(payload, response),
                    payload=payload,
                )

    reason = response.reason_phrase or "HTTP error"
    return ErrorDetails(
        message=f"{response.status_code} {reason}",
        error_code=_extract_error_code(payload),
        request_id=_extract_request_id(payload, response),
        trace_id=_extract_trace_id(payload, response),
        payload=payload if isinstance(payload, dict) else None,
    )


def _extract_error_code(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None

    direct_code = payload.get("code")
    if isinstance(direct_code, str) and direct_code:
        return direct_code

    nested_error = payload.get("error")
    if isinstance(nested_error, dict):
        nested_code = nested_error.get("code")
        if isinstance(nested_code, str) and nested_code:
            return nested_code
    return None


def _extract_request_id(payload: object, response: httpx.Response) -> str | None:
    if isinstance(payload, dict):
        for key in ("requestId", "request_id"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
        nested_error = payload.get("error")
        if isinstance(nested_error, dict):
            for key in ("requestId", "request_id"):
                value = nested_error.get(key)
                if isinstance(value, str) and value:
                    return value

    for header in ("x-request-id", "x-vercel-id"):
        value = response.headers.get(header)
        if value:
            return value
    return None


def _extract_trace_id(payload: object, response: httpx.Response) -> str | None:
    if isinstance(payload, dict):
        for key in ("traceId", "trace_id"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
        nested_error = payload.get("error")
        if isinstance(nested_error, dict):
            for key in ("traceId", "trace_id"):
                value = nested_error.get(key)
                if isinstance(value, str) and value:
                    return value

    return response.headers.get("x-vercel-trace-id")


__all__ = [
    "SdkClientLineage",
    "SdkRequestState",
    "VercelRequestClient",
    "create_sync_request_client",
    "create_async_request_client",
]
