"""Core business logic for Vercel Projects API."""

from __future__ import annotations

import urllib.parse
from typing import Any

import httpx

from .._http import (
    DEFAULT_API_BASE_URL,
    DEFAULT_TIMEOUT,
    AsyncTransport,
    BaseTransport,
    BlockingTransport,
    JSONBody,
    create_vercel_async_client,
    create_vercel_client,
)
from .._telemetry.tracker import track


def _build_team_params(
    team_id: str | None = None,
    slug: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug
    return params


def _handle_error_response(resp: httpx.Response, operation: str) -> None:
    try:
        data = resp.json()
    except Exception:
        data = {"error": resp.text}
    raise RuntimeError(f"Failed to {operation}: {resp.status_code} {resp.reason_phrase} - {data}")


class _BaseProjectsClient:
    """Base class for Projects API with shared async implementation."""

    _transport: BaseTransport
    _token: str | None

    async def _get_projects(
        self,
        *,
        team_id: str | None = None,
        slug: str | None = None,
        query: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if query:
            params.update(query)
        if team_id:
            params.setdefault("teamId", team_id)
        if slug:
            params.setdefault("slug", slug)

        resp = await self._transport.send(
            "GET",
            "/v10/projects",
            params=params,
            timeout=timeout,
        )

        if resp.status_code != 200:
            _handle_error_response(resp, "get projects")

        return resp.json()

    async def _create_project(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        params = _build_team_params(team_id, slug)

        resp = await self._transport.send(
            "POST",
            "/v11/projects",
            params=params,
            body=JSONBody(body),
            timeout=timeout,
        )

        if not (200 <= resp.status_code < 300):
            _handle_error_response(resp, "create project")

        track("project_create", token=self._token)

        return resp.json()

    async def _update_project(
        self,
        id_or_name: str,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        params = _build_team_params(team_id, slug)

        resp = await self._transport.send(
            "PATCH",
            f"/v9/projects/{urllib.parse.quote(id_or_name, safe='')}",
            params=params,
            body=JSONBody(body),
            timeout=timeout,
        )

        if resp.status_code != 200:
            _handle_error_response(resp, "update project")

        track("project_update", token=self._token)

        return resp.json()

    async def _delete_project(
        self,
        id_or_name: str,
        *,
        team_id: str | None = None,
        slug: str | None = None,
        timeout: float | None = None,
    ) -> None:
        params = _build_team_params(team_id, slug)

        resp = await self._transport.send(
            "DELETE",
            f"/v9/projects/{urllib.parse.quote(id_or_name, safe='')}",
            params=params,
            timeout=timeout,
        )

        if resp.status_code != 204:
            _handle_error_response(resp, "delete project")

        track("project_delete", token=self._token)


class SyncProjectsClient(_BaseProjectsClient):
    def __init__(
        self,
        token: str | None,
        base_url: str = DEFAULT_API_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self._token = token
        client = create_vercel_client(token=token, timeout=timeout, base_url=base_url)
        self._transport = BlockingTransport(client)

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> SyncProjectsClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class AsyncProjectsClient(_BaseProjectsClient):
    def __init__(
        self,
        token: str | None,
        base_url: str = DEFAULT_API_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self._token = token
        client = create_vercel_async_client(token=token, timeout=timeout, base_url=base_url)
        self._transport = AsyncTransport(client)

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> AsyncProjectsClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()


__all__ = [
    "SyncProjectsClient",
    "AsyncProjectsClient",
    "_build_team_params",
    "_handle_error_response",
]
