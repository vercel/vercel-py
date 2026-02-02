"""Projects API client."""

from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING, Any

import httpx

from .._telemetry.tracker import track
from .iter_coroutine import iter_coroutine

if TYPE_CHECKING:
    from .config import ClientConfig
    from .transport import BaseTransport


class BaseProjectsClient:
    """Base projects client with shared async business logic."""

    def __init__(self, transport: BaseTransport, config: ClientConfig):
        self._transport = transport
        self._config = config

    def _build_params(
        self,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        resolved_team_id = team_id or self._config.default_team_id
        resolved_slug = slug or self._config.default_slug
        if resolved_team_id:
            params["teamId"] = resolved_team_id
        if resolved_slug:
            params["slug"] = resolved_slug
        return params

    def _handle_error(self, response: httpx.Response, operation: str) -> None:
        try:
            data = response.json()
        except Exception:
            data = {"error": response.text}
        raise RuntimeError(
            f"Failed to {operation}: {response.status_code} "
            f"{response.reason_phrase} - {data}"
        )

    async def _list(
        self,
        *,
        team_id: str | None = None,
        slug: str | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params = self._build_params(team_id, slug)
        if query:
            params.update(query)

        response = await self._transport.send("GET", "/v10/projects", params=params)

        if response.status_code != 200:
            self._handle_error(response, "list projects")

        return response.json()

    async def _create(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        params = self._build_params(team_id, slug)
        response = await self._transport.send(
            "POST", "/v11/projects", params=params, json=body
        )

        if not (200 <= response.status_code < 300):
            self._handle_error(response, "create project")

        track("project_create", token=self._config.access_token)
        return response.json()

    async def _update(
        self,
        id_or_name: str,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        params = self._build_params(team_id, slug)
        path = f"/v9/projects/{urllib.parse.quote(id_or_name, safe='')}"

        response = await self._transport.send("PATCH", path, params=params, json=body)

        if response.status_code != 200:
            self._handle_error(response, "update project")

        track("project_update", token=self._config.access_token)
        return response.json()

    async def _delete(
        self,
        id_or_name: str,
        *,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> None:
        params = self._build_params(team_id, slug)
        path = f"/v9/projects/{urllib.parse.quote(id_or_name, safe='')}"

        response = await self._transport.send("DELETE", path, params=params)

        if response.status_code != 204:
            self._handle_error(response, "delete project")

        track("project_delete", token=self._config.access_token)


class ProjectsClient(BaseProjectsClient):
    def list(
        self,
        *,
        team_id: str | None = None,
        slug: str | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return iter_coroutine(self._list(team_id=team_id, slug=slug, query=query))

    def create(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        return iter_coroutine(self._create(body=body, team_id=team_id, slug=slug))

    def update(
        self,
        id_or_name: str,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        return iter_coroutine(
            self._update(id_or_name, body=body, team_id=team_id, slug=slug)
        )

    def delete(
        self,
        id_or_name: str,
        *,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> None:
        return iter_coroutine(self._delete(id_or_name, team_id=team_id, slug=slug))


class AsyncProjectsClient(BaseProjectsClient):
    async def list(
        self,
        *,
        team_id: str | None = None,
        slug: str | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._list(team_id=team_id, slug=slug, query=query)

    async def create(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        return await self._create(body=body, team_id=team_id, slug=slug)

    async def update(
        self,
        id_or_name: str,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        return await self._update(id_or_name, body=body, team_id=team_id, slug=slug)

    async def delete(
        self,
        id_or_name: str,
        *,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> None:
        return await self._delete(id_or_name, team_id=team_id, slug=slug)
