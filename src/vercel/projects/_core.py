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
    HTTPConfig,
    JSONBody,
)
from .._telemetry.tracker import track


def _build_team_params(
    team_id: str | None = None,
    slug: str | None = None,
) -> dict[str, Any]:
    """Build query params for team scoping."""
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug
    return params


def _handle_error_response(
    resp: httpx.Response,
    operation: str,
) -> None:
    """Raise RuntimeError with formatted error message if response indicates failure."""
    try:
        data = resp.json()
    except Exception:
        data = {"error": resp.text}
    raise RuntimeError(
        f"Failed to {operation}: {resp.status_code} {resp.reason_phrase} - {data}"
    )


class _BaseProjectsClient:
    """
    Base class containing shared business logic for Projects API operations.

    All methods are async and use the abstract _transport property for HTTP requests.
    Subclasses must provide a concrete transport implementation.
    """

    _transport: BaseTransport

    async def _get_projects(
        self,
        *,
        team_id: str | None = None,
        slug: str | None = None,
        query: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Retrieve a list of projects.

        Parameters:
        - team_id: optional team to scope the query (maps to teamId)
        - slug: optional team slug (maps to slug)
        - query: additional query params (e.g. search, limit, repo, from, etc.)
        - timeout: request timeout in seconds (uses client default if not specified)

        Returns: dict with keys like {"projects": [...], "pagination": {...}}
        """
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
        """Create a new project.

        Parameters:
        - body: JSON payload (must include at least name)
        - team_id: optional team ID (maps to teamId)
        - slug: optional team slug (maps to slug)
        - timeout: request timeout in seconds (uses client default if not specified)

        Returns: dict with the created project data
        """
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

        # Track telemetry
        track("project_create", token=self._transport._config.token)

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
        """Update an existing project by id or name.

        Parameters:
        - id_or_name: project ID or name
        - body: JSON payload with fields to update
        - team_id: optional team ID (maps to teamId)
        - slug: optional team slug (maps to slug)
        - timeout: request timeout in seconds (uses client default if not specified)

        Returns: dict with the updated project data
        """
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

        # Track telemetry
        track("project_update", token=self._transport._config.token)

        return resp.json()

    async def _delete_project(
        self,
        id_or_name: str,
        *,
        team_id: str | None = None,
        slug: str | None = None,
        timeout: float | None = None,
    ) -> None:
        """Delete a project by id or name.

        Parameters:
        - id_or_name: project ID or name
        - team_id: optional team ID (maps to teamId)
        - slug: optional team slug (maps to slug)
        - timeout: request timeout in seconds (uses client default if not specified)

        Returns: None on success (204)
        """
        params = _build_team_params(team_id, slug)

        resp = await self._transport.send(
            "DELETE",
            f"/v9/projects/{urllib.parse.quote(id_or_name, safe='')}",
            params=params,
            timeout=timeout,
        )

        if resp.status_code != 204:
            _handle_error_response(resp, "delete project")

        # Track telemetry
        track("project_delete", token=self._transport._config.token)


class SyncProjectsClient(_BaseProjectsClient):
    """Sync client for Projects API operations."""

    def __init__(
        self,
        token: str | None,
        base_url: str = DEFAULT_API_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        config = HTTPConfig(
            base_url=base_url,
            timeout=timeout,
            token=token,
        )
        self._transport = BlockingTransport(config)


class AsyncProjectsClient(_BaseProjectsClient):
    """Async client for Projects API operations."""

    def __init__(
        self,
        token: str | None,
        base_url: str = DEFAULT_API_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        config = HTTPConfig(
            base_url=base_url,
            timeout=timeout,
            token=token,
        )
        self._transport = AsyncTransport(config)


__all__ = [
    "SyncProjectsClient",
    "AsyncProjectsClient",
    "_build_team_params",
    "_handle_error_response",
]
