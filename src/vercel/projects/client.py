"""Projects API client classes."""

from __future__ import annotations

from typing import Any

from .._http import (
    DEFAULT_API_BASE_URL,
    DEFAULT_TIMEOUT,
    AsyncTransport,
    BlockingTransport,
    HTTPConfig,
    create_vercel_async_client,
    create_vercel_client,
    iter_coroutine,
)
from ._core import _BaseProjectsClient


class ProjectsClient(_BaseProjectsClient):
    """Synchronous client for Vercel Projects API."""

    def __init__(
        self,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ):
        self._token = access_token
        effective_base_url = base_url or DEFAULT_API_BASE_URL
        effective_timeout = timeout or DEFAULT_TIMEOUT
        client = create_vercel_client(token=access_token, timeout=effective_timeout)
        config = HTTPConfig(base_url=effective_base_url, timeout=effective_timeout)
        self._transport = BlockingTransport(config, client=client)

    def get_projects(
        self,
        *,
        team_id: str | None = None,
        slug: str | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Retrieve a list of projects."""
        return iter_coroutine(self._get_projects(team_id=team_id, slug=slug, query=query))

    def create_project(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        """Create a new project."""
        return iter_coroutine(self._create_project(body=body, team_id=team_id, slug=slug))

    def update_project(
        self,
        *,
        id_or_name: str,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        """Update an existing project by id or name."""
        return iter_coroutine(
            self._update_project(id_or_name, body=body, team_id=team_id, slug=slug)
        )

    def delete_project(
        self,
        *,
        id_or_name: str,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> None:
        """Delete a project by id or name."""
        return iter_coroutine(self._delete_project(id_or_name, team_id=team_id, slug=slug))


class AsyncProjectsClient(_BaseProjectsClient):
    """Asynchronous client for Vercel Projects API."""

    def __init__(
        self,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ):
        self._token = access_token
        effective_base_url = base_url or DEFAULT_API_BASE_URL
        effective_timeout = timeout or DEFAULT_TIMEOUT
        client = create_vercel_async_client(token=access_token, timeout=effective_timeout)
        config = HTTPConfig(base_url=effective_base_url, timeout=effective_timeout)
        self._transport = AsyncTransport(config, client=client)

    async def get_projects(
        self,
        *,
        team_id: str | None = None,
        slug: str | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Retrieve a list of projects."""
        return await self._get_projects(team_id=team_id, slug=slug, query=query)

    async def create_project(
        self,
        *,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        """Create a new project."""
        return await self._create_project(body=body, team_id=team_id, slug=slug)

    async def update_project(
        self,
        *,
        id_or_name: str,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        """Update an existing project by id or name."""
        return await self._update_project(id_or_name, body=body, team_id=team_id, slug=slug)

    async def delete_project(
        self,
        *,
        id_or_name: str,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> None:
        """Delete a project by id or name."""
        return await self._delete_project(id_or_name, team_id=team_id, slug=slug)


__all__ = [
    "ProjectsClient",
    "AsyncProjectsClient",
]
