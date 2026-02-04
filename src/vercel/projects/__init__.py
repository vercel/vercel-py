"""Vercel Projects API - synchronous functions."""

from __future__ import annotations

from typing import Any

from .._http import (
    DEFAULT_API_BASE_URL,
    DEFAULT_TIMEOUT,
    iter_coroutine,
)
from ._core import SyncProjectsClient


def get_projects(
    *,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    query: dict[str, Any] | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Retrieve a list of projects.

    Parameters:
    - token: Vercel API token (defaults to env VERCEL_TOKEN)
    - team_id: optional team to scope the query (maps to teamId)
    - slug: optional team slug (maps to slug)
    - query: additional query params (e.g. search, limit, repo, from, etc.)
    - base_url: override API base URL
    - timeout: request timeout in seconds

    Returns: dict with keys like {"projects": [...], "pagination": {...}}
    """
    with SyncProjectsClient(token, base_url, timeout) as client:
        return iter_coroutine(client._get_projects(team_id=team_id, slug=slug, query=query))


def create_project(
    *,
    body: dict[str, Any],
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Create a new project.

    body: JSON payload (must include at least name)
    Optional query params: team_id -> teamId, slug -> slug
    """
    with SyncProjectsClient(token, base_url, timeout) as client:
        return iter_coroutine(client._create_project(body=body, team_id=team_id, slug=slug))


def update_project(
    id_or_name: str,
    *,
    body: dict[str, Any],
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Update an existing project by id or name."""
    with SyncProjectsClient(token, base_url, timeout) as client:
        return iter_coroutine(
            client._update_project(id_or_name, body=body, team_id=team_id, slug=slug)
        )


def delete_project(
    id_or_name: str,
    *,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT,
) -> None:
    """Delete a project by id or name. Returns None on success (204)."""
    with SyncProjectsClient(token, base_url, timeout) as client:
        return iter_coroutine(client._delete_project(id_or_name, team_id=team_id, slug=slug))


__all__ = ["get_projects", "create_project", "update_project", "delete_project"]
