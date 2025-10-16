from __future__ import annotations

import os
from typing import Any
import urllib.parse

import httpx

__all__ = [
    "get_projects",
    "create_project",
    "update_project",
    "delete_project",
]


DEFAULT_API_BASE_URL = "https://api.vercel.com"
DEFAULT_TIMEOUT = 60.0


def _require_token(token: str | None) -> str:
    env_token = os.getenv("VERCEL_TOKEN")
    resolved = token or env_token
    if not resolved:
        raise RuntimeError("Missing Vercel API token. Pass token=... or set VERCEL_TOKEN.")
    return resolved


def _request(
    method: str,
    path: str,
    *,
    token: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    params: dict[str, Any] | None = None,
    json: Any | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> httpx.Response:
    bearer = _require_token(token)
    url = base_url.rstrip("/") + path
    with httpx.Client(timeout=httpx.Timeout(timeout)) as client:
        resp = client.request(
            method,
            url,
            params=params or None,
            json=json,
            headers={
                "authorization": f"Bearer {bearer}",
                "accept": "application/json",
                "content-type": "application/json",
            },
        )
        return resp


async def _request_async(
    method: str,
    path: str,
    *,
    token: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    params: dict[str, Any] | None = None,
    json: Any | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> httpx.Response:
    bearer = _require_token(token)
    url = base_url.rstrip("/") + path
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        resp = await client.request(
            method,
            url,
            params=params or None,
            json=json,
            headers={
                "authorization": f"Bearer {bearer}",
                "accept": "application/json",
            },
        )
        return resp


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
    params: dict[str, Any] = {}
    if query:
        params.update(query)
    if team_id:
        params.setdefault("teamId", team_id)
    if slug:
        params.setdefault("slug", slug)

    resp = _request(
        "GET",
        "/v10/projects",
        token=token,
        base_url=base_url,
        params=params,
        timeout=timeout,
    )
    if resp.status_code != 200:
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text}
        raise RuntimeError(
            f"Failed to get projects: {resp.status_code} {resp.reason_phrase} - {data}"
        )
    return resp.json()


async def get_projects_async(
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
    params: dict[str, Any] = {}
    if query:
        params.update(query)
    if team_id:
        params.setdefault("teamId", team_id)
    if slug:
        params.setdefault("slug", slug)

    resp = await _request_async(
        "GET",
        "/v10/projects",
        token=token,
        base_url=base_url,
        params=params,
        timeout=timeout,
    )
    if resp.status_code != 200:
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text}
        raise RuntimeError(
            f"Failed to get projects: {resp.status_code} {resp.reason_phrase} - {data}"
        )
    return resp.json()


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
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug

    resp = _request(
        "POST",
        "/v11/projects",
        token=token,
        base_url=base_url,
        params=params,
        json=body,
        timeout=timeout,
    )
    if not (200 <= resp.status_code < 300):
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text}
        raise RuntimeError(
            f"Failed to create project: {resp.status_code} {resp.reason_phrase} - {data}"
        )
    return resp.json()


async def create_project_async(
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
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug

    resp = await _request_async(
        "POST",
        "/v11/projects",
        token=token,
        base_url=base_url,
        params=params,
        json=body,
        timeout=timeout,
    )
    if not (200 <= resp.status_code < 300):
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text}
        raise RuntimeError(
            f"Failed to create project: {resp.status_code} {resp.reason_phrase} - {data}"
        )
    return resp.json()


def update_project(
    id_or_name: str,
    *,
    body: dict[str, Any],
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Update an existing project by id or name."""
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug

    resp = _request(
        "PATCH",
        f"/v9/projects/{urllib.parse.quote(id_or_name, safe='')}",
        token=token,
        base_url=base_url,
        params=params,
        json=body,
        timeout=timeout,
    )
    if resp.status_code != 200:
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text}
        raise RuntimeError(
            f"Failed to update project: {resp.status_code} {resp.reason_phrase} - {data}"
        )
    return resp.json()


async def update_project_async(
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
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug

    resp = await _request_async(
        "PATCH",
        f"/v9/projects/{urllib.parse.quote(id_or_name, safe='')}",
        token=token,
        base_url=base_url,
        params=params,
        json=body,
        timeout=timeout,
    )
    if resp.status_code != 200:
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text}
        raise RuntimeError(
            f"Failed to update project: {resp.status_code} {resp.reason_phrase} - {data}"
        )
    return resp.json()


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
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug

    resp = _request(
        "DELETE",
        f"/v9/projects/{urllib.parse.quote(id_or_name, safe='')}",
        token=token,
        base_url=base_url,
        params=params,
        timeout=timeout,
    )
    if resp.status_code != 204:
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text}
        raise RuntimeError(
            f"Failed to delete project: {resp.status_code} {resp.reason_phrase} - {data}"
        )
    return None


async def delete_project_async(
    id_or_name: str,
    *,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT,
) -> None:
    """Delete a project by id or name. Returns None on success (204)."""
    params: dict[str, Any] = {}
    if team_id:
        params["teamId"] = team_id
    if slug:
        params["slug"] = slug

    resp = await _request_async(
        "DELETE",
        f"/v9/projects/{urllib.parse.quote(id_or_name, safe='')}",
        token=token,
        base_url=base_url,
        params=params,
        timeout=timeout,
    )
    if resp.status_code != 204:
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text}
        raise RuntimeError(
            f"Failed to delete project: {resp.status_code} {resp.reason_phrase} - {data}"
        )
    return None
