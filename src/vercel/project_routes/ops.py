from __future__ import annotations

from collections.abc import Callable, Coroutine
from datetime import timedelta
from typing import TypeVar

from vercel._internal.core.http import DEFAULT_API_BASE_URL
from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel._internal.project_routes.core import (
    AsyncProjectRoutesOpsClient,
    SyncProjectRoutesOpsClient,
)
from vercel.project_routes.types import (
    AddRouteRequestBody,
    AddRouteResponse,
    DeleteRoutesRequestBody,
    DeleteRoutesResponse,
    EditRouteRequestBody,
    GenerateRouteRequestBody,
    GenerateRouteResponse,
    GetRoutesResponse,
    GetRouteVersionsResponse,
    RouteDiff,
    RouteFilter,
    StageRoutesRequestBody,
    UpdateRouteVersionsRequestBody,
    VersionResponse,
)

_T = TypeVar("_T")


def _run_sync(
    operation: Callable[[SyncProjectRoutesOpsClient], Coroutine[None, None, _T]],
    *,
    token: str | None,
    base_url: str,
    timeout: float,
) -> _T:
    with SyncProjectRoutesOpsClient(
        token=token,
        base_url=base_url,
        timeout=timedelta(seconds=timeout),
    ) as client:
        return iter_coroutine(operation(client))


def get_routes(
    *,
    project_id: str,
    version_id: str | None = None,
    q: str | None = None,
    filter: RouteFilter | None = None,
    diff: RouteDiff | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> GetRoutesResponse:
    """Get a project's routing rules, optionally filtered or diffed."""
    return _run_sync(
        lambda client: client.get_routes(
            project_id=project_id,
            version_id=version_id,
            q=q,
            filter=filter,
            diff=diff,
            team_id=team_id,
            slug=slug,
        ),
        token=token,
        base_url=base_url,
        timeout=timeout,
    )


async def get_routes_async(
    *,
    project_id: str,
    version_id: str | None = None,
    q: str | None = None,
    filter: RouteFilter | None = None,
    diff: RouteDiff | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> GetRoutesResponse:
    """Asynchronously get a project's routing rules."""
    async with AsyncProjectRoutesOpsClient(
        token=token,
        base_url=base_url,
        timeout=timedelta(seconds=timeout),
    ) as client:
        return await client.get_routes(
            project_id=project_id,
            version_id=version_id,
            q=q,
            filter=filter,
            diff=diff,
            team_id=team_id,
            slug=slug,
        )


def stage_routes(
    *,
    project_id: str,
    body: StageRoutesRequestBody | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> VersionResponse:
    """Stage routing rules, merging by ID unless ``overwrite`` is true."""
    return _run_sync(
        lambda client: client.stage_routes(
            project_id=project_id,
            body=body,
            team_id=team_id,
            slug=slug,
        ),
        token=token,
        base_url=base_url,
        timeout=timeout,
    )


async def stage_routes_async(
    *,
    project_id: str,
    body: StageRoutesRequestBody | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> VersionResponse:
    """Asynchronously stage routing rules."""
    async with AsyncProjectRoutesOpsClient(
        token=token,
        base_url=base_url,
        timeout=timedelta(seconds=timeout),
    ) as client:
        return await client.stage_routes(
            project_id=project_id,
            body=body,
            team_id=team_id,
            slug=slug,
        )


def add_route(
    *,
    project_id: str,
    body: AddRouteRequestBody,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> AddRouteResponse:
    """Add one routing rule and stage the resulting version."""
    return _run_sync(
        lambda client: client.add_route(
            project_id=project_id,
            body=body,
            team_id=team_id,
            slug=slug,
        ),
        token=token,
        base_url=base_url,
        timeout=timeout,
    )


async def add_route_async(
    *,
    project_id: str,
    body: AddRouteRequestBody,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> AddRouteResponse:
    """Asynchronously add one routing rule."""
    async with AsyncProjectRoutesOpsClient(
        token=token,
        base_url=base_url,
        timeout=timedelta(seconds=timeout),
    ) as client:
        return await client.add_route(
            project_id=project_id,
            body=body,
            team_id=team_id,
            slug=slug,
        )


def delete_routes(
    *,
    project_id: str,
    body: DeleteRoutesRequestBody,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> DeleteRoutesResponse:
    """Delete routing rules by ID and stage the resulting version."""
    return _run_sync(
        lambda client: client.delete_routes(
            project_id=project_id,
            body=body,
            team_id=team_id,
            slug=slug,
        ),
        token=token,
        base_url=base_url,
        timeout=timeout,
    )


async def delete_routes_async(
    *,
    project_id: str,
    body: DeleteRoutesRequestBody,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> DeleteRoutesResponse:
    """Asynchronously delete routing rules by ID."""
    async with AsyncProjectRoutesOpsClient(
        token=token,
        base_url=base_url,
        timeout=timedelta(seconds=timeout),
    ) as client:
        return await client.delete_routes(
            project_id=project_id,
            body=body,
            team_id=team_id,
            slug=slug,
        )


def edit_route(
    *,
    project_id: str,
    route_id: str,
    body: EditRouteRequestBody,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> AddRouteResponse:
    """Replace a routing rule or restore its production value."""
    return _run_sync(
        lambda client: client.edit_route(
            project_id=project_id,
            route_id=route_id,
            body=body,
            team_id=team_id,
            slug=slug,
        ),
        token=token,
        base_url=base_url,
        timeout=timeout,
    )


async def edit_route_async(
    *,
    project_id: str,
    route_id: str,
    body: EditRouteRequestBody,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> AddRouteResponse:
    """Asynchronously replace or restore a routing rule."""
    async with AsyncProjectRoutesOpsClient(
        token=token,
        base_url=base_url,
        timeout=timedelta(seconds=timeout),
    ) as client:
        return await client.edit_route(
            project_id=project_id,
            route_id=route_id,
            body=body,
            team_id=team_id,
            slug=slug,
        )


def generate_route(
    *,
    project_id: str,
    body: GenerateRouteRequestBody,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> GenerateRouteResponse:
    """Generate a routing-rule suggestion from natural language."""
    return _run_sync(
        lambda client: client.generate_route(
            project_id=project_id,
            body=body,
            team_id=team_id,
            slug=slug,
        ),
        token=token,
        base_url=base_url,
        timeout=timeout,
    )


async def generate_route_async(
    *,
    project_id: str,
    body: GenerateRouteRequestBody,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> GenerateRouteResponse:
    """Asynchronously generate a routing-rule suggestion."""
    async with AsyncProjectRoutesOpsClient(
        token=token,
        base_url=base_url,
        timeout=timedelta(seconds=timeout),
    ) as client:
        return await client.generate_route(
            project_id=project_id,
            body=body,
            team_id=team_id,
            slug=slug,
        )


def get_route_versions(
    *,
    project_id: str,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> GetRouteVersionsResponse:
    """Get staged and production routing-rule versions."""
    return _run_sync(
        lambda client: client.get_route_versions(
            project_id=project_id,
            team_id=team_id,
            slug=slug,
        ),
        token=token,
        base_url=base_url,
        timeout=timeout,
    )


async def get_route_versions_async(
    *,
    project_id: str,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> GetRouteVersionsResponse:
    """Asynchronously get routing-rule versions."""
    async with AsyncProjectRoutesOpsClient(
        token=token,
        base_url=base_url,
        timeout=timedelta(seconds=timeout),
    ) as client:
        return await client.get_route_versions(
            project_id=project_id,
            team_id=team_id,
            slug=slug,
        )


def update_route_versions(
    *,
    project_id: str,
    body: UpdateRouteVersionsRequestBody,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> VersionResponse:
    """Promote, restore, or discard a routing-rule version."""
    return _run_sync(
        lambda client: client.update_route_versions(
            project_id=project_id,
            body=body,
            team_id=team_id,
            slug=slug,
        ),
        token=token,
        base_url=base_url,
        timeout=timeout,
    )


async def update_route_versions_async(
    *,
    project_id: str,
    body: UpdateRouteVersionsRequestBody,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> VersionResponse:
    """Asynchronously promote, restore, or discard a version."""
    async with AsyncProjectRoutesOpsClient(
        token=token,
        base_url=base_url,
        timeout=timedelta(seconds=timeout),
    ) as client:
        return await client.update_route_versions(
            project_id=project_id,
            body=body,
            team_id=team_id,
            slug=slug,
        )


__all__ = [
    "add_route",
    "add_route_async",
    "delete_routes",
    "delete_routes_async",
    "edit_route",
    "edit_route_async",
    "generate_route",
    "generate_route_async",
    "get_route_versions",
    "get_route_versions_async",
    "get_routes",
    "get_routes_async",
    "stage_routes",
    "stage_routes_async",
    "update_route_versions",
    "update_route_versions_async",
]
