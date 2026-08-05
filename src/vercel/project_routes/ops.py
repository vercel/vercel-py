from __future__ import annotations

from collections.abc import Callable, Coroutine, Sequence
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
    AddRouteResult,
    DeleteRoutesRequestBody,
    DeleteRoutesResult,
    EditRouteRequestBody,
    EditRouteResult,
    GeneratedRoute,
    GenerateRouteCurrent,
    GenerateRouteRequestBody,
    GetRoutesResult,
    Placement,
    Position,
    ProjectRoute,
    RedirectRoute,
    RewriteRoute,
    RouteDiff,
    RouteInput,
    RouteSpec,
    RouteType,
    RouteVersion,
    SetStatusRoute,
    StagedRouteInput,
    StageRoutesRequestBody,
    UpdateRouteVersionRequestBody,
    VersionAction,
)

_T = TypeVar("_T")


def _route_input(route: RouteSpec) -> RouteInput:
    if isinstance(route, RewriteRoute | RedirectRoute | SetStatusRoute):
        return route.to_route_input()
    return route


def _validate_get_routes(
    *, search: str | None, route_type: RouteType | None, diff: RouteDiff | None
) -> None:
    if diff is not None and (search is not None or route_type is not None):
        raise ValueError("diff cannot be combined with search or route_type.")


def _stage_body(
    routes: Sequence[ProjectRoute | StagedRouteInput], overwrite: bool | None
) -> StageRoutesRequestBody:
    body: StageRoutesRequestBody = {
        "routes": [
            route.to_staged_input() if isinstance(route, ProjectRoute) else route
            for route in routes
        ]
    }
    if overwrite is not None:
        body["overwrite"] = overwrite
    return body


def _add_body(
    route: RouteSpec, placement: Placement | None, reference_id: str | None
) -> AddRouteRequestBody:
    if placement in ("before", "after") and reference_id is None:
        raise ValueError(f"placement={placement!r} requires reference_id.")
    if reference_id is not None and placement not in ("before", "after"):
        raise ValueError('reference_id requires placement="before" or "after".')
    body: AddRouteRequestBody = {"route": _route_input(route)}
    if placement is not None:
        position: Position = {"placement": placement}
        if reference_id is not None:
            position["referenceId"] = reference_id
        body["position"] = position
    return body


def _delete_body(route_ids: Sequence[str]) -> DeleteRoutesRequestBody:
    if not route_ids:
        raise ValueError("route_ids must not be empty.")
    return {"routeIds": list(route_ids)}


def _edit_body(route: RouteSpec | None, restore: bool) -> EditRouteRequestBody:
    if (route is None) == (not restore):
        raise ValueError("Pass either route or restore=True, not both.")
    if route is not None:
        return {"route": _route_input(route)}
    return {"restore": True}


def _generate_body(
    prompt: str, current_route: GeneratedRoute | GenerateRouteCurrent | None
) -> GenerateRouteRequestBody:
    body: GenerateRouteRequestBody = {"prompt": prompt}
    if isinstance(current_route, GeneratedRoute):
        body["currentRoute"] = current_route.to_current_route()
    elif current_route is not None:
        body["currentRoute"] = current_route
    return body


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
    search: str | None = None,
    route_type: RouteType | None = None,
    diff: RouteDiff | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> GetRoutesResult:
    """Get a project's routing rules, optionally searched, filtered, or diffed."""
    _validate_get_routes(search=search, route_type=route_type, diff=diff)
    return _run_sync(
        lambda client: client.get_routes(
            project_id=project_id,
            version_id=version_id,
            search=search,
            route_type=route_type,
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
    search: str | None = None,
    route_type: RouteType | None = None,
    diff: RouteDiff | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> GetRoutesResult:
    """Asynchronously get a project's routing rules."""
    _validate_get_routes(search=search, route_type=route_type, diff=diff)
    async with AsyncProjectRoutesOpsClient(
        token=token,
        base_url=base_url,
        timeout=timedelta(seconds=timeout),
    ) as client:
        return await client.get_routes(
            project_id=project_id,
            version_id=version_id,
            search=search,
            route_type=route_type,
            diff=diff,
            team_id=team_id,
            slug=slug,
        )


def stage_routes(
    *,
    project_id: str,
    routes: Sequence[ProjectRoute | StagedRouteInput],
    overwrite: bool | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> RouteVersion:
    """Stage routing rules, merging by ID unless ``overwrite`` is true."""
    body = _stage_body(routes, overwrite)
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
    routes: Sequence[ProjectRoute | StagedRouteInput],
    overwrite: bool | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> RouteVersion:
    """Asynchronously stage routing rules."""
    body = _stage_body(routes, overwrite)
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
    route: RouteSpec,
    placement: Placement | None = None,
    reference_id: str | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> AddRouteResult:
    """Add one routing rule and stage the resulting version.

    ``placement`` positions the route ("start", "end", "before", "after");
    "before" and "after" require ``reference_id``.
    """
    body = _add_body(route, placement, reference_id)
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
    route: RouteSpec,
    placement: Placement | None = None,
    reference_id: str | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> AddRouteResult:
    """Asynchronously add one routing rule."""
    body = _add_body(route, placement, reference_id)
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
    route_ids: Sequence[str],
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> DeleteRoutesResult:
    """Delete routing rules by ID and stage the resulting version."""
    body = _delete_body(route_ids)
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
    route_ids: Sequence[str],
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> DeleteRoutesResult:
    """Asynchronously delete routing rules by ID."""
    body = _delete_body(route_ids)
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
    route: RouteSpec | None = None,
    restore: bool = False,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> EditRouteResult:
    """Replace a routing rule, or restore its production value with ``restore=True``."""
    body = _edit_body(route, restore)
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
    route: RouteSpec | None = None,
    restore: bool = False,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> EditRouteResult:
    """Asynchronously replace or restore a routing rule."""
    body = _edit_body(route, restore)
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
    prompt: str,
    current_route: GeneratedRoute | GenerateRouteCurrent | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> GeneratedRoute:
    """Generate a routing-rule suggestion from natural language.

    Pass a previous ``GeneratedRoute`` as ``current_route`` to refine it.
    """
    body = _generate_body(prompt, current_route)
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
    prompt: str,
    current_route: GeneratedRoute | GenerateRouteCurrent | None = None,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> GeneratedRoute:
    """Asynchronously generate a routing-rule suggestion."""
    body = _generate_body(prompt, current_route)
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
) -> list[RouteVersion]:
    """Get routing-rule versions, staged first, then newest production first."""
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
) -> list[RouteVersion]:
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


def update_route_version(
    *,
    project_id: str,
    version_id: str,
    action: VersionAction,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> RouteVersion:
    """Promote, restore, or discard a routing-rule version."""
    body: UpdateRouteVersionRequestBody = {"id": version_id, "action": action}
    return _run_sync(
        lambda client: client.update_route_version(
            project_id=project_id,
            body=body,
            team_id=team_id,
            slug=slug,
        ),
        token=token,
        base_url=base_url,
        timeout=timeout,
    )


async def update_route_version_async(
    *,
    project_id: str,
    version_id: str,
    action: VersionAction,
    token: str | None = None,
    team_id: str | None = None,
    slug: str | None = None,
    base_url: str = DEFAULT_API_BASE_URL,
    timeout: float = 30.0,
) -> RouteVersion:
    """Asynchronously promote, restore, or discard a version."""
    body: UpdateRouteVersionRequestBody = {"id": version_id, "action": action}
    async with AsyncProjectRoutesOpsClient(
        token=token,
        base_url=base_url,
        timeout=timedelta(seconds=timeout),
    ) as client:
        return await client.update_route_version(
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
    "update_route_version",
    "update_route_version_async",
]
