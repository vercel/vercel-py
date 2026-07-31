from __future__ import annotations

from collections.abc import Sequence

from vercel._internal.core.http import DEFAULT_API_BASE_URL
from vercel.project_routes.ops import (
    add_route,
    add_route_async,
    delete_routes,
    delete_routes_async,
    edit_route,
    edit_route_async,
    generate_route,
    generate_route_async,
    get_route_versions,
    get_route_versions_async,
    get_routes,
    get_routes_async,
    stage_routes,
    stage_routes_async,
    update_route_versions,
    update_route_versions_async,
)
from vercel.project_routes.types import (
    AddRouteResponse,
    DeleteRoutesResponse,
    GenerateRouteCurrent,
    GenerateRouteResponse,
    GetRoutesResponse,
    GetRouteVersionsResponse,
    Position,
    RewriteRoute,
    RouteDiff,
    RouteFilter,
    RouteInput,
    StagedRouteInput,
    VersionAction,
    VersionResponse,
)


class ProjectRoutesClient:
    """Synchronous client for project-level routing rules."""

    def __init__(
        self,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ) -> None:
        self._access_token = access_token
        self._base_url = base_url or DEFAULT_API_BASE_URL
        self._timeout = 30.0 if timeout is None else timeout

    def get_routes(
        self,
        *,
        project_id: str,
        version_id: str | None = None,
        q: str | None = None,
        filter: RouteFilter | None = None,
        diff: RouteDiff | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> GetRoutesResponse:
        return get_routes(
            project_id=project_id,
            version_id=version_id,
            q=q,
            filter=filter,
            diff=diff,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    def stage_routes(
        self,
        *,
        project_id: str,
        routes: Sequence[StagedRouteInput] | None = None,
        overwrite: bool | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> VersionResponse:
        return stage_routes(
            project_id=project_id,
            routes=routes,
            overwrite=overwrite,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    def add_route(
        self,
        *,
        project_id: str,
        route: RewriteRoute | RouteInput,
        position: Position | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> AddRouteResponse:
        return add_route(
            project_id=project_id,
            route=route,
            position=position,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    def delete_routes(
        self,
        *,
        project_id: str,
        route_ids: Sequence[str],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> DeleteRoutesResponse:
        return delete_routes(
            project_id=project_id,
            route_ids=route_ids,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    def edit_route(
        self,
        *,
        project_id: str,
        route_id: str,
        route: RewriteRoute | RouteInput | None = None,
        restore: bool | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> AddRouteResponse:
        return edit_route(
            project_id=project_id,
            route_id=route_id,
            route=route,
            restore=restore,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    def generate_route(
        self,
        *,
        project_id: str,
        prompt: str,
        current_route: GenerateRouteCurrent | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> GenerateRouteResponse:
        return generate_route(
            project_id=project_id,
            prompt=prompt,
            current_route=current_route,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    def get_route_versions(
        self,
        *,
        project_id: str,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> GetRouteVersionsResponse:
        return get_route_versions(
            project_id=project_id,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    def update_route_versions(
        self,
        *,
        project_id: str,
        version_id: str,
        action: VersionAction,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> VersionResponse:
        return update_route_versions(
            project_id=project_id,
            version_id=version_id,
            action=action,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )


class AsyncProjectRoutesClient:
    """Asynchronous client for project-level routing rules."""

    def __init__(
        self,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ) -> None:
        self._access_token = access_token
        self._base_url = base_url or DEFAULT_API_BASE_URL
        self._timeout = 30.0 if timeout is None else timeout

    async def get_routes(
        self,
        *,
        project_id: str,
        version_id: str | None = None,
        q: str | None = None,
        filter: RouteFilter | None = None,
        diff: RouteDiff | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> GetRoutesResponse:
        return await get_routes_async(
            project_id=project_id,
            version_id=version_id,
            q=q,
            filter=filter,
            diff=diff,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    async def stage_routes(
        self,
        *,
        project_id: str,
        routes: Sequence[StagedRouteInput] | None = None,
        overwrite: bool | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> VersionResponse:
        return await stage_routes_async(
            project_id=project_id,
            routes=routes,
            overwrite=overwrite,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    async def add_route(
        self,
        *,
        project_id: str,
        route: RewriteRoute | RouteInput,
        position: Position | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> AddRouteResponse:
        return await add_route_async(
            project_id=project_id,
            route=route,
            position=position,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    async def delete_routes(
        self,
        *,
        project_id: str,
        route_ids: Sequence[str],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> DeleteRoutesResponse:
        return await delete_routes_async(
            project_id=project_id,
            route_ids=route_ids,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    async def edit_route(
        self,
        *,
        project_id: str,
        route_id: str,
        route: RewriteRoute | RouteInput | None = None,
        restore: bool | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> AddRouteResponse:
        return await edit_route_async(
            project_id=project_id,
            route_id=route_id,
            route=route,
            restore=restore,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    async def generate_route(
        self,
        *,
        project_id: str,
        prompt: str,
        current_route: GenerateRouteCurrent | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> GenerateRouteResponse:
        return await generate_route_async(
            project_id=project_id,
            prompt=prompt,
            current_route=current_route,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    async def get_route_versions(
        self,
        *,
        project_id: str,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> GetRouteVersionsResponse:
        return await get_route_versions_async(
            project_id=project_id,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    async def update_route_versions(
        self,
        *,
        project_id: str,
        version_id: str,
        action: VersionAction,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> VersionResponse:
        return await update_route_versions_async(
            project_id=project_id,
            version_id=version_id,
            action=action,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )


__all__ = ["AsyncProjectRoutesClient", "ProjectRoutesClient"]
