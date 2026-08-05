from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar, cast
from urllib.parse import quote

from vercel._internal.core.http import (
    DEFAULT_API_BASE_URL,
    AsyncTransport,
    BaseTransport,
    JSONBody,
    ReadResponsePolicy,
    SyncTransport,
    TransportOptions,
    create_base_async_client,
    create_base_client,
    extract_structured_error,
)
from vercel.project_routes.errors import ProjectRoutesError
from vercel.project_routes.types import (
    AddRouteRequestBody,
    AddRouteResult,
    DeleteRoutesRequestBody,
    DeleteRoutesResult,
    EditRouteRequestBody,
    EditRouteResult,
    GeneratedPathCondition,
    GeneratedRoute,
    GeneratedRouteAction,
    GeneratedRouteCondition,
    GeneratedRouteHeader,
    GenerateRouteRequestBody,
    GetRoutesResult,
    ProjectRoute,
    RouteDefinition,
    RouteDiff,
    RouteLimit,
    RouteType,
    RouteVersion,
    StageRoutesRequestBody,
    UpdateRouteVersionRequestBody,
)

_T = TypeVar("_T")


def _require_token(token: str | None) -> str:
    resolved = token or os.getenv("VERCEL_TOKEN")
    if not resolved:
        raise ProjectRoutesError(
            "Missing Vercel API token. Pass access_token=... or set VERCEL_TOKEN."
        )
    return resolved


def _params(*, team_id: str | None, slug: str | None) -> dict[str, str | bool]:
    params: dict[str, str | bool] = {}
    if team_id is not None:
        params["teamId"] = team_id
    if slug is not None:
        params["slug"] = slug
    return params


def _path_part(value: str) -> str:
    return quote(value, safe="")


def _error_code(parsed: object | None) -> str | None:
    if isinstance(parsed, dict):
        error = parsed.get("error")
        if isinstance(error, dict) and isinstance(error.get("code"), str):
            return cast(str, error["code"])
    return None


def _parse(payload: dict[str, Any], parser: Callable[[dict[str, Any]], _T]) -> _T:
    try:
        return parser(payload)
    except (KeyError, TypeError, ValueError) as exc:
        raise ProjectRoutesError(
            "Project routes API returned an unexpected response body.",
            response_body=payload,
        ) from exc


def _parse_version(data: dict[str, Any]) -> RouteVersion:
    return RouteVersion(
        id=data["id"],
        last_modified=datetime.fromtimestamp(data["lastModified"] / 1000, tz=timezone.utc),
        created_by=data["createdBy"],
        s3_key=data["s3Key"],
        is_staging=data.get("isStaging"),
        is_live=data.get("isLive"),
        rule_count=data.get("ruleCount"),
        alias=data.get("alias"),
    )


def _parse_route(data: dict[str, Any]) -> ProjectRoute:
    return ProjectRoute(
        id=data["id"],
        name=data["name"],
        route=cast(RouteDefinition, data["route"]),
        description=data.get("description"),
        enabled=data.get("enabled"),
        staged=data.get("staged"),
        raw_src=data.get("rawSrc"),
        raw_dest=data.get("rawDest"),
        src_syntax=data.get("srcSyntax"),
        route_type=data.get("routeType"),
    )


def _parse_get_routes(payload: dict[str, Any]) -> GetRoutesResult:
    version = payload.get("version")
    limit = payload.get("limit")
    return GetRoutesResult(
        routes=[_parse_route(route) for route in payload.get("routes", [])],
        version=_parse_version(version) if version is not None else None,
        limit=(
            RouteLimit(max_routes=limit["maxRoutes"], current_routes=limit["currentRoutes"])
            if limit is not None
            else None
        ),
        diff_count=payload.get("diffCount"),
    )


def _parse_add_route(payload: dict[str, Any]) -> AddRouteResult:
    return AddRouteResult(
        route=_parse_route(payload["route"]),
        version=_parse_version(payload["version"]),
    )


def _parse_edit_route(payload: dict[str, Any]) -> EditRouteResult:
    route = payload.get("route")
    return EditRouteResult(
        version=_parse_version(payload["version"]),
        route=_parse_route(route) if route is not None else None,
    )


def _parse_delete_routes(payload: dict[str, Any]) -> DeleteRoutesResult:
    return DeleteRoutesResult(
        deleted_count=payload["deletedCount"],
        version=_parse_version(payload["version"]),
    )


def _parse_generated_header(data: dict[str, Any]) -> GeneratedRouteHeader:
    return GeneratedRouteHeader(key=data["key"], op=data["op"], value=data.get("value"))


def _parse_generated_action(data: dict[str, Any]) -> GeneratedRouteAction:
    return GeneratedRouteAction(
        type=data["type"],
        sub_type=data.get("subType"),
        dest=data.get("dest"),
        status=data.get("status"),
        headers=[_parse_generated_header(header) for header in data.get("headers", [])],
    )


def _parse_generated_condition(data: dict[str, Any]) -> GeneratedRouteCondition:
    return GeneratedRouteCondition(
        field=data["field"],
        operator=data["operator"],
        missing=data.get("missing", False),
        key=data.get("key"),
        value=data.get("value"),
    )


def _parse_generated_route(data: dict[str, Any]) -> GeneratedRoute:
    path_condition = data["pathCondition"]
    return GeneratedRoute(
        name=data["name"],
        description=data["description"],
        path_condition=GeneratedPathCondition(
            value=path_condition["value"],
            syntax=path_condition["syntax"],
        ),
        actions=[_parse_generated_action(action) for action in data["actions"]],
        conditions=[
            _parse_generated_condition(condition) for condition in data.get("conditions", [])
        ],
    )


class ProjectRoutesRequestClient:
    def __init__(self, *, transport: BaseTransport, token: str | None) -> None:
        self._transport = transport
        self._token = token

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str | bool] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = await self._transport.send(
            method,
            path,
            token=_require_token(self._token),
            params=params,
            body=JSONBody(dict(body)) if body is not None else None,
            headers={"accept": "application/json"},
            read_response=ReadResponsePolicy.ALWAYS,
        )
        if not response.is_success:
            message, parsed = extract_structured_error(response)
            raise ProjectRoutesError(
                f"Project routes request failed: {message}",
                status_code=response.status_code,
                code=_error_code(parsed),
                response_body=parsed,
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise ProjectRoutesError(
                "Project routes API returned invalid JSON.",
                status_code=response.status_code,
            ) from exc
        if not isinstance(payload, dict):
            raise ProjectRoutesError(
                "Project routes API returned an unexpected response body.",
                status_code=response.status_code,
                response_body=payload,
            )
        return cast(dict[str, Any], payload)

    def close(self) -> None:
        if not isinstance(self._transport, SyncTransport):
            raise RuntimeError("close() is only valid for the synchronous client")
        self._transport.close()

    async def aclose(self) -> None:
        if not isinstance(self._transport, AsyncTransport):
            raise RuntimeError("aclose() is only valid for the asynchronous client")
        await self._transport.aclose()


def create_sync_request_client(
    *, token: str | None, base_url: str, timeout: timedelta
) -> ProjectRoutesRequestClient:
    options = TransportOptions(
        timeout=timeout,
        base_url=base_url,
        max_connections=100,
        enable_http2=False,
    )
    return ProjectRoutesRequestClient(
        transport=SyncTransport(create_base_client(options)),
        token=token,
    )


def create_async_request_client(
    *, token: str | None, base_url: str, timeout: timedelta
) -> ProjectRoutesRequestClient:
    options = TransportOptions(
        timeout=timeout,
        base_url=base_url,
        max_connections=100,
        enable_http2=False,
    )
    return ProjectRoutesRequestClient(
        transport=AsyncTransport(create_base_async_client(options)),
        token=token,
    )


class BaseProjectRoutesOpsClient:
    def __init__(self, *, request_client: ProjectRoutesRequestClient) -> None:
        self._request_client = request_client

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str | bool] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._request_client.request(
            method,
            path,
            params=params,
            body=body,
        )

    async def get_routes(
        self,
        *,
        project_id: str,
        version_id: str | None = None,
        search: str | None = None,
        route_type: RouteType | None = None,
        diff: RouteDiff | None = None,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> GetRoutesResult:
        params: dict[str, str | bool] = _params(team_id=team_id, slug=slug)
        if version_id is not None:
            params["versionId"] = version_id
        if search is not None:
            params["q"] = search
        if route_type is not None:
            params["filter"] = route_type
        if diff is not None:
            params["diff"] = diff
        payload = await self._request(
            "GET",
            f"v1/projects/{_path_part(project_id)}/routes",
            params=params,
        )
        return _parse(payload, _parse_get_routes)

    async def stage_routes(
        self,
        *,
        project_id: str,
        body: StageRoutesRequestBody,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> RouteVersion:
        payload = await self._request(
            "PUT",
            f"v1/projects/{_path_part(project_id)}/routes",
            params=_params(team_id=team_id, slug=slug),
            body=body,
        )
        return _parse(payload, lambda data: _parse_version(data["version"]))

    async def add_route(
        self,
        *,
        project_id: str,
        body: AddRouteRequestBody,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> AddRouteResult:
        payload = await self._request(
            "POST",
            f"v1/projects/{_path_part(project_id)}/routes",
            params=_params(team_id=team_id, slug=slug),
            body=body,
        )
        return _parse(payload, _parse_add_route)

    async def delete_routes(
        self,
        *,
        project_id: str,
        body: DeleteRoutesRequestBody,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> DeleteRoutesResult:
        payload = await self._request(
            "DELETE",
            f"v1/projects/{_path_part(project_id)}/routes",
            params=_params(team_id=team_id, slug=slug),
            body=body,
        )
        return _parse(payload, _parse_delete_routes)

    async def edit_route(
        self,
        *,
        project_id: str,
        route_id: str,
        body: EditRouteRequestBody,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> EditRouteResult:
        payload = await self._request(
            "PATCH",
            f"v1/projects/{_path_part(project_id)}/routes/{_path_part(route_id)}",
            params=_params(team_id=team_id, slug=slug),
            body=body,
        )
        return _parse(payload, _parse_edit_route)

    async def generate_route(
        self,
        *,
        project_id: str,
        body: GenerateRouteRequestBody,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> GeneratedRoute:
        payload = await self._request(
            "POST",
            f"v1/projects/{_path_part(project_id)}/routes/generate",
            params=_params(team_id=team_id, slug=slug),
            body=body,
        )
        error = payload.get("error")
        route = payload.get("route")
        if error or route is None:
            raise ProjectRoutesError(
                f"Route generation failed: {error or 'no route was returned'}",
                response_body=payload,
            )
        return _parse(payload, lambda data: _parse_generated_route(data["route"]))

    async def get_route_versions(
        self,
        *,
        project_id: str,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> list[RouteVersion]:
        payload = await self._request(
            "GET",
            f"v1/projects/{_path_part(project_id)}/routes/versions",
            params=_params(team_id=team_id, slug=slug),
        )
        return _parse(
            payload,
            lambda data: [_parse_version(version) for version in data["versions"]],
        )

    async def update_route_version(
        self,
        *,
        project_id: str,
        body: UpdateRouteVersionRequestBody,
        team_id: str | None = None,
        slug: str | None = None,
    ) -> RouteVersion:
        payload = await self._request(
            "POST",
            f"v1/projects/{_path_part(project_id)}/routes/versions",
            params=_params(team_id=team_id, slug=slug),
            body=body,
        )
        return _parse(payload, lambda data: _parse_version(data["version"]))


class SyncProjectRoutesOpsClient(BaseProjectRoutesOpsClient):
    def __init__(
        self,
        *,
        token: str | None = None,
        base_url: str = DEFAULT_API_BASE_URL,
        timeout: timedelta = timedelta(seconds=30),
    ) -> None:
        super().__init__(
            request_client=create_sync_request_client(
                token=token,
                base_url=base_url,
                timeout=timeout,
            )
        )

    def close(self) -> None:
        self._request_client.close()

    def __enter__(self) -> SyncProjectRoutesOpsClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class AsyncProjectRoutesOpsClient(BaseProjectRoutesOpsClient):
    def __init__(
        self,
        *,
        token: str | None = None,
        base_url: str = DEFAULT_API_BASE_URL,
        timeout: timedelta = timedelta(seconds=30),
    ) -> None:
        super().__init__(
            request_client=create_async_request_client(
                token=token,
                base_url=base_url,
                timeout=timeout,
            )
        )

    async def aclose(self) -> None:
        await self._request_client.aclose()

    async def __aenter__(self) -> AsyncProjectRoutesOpsClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()


__all__ = [
    "AsyncProjectRoutesOpsClient",
    "BaseProjectRoutesOpsClient",
    "ProjectRoutesRequestClient",
    "SyncProjectRoutesOpsClient",
]
