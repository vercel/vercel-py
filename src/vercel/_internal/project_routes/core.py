from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, TypeVar, cast
from urllib.parse import quote

import pydantic

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
    AddRouteResult,
    DeleteRoutesResult,
    EditRouteResult,
    GeneratedRoute,
    GetRoutesResult,
    RouteDiff,
    RouteType,
    RouteVersion,
)

_ModelT = TypeVar("_ModelT", bound=pydantic.BaseModel)


class _VersionEnvelope(pydantic.BaseModel):
    version: RouteVersion


class _VersionsEnvelope(pydantic.BaseModel):
    versions: list[RouteVersion]


class _GenerateEnvelope(pydantic.BaseModel):
    route: GeneratedRoute | None = None
    error: str | None = None


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


def _validate(payload: dict[str, Any], model: type[_ModelT]) -> _ModelT:
    try:
        return model.model_validate(payload)
    except pydantic.ValidationError as exc:
        raise ProjectRoutesError(
            "Project routes API returned an unexpected response body.",
            response_body=payload,
        ) from exc


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
        return _validate(payload, GetRoutesResult)

    async def stage_routes(
        self,
        *,
        project_id: str,
        body: Mapping[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> RouteVersion:
        payload = await self._request(
            "PUT",
            f"v1/projects/{_path_part(project_id)}/routes",
            params=_params(team_id=team_id, slug=slug),
            body=body,
        )
        return _validate(payload, _VersionEnvelope).version

    async def add_route(
        self,
        *,
        project_id: str,
        body: Mapping[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> AddRouteResult:
        payload = await self._request(
            "POST",
            f"v1/projects/{_path_part(project_id)}/routes",
            params=_params(team_id=team_id, slug=slug),
            body=body,
        )
        return _validate(payload, AddRouteResult)

    async def delete_routes(
        self,
        *,
        project_id: str,
        body: Mapping[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> DeleteRoutesResult:
        payload = await self._request(
            "DELETE",
            f"v1/projects/{_path_part(project_id)}/routes",
            params=_params(team_id=team_id, slug=slug),
            body=body,
        )
        return _validate(payload, DeleteRoutesResult)

    async def edit_route(
        self,
        *,
        project_id: str,
        route_id: str,
        body: Mapping[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> EditRouteResult:
        payload = await self._request(
            "PATCH",
            f"v1/projects/{_path_part(project_id)}/routes/{_path_part(route_id)}",
            params=_params(team_id=team_id, slug=slug),
            body=body,
        )
        return _validate(payload, EditRouteResult)

    async def generate_route(
        self,
        *,
        project_id: str,
        body: Mapping[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> GeneratedRoute:
        payload = await self._request(
            "POST",
            f"v1/projects/{_path_part(project_id)}/routes/generate",
            params=_params(team_id=team_id, slug=slug),
            body=body,
        )
        result = _validate(payload, _GenerateEnvelope)
        if result.error or result.route is None:
            raise ProjectRoutesError(
                f"Route generation failed: {result.error or 'no route was returned'}",
                response_body=payload,
            )
        return result.route

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
        return _validate(payload, _VersionsEnvelope).versions

    async def update_route_version(
        self,
        *,
        project_id: str,
        body: Mapping[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> RouteVersion:
        payload = await self._request(
            "POST",
            f"v1/projects/{_path_part(project_id)}/routes/versions",
            params=_params(team_id=team_id, slug=slug),
            body=body,
        )
        return _validate(payload, _VersionEnvelope).version


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
