"""Internal Sandbox v2 API client."""

from collections.abc import Mapping
from typing import TypeVar, cast
from urllib.parse import quote

from httpx._types import QueryParamTypes
from pydantic import BaseModel, ValidationError

from vercel._internal.http import (
    AsyncTransport,
    BaseTransport,
    JSONBody,
    SyncTransport,
    extract_structured_error,
)
from vercel._internal.time import MILLISECOND, parse_duration
from vercel._internal.unstable.sandbox.errors import SandboxApiError, SandboxResponseError
from vercel._internal.unstable.sandbox.models import (
    CreateSandboxRequest,
    DestroyRuntimeSessionRequest,
    DestroySandboxRequest,
    DurationInput,
    GetSandboxRequest,
    JSONObject,
    JSONValue,
    QuerySandboxesRequest,
    SandboxesResponse,
    SandboxResponse,
)
from vercel._internal.unstable.sandbox.options import (
    SandboxCredentials,
    SandboxCredentialsFactory,
)

USER_AGENT = "vercel-py/unstable"
ResponseModelT = TypeVar("ResponseModelT", bound=BaseModel)


def _drop_none(data: Mapping[str, JSONValue | None]) -> JSONObject:
    return {key: value for key, value in data.items() if value is not None}


def _validate_response(model: type[ResponseModelT], data: JSONObject) -> ResponseModelT:
    try:
        return model.model_validate(data)
    except ValidationError as exc:
        raise SandboxResponseError(
            "Sandbox API response did not match the expected v2 shape",
            data=data,
        ) from exc


class SandboxApiClient:
    def __init__(
        self,
        *,
        base_url: str,
        credentials_factory: SandboxCredentialsFactory,
        transport: BaseTransport,
    ) -> None:
        self.credentials_factory = credentials_factory
        self._base_url = base_url
        self._transport = transport

    @property
    def base_url(self) -> str:
        return self._base_url

    async def _resolve_credentials(self) -> SandboxCredentials:
        return await self.credentials_factory()

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        credentials: SandboxCredentials,
        body: JSONObject | None = None,
        params: Mapping[str, JSONValue | None] | None = None,
    ) -> JSONObject:
        query = cast(
            QueryParamTypes,
            _drop_none(
                {
                    "teamId": credentials.team_id,
                    **dict(params or {}),
                }
            ),
        )
        response = await self._transport.send(
            method,
            path,
            token=credentials.token,
            params=query,
            body=JSONBody(body) if body is not None else None,
            headers={
                "content-type": "application/json",
                "user-agent": USER_AGENT,
            },
        )

        try:
            response.read()
        except RuntimeError:
            await response.aread()

        if not response.is_success:
            message, data = extract_structured_error(response)
            raise SandboxApiError(response, message, data=data)

        try:
            data = response.json()
        except ValueError as exc:
            raise SandboxResponseError(
                "Sandbox API response body could not be decoded as JSON"
            ) from exc

        if not isinstance(data, dict):
            raise SandboxResponseError("Sandbox API response must be a JSON object", data=data)
        return cast(JSONObject, data)

    async def create_sandbox(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        runtime: str | None = None,
        source: JSONValue | None = None,
        ports: list[int] | None = None,
        timeout: DurationInput = None,
        resources: JSONValue | None = None,
        persistent: bool | None = None,
        network_policy: JSONValue | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        snapshot_expiration: DurationInput = None,
        keep_last_snapshots: JSONValue | None = None,
    ) -> SandboxResponse:
        credentials = await self._resolve_credentials()
        request = CreateSandboxRequest(
            project_id=project_id or credentials.project_id,
            name=name,
            runtime=runtime,
            source=source,
            ports=ports,
            timeout=parse_duration(timeout, MILLISECOND),
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            env=dict(env) if env is not None else None,
            tags=dict(tags) if tags is not None else None,
            snapshot_expiration=parse_duration(snapshot_expiration, MILLISECOND),
            keep_last_snapshots=keep_last_snapshots,
        )
        data = await self._request_json(
            "POST", "v2/sandboxes", credentials=credentials, body=request.to_api_dict()
        )
        return _validate_response(SandboxResponse, data)

    async def get_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
        resume: bool = True,
        include_system_routes: bool | None = None,
    ) -> SandboxResponse:
        credentials = await self._resolve_credentials()
        request = GetSandboxRequest(
            name=name,
            project_id=project_id or credentials.project_id,
            resume=resume,
            include_system_routes=include_system_routes,
        )
        data = await self._request_json(
            "GET",
            f"v2/sandboxes/{quote(request.name, safe='')}",
            credentials=credentials,
            params=request.to_api_dict(exclude={"name"}),
        )
        return _validate_response(SandboxResponse, data)

    async def query_sandboxes(
        self,
        *,
        project_id: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        name_prefix: str | None = None,
        tags: str | list[str] | None = None,
    ) -> SandboxesResponse:
        credentials = await self._resolve_credentials()
        request = QuerySandboxesRequest(
            project_id=project_id or credentials.project_id,
            limit=limit,
            cursor=cursor,
            sort_by=sort_by,
            sort_order=sort_order,
            name_prefix=name_prefix,
            tags=tags,
        )
        data = await self._request_json(
            "GET",
            "v2/sandboxes",
            credentials=credentials,
            params=request.to_api_dict(),
        )
        return _validate_response(SandboxesResponse, data)

    async def destroy_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
    ) -> SandboxResponse:
        credentials = await self._resolve_credentials()
        request = DestroySandboxRequest(name=name, project_id=project_id or credentials.project_id)
        data = await self._request_json(
            "DELETE",
            f"v2/sandboxes/{quote(request.name, safe='')}",
            credentials=credentials,
            params=request.to_api_dict(exclude={"name"}),
        )
        return _validate_response(SandboxResponse, data)

    async def create_runtime_session(
        self,
        *,
        name: str,
        project_id: str | None = None,
        resume: bool = True,
        include_system_routes: bool | None = None,
    ) -> SandboxResponse:
        return await self.get_sandbox(
            name=name,
            project_id=project_id,
            resume=resume,
            include_system_routes=include_system_routes,
        )

    async def destroy_runtime_session(self, *, session_id: str) -> SandboxResponse:
        credentials = await self._resolve_credentials()
        request = DestroyRuntimeSessionRequest(session_id=session_id)
        data = await self._request_json(
            "POST",
            f"v2/sandboxes/sessions/{quote(request.session_id, safe='')}/stop",
            credentials=credentials,
            body={},
        )
        return _validate_response(SandboxResponse, data)

    def close(self) -> None:
        if isinstance(self._transport, SyncTransport):
            self._transport.close()

    async def aclose(self) -> None:
        if isinstance(self._transport, AsyncTransport):
            await self._transport.aclose()
