"""Internal Sandbox v2 API client."""

import io
import platform
import posixpath
import sys
import tarfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import version as _pkg_version
from typing import TypeVar, cast

from httpx import Response
from httpx._types import QueryParamTypes
from pydantic import BaseModel, ValidationError

from vercel._internal.http import (
    AsyncTransport,
    BaseTransport,
    BytesBody,
    JSONBody,
    RequestBody,
    SyncTransport,
    extract_structured_error,
)
from vercel._internal.time import MILLISECOND, parse_duration
from vercel._internal.unstable.sandbox.errors import SandboxApiError, SandboxResponseError
from vercel._internal.unstable.sandbox.models import (
    CommandResponse,
    CommandsResponse,
    CreateSandboxRequest,
    CreateSnapshotRequest,
    CreateSnapshotResponse,
    DurationInput,
    ExtendTimeoutRequest,
    FilesystemPathRequest,
    GetSandboxRequest,
    JSONObject,
    JSONValue,
    MkdirRequest,
    QuerySandboxesRequest,
    QuerySessionsRequest,
    QuerySnapshotsRequest,
    RunCommandRequest,
    RuntimeSessionResponse,
    RuntimeSessionsResponse,
    SandboxesResponse,
    SandboxResources,
    SandboxResponse,
    SandboxSource,
    SnapshotResponse,
    SnapshotRetention,
    SnapshotsResponse,
    TagFilter,
    UpdateSandboxRequest,
    WriteFile,
)
from vercel._internal.unstable.sandbox.options import (
    SandboxCredentials,
    SandboxCredentialsFactory,
)
from vercel._internal.url import format_url_path

try:
    VERSION = _pkg_version("vercel")
except Exception:
    VERSION = "development"

PLATFORM = platform.uname()
USER_AGENT = (
    f"vercel/unstable/sandbox/{VERSION} "
    f"(Python/{sys.version}; {PLATFORM.system}/{PLATFORM.machine})"
)
ResponseModelT = TypeVar("ResponseModelT", bound=BaseModel)


@dataclass(frozen=True, slots=True)
class _QuerySandboxesResult:
    response: SandboxesResponse
    project_id: str


def _drop_none(data: Mapping[str, JSONValue | None]) -> JSONObject:
    return {key: value for key, value in data.items() if value is not None}


def _normalize_mode(mode: object) -> int | None:
    match mode:
        case None:
            return None
        case bool():
            raise TypeError("mode must be an integer between 0 and 0o777")
        case int() if 0 <= mode <= 0o777:
            return mode
        case int():
            raise ValueError("mode must be an integer between 0 and 0o777")
        case _:
            raise TypeError("mode must be an integer between 0 and 0o777")


def _normalize_tar_path(path: str, *, cwd: str) -> str:
    if posixpath.isabs(path):
        absolute_path = posixpath.normpath(path)
    else:
        absolute_path = posixpath.normpath(posixpath.join(cwd, path))
    return posixpath.relpath(absolute_path, "/")


def _build_write_files_tarball(
    files: Sequence[WriteFile],
    *,
    cwd: str,
    encoding: str,
) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for file in files:
            content = file.content
            data = content.encode(encoding) if isinstance(content, str) else content
            info = tarfile.TarInfo(name=_normalize_tar_path(file.path, cwd=cwd))
            mode = _normalize_mode(file.mode)
            if mode is not None:
                info.mode = mode
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


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
        self._credentials_factory = credentials_factory
        self._base_url = base_url
        self._transport = transport

    @property
    def base_url(self) -> str:
        return self._base_url

    async def _request(
        self,
        method: str,
        path: str,
        *,
        credentials: SandboxCredentials,
        body: RequestBody = None,
        params: Mapping[str, JSONValue | None] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Response:
        query = cast(
            QueryParamTypes,
            _drop_none(
                {
                    "teamId": credentials.team_id,
                    **dict(params or {}),
                }
            ),
        )
        request_headers = {
            "user-agent": USER_AGENT,
            **dict(headers or {}),
        }
        response = await self._transport.send(
            method,
            path,
            token=credentials.token,
            params=query,
            body=body,
            headers=request_headers,
        )

        try:
            response.read()
        except RuntimeError:
            await response.aread()

        if not response.is_success:
            message, data = extract_structured_error(response)
            raise SandboxApiError(response, message, data=data)

        return response

    async def _request_stream(
        self,
        method: str,
        path: str,
        *,
        credentials: SandboxCredentials,
        body: RequestBody = None,
        params: Mapping[str, JSONValue | None] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Response:
        query = cast(
            QueryParamTypes,
            _drop_none(
                {
                    "teamId": credentials.team_id,
                    **dict(params or {}),
                }
            ),
        )
        request_headers = {
            "user-agent": USER_AGENT,
            **dict(headers or {}),
        }
        response = await self._transport.send(
            method,
            path,
            token=credentials.token,
            params=query,
            body=body,
            headers=request_headers,
            stream=True,
        )

        if response.is_success:
            return response

        try:
            response.read()
        except RuntimeError:
            await response.aread()

        message, data = extract_structured_error(response)
        raise SandboxApiError(response, message, data=data)

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        credentials: SandboxCredentials,
        body: JSONValue | None = None,
        params: Mapping[str, JSONValue | None] | None = None,
    ) -> JSONObject:
        response = await self._request(
            method,
            path,
            credentials=credentials,
            body=JSONBody(body) if body is not None else None,
            params=params,
            headers={"content-type": "application/json"},
        )

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
        source: SandboxSource | None = None,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: JSONValue | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        snapshot_expiration: DurationInput = None,
        snapshot_retention: SnapshotRetention | None = None,
    ) -> SandboxResponse:
        credentials = await self._credentials_factory()
        request = CreateSandboxRequest(
            project_id=project_id or credentials.project_id,
            name=name,
            runtime=runtime,
            source=source,
            ports=ports,
            timeout=parse_duration(execution_time_limit, MILLISECOND),
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            env=dict(env) if env is not None else None,
            tags=dict(tags) if tags is not None else None,
            snapshot_expiration=parse_duration(snapshot_expiration, MILLISECOND),
            keep_last_snapshots=snapshot_retention,
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
        credentials = await self._credentials_factory()
        request = GetSandboxRequest(
            project_id=project_id or credentials.project_id,
            resume=resume,
            include_system_routes=include_system_routes,
        )
        data = await self._request_json(
            "GET",
            format_url_path("v2/sandboxes/{name}", name=name),
            credentials=credentials,
            params=request.to_api_dict(),
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
        tags: Sequence[TagFilter] | None = None,
    ) -> _QuerySandboxesResult:
        credentials = await self._credentials_factory()
        effective_project_id = project_id or credentials.project_id
        request = QuerySandboxesRequest(
            project_id=effective_project_id,
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
        return _QuerySandboxesResult(
            response=_validate_response(SandboxesResponse, data),
            project_id=effective_project_id,
        )

    async def destroy_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
    ) -> SandboxResponse:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "DELETE",
            format_url_path("v2/sandboxes/{name}", name=name),
            credentials=credentials,
            params={"projectId": project_id or credentials.project_id},
        )
        return _validate_response(SandboxResponse, data)

    async def update_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
        runtime: str | None = None,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: JSONValue | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        snapshot_expiration: DurationInput = None,
        snapshot_retention: SnapshotRetention | None = None,
        current_snapshot_id: str | None = None,
    ) -> SandboxResponse:
        credentials = await self._credentials_factory()
        request = UpdateSandboxRequest(
            runtime=runtime,
            ports=ports,
            timeout=parse_duration(execution_time_limit, MILLISECOND),
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            env=dict(env) if env is not None else None,
            tags=dict(tags) if tags is not None else None,
            snapshot_expiration=parse_duration(snapshot_expiration, MILLISECOND),
            keep_last_snapshots=snapshot_retention,
            current_snapshot_id=current_snapshot_id,
        )
        data = await self._request_json(
            "PATCH",
            format_url_path("v2/sandboxes/{name}", name=name),
            credentials=credentials,
            params={"projectId": project_id or credentials.project_id},
            body=request.to_api_dict(),
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

    async def stop_runtime_session(self, *, session_id: str) -> SandboxResponse:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "POST",
            format_url_path("v2/sandboxes/sessions/{session_id}/stop", session_id=session_id),
            credentials=credentials,
            body={},
        )
        return _validate_response(SandboxResponse, data)

    async def destroy_runtime_session(self, *, session_id: str) -> SandboxResponse:
        return await self.stop_runtime_session(session_id=session_id)

    async def get_runtime_session(
        self,
        *,
        session_id: str,
        include_system_routes: bool | None = None,
    ) -> RuntimeSessionResponse:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "GET",
            format_url_path("v2/sandboxes/sessions/{session_id}", session_id=session_id),
            credentials=credentials,
            params={
                "__includeSystemRoutes": (
                    None
                    if include_system_routes is None
                    else "true"
                    if include_system_routes
                    else "false"
                )
            },
        )
        return _validate_response(RuntimeSessionResponse, data)

    async def query_runtime_sessions(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> RuntimeSessionsResponse:
        credentials = await self._credentials_factory()
        request = QuerySessionsRequest(
            project_id=project_id or credentials.project_id,
            name=name,
            limit=limit,
            cursor=cursor,
            sort_order=sort_order,
        )
        data = await self._request_json(
            "GET",
            "v2/sandboxes/sessions",
            credentials=credentials,
            params=request.to_api_dict(),
        )
        return _validate_response(RuntimeSessionsResponse, data)

    async def extend_runtime_session_timeout(
        self,
        *,
        session_id: str,
        duration: DurationInput,
    ) -> RuntimeSessionResponse:
        credentials = await self._credentials_factory()
        request = ExtendTimeoutRequest(duration=duration)
        data = await self._request_json(
            "POST",
            format_url_path(
                "v2/sandboxes/sessions/{session_id}/extend-timeout",
                session_id=session_id,
            ),
            credentials=credentials,
            body=request.to_api_dict(),
        )
        return _validate_response(RuntimeSessionResponse, data)

    async def update_runtime_session_network_policy(
        self,
        *,
        session_id: str,
        network_policy: JSONValue,
    ) -> RuntimeSessionResponse:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "POST",
            format_url_path(
                "v2/sandboxes/sessions/{session_id}/network-policy",
                session_id=session_id,
            ),
            credentials=credentials,
            body=network_policy,
        )
        return _validate_response(RuntimeSessionResponse, data)

    async def create_snapshot(
        self,
        *,
        session_id: str,
        expiration: DurationInput = None,
    ) -> CreateSnapshotResponse:
        credentials = await self._credentials_factory()
        body: JSONValue | None = None
        if expiration is not None:
            request = CreateSnapshotRequest(expiration=expiration)
            body = request.to_api_dict()
        data = await self._request_json(
            "POST",
            format_url_path("v2/sandboxes/sessions/{session_id}/snapshot", session_id=session_id),
            credentials=credentials,
            body=body,
        )
        return _validate_response(CreateSnapshotResponse, data)

    async def query_snapshots(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> SnapshotsResponse:
        credentials = await self._credentials_factory()
        request = QuerySnapshotsRequest(
            project_id=project_id or credentials.project_id,
            name=name,
            limit=limit,
            cursor=cursor,
            sort_order=sort_order,
        )
        data = await self._request_json(
            "GET",
            "v2/sandboxes/snapshots",
            credentials=credentials,
            params=request.to_api_dict(),
        )
        return _validate_response(SnapshotsResponse, data)

    async def get_snapshot(self, *, snapshot_id: str) -> SnapshotResponse:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "GET",
            format_url_path("v2/sandboxes/snapshots/{snapshot_id}", snapshot_id=snapshot_id),
            credentials=credentials,
        )
        return _validate_response(SnapshotResponse, data)

    async def delete_snapshot(self, *, snapshot_id: str) -> SnapshotResponse:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "DELETE",
            format_url_path("v2/sandboxes/snapshots/{snapshot_id}", snapshot_id=snapshot_id),
            credentials=credentials,
        )
        return _validate_response(SnapshotResponse, data)

    async def run_command(
        self,
        *,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
    ) -> CommandResponse:
        credentials = await self._credentials_factory()
        request = RunCommandRequest(
            command=command,
            args=args or [],
            cwd=cwd,
            env=dict(env) if env is not None else None,
            sudo=sudo,
        )
        data = await self._request_json(
            "POST",
            format_url_path("v2/sandboxes/sessions/{session_id}/cmd", session_id=session_id),
            credentials=credentials,
            body=request.to_api_dict(),
        )
        return _validate_response(CommandResponse, data)

    async def get_command(
        self,
        *,
        session_id: str,
        command_id: str,
        wait: bool = True,
    ) -> CommandResponse:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "GET",
            format_url_path(
                "v2/sandboxes/sessions/{session_id}/cmd/{command_id}",
                session_id=session_id,
                command_id=command_id,
            ),
            credentials=credentials,
            params={"wait": "true" if wait else "false"},
        )
        return _validate_response(CommandResponse, data)

    async def query_commands(self, *, session_id: str) -> CommandsResponse:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "GET",
            format_url_path("v2/sandboxes/sessions/{session_id}/cmd", session_id=session_id),
            credentials=credentials,
        )
        return _validate_response(CommandsResponse, data)

    async def mkdir(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None = None,
        recursive: bool = True,
    ) -> None:
        credentials = await self._credentials_factory()
        request = MkdirRequest(path=path, cwd=cwd, recursive=recursive)
        await self._request(
            "POST",
            format_url_path("v2/sandboxes/sessions/{session_id}/fs/mkdir", session_id=session_id),
            credentials=credentials,
            body=JSONBody(request.to_api_dict()),
        )

    async def read_file(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None = None,
    ) -> bytes:
        credentials = await self._credentials_factory()
        request = FilesystemPathRequest(path=path, cwd=cwd)
        response = await self._request(
            "POST",
            format_url_path("v2/sandboxes/sessions/{session_id}/fs/read", session_id=session_id),
            credentials=credentials,
            body=JSONBody(request.to_api_dict()),
        )
        return response.content

    async def write_files(
        self,
        *,
        session_id: str,
        files: Sequence[WriteFile],
        cwd: str,
        encoding: str = "utf-8",
    ) -> None:
        credentials = await self._credentials_factory()
        payload = _build_write_files_tarball(files, cwd=cwd, encoding=encoding)
        await self._request(
            "POST",
            format_url_path("v2/sandboxes/sessions/{session_id}/fs/write", session_id=session_id),
            credentials=credentials,
            body=BytesBody(payload, "application/gzip"),
            headers={"x-cwd": "/"},
        )

    async def kill_command(
        self,
        *,
        session_id: str,
        command_id: str,
        signal: int,
    ) -> CommandResponse:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "POST",
            format_url_path(
                "v2/sandboxes/sessions/{session_id}/cmd/{command_id}/kill",
                session_id=session_id,
                command_id=command_id,
            ),
            credentials=credentials,
            body={"signal": signal},
        )
        return _validate_response(CommandResponse, data)

    async def command_logs_response(
        self,
        *,
        session_id: str,
        command_id: str,
    ) -> Response:
        credentials = await self._credentials_factory()
        return await self._request_stream(
            "GET",
            format_url_path(
                "v2/sandboxes/sessions/{session_id}/cmd/{command_id}/logs",
                session_id=session_id,
                command_id=command_id,
            ),
            credentials=credentials,
        )

    def close(self) -> None:
        if isinstance(self._transport, SyncTransport):
            self._transport.close()

    async def aclose(self) -> None:
        if isinstance(self._transport, AsyncTransport):
            await self._transport.aclose()
