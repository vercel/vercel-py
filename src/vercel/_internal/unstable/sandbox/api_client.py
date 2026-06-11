"""Internal Sandbox v2 API client."""

import io
import json
import platform
import posixpath
import sys
import tarfile
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import timedelta
from importlib.metadata import version as _pkg_version
from typing import Literal, TypeVar, cast

from httpx import AsyncByteStream, Response
from httpx._types import QueryParamTypes
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_serializer,
    field_validator,
)

from vercel._internal.http import (
    BaseTransport,
    BytesBody,
    JSONBody,
    ReadResponsePolicy,
    RequestBody,
    extract_structured_error,
)
from vercel._internal.time import MILLISECOND, parse_duration, to_ms_int
from vercel._internal.unstable.sandbox.errors import (
    SandboxApiError,
    SandboxResponseError,
    SandboxStreamError,
)
from vercel._internal.unstable.sandbox.models import (
    _OMITTED,
    JSONObject,
    JSONValue,
    NetworkPolicy,
    ProcessLog,
    ProcessLogStream,
    SandboxResources,
    SandboxSource,
    SandboxStatus,
    SnapshotExpiration,
    SnapshotRetention,
    SnapshotRetentionUpdate,
    TagFilter,
    _Omitted,
    _parse_network_policy,
    _serialize_network_policy,
    _WriteFile,
)
from vercel._internal.unstable.sandbox.options import (
    SandboxCredentials,
    SandboxCredentialsFactory,
)
from vercel._internal.unstable.sandbox.process_output import ProcessOutputRouter
from vercel._internal.unstable.sandbox.state import (
    CompletedProcessState,
    ProcessState,
    RuntimeSessionsPageState,
    SandboxesPageState,
    SandboxRouteState,
    SandboxRuntimeSessionState,
    SandboxState,
    SnapshotRetentionState,
    SnapshotSessionState,
    SnapshotsPageState,
    SnapshotState,
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


class _ApiModel(BaseModel):
    model_config = ConfigDict(
        extra="ignore", frozen=True, populate_by_name=True, serialize_by_alias=True
    )


class _ApiRequestModel(_ApiModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        serialize_by_alias=True,
    )

    def to_api_dict(self) -> JSONObject:
        return cast(JSONObject, self.model_dump(by_alias=True, exclude_none=True))


class _CreateSandboxRequest(_ApiRequestModel):
    project_id: str = Field(serialization_alias="projectId")
    name: str | None = None
    runtime: str | None = None
    source: SandboxSource | None = None
    ports: list[int] | None = None
    timeout: timedelta | None = None
    resources: SandboxResources | None = None
    persistent: bool | None = None
    network_policy: NetworkPolicy | None = Field(default=None, serialization_alias="networkPolicy")
    env: dict[str, str] | None = None
    tags: dict[str, str] | None = None
    snapshot_expiration: SnapshotExpiration | None = Field(
        default=None, serialization_alias="snapshotExpiration"
    )
    keep_last_snapshots: SnapshotRetention | None = Field(
        default=None, serialization_alias="keepLastSnapshots"
    )

    @field_serializer("timeout")
    def _serialize_duration(self, value: timedelta | None) -> int | None:
        return None if value is None else to_ms_int(value)

    @field_serializer("snapshot_expiration")
    def _serialize_snapshot_expiration(self, value: SnapshotExpiration | None) -> int | None:
        return None if value is None else to_ms_int(value.value)

    @field_serializer("keep_last_snapshots")
    def _serialize_retention(self, value: SnapshotRetention | None) -> JSONObject | None:
        return None if value is None else value.to_api_dict()

    @field_serializer("network_policy")
    def _serialize_network_policy(self, value: NetworkPolicy | None) -> JSONObject | None:
        return None if value is None else _serialize_network_policy(value)

    @field_validator("network_policy", mode="before")
    @classmethod
    def _validate_network_policy(cls, value: object) -> NetworkPolicy | None:
        if value is None or isinstance(value, NetworkPolicy):
            return value
        raise TypeError("network_policy must be a NetworkPolicy")


class _UpdateSandboxRequest(_ApiRequestModel):
    runtime: str | None = None
    ports: list[int] | None = None
    timeout: timedelta | None = None
    resources: SandboxResources | None = None
    persistent: bool | None = None
    network_policy: NetworkPolicy | None = Field(default=None, serialization_alias="networkPolicy")
    env: dict[str, str] | None = None
    tags: dict[str, str] | None = None
    snapshot_expiration: SnapshotExpiration | None = Field(
        default=None, serialization_alias="snapshotExpiration"
    )
    current_snapshot_id: str | None = Field(default=None, serialization_alias="currentSnapshotId")

    @field_serializer("timeout")
    def _serialize_duration(self, value: timedelta | None) -> int | None:
        return None if value is None else to_ms_int(value)

    @field_serializer("snapshot_expiration")
    def _serialize_snapshot_expiration(self, value: SnapshotExpiration | None) -> int | None:
        return None if value is None else to_ms_int(value.value)

    @field_serializer("network_policy")
    def _serialize_network_policy(self, value: NetworkPolicy | None) -> JSONObject | None:
        return None if value is None else _serialize_network_policy(value)

    @field_validator("network_policy", mode="before")
    @classmethod
    def _validate_network_policy(cls, value: object) -> NetworkPolicy | None:
        if value is None or isinstance(value, NetworkPolicy):
            return value
        raise TypeError("network_policy must be a NetworkPolicy")


class _GetSandboxRequest(_ApiRequestModel):
    project_id: str = Field(serialization_alias="projectId")
    resume: bool = False
    include_system_routes: bool | None = Field(
        default=None, serialization_alias="__includeSystemRoutes"
    )

    @field_serializer("resume", "include_system_routes")
    def _serialize_bool(self, value: bool | None) -> str | None:
        return None if value is None else "true" if value else "false"


class _QuerySandboxesRequest(_ApiRequestModel):
    project_id: str = Field(serialization_alias="project")
    limit: int | None = None
    cursor: str | None = None
    sort_by: str | None = Field(default=None, serialization_alias="sortBy")
    sort_order: str | None = Field(default=None, serialization_alias="sortOrder")
    name_prefix: str | None = Field(default=None, serialization_alias="namePrefix")
    tag: TagFilter | None = Field(default=None, serialization_alias="tags")

    @field_serializer("tag")
    def _serialize_tag(self, value: TagFilter | None) -> str | None:
        return None if value is None else value.to_query_value()


class _QuerySessionsRequest(_ApiRequestModel):
    project_id: str = Field(serialization_alias="project")
    name: str | None = None
    limit: int | None = None
    cursor: str | None = None
    sort_order: str | None = Field(default=None, serialization_alias="sortOrder")


class _QuerySnapshotsRequest(_QuerySessionsRequest):
    pass


class _CreateSnapshotRequest(_ApiRequestModel):
    expiration: SnapshotExpiration | None = None

    @field_serializer("expiration")
    def _serialize_duration(self, value: SnapshotExpiration | None) -> int | None:
        return None if value is None else to_ms_int(value.value)


class _ExtendTimeoutRequest(_ApiRequestModel):
    duration: timedelta

    @field_serializer("duration")
    def _serialize_duration(self, value: timedelta) -> int:
        return to_ms_int(value)


class _RunCommandRequest(_ApiRequestModel):
    command: str
    args: list[str] | None = None
    cwd: str | None = None
    env: dict[str, str] | None = None
    sudo: bool | None = None
    wait: bool | None = None
    logs: bool | None = None
    timeout: timedelta | None = None

    @field_serializer("timeout")
    def _serialize_timeout(self, value: timedelta | None) -> int | None:
        return None if value is None else to_ms_int(value)


class _FilesystemPathRequest(_ApiRequestModel):
    path: str
    cwd: str | None = None


class _MkdirRequest(_FilesystemPathRequest):
    recursive: bool = True


class _SandboxRoutePayload(_ApiModel):
    url: str
    port: int
    subdomain: str
    system: bool = False


class _CommandPayload(_ApiModel):
    id: str
    name: str
    args: list[str]
    cwd: str
    session_id: str = Field(
        validation_alias=AliasChoices("session_id", "sessionId"),
        serialization_alias="sessionId",
    )
    exit_code: int | None = Field(
        default=None,
        validation_alias=AliasChoices("exit_code", "exitCode"),
        serialization_alias="exitCode",
    )
    started_at: int = Field(
        validation_alias=AliasChoices("started_at", "startedAt"),
        serialization_alias="startedAt",
    )


class _RuntimeSessionPayload(_ApiModel):
    id: str
    sandbox_name: str | None = Field(
        default=None,
        validation_alias=AliasChoices("sandbox_name", "sourceSandboxName"),
        serialization_alias="sourceSandboxName",
    )
    project_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("project_id", "projectId"),
        serialization_alias="projectId",
    )
    status: SandboxStatus | None = None
    runtime: str | None = None
    cwd: str | None = None
    region: str | None = None
    memory: int | None = None
    vcpus: int | None = None
    execution_time_limit: int | None = Field(
        default=None,
        validation_alias=AliasChoices("execution_time_limit", "timeout"),
        serialization_alias="timeout",
    )
    network_policy: JSONValue | None = Field(
        default=None,
        validation_alias=AliasChoices("network_policy", "networkPolicy"),
        serialization_alias="networkPolicy",
    )
    requested_at: int | None = Field(
        default=None,
        validation_alias=AliasChoices("requested_at", "requestedAt"),
        serialization_alias="requestedAt",
    )
    started_at: int | None = Field(
        default=None,
        validation_alias=AliasChoices("started_at", "startedAt"),
        serialization_alias="startedAt",
    )
    stopped_at: int | None = Field(
        default=None,
        validation_alias=AliasChoices("stopped_at", "stoppedAt"),
        serialization_alias="stoppedAt",
    )


class _SnapshotRetentionPayload(_ApiModel):
    count: int
    expiration: int | None = None
    delete_evicted: bool = Field(
        default=True,
        validation_alias=AliasChoices("delete_evicted", "deleteEvicted"),
        serialization_alias="deleteEvicted",
    )


class _SandboxPayload(_ApiModel):
    name: str
    current_session_id: str = Field(
        validation_alias=AliasChoices("current_session_id", "currentSessionId"),
        serialization_alias="currentSessionId",
    )
    runtime: str | None = None
    status: SandboxStatus | None = None
    persistent: bool | None = None
    current_snapshot_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("current_snapshot_id", "currentSnapshotId"),
        serialization_alias="currentSnapshotId",
    )
    project_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("project_id", "projectId"),
        serialization_alias="projectId",
    )
    cwd: str | None = None
    region: str | None = None
    memory: int | None = None
    vcpus: int | None = None
    execution_time_limit: int | None = Field(
        default=None,
        validation_alias=AliasChoices("execution_time_limit", "timeout"),
        serialization_alias="timeout",
    )
    network_policy: JSONValue | None = Field(
        default=None,
        validation_alias=AliasChoices("network_policy", "networkPolicy"),
        serialization_alias="networkPolicy",
    )
    snapshot_expiration: int | None = Field(
        default=None,
        validation_alias=AliasChoices("snapshot_expiration", "snapshotExpiration"),
        serialization_alias="snapshotExpiration",
    )
    snapshot_retention: _SnapshotRetentionPayload | None = Field(
        default=None,
        validation_alias=AliasChoices("snapshot_retention", "keepLastSnapshots"),
        serialization_alias="keepLastSnapshots",
    )
    status_updated_at: int | None = Field(
        default=None,
        validation_alias=AliasChoices("status_updated_at", "statusUpdatedAt"),
        serialization_alias="statusUpdatedAt",
    )
    created_at: int | None = Field(
        default=None,
        validation_alias=AliasChoices("created_at", "createdAt"),
        serialization_alias="createdAt",
    )
    updated_at: int | None = Field(
        default=None,
        validation_alias=AliasChoices("updated_at", "updatedAt"),
        serialization_alias="updatedAt",
    )
    tags: dict[str, str] | None = None
    routes: tuple[_SandboxRoutePayload, ...] = ()
    current_session: _RuntimeSessionPayload | None = None
    raw: JSONObject | None = None


class _SnapshotPayload(_ApiModel):
    id: str
    source_session_id: str = Field(
        validation_alias=AliasChoices("source_session_id", "sourceSessionId"),
        serialization_alias="sourceSessionId",
    )
    region: str
    status: Literal["created", "deleted", "failed"]
    size_bytes: int = Field(
        validation_alias=AliasChoices("size_bytes", "sizeBytes"), serialization_alias="sizeBytes"
    )
    expires_at: int | None = Field(
        default=None,
        validation_alias=AliasChoices("expires_at", "expiresAt"),
        serialization_alias="expiresAt",
    )
    created_at: int = Field(
        validation_alias=AliasChoices("created_at", "createdAt"), serialization_alias="createdAt"
    )
    updated_at: int = Field(
        validation_alias=AliasChoices("updated_at", "updatedAt"), serialization_alias="updatedAt"
    )
    last_used_at: int | None = Field(
        default=None,
        validation_alias=AliasChoices("last_used_at", "lastUsedAt"),
        serialization_alias="lastUsedAt",
    )
    creation_method: str | None = Field(
        default=None,
        validation_alias=AliasChoices("creation_method", "creationMethod"),
        serialization_alias="creationMethod",
    )
    parent_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("parent_id", "parentId"),
        serialization_alias="parentId",
    )


class _Pagination(_ApiModel):
    count: int
    next: str | None = None
    prev: str | None = None


class _CommandResponse(_ApiModel):
    command: _CommandPayload | None = None

    def to_command(self) -> ProcessState:
        if self.command is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'command'",
                data=self.model_dump(by_alias=True),
            )
        return _command_state(self.command)


class _CommandsResponse(_ApiModel):
    commands: list[_CommandPayload]


class _RuntimeSessionResponse(_ApiModel):
    session: _RuntimeSessionPayload | None = None
    routes: list[_SandboxRoutePayload] = Field(default_factory=list)

    def to_runtime_session(self) -> SandboxRuntimeSessionState:
        if self.session is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'session'",
                data=self.model_dump(by_alias=True),
            )
        return _runtime_session_state(self.session)


class _RuntimeSessionsResponse(_ApiModel):
    sessions: list[_RuntimeSessionPayload]
    pagination: _Pagination | None = None


class _SandboxResponse(_ApiModel):
    sandbox: _SandboxPayload | None = None
    session: _RuntimeSessionPayload | None = None
    routes: list[_SandboxRoutePayload] = Field(default_factory=list)
    resumed: bool | None = None

    def to_sandbox(
        self, *, project_id: str | None = None, sparse_attachments: bool = False
    ) -> SandboxState:
        if self.sandbox is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'sandbox'",
                data=self.model_dump(by_alias=True),
            )
        raw = cast(
            JSONObject,
            self.sandbox.model_dump(
                by_alias=True, exclude_none=True, exclude={"routes", "current_session", "raw"}
            ),
        )
        payload = self.sandbox
        session = self.session
        updates: dict[str, object] = {}
        if session is not None and not sparse_attachments:
            if payload.project_id is None and session.project_id is not None:
                updates["project_id"] = session.project_id
            for name in (
                "runtime",
                "status",
                "cwd",
                "region",
                "memory",
                "vcpus",
                "execution_time_limit",
                "network_policy",
            ):
                if getattr(payload, name) is None:
                    updates[name] = getattr(session, name)
        payload = payload.model_copy(update=updates)
        return _sandbox_state(
            payload,
            routes=tuple(_route_state(route) for route in self.routes),
            current_session=(None if session is None else _runtime_session_state(session)),
            raw=raw,
            project_id=project_id,
            routes_attached=not sparse_attachments or "routes" in self.model_fields_set,
            current_session_attached=not sparse_attachments or "session" in self.model_fields_set,
        )


class _SandboxesResponse(_ApiModel):
    sandboxes: list[_SandboxPayload]
    pagination: _Pagination | None = None


class _CreateSnapshotResponse(_ApiModel):
    snapshot: _SnapshotPayload | None = None
    session: _RuntimeSessionPayload | None = None

    def to_snapshot_and_session(self) -> SnapshotSessionState:
        if self.snapshot is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'snapshot'",
                data=self.model_dump(by_alias=True),
            )
        if self.session is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'session'",
                data=self.model_dump(by_alias=True),
            )
        return SnapshotSessionState(
            snapshot=_snapshot_state(self.snapshot),
            session=_runtime_session_state(self.session),
        )


class _SnapshotResponse(_ApiModel):
    snapshot: _SnapshotPayload | None = None

    def to_snapshot(self) -> SnapshotState:
        if self.snapshot is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'snapshot'",
                data=self.model_dump(by_alias=True),
            )
        return _snapshot_state(self.snapshot)


class _SnapshotsResponse(_ApiModel):
    snapshots: list[_SnapshotPayload]
    pagination: _Pagination | None = None


def _route_state(payload: _SandboxRoutePayload) -> SandboxRouteState:
    return SandboxRouteState(
        url=payload.url,
        port=payload.port,
        subdomain=payload.subdomain,
        system=payload.system,
    )


def _command_state(payload: _CommandPayload) -> ProcessState:
    return ProcessState(
        id=payload.id,
        name=payload.name,
        args=tuple(payload.args),
        cwd=payload.cwd,
        session_id=payload.session_id,
        returncode=payload.exit_code,
        started_at=payload.started_at,
    )


def _runtime_session_state(payload: _RuntimeSessionPayload) -> SandboxRuntimeSessionState:
    return SandboxRuntimeSessionState(
        id=payload.id,
        sandbox_name=payload.sandbox_name,
        project_id=payload.project_id,
        status=payload.status,
        runtime=payload.runtime,
        cwd=payload.cwd,
        region=payload.region,
        memory=payload.memory,
        vcpus=payload.vcpus,
        execution_time_limit=parse_duration(payload.execution_time_limit, MILLISECOND),
        network_policy=_parse_response_network_policy(payload.network_policy),
        requested_at=payload.requested_at,
        started_at=payload.started_at,
        stopped_at=payload.stopped_at,
    )


def _sandbox_state(
    payload: _SandboxPayload,
    *,
    routes: tuple[SandboxRouteState, ...] = (),
    current_session: SandboxRuntimeSessionState | None = None,
    raw: JSONObject | None = None,
    project_id: str | None = None,
    routes_attached: bool = True,
    current_session_attached: bool = True,
) -> SandboxState:
    return SandboxState(
        name=payload.name,
        current_session_id=payload.current_session_id,
        runtime=payload.runtime,
        status=payload.status,
        persistent=payload.persistent,
        current_snapshot_id=payload.current_snapshot_id,
        project_id=payload.project_id or project_id,
        cwd=payload.cwd,
        region=payload.region,
        memory=payload.memory,
        vcpus=payload.vcpus,
        execution_time_limit=parse_duration(payload.execution_time_limit, MILLISECOND),
        network_policy=_parse_response_network_policy(payload.network_policy),
        snapshot_expiration=parse_duration(payload.snapshot_expiration, MILLISECOND),
        snapshot_retention=(
            None
            if payload.snapshot_retention is None
            else SnapshotRetentionState(
                count=payload.snapshot_retention.count,
                expiration=parse_duration(payload.snapshot_retention.expiration, MILLISECOND),
                delete_evicted=payload.snapshot_retention.delete_evicted,
            )
        ),
        status_updated_at=payload.status_updated_at,
        created_at=payload.created_at,
        updated_at=payload.updated_at,
        tags=None if payload.tags is None else dict(payload.tags),
        routes=routes,
        current_session=current_session,
        raw=raw,
        _routes_attached=routes_attached,
        _current_session_attached=current_session_attached,
    )


def _parse_response_network_policy(value: object) -> NetworkPolicy | None:
    try:
        return _parse_network_policy(value)
    except (TypeError, ValueError) as exc:
        raise SandboxResponseError(
            "Sandbox API response included a malformed network policy",
            data=value,
        ) from exc


def _snapshot_state(payload: _SnapshotPayload) -> SnapshotState:
    return SnapshotState(
        id=payload.id,
        source_session_id=payload.source_session_id,
        region=payload.region,
        status=payload.status,
        size_bytes=payload.size_bytes,
        expires_at=payload.expires_at,
        created_at=payload.created_at,
        updated_at=payload.updated_at,
        last_used_at=payload.last_used_at,
        creation_method=payload.creation_method,
        parent_id=payload.parent_id,
    )


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
    if not posixpath.isabs(cwd):
        raise ValueError("cwd must be an absolute path")
    if posixpath.isabs(path):
        absolute_path = posixpath.normpath(path)
    else:
        absolute_path = posixpath.normpath(posixpath.join(cwd, path))
    return posixpath.relpath(absolute_path, "/")


def _build_write_files_tarball(
    files: Sequence[_WriteFile],
    *,
    cwd: str,
) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for file in files:
            info = tarfile.TarInfo(name=_normalize_tar_path(file.path, cwd=cwd))
            mode = _normalize_mode(file.mode)
            if mode is not None:
                info.mode = mode
            info.size = len(file.content)
            tar.addfile(info, io.BytesIO(file.content))
    # BytesBody currently requires bytes, so finalizing the in-memory archive
    # makes one additional copy. Streaming uploads are intentionally deferred.
    return buffer.getvalue()


def _validate_response(model: type[ResponseModelT], data: JSONObject) -> ResponseModelT:
    try:
        return model.model_validate(data)
    except ValidationError as exc:
        raise SandboxResponseError(
            "Sandbox API response did not match the expected v2 shape",
            data=data,
        ) from exc


def _parse_run_process_record(line: str) -> JSONObject:
    try:
        record = json.loads(line)
    except json.JSONDecodeError as exc:
        raise SandboxResponseError(
            "Sandbox process response included malformed NDJSON",
            data=line,
        ) from exc
    if not isinstance(record, dict):
        raise SandboxResponseError(
            "Sandbox process response included a non-object NDJSON record",
            data=record,
        )
    return cast(JSONObject, record)


async def _response_lines(response: Response) -> AsyncIterator[str]:
    if isinstance(response.stream, AsyncByteStream):
        async for line in response.aiter_lines():
            yield line
    else:
        for line in response.iter_lines():
            yield line


async def _close_stream_response(response: Response) -> None:
    if isinstance(response.stream, AsyncByteStream):
        await response.aclose()
    else:
        response.close()


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

    def _url(self, path: str) -> str:
        return self._base_url.rstrip("/") + "/" + path.lstrip("/")

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
            self._url(path),
            token=credentials.token,
            params=query,
            body=body,
            headers=request_headers,
            read_response=ReadResponsePolicy.ALWAYS,
        )

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
            self._url(path),
            token=credentials.token,
            params=query,
            body=body,
            headers=request_headers,
            stream=True,
            read_response=ReadResponsePolicy.NON_SUCCESS_ONLY,
        )

        if response.is_success:
            return response

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
        execution_time_limit: timedelta | None = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: NetworkPolicy | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        snapshot_expiration: SnapshotExpiration | None = None,
        snapshot_retention: SnapshotRetention | None = None,
    ) -> SandboxState:
        credentials = await self._credentials_factory()
        request = _CreateSandboxRequest(
            project_id=project_id or credentials.project_id,
            name=name,
            runtime=runtime,
            source=source,
            ports=ports,
            timeout=execution_time_limit,
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            env=dict(env) if env is not None else None,
            tags=dict(tags) if tags is not None else None,
            snapshot_expiration=snapshot_expiration,
            keep_last_snapshots=snapshot_retention,
        )
        data = await self._request_json(
            "POST", "v2/sandboxes", credentials=credentials, body=request.to_api_dict()
        )
        return _validate_response(_SandboxResponse, data).to_sandbox()

    async def get_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
        resume: bool = False,
        include_system_routes: bool | None = None,
    ) -> SandboxState:
        credentials = await self._credentials_factory()
        request = _GetSandboxRequest(
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
        return _validate_response(_SandboxResponse, data).to_sandbox()

    async def query_sandboxes(
        self,
        *,
        project_id: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        name_prefix: str | None = None,
        tag: TagFilter | None = None,
    ) -> SandboxesPageState:
        credentials = await self._credentials_factory()
        effective_project_id = project_id or credentials.project_id
        request = _QuerySandboxesRequest(
            project_id=effective_project_id,
            limit=limit,
            cursor=cursor,
            sort_by=sort_by,
            sort_order=sort_order,
            name_prefix=name_prefix,
            tag=tag,
        )
        data = await self._request_json(
            "GET",
            "v2/sandboxes",
            credentials=credentials,
            params=request.to_api_dict(),
        )
        response = _validate_response(_SandboxesResponse, data)
        return SandboxesPageState(
            sandboxes=tuple(
                _sandbox_state(sandbox, project_id=effective_project_id)
                for sandbox in response.sandboxes
            ),
            next_cursor=response.pagination.next if response.pagination is not None else None,
        )

    async def destroy_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
    ) -> SandboxState:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "DELETE",
            format_url_path("v2/sandboxes/{name}", name=name),
            credentials=credentials,
            params={"projectId": project_id or credentials.project_id},
        )
        return _validate_response(_SandboxResponse, data).to_sandbox()

    async def update_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
        runtime: str | None = None,
        ports: list[int] | None = None,
        execution_time_limit: timedelta | None = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: NetworkPolicy | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        snapshot_expiration: SnapshotExpiration | None = None,
        snapshot_retention: SnapshotRetentionUpdate = _OMITTED,
        current_snapshot_id: str | None = None,
    ) -> SandboxState:
        credentials = await self._credentials_factory()
        effective_project_id = project_id or credentials.project_id
        request = _UpdateSandboxRequest(
            runtime=runtime,
            ports=ports,
            timeout=execution_time_limit,
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            env=dict(env) if env is not None else None,
            tags=dict(tags) if tags is not None else None,
            snapshot_expiration=snapshot_expiration,
            current_snapshot_id=current_snapshot_id,
        )
        body = request.to_api_dict()
        if not isinstance(snapshot_retention, _Omitted):
            body["keepLastSnapshots"] = (
                None if snapshot_retention is None else snapshot_retention.to_api_dict()
            )
        data = await self._request_json(
            "PATCH",
            format_url_path("v2/sandboxes/{name}", name=name),
            credentials=credentials,
            params={"projectId": effective_project_id},
            body=body,
        )
        return _validate_response(_SandboxResponse, data).to_sandbox(
            project_id=effective_project_id,
            sparse_attachments=True,
        )

    async def resume_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
        include_system_routes: bool | None = None,
    ) -> SandboxState:
        return await self.get_sandbox(
            name=name,
            project_id=project_id,
            resume=True,
            include_system_routes=include_system_routes,
        )

    async def stop_runtime_session(self, *, session_id: str) -> SandboxRuntimeSessionState:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "POST",
            format_url_path("v2/sandboxes/sessions/{session_id}/stop", session_id=session_id),
            credentials=credentials,
            body={},
        )
        return _validate_response(_RuntimeSessionResponse, data).to_runtime_session()

    async def destroy_runtime_session(self, *, session_id: str) -> SandboxRuntimeSessionState:
        return await self.stop_runtime_session(session_id=session_id)

    async def get_runtime_session(
        self,
        *,
        session_id: str,
        include_system_routes: bool | None = None,
    ) -> SandboxRuntimeSessionState:
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
        return _validate_response(_RuntimeSessionResponse, data).to_runtime_session()

    async def query_runtime_sessions(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> RuntimeSessionsPageState:
        credentials = await self._credentials_factory()
        request = _QuerySessionsRequest(
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
        response = _validate_response(_RuntimeSessionsResponse, data)
        return RuntimeSessionsPageState(
            sessions=tuple(_runtime_session_state(session) for session in response.sessions),
            next_cursor=response.pagination.next if response.pagination is not None else None,
        )

    async def extend_runtime_session_timeout(
        self,
        *,
        session_id: str,
        duration: timedelta,
    ) -> SandboxRuntimeSessionState:
        credentials = await self._credentials_factory()
        request = _ExtendTimeoutRequest(duration=duration)
        data = await self._request_json(
            "POST",
            format_url_path(
                "v2/sandboxes/sessions/{session_id}/extend-timeout",
                session_id=session_id,
            ),
            credentials=credentials,
            body=request.to_api_dict(),
        )
        return _validate_response(_RuntimeSessionResponse, data).to_runtime_session()

    async def update_runtime_session_network_policy(
        self,
        *,
        session_id: str,
        network_policy: NetworkPolicy,
    ) -> SandboxRuntimeSessionState:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "POST",
            format_url_path(
                "v2/sandboxes/sessions/{session_id}/network-policy",
                session_id=session_id,
            ),
            credentials=credentials,
            body=_serialize_network_policy(network_policy),
        )
        return _validate_response(_RuntimeSessionResponse, data).to_runtime_session()

    async def create_snapshot(
        self,
        *,
        session_id: str,
        expiration: SnapshotExpiration | None = None,
    ) -> SnapshotSessionState:
        credentials = await self._credentials_factory()
        body: JSONValue | None = None
        if expiration is not None:
            request = _CreateSnapshotRequest(expiration=expiration)
            body = request.to_api_dict()
        data = await self._request_json(
            "POST",
            format_url_path("v2/sandboxes/sessions/{session_id}/snapshot", session_id=session_id),
            credentials=credentials,
            body=body,
        )
        return _validate_response(_CreateSnapshotResponse, data).to_snapshot_and_session()

    async def query_snapshots(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> SnapshotsPageState:
        credentials = await self._credentials_factory()
        request = _QuerySnapshotsRequest(
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
        response = _validate_response(_SnapshotsResponse, data)
        return SnapshotsPageState(
            snapshots=tuple(_snapshot_state(snapshot) for snapshot in response.snapshots),
            next_cursor=response.pagination.next if response.pagination is not None else None,
        )

    async def get_snapshot(self, *, snapshot_id: str) -> SnapshotState:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "GET",
            format_url_path("v2/sandboxes/snapshots/{snapshot_id}", snapshot_id=snapshot_id),
            credentials=credentials,
        )
        return _validate_response(_SnapshotResponse, data).to_snapshot()

    async def delete_snapshot(self, *, snapshot_id: str) -> SnapshotState:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "DELETE",
            format_url_path("v2/sandboxes/snapshots/{snapshot_id}", snapshot_id=snapshot_id),
            credentials=credentials,
        )
        return _validate_response(_SnapshotResponse, data).to_snapshot()

    async def create_process(
        self,
        *,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: timedelta | None = None,
    ) -> ProcessState:
        credentials = await self._credentials_factory()
        request = _RunCommandRequest(
            command=command,
            args=args or [],
            cwd=cwd,
            env=dict(env) if env is not None else None,
            sudo=sudo,
            timeout=kill_after,
        )
        data = await self._request_json(
            "POST",
            format_url_path("v2/sandboxes/sessions/{session_id}/cmd", session_id=session_id),
            credentials=credentials,
            body=request.to_api_dict(),
        )
        return _validate_response(_CommandResponse, data).to_command()

    async def run_process(
        self,
        *,
        session_id: str,
        command: str,
        args: Sequence[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: timedelta | None = None,
        output_router: ProcessOutputRouter,
    ) -> CompletedProcessState:
        credentials = await self._credentials_factory()
        request = _RunCommandRequest(
            command=command,
            args=list(args) if args is not None else None,
            cwd=cwd,
            env=dict(env) if env is not None else None,
            sudo=sudo,
            wait=True,
            logs=True,
            timeout=kill_after,
        )
        response = await self._request_stream(
            "POST",
            format_url_path("v2/sandboxes/sessions/{session_id}/cmd", session_id=session_id),
            credentials=credentials,
            params={"wait": "true", "logs": "true"},
            body=JSONBody(request.to_api_dict()),
            headers={"connection": "close"},
        )

        initial: ProcessState | None = None
        final: ProcessState | None = None
        try:
            async for line in _response_lines(response):
                if not line:
                    continue
                record = _parse_run_process_record(line)
                if "command" in record:
                    process = _validate_response(_CommandResponse, record).to_command()
                    if initial is None:
                        initial = process
                    elif final is None:
                        final = process
                    else:
                        raise SandboxResponseError(
                            "Sandbox process response included extra process metadata",
                            data=record,
                        )
                    continue

                stream = record.get("stream")
                data = record.get("data")
                if (stream == "stdout" or stream == "stderr") and isinstance(data, str):
                    if initial is None or final is not None:
                        raise SandboxResponseError(
                            "Sandbox process response included output outside process metadata",
                            data=record,
                        )
                    output_router.route(ProcessLog(stream=ProcessLogStream(stream), data=data))
                    continue
                if stream == "error" and isinstance(data, dict):
                    code = data.get("code")
                    message = data.get("message")
                    if isinstance(code, str) and isinstance(message, str):
                        raise SandboxStreamError(message, code=code)
                raise SandboxResponseError(
                    "Sandbox process response included an unexpected NDJSON record",
                    data=record,
                )
        finally:
            await _close_stream_response(response)

        if initial is None:
            raise SandboxResponseError("Sandbox process response is missing initial metadata")
        if final is None:
            raise SandboxResponseError("Sandbox process response is missing final metadata")
        if initial.id != final.id or initial.session_id != final.session_id:
            raise SandboxResponseError(
                "Sandbox process response returned a different final process identity",
                data={"initial": initial, "final": final},
            )
        if final.returncode is None:
            raise SandboxResponseError(
                "Sandbox process response final metadata is missing a return code",
                data=final,
            )
        stdout, stderr = output_router.captured()
        return CompletedProcessState(process=final, stdout=stdout, stderr=stderr)

    async def get_command(
        self,
        *,
        session_id: str,
        command_id: str,
        wait: bool = True,
    ) -> ProcessState:
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
        return _validate_response(_CommandResponse, data).to_command()

    async def query_commands(self, *, session_id: str) -> list[ProcessState]:
        credentials = await self._credentials_factory()
        data = await self._request_json(
            "GET",
            format_url_path("v2/sandboxes/sessions/{session_id}/cmd", session_id=session_id),
            credentials=credentials,
        )
        response = _validate_response(_CommandsResponse, data)
        return [_command_state(command) for command in response.commands]

    async def mkdir(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None = None,
        recursive: bool = True,
    ) -> None:
        credentials = await self._credentials_factory()
        request = _MkdirRequest(path=path, cwd=cwd, recursive=recursive)
        await self._request(
            "POST",
            format_url_path("v2/sandboxes/sessions/{session_id}/fs/mkdir", session_id=session_id),
            credentials=credentials,
            body=JSONBody(request.to_api_dict()),
        )

    async def read_bytes(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None = None,
    ) -> bytes:
        credentials = await self._credentials_factory()
        request = _FilesystemPathRequest(path=path, cwd=cwd)
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
        files: Sequence[_WriteFile],
        cwd: str,
    ) -> None:
        credentials = await self._credentials_factory()
        payload = _build_write_files_tarball(files, cwd=cwd)
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
    ) -> ProcessState:
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
        return _validate_response(_CommandResponse, data).to_command()

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
            headers={"connection": "close"},
        )
