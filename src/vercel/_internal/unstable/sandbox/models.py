"""Pydantic codecs and public handles for Sandbox v2 data."""

import signal as signal_module
from collections.abc import AsyncIterator, Iterator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Literal, TypeAlias, cast

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue as PydanticJsonValue,
    PrivateAttr,
    field_serializer,
    field_validator,
)

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.polyfills import StrEnum
from vercel._internal.time import MILLISECOND, parse_duration, to_ms_int
from vercel._internal.unstable.sandbox.errors import (
    SandboxCleanupError,
    SandboxInvalidHandleError,
    SandboxResponseError,
)
from vercel._internal.unstable.session import SdkSession, SyncSdkSession

if TYPE_CHECKING:
    from vercel._internal.unstable.sandbox.operations import CreateRuntimeSessionOperation
    from vercel._internal.unstable.sandbox.service import SandboxService

JSONValue: TypeAlias = PydanticJsonValue
JSONObject: TypeAlias = dict[str, JSONValue]
DurationInput: TypeAlias = int | float | timedelta | None
_COMMAND_NOT_ATTACHED = "Sandbox command handle is not attached to an SDK session"


@dataclass(frozen=True, slots=True)
class WriteFile:
    path: str
    content: str | bytes
    mode: int | None = None


class SandboxStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    ABORTED = "aborted"
    SNAPSHOTTING = "snapshotting"


class _ApiModel(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True, serialize_by_alias=True)


class _ApiRequestModel(_ApiModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, serialize_by_alias=True)

    def to_api_dict(self, *, exclude: set[str] | None = None) -> JSONObject:
        return cast(
            JSONObject,
            self.model_dump(by_alias=True, exclude_none=True, exclude=exclude or set()),
        )


def _duration_to_milliseconds(value: object) -> timedelta | None:
    return parse_duration(value, MILLISECOND)


class GitSource(_ApiRequestModel):
    """Git repository source for creating a sandbox."""

    type: Literal["git"] = "git"
    url: str
    depth: int | None = None
    revision: str | None = None
    username: str | None = None
    password: str | None = None


class TarballSource(_ApiRequestModel):
    """Tarball URL source for creating a sandbox."""

    type: Literal["tarball"] = "tarball"
    url: str


class SnapshotSource(_ApiRequestModel):
    """Snapshot source for creating a sandbox."""

    type: Literal["snapshot"] = "snapshot"
    snapshot_id: str = Field(serialization_alias="snapshotId")


SandboxSource: TypeAlias = GitSource | TarballSource | SnapshotSource


class SandboxResources(_ApiRequestModel):
    """CPU and memory request values for sandbox creation."""

    vcpus: int | None = None
    memory: int | None = None


class SnapshotRetention(_ApiRequestModel):
    """Snapshot retention policy for sandboxes created from a sandbox."""

    count: int
    expiration: DurationInput = None
    delete_evicted: bool = Field(default=True, serialization_alias="deleteEvicted")

    @field_validator("expiration", mode="before")
    @classmethod
    def _coerce_duration(cls, value: object) -> timedelta | None:
        return _duration_to_milliseconds(value)

    @field_serializer("expiration")
    def _serialize_duration(self, value: DurationInput) -> int | None:
        duration = parse_duration(value, MILLISECOND)
        if duration is None:
            return None
        return to_ms_int(duration)


class TagFilter(_ApiRequestModel):
    """Exact-match sandbox tag query filter."""

    key: str
    value: str

    def to_query_value(self) -> str:
        return f"{self.key}:{self.value}"


def _dump_response_sandbox(sandbox: "BaseSandbox") -> JSONObject:
    return cast(
        JSONObject,
        sandbox.model_dump(
            by_alias=True,
            exclude_none=True,
            exclude={"current_session", "routes", "raw"},
        ),
    )


class CreateSandboxRequest(_ApiRequestModel):
    project_id: str = Field(serialization_alias="projectId")
    name: str | None = None
    runtime: str | None = None
    source: SandboxSource | None = None
    ports: list[int] | None = None
    timeout: timedelta | None = None
    resources: SandboxResources | None = None
    persistent: bool | None = None
    network_policy: JSONValue | None = Field(
        default=None,
        serialization_alias="networkPolicy",
    )
    env: dict[str, str] | None = None
    tags: dict[str, str] | None = None
    snapshot_expiration: timedelta | None = Field(
        default=None,
        serialization_alias="snapshotExpiration",
    )
    keep_last_snapshots: SnapshotRetention | None = Field(
        default=None,
        serialization_alias="keepLastSnapshots",
    )

    @field_validator("timeout", "snapshot_expiration", mode="before")
    @classmethod
    def _coerce_duration(cls, value: object) -> timedelta | None:
        return _duration_to_milliseconds(value)

    @field_serializer("timeout", "snapshot_expiration")
    def _serialize_duration(self, value: timedelta | None) -> int | None:
        if value is None:
            return None
        return to_ms_int(value)


class UpdateSandboxRequest(_ApiRequestModel):
    runtime: str | None = None
    ports: list[int] | None = None
    timeout: timedelta | None = None
    resources: SandboxResources | None = None
    persistent: bool | None = None
    network_policy: JSONValue | None = Field(
        default=None,
        serialization_alias="networkPolicy",
    )
    env: dict[str, str] | None = None
    tags: dict[str, str] | None = None
    snapshot_expiration: timedelta | None = Field(
        default=None,
        serialization_alias="snapshotExpiration",
    )
    keep_last_snapshots: SnapshotRetention | None = Field(
        default=None,
        serialization_alias="keepLastSnapshots",
    )
    current_snapshot_id: str | None = Field(
        default=None,
        serialization_alias="currentSnapshotId",
    )

    @field_validator("timeout", "snapshot_expiration", mode="before")
    @classmethod
    def _coerce_duration(cls, value: object) -> timedelta | None:
        return _duration_to_milliseconds(value)

    @field_serializer("timeout", "snapshot_expiration")
    def _serialize_duration(self, value: timedelta | None) -> int | None:
        if value is None:
            return None
        return to_ms_int(value)


class GetSandboxRequest(_ApiRequestModel):
    project_id: str = Field(serialization_alias="projectId")
    resume: bool = True
    include_system_routes: bool | None = Field(
        default=None,
        serialization_alias="__includeSystemRoutes",
    )

    @field_serializer("resume", "include_system_routes")
    def _serialize_bool(self, value: bool | None) -> str | None:
        if value is None:
            return None
        return "true" if value else "false"


class QuerySandboxesRequest(_ApiRequestModel):
    project_id: str = Field(serialization_alias="project")
    limit: int | None = None
    cursor: str | None = None
    sort_by: str | None = Field(
        default=None,
        serialization_alias="sortBy",
    )
    sort_order: str | None = Field(
        default=None,
        serialization_alias="sortOrder",
    )
    name_prefix: str | None = Field(
        default=None,
        serialization_alias="namePrefix",
    )
    tags: Sequence[TagFilter] | None = None

    @field_serializer("tags")
    def _serialize_tags(self, value: Sequence[TagFilter] | None) -> list[str] | None:
        if value is None:
            return None
        return [tag.to_query_value() for tag in value]


class QuerySessionsRequest(_ApiRequestModel):
    project_id: str = Field(serialization_alias="project")
    name: str | None = None
    limit: int | None = None
    cursor: str | None = None
    sort_order: str | None = Field(
        default=None,
        serialization_alias="sortOrder",
    )


class QuerySnapshotsRequest(_ApiRequestModel):
    project_id: str = Field(serialization_alias="project")
    name: str | None = None
    limit: int | None = None
    cursor: str | None = None
    sort_order: str | None = Field(
        default=None,
        serialization_alias="sortOrder",
    )


class CreateSnapshotRequest(_ApiRequestModel):
    expiration: DurationInput = None

    @field_validator("expiration", mode="before")
    @classmethod
    def _coerce_duration(cls, value: object) -> timedelta | None:
        return _duration_to_milliseconds(value)

    @field_serializer("expiration")
    def _serialize_duration(self, value: timedelta | None) -> int | None:
        if value is None:
            return None
        return to_ms_int(value)


class ExtendTimeoutRequest(_ApiRequestModel):
    duration: DurationInput

    @field_validator("duration", mode="before")
    @classmethod
    def _coerce_duration(cls, value: object) -> timedelta:
        duration = parse_duration(value, MILLISECOND)
        if duration is None:
            raise TypeError("duration is required")
        return duration

    @field_serializer("duration")
    def _serialize_duration(self, value: DurationInput) -> int:
        duration = parse_duration(value, MILLISECOND)
        if duration is None:
            raise TypeError("duration is required")
        return to_ms_int(duration)


class RunCommandRequest(_ApiRequestModel):
    command: str
    args: list[str] | None = None
    cwd: str | None = None
    env: dict[str, str] | None = None
    sudo: bool | None = None


class FilesystemPathRequest(_ApiRequestModel):
    path: str
    cwd: str | None = None


class MkdirRequest(FilesystemPathRequest):
    recursive: bool = True


class SandboxRoute(_ApiModel):
    url: str
    port: int
    subdomain: str
    system: bool = False


def _signal_number(value: int | str | signal_module.Signals | None) -> int:
    if value is None:
        return int(signal_module.Signals.SIGTERM)
    if isinstance(value, signal_module.Signals):
        return int(value)
    if isinstance(value, int):
        return value
    normalized = value.upper()
    if not normalized.startswith("SIG"):
        normalized = f"SIG{normalized}"
    try:
        return int(signal_module.Signals[normalized])
    except KeyError as exc:
        raise ValueError(f"Unknown signal: {value!r}") from exc


class SandboxCommandLog(_ApiModel):
    """One streamed command output event."""

    data: str
    stream: Literal["unspecified", "stdin", "stdout", "stderr", "error"]


class BaseSandboxCommand(_ApiModel):
    _sdk_session: SdkSession | SyncSdkSession | None = PrivateAttr(default=None)
    _stdout_cache: str | None = PrivateAttr(default=None)
    _stderr_cache: str | None = PrivateAttr(default=None)

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

    @property
    def status(self) -> Literal["running", "exited"]:
        return "running" if self.exit_code is None else "exited"

    def _bind_sdk_session(self, sdk_session: SdkSession | SyncSdkSession) -> None:
        self._sdk_session = sdk_session

    def _require_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SdkSession):
            raise SandboxInvalidHandleError(_COMMAND_NOT_ATTACHED)
        return self._sdk_session.sandbox_service()

    def _require_sync_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SyncSdkSession):
            raise SandboxInvalidHandleError(_COMMAND_NOT_ATTACHED)
        return self._sdk_session.sandbox_service()


class SandboxCommand(BaseSandboxCommand):
    """A command handle bound to an async SDK session."""

    async def refresh(self, *, wait: bool = False) -> "SandboxCommand":
        service = self._require_sandbox_service()
        command = await service.get_command(
            session_id=self.session_id,
            command_id=self.id,
            wait=wait,
        )
        return command

    async def wait(self) -> "SandboxCommand":
        return await self.refresh(wait=True)

    async def kill(
        self,
        signal: int | str | signal_module.Signals | None = None,
    ) -> "SandboxCommand":
        service = self._require_sandbox_service()
        command = await service.kill_command(
            session_id=self.session_id,
            command_id=self.id,
            signal=_signal_number(signal),
        )
        return command

    def logs(self) -> AsyncIterator[SandboxCommandLog]:
        service = self._require_sandbox_service()

        async def iter_logs() -> AsyncIterator[SandboxCommandLog]:
            async for line in service.command_logs(
                session_id=self.session_id,
                command_id=self.id,
            ):
                yield line

        return iter_logs()

    async def output(self, stream: Literal["stdout", "stderr", "both"] = "both") -> str:
        stdout = ""
        stderr = ""
        async for line in self.logs():
            if line.stream == "stdout":
                stdout += line.data
            elif line.stream == "stderr":
                stderr += line.data
        self._stdout_cache = stdout
        self._stderr_cache = stderr
        if stream == "stdout":
            return stdout
        if stream == "stderr":
            return stderr
        return stdout + stderr

    async def stdout(self) -> str:
        return await self.output("stdout")

    async def stderr(self) -> str:
        return await self.output("stderr")


class SyncSandboxCommand(BaseSandboxCommand):
    """Synchronous mirror of `SandboxCommand`."""

    def refresh(self, *, wait: bool = False) -> "SyncSandboxCommand":
        service = self._require_sync_sandbox_service()
        command = cast(
            SyncSandboxCommand,
            iter_coroutine(
                service.get_command(
                    session_id=self.session_id,
                    command_id=self.id,
                    wait=wait,
                )
            ),
        )
        return command

    def wait(self) -> "SyncSandboxCommand":
        return self.refresh(wait=True)

    def kill(
        self,
        signal: int | str | signal_module.Signals | None = None,
    ) -> "SyncSandboxCommand":
        service = self._require_sync_sandbox_service()
        command = cast(
            SyncSandboxCommand,
            iter_coroutine(
                service.kill_command(
                    session_id=self.session_id,
                    command_id=self.id,
                    signal=_signal_number(signal),
                )
            ),
        )
        return command

    def logs(self) -> Iterator[SandboxCommandLog]:
        service = self._require_sync_sandbox_service()
        response = iter_coroutine(
            service.command_logs_response(
                session_id=self.session_id,
                command_id=self.id,
            )
        )

        def iter_logs() -> Iterator[SandboxCommandLog]:
            try:
                for line in response.iter_lines():
                    if line:
                        yield SandboxCommandLog.model_validate_json(line)
            finally:
                response.close()

        return iter_logs()

    def output(self, stream: Literal["stdout", "stderr", "both"] = "both") -> str:
        stdout = ""
        stderr = ""
        for line in self.logs():
            if line.stream == "stdout":
                stdout += line.data
            elif line.stream == "stderr":
                stderr += line.data
        self._stdout_cache = stdout
        self._stderr_cache = stderr
        if stream == "stdout":
            return stdout
        if stream == "stderr":
            return stderr
        return stdout + stderr

    def stdout(self) -> str:
        return self.output("stdout")

    def stderr(self) -> str:
        return self.output("stderr")


class CommandResponse(_ApiModel):
    command: SandboxCommand | None = None

    def to_command(self) -> SandboxCommand:
        if self.command is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'command'",
                data=self.model_dump(by_alias=True),
            )
        return self.command


class CommandsResponse(_ApiModel):
    commands: list[SandboxCommand]


class RuntimeSessionResponse(_ApiModel):
    session: "SandboxRuntimeSession | None" = None
    routes: list[SandboxRoute] = Field(default_factory=list)

    def to_runtime_session(self) -> "SandboxRuntimeSession":
        if self.session is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'session'",
                data=self.model_dump(by_alias=True),
            )
        return self.session


class RuntimeSessionsResponse(_ApiModel):
    sessions: list["SandboxRuntimeSession"]
    pagination: "Pagination | None" = None


class BaseSnapshot(_ApiModel):
    _sdk_session: SdkSession | SyncSdkSession | None = PrivateAttr(default=None)

    id: str
    source_session_id: str = Field(
        validation_alias=AliasChoices("source_session_id", "sourceSessionId"),
        serialization_alias="sourceSessionId",
    )
    region: str
    status: Literal["created", "deleted", "failed"]
    size_bytes: int = Field(
        validation_alias=AliasChoices("size_bytes", "sizeBytes"),
        serialization_alias="sizeBytes",
    )
    expires_at: int | None = Field(
        default=None,
        validation_alias=AliasChoices("expires_at", "expiresAt"),
        serialization_alias="expiresAt",
    )
    created_at: int = Field(
        validation_alias=AliasChoices("created_at", "createdAt"),
        serialization_alias="createdAt",
    )
    updated_at: int = Field(
        validation_alias=AliasChoices("updated_at", "updatedAt"),
        serialization_alias="updatedAt",
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

    def _bind_sdk_session(self, sdk_session: SdkSession | SyncSdkSession) -> None:
        self._sdk_session = sdk_session

    def _require_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SdkSession):
            raise SandboxInvalidHandleError("Snapshot handle is not attached to an SDK session")
        return self._sdk_session.sandbox_service()

    def _require_sync_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SyncSdkSession):
            raise SandboxInvalidHandleError("Snapshot handle is not attached to an SDK session")
        return self._sdk_session.sandbox_service()


class Snapshot(BaseSnapshot):
    """A Sandbox v2 filesystem snapshot bound to an async SDK session."""

    async def delete(self) -> "Snapshot":
        service = self._require_sandbox_service()
        return await service.delete_snapshot(snapshot_id=self.id)


class SyncSnapshot(BaseSnapshot):
    """Synchronous mirror of `Snapshot`."""

    def delete(self) -> "SyncSnapshot":
        service = self._require_sync_sandbox_service()
        return cast(SyncSnapshot, iter_coroutine(service.delete_snapshot(snapshot_id=self.id)))


class BaseSandboxRuntimeSession(_ApiModel):
    _sdk_session: SdkSession | SyncSdkSession | None = PrivateAttr(default=None)

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

    def _bind_sdk_session(self, sdk_session: SdkSession | SyncSdkSession) -> None:
        self._sdk_session = sdk_session

    def _require_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SdkSession):
            raise SandboxInvalidHandleError(
                "Sandbox runtime-session handle is not attached to an SDK session"
            )
        return self._sdk_session.sandbox_service()

    def _require_sync_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SyncSdkSession):
            raise SandboxInvalidHandleError(
                "Sandbox runtime-session handle is not attached to an SDK session"
            )
        return self._sdk_session.sandbox_service()

    def _write_files_cwd(self, cwd: str | None) -> str:
        return cwd or self.cwd or "/vercel/sandbox"


class SandboxRuntimeSession(BaseSandboxRuntimeSession):
    """A running Sandbox v2 session bound to an SDK session.

    Session-scoped behavior such as commands and stop operations belongs on this
    handle. The handle remains bound to the SDK session that created it.
    """

    async def run_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SandboxCommand:
        service = self._require_sandbox_service()
        return await service.run_command(
            session_id=self.id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
        )

    async def start_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SandboxCommand:
        service = self._require_sandbox_service()
        return await service.start_command(
            session_id=self.id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
        )

    async def get_command(self, command_id: str, *, wait: bool = False) -> SandboxCommand:
        service = self._require_sandbox_service()
        return await service.get_command(
            session_id=self.id,
            command_id=command_id,
            wait=wait,
        )

    async def query_commands(self) -> list[SandboxCommand]:
        service = self._require_sandbox_service()
        return await service.query_commands(session_id=self.id)

    async def refresh(
        self,
        *,
        include_system_routes: bool | None = None,
    ) -> "SandboxRuntimeSession":
        service = self._require_sandbox_service()
        return await service.get_runtime_session(
            session_id=self.id,
            include_system_routes=include_system_routes,
        )

    async def extend_execution_time_limit(
        self,
        duration: DurationInput,
    ) -> "SandboxRuntimeSession":
        service = self._require_sandbox_service()
        return await service.extend_runtime_session_timeout(
            session_id=self.id,
            duration=duration,
        )

    async def update_network_policy(self, network_policy: JSONValue) -> "SandboxRuntimeSession":
        service = self._require_sandbox_service()
        return await service.update_runtime_session_network_policy(
            session_id=self.id,
            network_policy=network_policy,
        )

    async def mkdir(
        self,
        path: str,
        *,
        cwd: str | None = None,
        recursive: bool = True,
    ) -> None:
        service = self._require_sandbox_service()
        await service.mkdir(
            session_id=self.id,
            path=path,
            cwd=cwd,
            recursive=recursive,
        )

    async def read_file(self, path: str, *, cwd: str | None = None) -> bytes:
        service = self._require_sandbox_service()
        return await service.read_file(
            session_id=self.id,
            path=path,
            cwd=cwd,
        )

    async def read_text(
        self,
        path: str,
        *,
        cwd: str | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str:
        content = await self.read_file(path, cwd=cwd)
        return content.decode(encoding, errors=errors)

    async def write_files(
        self,
        files: Sequence[WriteFile],
        *,
        cwd: str | None = None,
        encoding: str = "utf-8",
    ) -> None:
        service = self._require_sandbox_service()
        await service.write_files(
            session_id=self.id,
            files=files,
            cwd=self._write_files_cwd(cwd),
            encoding=encoding,
        )

    async def snapshot(self, *, expiration: DurationInput = None) -> Snapshot:
        service = self._require_sandbox_service()
        snapshot, _ = await service.create_snapshot(
            session_id=self.id,
            expiration=expiration,
        )
        return snapshot

    def command_logs(self, command_id: str) -> AsyncIterator[SandboxCommandLog]:
        service = self._require_sandbox_service()
        return service.command_logs(
            session_id=self.id,
            command_id=command_id,
        )

    async def stop(self) -> "SandboxRuntimeSession":
        service = self._require_sandbox_service()
        return await service.stop_runtime_session(
            session_id=self.id,
        )


class SyncSandboxRuntimeSession(BaseSandboxRuntimeSession):
    """Synchronous mirror of `SandboxRuntimeSession`."""

    def __enter__(self) -> "SyncSandboxRuntimeSession":
        self._require_sync_sandbox_service()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        service = self._require_sync_sandbox_service()

        try:
            iter_coroutine(service.destroy_runtime_session(session_id=self.id))
        except Exception as exc:
            raise SandboxCleanupError(
                f"Failed to clean up sandbox runtime session {self.id!r}",
                resource_type="sandbox_runtime_session",
                resource_id=self.id,
                cause=exc,
            ) from exc
        return None

    def run_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SyncSandboxCommand:
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandboxCommand,
            iter_coroutine(
                service.run_command(
                    session_id=self.id,
                    command=command,
                    args=args,
                    cwd=cwd,
                    env=env,
                    sudo=sudo,
                )
            ),
        )

    def start_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SyncSandboxCommand:
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandboxCommand,
            iter_coroutine(
                service.start_command(
                    session_id=self.id,
                    command=command,
                    args=args,
                    cwd=cwd,
                    env=env,
                    sudo=sudo,
                )
            ),
        )

    def get_command(self, command_id: str, *, wait: bool = False) -> SyncSandboxCommand:
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandboxCommand,
            iter_coroutine(
                service.get_command(
                    session_id=self.id,
                    command_id=command_id,
                    wait=wait,
                )
            ),
        )

    def query_commands(self) -> list[SyncSandboxCommand]:
        service = self._require_sync_sandbox_service()
        return cast(
            list[SyncSandboxCommand],
            iter_coroutine(service.query_commands(session_id=self.id)),
        )

    def refresh(
        self,
        *,
        include_system_routes: bool | None = None,
    ) -> "SyncSandboxRuntimeSession":
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandboxRuntimeSession,
            iter_coroutine(
                service.get_runtime_session(
                    session_id=self.id,
                    include_system_routes=include_system_routes,
                )
            ),
        )

    def extend_execution_time_limit(
        self,
        duration: DurationInput,
    ) -> "SyncSandboxRuntimeSession":
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandboxRuntimeSession,
            iter_coroutine(
                service.extend_runtime_session_timeout(
                    session_id=self.id,
                    duration=duration,
                )
            ),
        )

    def update_network_policy(self, network_policy: JSONValue) -> "SyncSandboxRuntimeSession":
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandboxRuntimeSession,
            iter_coroutine(
                service.update_runtime_session_network_policy(
                    session_id=self.id,
                    network_policy=network_policy,
                )
            ),
        )

    def mkdir(
        self,
        path: str,
        *,
        cwd: str | None = None,
        recursive: bool = True,
    ) -> None:
        service = self._require_sync_sandbox_service()
        iter_coroutine(
            service.mkdir(
                session_id=self.id,
                path=path,
                cwd=cwd,
                recursive=recursive,
            )
        )

    def read_file(self, path: str, *, cwd: str | None = None) -> bytes:
        service = self._require_sync_sandbox_service()
        return cast(
            bytes,
            iter_coroutine(
                service.read_file(
                    session_id=self.id,
                    path=path,
                    cwd=cwd,
                )
            ),
        )

    def read_text(
        self,
        path: str,
        *,
        cwd: str | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str:
        return self.read_file(path, cwd=cwd).decode(encoding, errors=errors)

    def write_files(
        self,
        files: Sequence[WriteFile],
        *,
        cwd: str | None = None,
        encoding: str = "utf-8",
    ) -> None:
        service = self._require_sync_sandbox_service()
        iter_coroutine(
            service.write_files(
                session_id=self.id,
                files=files,
                cwd=self._write_files_cwd(cwd),
                encoding=encoding,
            )
        )

    def snapshot(self, *, expiration: DurationInput = None) -> SyncSnapshot:
        service = self._require_sync_sandbox_service()
        snapshot, _ = iter_coroutine(
            service.create_snapshot(
                session_id=self.id,
                expiration=expiration,
            )
        )
        return cast(SyncSnapshot, snapshot)

    def command_logs(self, command_id: str) -> Iterator[SandboxCommandLog]:
        service = self._require_sync_sandbox_service()
        response = iter_coroutine(
            service.command_logs_response(
                session_id=self.id,
                command_id=command_id,
            )
        )

        def iter_logs() -> Iterator[SandboxCommandLog]:
            try:
                for line in response.iter_lines():
                    if line:
                        yield SandboxCommandLog.model_validate_json(line)
            finally:
                response.close()

        return iter_logs()

    def stop(self) -> "SyncSandboxRuntimeSession":
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandboxRuntimeSession,
            iter_coroutine(service.stop_runtime_session(session_id=self.id)),
        )


class BaseSandbox(_ApiModel):
    _sdk_session: SdkSession | SyncSdkSession | None = PrivateAttr(default=None)

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
    routes: tuple[SandboxRoute, ...] = ()
    current_session: BaseSandboxRuntimeSession | None = None
    raw: JSONObject | None = None

    def _bind_sdk_session(self, sdk_session: SdkSession | SyncSdkSession) -> None:
        self._sdk_session = sdk_session
        if self.current_session is not None:
            self.current_session._bind_sdk_session(sdk_session)

    def _require_sdk_session(self) -> SdkSession:
        if not isinstance(self._sdk_session, SdkSession):
            raise SandboxInvalidHandleError("Sandbox handle is not attached to an SDK session")
        return self._sdk_session

    def _require_sandbox_service(self) -> "SandboxService":
        return self._require_sdk_session().sandbox_service()

    def _require_sync_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SyncSdkSession):
            raise SandboxInvalidHandleError("Sandbox handle is not attached to an SDK session")
        return self._sdk_session.sandbox_service()

    def _write_files_cwd(self, cwd: str | None) -> str:
        if cwd is not None:
            return cwd
        if self.current_session is not None and self.current_session.cwd is not None:
            return self.current_session.cwd
        return self.cwd or "/vercel/sandbox"


class Sandbox(BaseSandbox):
    """A Sandbox v2 handle bound to an SDK session.

    Sandbox identity behavior such as creating sessions, running commands on the
    current session, and destroying the sandbox belongs here. The handle remains
    bound to the SDK session that created it.
    """

    current_session: SandboxRuntimeSession | None = None

    def session(self) -> "CreateRuntimeSessionOperation":
        """Create or resume a runtime session for this sandbox."""
        sdk_session = self._require_sdk_session()

        from vercel._internal.unstable.sandbox.operations import create_runtime_session_operation

        return create_runtime_session_operation(sandbox=self, session=sdk_session)

    async def run_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SandboxCommand:
        service = self._require_sandbox_service()
        return await service.run_command(
            session_id=self.current_session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
        )

    async def start_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SandboxCommand:
        service = self._require_sandbox_service()
        return await service.start_command(
            session_id=self.current_session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
        )

    async def get_command(self, command_id: str, *, wait: bool = False) -> SandboxCommand:
        service = self._require_sandbox_service()
        return await service.get_command(
            session_id=self.current_session_id,
            command_id=command_id,
            wait=wait,
        )

    async def query_commands(self) -> list[SandboxCommand]:
        service = self._require_sandbox_service()
        return await service.query_commands(
            session_id=self.current_session_id,
        )

    async def list_sessions(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[SandboxRuntimeSession]:
        service = self._require_sandbox_service()
        page = await service.query_sessions_page(
            project_id=self.project_id,
            name=self.name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )
        return page.sessions

    async def list_snapshots(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[Snapshot]:
        service = self._require_sandbox_service()
        page = await service.query_snapshots_page(
            project_id=self.project_id,
            name=self.name,
            page_size=page_size,
            cursor=cursor,
            sort_order=sort_order,
        )
        return page.snapshots

    async def extend_execution_time_limit(
        self,
        duration: DurationInput,
    ) -> SandboxRuntimeSession:
        service = self._require_sandbox_service()
        return await service.extend_runtime_session_timeout(
            session_id=self.current_session_id,
            duration=duration,
        )

    async def update_network_policy(self, network_policy: JSONValue) -> SandboxRuntimeSession:
        service = self._require_sandbox_service()
        return await service.update_runtime_session_network_policy(
            session_id=self.current_session_id,
            network_policy=network_policy,
        )

    async def mkdir(
        self,
        path: str,
        *,
        cwd: str | None = None,
        recursive: bool = True,
    ) -> None:
        service = self._require_sandbox_service()
        await service.mkdir(
            session_id=self.current_session_id,
            path=path,
            cwd=cwd,
            recursive=recursive,
        )

    async def read_file(self, path: str, *, cwd: str | None = None) -> bytes:
        service = self._require_sandbox_service()
        return await service.read_file(
            session_id=self.current_session_id,
            path=path,
            cwd=cwd,
        )

    async def read_text(
        self,
        path: str,
        *,
        cwd: str | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str:
        content = await self.read_file(path, cwd=cwd)
        return content.decode(encoding, errors=errors)

    async def write_files(
        self,
        files: Sequence[WriteFile],
        *,
        cwd: str | None = None,
        encoding: str = "utf-8",
    ) -> None:
        service = self._require_sandbox_service()
        await service.write_files(
            session_id=self.current_session_id,
            files=files,
            cwd=self._write_files_cwd(cwd),
            encoding=encoding,
        )

    async def snapshot(self, *, expiration: DurationInput = None) -> Snapshot:
        service = self._require_sandbox_service()
        snapshot, _ = await service.create_snapshot(
            session_id=self.current_session_id,
            expiration=expiration,
        )
        return snapshot

    async def destroy(self) -> "Sandbox":
        service = self._require_sandbox_service()
        return await service.destroy_sandbox(
            name=self.name,
            project_id=self.project_id,
        )

    async def update(
        self,
        *,
        runtime: str | None = None,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: JSONValue | None = None,
        env: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
        snapshot_expiration: DurationInput = None,
        snapshot_retention: SnapshotRetention | None = None,
        current_snapshot_id: str | None = None,
    ) -> "Sandbox":
        service = self._require_sandbox_service()
        return await service.update_sandbox(
            name=self.name,
            project_id=self.project_id,
            runtime=runtime,
            ports=ports,
            execution_time_limit=execution_time_limit,
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            env=env,
            tags=tags,
            snapshot_expiration=snapshot_expiration,
            snapshot_retention=snapshot_retention,
            current_snapshot_id=current_snapshot_id,
        )


class SyncSandbox(BaseSandbox):
    """Synchronous mirror of `Sandbox`."""

    current_session: SyncSandboxRuntimeSession | None = None

    def __enter__(self) -> "SyncSandbox":
        self._require_sync_sandbox_service()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        service = self._require_sync_sandbox_service()

        try:
            iter_coroutine(
                service.destroy_sandbox(
                    name=self.name,
                    project_id=self.project_id,
                )
            )
        except Exception as exc:
            raise SandboxCleanupError(
                f"Failed to clean up sandbox {self.name!r}",
                resource_type="sandbox",
                resource_id=self.name,
                cause=exc,
            ) from exc
        return None

    def session(self) -> SyncSandboxRuntimeSession:
        """Create or resume a runtime session for this sandbox."""
        service = self._require_sync_sandbox_service()

        runtime_session = cast(
            SyncSandboxRuntimeSession,
            iter_coroutine(
                service.create_runtime_session(
                    name=self.name,
                    project_id=self.project_id,
                )
            ),
        )
        return runtime_session

    def run_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SyncSandboxCommand:
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandboxCommand,
            iter_coroutine(
                service.run_command(
                    session_id=self.current_session_id,
                    command=command,
                    args=args,
                    cwd=cwd,
                    env=env,
                    sudo=sudo,
                )
            ),
        )

    def start_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SyncSandboxCommand:
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandboxCommand,
            iter_coroutine(
                service.start_command(
                    session_id=self.current_session_id,
                    command=command,
                    args=args,
                    cwd=cwd,
                    env=env,
                    sudo=sudo,
                )
            ),
        )

    def get_command(self, command_id: str, *, wait: bool = False) -> SyncSandboxCommand:
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandboxCommand,
            iter_coroutine(
                service.get_command(
                    session_id=self.current_session_id,
                    command_id=command_id,
                    wait=wait,
                )
            ),
        )

    def query_commands(self) -> list[SyncSandboxCommand]:
        service = self._require_sync_sandbox_service()
        return cast(
            list[SyncSandboxCommand],
            iter_coroutine(
                service.query_commands(
                    session_id=self.current_session_id,
                )
            ),
        )

    def list_sessions(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[SyncSandboxRuntimeSession]:
        service = self._require_sync_sandbox_service()
        page = iter_coroutine(
            service.query_sessions_page(
                project_id=self.project_id,
                name=self.name,
                page_size=page_size,
                cursor=cursor,
                sort_order=sort_order,
            )
        )
        return cast(list[SyncSandboxRuntimeSession], page.sessions)

    def list_snapshots(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[SyncSnapshot]:
        service = self._require_sync_sandbox_service()
        page = iter_coroutine(
            service.query_snapshots_page(
                project_id=self.project_id,
                name=self.name,
                page_size=page_size,
                cursor=cursor,
                sort_order=sort_order,
            )
        )
        return cast(list[SyncSnapshot], page.snapshots)

    def extend_execution_time_limit(
        self,
        duration: DurationInput,
    ) -> SyncSandboxRuntimeSession:
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandboxRuntimeSession,
            iter_coroutine(
                service.extend_runtime_session_timeout(
                    session_id=self.current_session_id,
                    duration=duration,
                )
            ),
        )

    def update_network_policy(self, network_policy: JSONValue) -> SyncSandboxRuntimeSession:
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandboxRuntimeSession,
            iter_coroutine(
                service.update_runtime_session_network_policy(
                    session_id=self.current_session_id,
                    network_policy=network_policy,
                )
            ),
        )

    def mkdir(
        self,
        path: str,
        *,
        cwd: str | None = None,
        recursive: bool = True,
    ) -> None:
        service = self._require_sync_sandbox_service()
        iter_coroutine(
            service.mkdir(
                session_id=self.current_session_id,
                path=path,
                cwd=cwd,
                recursive=recursive,
            )
        )

    def read_file(self, path: str, *, cwd: str | None = None) -> bytes:
        service = self._require_sync_sandbox_service()
        return cast(
            bytes,
            iter_coroutine(
                service.read_file(
                    session_id=self.current_session_id,
                    path=path,
                    cwd=cwd,
                )
            ),
        )

    def read_text(
        self,
        path: str,
        *,
        cwd: str | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str:
        return self.read_file(path, cwd=cwd).decode(encoding, errors=errors)

    def write_files(
        self,
        files: Sequence[WriteFile],
        *,
        cwd: str | None = None,
        encoding: str = "utf-8",
    ) -> None:
        service = self._require_sync_sandbox_service()
        iter_coroutine(
            service.write_files(
                session_id=self.current_session_id,
                files=files,
                cwd=self._write_files_cwd(cwd),
                encoding=encoding,
            )
        )

    def snapshot(self, *, expiration: DurationInput = None) -> SyncSnapshot:
        service = self._require_sync_sandbox_service()
        snapshot, _ = iter_coroutine(
            service.create_snapshot(
                session_id=self.current_session_id,
                expiration=expiration,
            )
        )
        return cast(SyncSnapshot, snapshot)

    def destroy(self) -> "SyncSandbox":
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandbox,
            iter_coroutine(
                service.destroy_sandbox(
                    name=self.name,
                    project_id=self.project_id,
                )
            ),
        )

    def update(
        self,
        *,
        runtime: str | None = None,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: JSONValue | None = None,
        env: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
        snapshot_expiration: DurationInput = None,
        snapshot_retention: SnapshotRetention | None = None,
        current_snapshot_id: str | None = None,
    ) -> "SyncSandbox":
        service = self._require_sync_sandbox_service()
        return cast(
            SyncSandbox,
            iter_coroutine(
                service.update_sandbox(
                    name=self.name,
                    project_id=self.project_id,
                    runtime=runtime,
                    ports=ports,
                    execution_time_limit=execution_time_limit,
                    resources=resources,
                    persistent=persistent,
                    network_policy=network_policy,
                    env=env,
                    tags=tags,
                    snapshot_expiration=snapshot_expiration,
                    snapshot_retention=snapshot_retention,
                    current_snapshot_id=current_snapshot_id,
                )
            ),
        )


class Pagination(_ApiModel):
    count: int
    next: str | None = None
    prev: str | None = None


class SandboxResponse(_ApiModel):
    sandbox: Sandbox | None = None
    session: SandboxRuntimeSession | None = None
    routes: list[SandboxRoute] = Field(default_factory=list)
    resumed: bool | None = None

    def to_sandbox(self) -> Sandbox:
        if self.sandbox is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'sandbox'",
                data=self.model_dump(by_alias=True),
            )

        updates: dict[str, object] = {
            "routes": tuple(self.routes),
            "current_session": self.session,
            "raw": _dump_response_sandbox(self.sandbox),
        }
        if self.session is not None:
            if self.sandbox.project_id is None and self.session.project_id is not None:
                updates["project_id"] = self.session.project_id
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
                if getattr(self.sandbox, name) is None:
                    updates[name] = getattr(self.session, name)

        return self.sandbox.model_copy(update=updates)


class SandboxesResponse(_ApiModel):
    sandboxes: list[Sandbox]
    pagination: Pagination | None = None


class CreateSnapshotResponse(_ApiModel):
    snapshot: Snapshot | None = None
    session: SandboxRuntimeSession | None = None

    def to_snapshot_and_session(self) -> tuple[Snapshot, SandboxRuntimeSession]:
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
        return self.snapshot, self.session


class SnapshotResponse(_ApiModel):
    snapshot: Snapshot | None = None

    def to_snapshot(self) -> Snapshot:
        if self.snapshot is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'snapshot'",
                data=self.model_dump(by_alias=True),
            )
        return self.snapshot


class SnapshotsResponse(_ApiModel):
    snapshots: list[Snapshot]
    pagination: Pagination | None = None
