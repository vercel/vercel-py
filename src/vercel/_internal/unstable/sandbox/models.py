"""Pydantic codecs and public handles for Sandbox v2 data."""

from datetime import timedelta
from typing import TYPE_CHECKING, Any, TypeAlias, cast

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

from vercel._internal.polyfills import StrEnum
from vercel._internal.time import MILLISECOND, parse_duration, to_ms_int
from vercel._internal.unstable.sandbox.errors import (
    SandboxInvalidHandleError,
    SandboxResponseError,
)
from vercel._internal.unstable.session import AliveToken

if TYPE_CHECKING:
    from vercel._internal.unstable.sandbox.operations import CreateRuntimeSessionOperation
    from vercel._internal.unstable.session import SdkSession

JSONValue: TypeAlias = PydanticJsonValue
JSONObject: TypeAlias = dict[str, JSONValue]
DurationInput: TypeAlias = int | float | timedelta | None


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


def _dump_response_sandbox(sandbox: "Sandbox") -> JSONObject:
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
    source: JSONValue | None = None
    ports: list[int] | None = None
    timeout: timedelta | None = None
    resources: JSONValue | None = None
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
    keep_last_snapshots: JSONValue | None = Field(
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


class GetSandboxRequest(_ApiRequestModel):
    name: str
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
    tags: str | list[str] | None = None


class DestroySandboxRequest(_ApiRequestModel):
    name: str
    project_id: str = Field(serialization_alias="projectId")


class DestroyRuntimeSessionRequest(_ApiRequestModel):
    session_id: str = Field(serialization_alias="sessionId")


class SandboxRoute(_ApiModel):
    url: str
    port: int
    subdomain: str
    system: bool = False


class SandboxRuntimeSession(_ApiModel):
    _session_alive_token: AliveToken | None = PrivateAttr(default=None)
    _resource_alive_token: AliveToken | None = PrivateAttr(default=None)

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
    timeout: int | None = None
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

    def _bind_alive_tokens(
        self,
        *,
        session_token: AliveToken,
        resource_token: AliveToken | None = None,
    ) -> None:
        self._session_alive_token = session_token
        if resource_token is not None:
            self._resource_alive_token = resource_token

    def _attach_resource_token(self, resource_token: AliveToken) -> None:
        self._resource_alive_token = resource_token

    def _raise_if_invalid(self) -> None:
        if self._session_alive_token is None:
            raise SandboxInvalidHandleError(
                "Sandbox runtime-session handle is not attached to an SDK session"
            )
        if not self._session_alive_token.is_alive:
            raise SandboxInvalidHandleError("Sandbox runtime-session handle is no longer valid")
        if self._resource_alive_token is not None and not self._resource_alive_token.is_alive:
            raise SandboxInvalidHandleError("Sandbox runtime-session handle is no longer valid")

    async def run_command(self, command: str, args: list[str] | None = None) -> Any:
        self._raise_if_invalid()
        raise NotImplementedError("Sandbox runtime-session commands are not implemented yet")


class Sandbox(_ApiModel):
    _session_alive_token: AliveToken | None = PrivateAttr(default=None)
    _resource_alive_token: AliveToken | None = PrivateAttr(default=None)
    _sdk_session: "SdkSession | None" = PrivateAttr(default=None)

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
    timeout: int | None = None
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
    current_session: SandboxRuntimeSession | None = None
    raw: JSONObject | None = None

    def _bind_alive_tokens(
        self,
        *,
        session_token: AliveToken,
        resource_token: AliveToken | None = None,
        sdk_session: "SdkSession | None" = None,
    ) -> None:
        self._session_alive_token = session_token
        if sdk_session is not None:
            self._sdk_session = sdk_session
        if resource_token is not None:
            self._resource_alive_token = resource_token
        if self.current_session is not None:
            self.current_session._bind_alive_tokens(
                session_token=session_token,
                resource_token=resource_token,
            )

    def _attach_resource_token(self, resource_token: AliveToken) -> None:
        self._resource_alive_token = resource_token
        if self.current_session is not None:
            self.current_session._bind_alive_tokens(
                session_token=self._session_alive_token or resource_token,
                resource_token=resource_token,
            )

    def _raise_if_invalid(self) -> None:
        if self._session_alive_token is None:
            raise SandboxInvalidHandleError("Sandbox handle is not attached to an SDK session")
        if not self._session_alive_token.is_alive:
            raise SandboxInvalidHandleError("Sandbox handle is no longer valid")
        if self._resource_alive_token is not None and not self._resource_alive_token.is_alive:
            raise SandboxInvalidHandleError("Sandbox handle is no longer valid")

    def session(self) -> "CreateRuntimeSessionOperation":
        self._raise_if_invalid()
        if self._sdk_session is None:
            raise SandboxInvalidHandleError("Sandbox handle is not attached to an SDK session")

        from vercel._internal.unstable.sandbox.operations import create_runtime_session_operation

        return create_runtime_session_operation(sandbox=self, session=self._sdk_session)

    async def run_command(self, command: str, args: list[str] | None = None) -> Any:
        self._raise_if_invalid()
        raise NotImplementedError("Sandbox commands are not implemented yet")


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
            for name in ("runtime", "status", "cwd", "region", "memory", "vcpus", "timeout"):
                if getattr(self.sandbox, name) is None:
                    updates[name] = getattr(self.session, name)

        return self.sandbox.model_copy(update=updates)


class SandboxesResponse(_ApiModel):
    sandboxes: list[Sandbox]
    pagination: Pagination | None = None
